// Package discovery hydrates the store, resolves local source identities, and
// publishes immutable discovery records, ownership claims and resource-graph readers.
// Source aliasing uses authored URL/ref values before parent rendering.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// Result summarizes what discovery hydrated into the store.
type Result struct {
	// RepoRoot is the bootstrap input's ownership coordinate. Admitted
	// inputs may use a synthetic label with no corresponding directory.
	RepoRoot string
	// Index contains the immutable static discovery facts consumed by rendering.
	Index *manifest.DiscoveryIndex
	// WipeSecrets reflects the loader's WipeSecrets setting. The
	// orchestrator forwards it to lazy-promotion so SOPS Secrets
	// stay wiped on demand the same way they were at file-load.
	WipeSecrets bool
	// DisableChartDigestTracking reflects inline instance patches in file-parsed
	// HelmReleases, including releases deferred until render emission or exact promotion.
	DisableChartDigestTracking bool
}

// Config is the input contract for Run. Store is mandatory.
type Config struct {
	// Input is authoritative over a preseeded bootstrap artifact or disk admission.
	Input *store.SourceArtifact
	// Path is the scan entry point — the directory the file walker
	// starts at (a Flux cluster's entry, e.g. kubernetes/flux/cluster).
	Path string
	// RepoRoot is the source root that Kustomization spec.path values
	// resolve against (the GitRepository artifact root). Supplied
	// explicitly by SDK consumers rendering extracted trees that have no
	// .git. Without an admitted bootstrap input, an empty value selects
	// the repository containing Path or Path itself. A preseeded
	// input retains its own coordinate. Path must sit at or under RepoRoot.
	RepoRoot string
	// SelfURLs are the remote URL(s) this tree represents. A user-authored
	// GitRepository matching one is the cluster pulling itself: no ref or
	// HEAD uses the working tree, another local ref uses a cached committed
	// artifact, and unavailable refs or non-HEAD sparse/submodule options
	// warn and fetch normally. SDK consumers supply these for extracted
	// trees with no .git/config; empty uses the working tree's git remotes.
	SelfURLs []string
	// KRMIgnoreFile, when non-empty, is read in place of <Path>/.krmignore
	// for the initial scan; spec.path targets followed afterwards keep
	// their own per-directory .krmignore. Lets one checkout carry several
	// scan scopes (e.g. .krmignore.staging / .krmignore.production) and
	// pick one per run.
	KRMIgnoreFile string
	Store         *store.Store
	WipeSecrets   bool
}

// Run performs the full discovery phase against cfg and writes results
// into cfg.Store. Returns the structural metadata the orchestrator
// needs for change-filter construction and controller wiring.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Store == nil {
		return nil, errors.New("discovery: Store is required")
	}
	l := loader.New(cfg.Store)
	l.Options.WipeSecrets = cfg.WipeSecrets
	// Discovery drivers and sources reach the Store immediately; other resources
	// retain indexed evidence for render emission and exact promotion.
	d := &discoverer{cfg: cfg, loader: l, aliasViews: make(map[string]fs.FS)}
	_, err := d.resolveEntrySources(ctx, false)
	if err != nil {
		return nil, err
	}
	repoRoot := cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact).LocalPath
	if err := d.loadManifests(ctx, repoRoot); err != nil {
		return nil, err
	}
	d.loader.ApplyNamespaces(repoRoot)
	// Resolve bare ${VAR} in Kustomization dependsOn against the
	// cluster's postBuild substitute values, now that the full KS set is
	// discovered (so the substitute union is complete and its conflict
	// check is sound). Without this a child KS's
	// `dependsOn: 0-${CLUSTER_NAME}-config` never matches the real
	// 0-biohazard-config the parent's render would have substituted.
	loader.ResolveDependsOnSubstitutions(d.cfg.Store)
	var standaloneSecrets []manifest.NamedResource
	if cfg.WipeSecrets {
		for id := range l.Index.Files() {
			if id.Group == "" && id.Kind == manifest.KindSecret && d.cfg.Store.GetObject(id) == nil {
				standaloneSecrets = append(standaloneSecrets, id)
			}
		}
	}
	l.FinalizeIndex(repoRoot)
	cfg.Store.SetDiscoveryIndex(l.Index)
	d.promoteOrphans(standaloneSecrets)
	return &Result{
		RepoRoot:                   repoRoot,
		Index:                      l.Index,
		WipeSecrets:                cfg.WipeSecrets,
		DisableChartDigestTracking: l.DisableChartDigestTracking,
	}, nil
}

type discoverer struct {
	cfg             Config
	loader          *loader.Loader
	resolvedSources map[manifest.NamedResource]entryResolution
	remotes         map[string]struct{}
	aliasViews      map[string]fs.FS
	hasPins         bool
}

// loadManifests scans cfg.Path, then iteratively follows each loaded
// Flux Kustomization's spec.path until a fixed point is reached.
// ResourceSets are loaded into the store by the file walker but expanded
// during the run by the ResourceSet controller (a first-class DAG node),
// not here — discovery only resolves KS spec.path references.
func (d *discoverer) loadManifests(ctx context.Context, repoRoot string) error {
	l := d.loader
	l.SourceRoot = repoRoot

	scanRoot := repoRoot
	if d.cfg.Path != "" {
		var err error
		scanRoot, err = filepath.Abs(d.cfg.Path)
		if err != nil {
			return err
		}
	}
	input := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	name, err := filepath.Rel(input.LocalPath, scanRoot)
	if err != nil {
		return err
	}
	if info, err := fs.Stat(input.FS, filepath.ToSlash(name)); err != nil {
		return fmt.Errorf("--path %q: %w", d.cfg.Path, err)
	} else if !info.IsDir() {
		return fmt.Errorf("--path %q is not a directory", d.cfg.Path)
	}
	total := 0
	l.IgnoreFile = d.cfg.KRMIgnoreFile
	if err := d.loadAt(ctx, scanRoot, &total); err != nil {
		return err
	}
	l.IgnoreFile = ""
	// Projection may introduce a new effective source reference. Resolve it
	// before Load admits the selected spec.path edge to scope construction.
	l.PreferExisting = true
	type expansion struct {
		ks     *manifest.Kustomization
		source *store.SourceArtifact
		object manifest.BaseManifest
	}
	ksExpanded := map[expansion]struct{}{}
	var pending []expansion
	for {
		l.ApplyNamespaces(repoRoot)
		_, err := d.resolveEntrySources(ctx, false)
		if err != nil {
			return err
		}
		if d.hasPins {
			d.discardPinnedWorkingTreeFiles(repoRoot)
		}
		kustomizations := d.cfg.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
		for _, ks := range kustomizations {
			art, _ := d.cfg.Store.GetArtifact(ks.SourceID()).(*store.SourceArtifact)
			key := expansion{ks: ks, source: art, object: d.cfg.Store.GetObject(ks.SourceID())}
			if _, seen := ksExpanded[key]; seen {
				continue
			}
			ksExpanded[key] = struct{}{}
			pending = append(pending, key)
		}
		added := 0
		next := pending[:0]
		for _, key := range pending {
			ks := key.ks
			if ks.IsFragment() {
				continue
			}
			sourceRoot, _, external := loader.SourceView(d.cfg.Store, ks, repoRoot)
			if external && d.cfg.Store.GetObject(ks.SourceID()) == nil {
				next = append(next, key)
				continue
			}
			target := filepath.Join(sourceRoot, manifest.NormalizeClaimBase(ks.Path))
			if !safepath.Contains(sourceRoot, target) {
				continue
			}
			if err := d.loadAt(ctx, target, &total); err != nil {
				return err
			}
			added++
		}
		pending = next
		if added == 0 {
			missing, err := d.resolveEntrySources(ctx, true)
			if err != nil {
				return err
			}
			if missing == 0 {
				break
			}
		}
	}
	l.PreferExisting = false
	d.warnBootstrapAliases(repoRoot)
	slog.Debug("discovery: loaded objects", "count", total, "scan_root", scanRoot, "source_root", repoRoot)
	return nil
}

// A broad --path scan can overlap a pinned subtree. Its working-tree objects
// must be discarded before the committed subtree supplies discovery metadata.
// Resolved sources must retain their objects and artifacts so a pinned root
// claim cannot replace its own source with a working-tree alias.
func (d *discoverer) discardPinnedWorkingTreeFiles(repoRoot string) {
	pinnedSources := make(map[manifest.NamedResource]struct{})
	pinnedKS := make(map[manifest.NamedResource]struct{})
	for _, ks := range d.cfg.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
		ref := ks.SourceID()
		if _, pinned, _ := loader.SourceView(d.cfg.Store, ks, repoRoot); pinned {
			pinnedSources[ref] = struct{}{}
			pinnedKS[ks.Named()] = struct{}{}
		}
	}
	var prefixes []manifest.KSClaim
	for _, c := range d.loader.Claims(repoRoot) {
		if _, pinned := pinnedKS[c.ID]; pinned {
			prefixes = append(prefixes, c)
		}
	}
	if len(prefixes) == 0 {
		return
	}
	var aliases []*store.SourceArtifact
	for id := range d.resolvedSources {
		art, ok := d.cfg.Store.GetArtifact(id).(*store.SourceArtifact)
		if ok && art.InputAlias {
			aliases = append(aliases, art)
		}
	}
	for id, record := range d.loader.Index.Files() {
		file := record.File
		if _, resolved := d.resolvedSources[id]; resolved {
			continue
		}
		if _, pinned := pinnedSources[id]; pinned {
			continue
		}
		if !safepath.Contains(repoRoot, filepath.Join(repoRoot, filepath.FromSlash(file))) {
			for _, root := range aliases {
				if safepath.Contains(root.LocalPath, record.Path) {
					rel, _ := filepath.Rel(root.LocalPath, record.Path)
					file = filepath.ToSlash(filepath.Join(root.LocalRoot, rel))
					file, _ = filepath.Rel(repoRoot, file)
					break
				}
			}
			if !safepath.Contains(repoRoot, filepath.Join(repoRoot, filepath.FromSlash(file))) {
				continue
			}
		}
		_, owned := manifest.StrictParent(prefixes, file, id, nil)
		if !owned {
			continue
		}
		d.cfg.Store.DeleteObject(id)
		delete(d.loader.Index.Records, id)
	}
}

// The Loader deduplicates unchanged source views; missing followed paths are deferred.
func (d *discoverer) loadAt(ctx context.Context, dir string, total *int) error {
	n, err := d.loader.Load(ctx, dir)
	if d.loader.IgnoreFile == "" && errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	*total += n
	return nil
}
