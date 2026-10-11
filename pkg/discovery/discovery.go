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
	"os"
	"path/filepath"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Result summarizes what discovery hydrated into the store.
type Result struct {
	// RepoRoot is the resolved working-tree anchor (with .git ancestor
	// walk + symlink resolution applied).
	RepoRoot string
	// Index contains the immutable static discovery facts consumed by rendering.
	Index *manifest.DiscoveryIndex
	// Producers maps a target Secret to the in-repo ExternalSecret /
	// SealedSecret that declares it, seeded by the same self-produce walk.
	// The source + HR controllers consult it to skip a missing auth /
	// valuesFrom Secret that has a declared producer, without
	// --allow-missing-secrets. Augmented at render time by the HR
	// controller's EventObjectAdded listener. See manifest.ProducerIndex.
	Producers *manifest.ProducerIndex
	// WipeSecrets reflects the loader's WipeSecrets setting. The
	// orchestrator forwards it to lazy-promotion so SOPS Secrets
	// stay wiped on demand the same way they were at file-load.
	WipeSecrets bool
	// DisableChartDigestTracking reflects inline instance patches in file-parsed
	// HelmReleases, including releases kept out of the store by DiscoveryOnly.
	DisableChartDigestTracking bool
}

// Config is the input contract for Run. Store is mandatory.
type Config struct {
	// Path is the scan entry point — the directory the file walker
	// starts at (a Flux cluster's entry, e.g. kubernetes/flux/cluster).
	Path string
	// RepoRoot is the source root that Kustomization spec.path values
	// resolve against (the GitRepository artifact root). Supplied
	// explicitly by SDK consumers rendering extracted trees that have no
	// .git; the CLI defaults it to the .git ancestor of Path. Empty ⇒
	// fall back to the .git walk (FindRepoRoot), preserving local
	// behavior. Path must sit at or under RepoRoot.
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
	l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	l.Options.WipeSecrets = cfg.WipeSecrets
	// Render-driven discovery: only Kustomizations and the discovery-
	// meta pair (ResourceSet, RSIP) reach the Store from the file
	// walker. HRs, sources, CMs, Secrets, and raw manifests flow
	// through Existence — picked up later by KS render via
	// emitRenderedChildren, the orchestrator's orphan-promotion
	// sweep, or depwait's lazy-promotion fallback.
	l.Options.DiscoveryOnly = true
	d := &discoverer{cfg: cfg, loader: l}
	repoRoot, err := d.seedBootstrapSource()
	if err != nil {
		return nil, err
	}
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
			if id.Kind == manifest.KindSecret && d.cfg.Store.GetObject(id) == nil {
				standaloneSecrets = append(standaloneSecrets, id)
			}
		}
	}
	producers := &manifest.ProducerIndex{}
	l.FinalizeIndex(repoRoot, producers)
	d.promoteOrphans(standaloneSecrets)
	return &Result{
		RepoRoot:                   repoRoot,
		Index:                      l.Index,
		Producers:                  producers,
		WipeSecrets:                cfg.WipeSecrets,
		DisableChartDigestTracking: l.DisableChartDigestTracking,
	}, nil
}

type discoverer struct {
	cfg             Config
	loader          *loader.Loader
	resolvedSources map[manifest.NamedResource]*manifest.GitRepository
	remotes         map[string]struct{}
	remotesLoaded   bool
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
		if abs, err := ResolveScanPath(d.cfg.Path); err == nil {
			scanRoot = abs
		}
	}
	if info, err := os.Stat(scanRoot); err != nil {
		return fmt.Errorf("--path %q: %w", d.cfg.Path, err)
	} else if !info.IsDir() {
		return fmt.Errorf("--path %q is not a directory", d.cfg.Path)
	}
	scanned := map[string]struct{}{}
	total := 0
	l.IgnoreFile = d.cfg.KRMIgnoreFile
	if err := d.loadAt(ctx, scanRoot, scanned, &total); err != nil {
		return err
	}
	l.IgnoreFile = ""
	// Pins resolve on final namespace ids. Extra inheritance passes are needed
	// only when both an authored ref and a self-matching URL make a pin possible;
	// unpinned followed paths must wait for the complete Kustomization set in Run.
	d.loader.ApplyNamespaces(repoRoot)
	namespaced := total

	// Pins must be resolved before any spec.path is followed so a pinned
	// Kustomization's subtree comes from its committed artifact, never the
	// working tree. Sources found through followed paths resolve on the next pass.
	// PreferExisting lets repeated AddObject re-emission
	// be a no-op so the loop terminates on convergence (no new objects
	// added). ResourceSets that emit child Kustomizations referencing new
	// spec.paths are handled at run time — the RS controller emits the
	// child KS via AddObject and the scheduler discovers it as a new node;
	// discovery no longer pre-expands RSes.
	l.PreferExisting = true
	ksExpanded := map[manifest.NamedResource]struct{}{}
	var aliased []manifest.NamedResource
	for {
		if total != namespaced {
			for id := range d.loader.Index.Records {
				if id.Kind != manifest.KindGitRepository {
					continue
				}
				repo, ok := d.cfg.Store.Get[*manifest.GitRepository](id)
				if ok && repo.Reference != nil && manifest.GitRefString(*repo.Reference) != "" {
					if _, match := d.selfRemotes(repoRoot)[normalizeGitURL(repo.URL)]; match {
						d.loader.ApplyNamespaces(repoRoot)
						break
					}
				}
			}
			namespaced = total
		}
		overridden, err := d.overrideSelfReferentialGitRepositories(ctx, repoRoot)
		if err != nil {
			return err
		}
		aliased = append(aliased, overridden...)
		if d.hasPins {
			d.discardPinnedWorkingTreeFiles(repoRoot)
		}
		kustomizations := d.cfg.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization)
		if d.hasPins {
			// A source discovered through a followed path can pin an expanded consumer.
			for _, ks := range kustomizations {
				if _, pinned, _ := loader.SourceView(d.cfg.Store, ks, repoRoot); pinned {
					delete(ksExpanded, ks.Named())
				}
			}
		}
		added := 0
		for _, ks := range kustomizations {
			id := ks.Named()
			if _, seen := ksExpanded[id]; seen {
				continue
			}
			ksExpanded[id] = struct{}{}
			if ks.Path == "" && ks.SourceName == "" {
				continue
			}
			sourceRoot, _, external := loader.SourceView(d.cfg.Store, ks, repoRoot)
			ref := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
			if external && ks.SourceKind == manifest.KindOCIRepository && d.cfg.Store.GetObject(ref) == nil && d.cfg.Store.GetArtifact(ref) == nil {
				continue
			}
			target := filepath.Join(sourceRoot, filepath.FromSlash(stripDotSlash(ks.Path)))
			// Canonicalize via EvalSymlinks so two spec.paths that
			// resolve to the same on-disk directory (one direct, one
			// through a symlink) share a scanned-set key. Without
			// this, a symlinked spec.path re-walks an already-scanned
			// subtree. Best-effort: fall back to the joined path on
			// any error (typical: target doesn't exist; the existing
			// pathUnderRoot+Stat check at loadAt handles that).
			if resolved, err := filepath.EvalSymlinks(target); err == nil {
				target = resolved
			}
			if _, seen := scanned[target]; seen {
				continue
			}
			if !pathUnderRoot(target, sourceRoot) {
				continue
			}
			if err := d.loadAt(ctx, target, scanned, &total); err != nil {
				return err
			}
			added++
		}
		if added == 0 {
			break
		}
	}
	l.PreferExisting = false
	aliased = append(d.aliasMissingKustomizationSources(repoRoot), aliased...)
	warnIfMultipleBootstrapAliases(aliased, repoRoot)
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
	for _, ks := range d.cfg.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
		ref := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
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
	for id, record := range d.loader.Index.Files() {
		file := record.File
		if _, resolved := d.resolvedSources[id]; resolved {
			continue
		}
		if _, pinned := pinnedSources[id]; pinned {
			continue
		}
		if !pathUnderRoot(filepath.Join(repoRoot, filepath.FromSlash(file)), repoRoot) {
			continue
		}
		_, owned := manifest.StrictParent(prefixes, file, id, nil)
		if !owned {
			continue
		}
		d.cfg.Store.DeleteObject(id)
		delete(d.loader.Index.Records, id)
	}
}

// loadAt scans dir if not already scanned, marks it, and accumulates
// the loaded object count.
func (d *discoverer) loadAt(ctx context.Context, dir string, scanned map[string]struct{}, total *int) error {
	if _, seen := scanned[dir]; seen {
		return nil
	}
	scanned[dir] = struct{}{}
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
