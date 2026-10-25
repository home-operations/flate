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

	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Result summarizes what discovery hydrated into the store.
type Result struct {
	// Documents contains sanitized built resources, including discovery seeds only from the selected entry.
	Documents []map[string]any
	// RepoRoot is the bootstrap input's ownership coordinate. Admitted
	// inputs may use a synthetic label with no corresponding directory.
	RepoRoot string
	// Index contains the immutable static discovery facts consumed by rendering.
	Index *manifest.DiscoveryIndex
	// WipeSecrets reflects the loader's WipeSecrets setting. The
	// orchestrator forwards it to lazy-promotion so SOPS Secrets
	// stay wiped on demand the same way they were at file-load.
	WipeSecrets bool
	// DependencySubstitutions is frozen after complete discovery and before rendering.
	DependencySubstitutions values.DependencySubstitutions
}

// Config is the input contract for Run. Store is mandatory.
type Config struct {
	// Input is authoritative over a preseeded bootstrap artifact or disk admission.
	Input *store.SourceArtifact
	// Path is the scan entry point, defaulting to the admitted input's LocalPath.
	// Relative paths resolve against the working directory.
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
	// Trees is required when discovered packages need a bare Kustomize build.
	Trees        *kustomize.TreeCache
	Store        *store.Store
	WipeSecrets  bool
	RemoteInputs *source.HTTPInputs
	RemoteRetry  cache.RetryConfig
}

// Run performs the full discovery phase against cfg and writes results
// into cfg.Store. Returns the structural metadata the orchestrator
// needs for change-filter construction and controller wiring.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Store == nil {
		return nil, errors.New("discovery: Store is required")
	}
	l := loader.New(cfg.Store)
	l.Options = loader.Options{WipeSecrets: cfg.WipeSecrets, RemoteInputs: cfg.RemoteInputs, RemoteRetry: cfg.RemoteRetry}
	d := &discoverer{cfg: cfg, loader: l, aliasViews: make(map[string]fs.FS)}
	_, err := d.resolveEntrySources(ctx, false)
	if err != nil {
		return nil, err
	}
	repoRoot := cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact).LocalPath
	if err := d.loadManifests(ctx, repoRoot); err != nil {
		return nil, err
	}
	var kustomizations []*manifest.Kustomization
	for _, child := range d.loader.Kustomizations() {
		kustomizations = append(kustomizations, child.Obj.(*manifest.Kustomization))
	}
	substitutions := values.NewDependencySubstitutions(kustomizations)
	for _, ks := range kustomizations {
		if resolved := manifest.MapDependsOn(ks, substitutions.Resolve, substitutions.Resolve); resolved != ks && cfg.Store.GetObject(ks.Named()) != nil {
			_, doc, emission := cfg.Store.GetEmission(ks.Named())
			emission.Finish()
			cfg.Store.AddObject(resolved, doc)
		}
	}
	var standaloneSecrets []manifest.NamedResource
	if cfg.WipeSecrets {
		for id := range l.Index.Files() {
			if id.Group == "" && id.Kind == manifest.KindSecret && d.cfg.Store.GetObject(id) == nil {
				standaloneSecrets = append(standaloneSecrets, id)
			}
		}
	}
	if err := l.FinalizeIndex(ctx, repoRoot); err != nil {
		return nil, err
	}
	cfg.Store.SetDiscoveryIndex(l.Index)
	d.promoteOrphans(ctx, standaloneSecrets)
	documents := d.documents[:0]
	for i, doc := range d.documents {
		id := manifest.DocIdentity(doc)
		record, ok := l.Index.Lookup(id)
		current := cfg.Store.GetObject(id)
		if !ok || record.Builder != d.documentOrigins[i].builder || current == nil {
			continue
		}
		if current != d.documentOrigins[i].object {
			doc = manifest.OverlayPayload(current, doc)
		}
		documents = append(documents, doc)
	}
	d.documents = documents
	return &Result{
		Documents:               d.documents,
		DependencySubstitutions: substitutions,
		RepoRoot:                repoRoot,
		Index:                   l.Index,
		WipeSecrets:             cfg.WipeSecrets,
	}, nil
}

type discoverer struct {
	documents       []map[string]any
	documentOrigins []documentOrigin
	cfg             Config
	loader          *loader.Loader
	resolvedSources map[manifest.NamedResource]entryResolution
	remotes         map[string]struct{}
	aliasViews      map[string]fs.FS
	hasPins         bool
}

type documentOrigin struct {
	builder string
	object  manifest.BaseManifest
}

func (d *discoverer) loadManifests(ctx context.Context, repoRoot string) error {
	l := d.loader
	l.SourceRoot = repoRoot

	input := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	name, err := filepath.Rel(input.LocalPath, d.cfg.Path)
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
	if err := d.loadAt(ctx, d.cfg.Path, &total); err != nil {
		return err
	}
	l.IgnoreFile = ""
	// Projection may introduce a new effective source reference. Resolve it
	// before Load admits its spec.path edge to scope construction.
	l.PreferExisting = true
	type expansion struct {
		ks     *manifest.Kustomization
		source *store.SourceArtifact
		object manifest.BaseManifest
	}
	ksExpanded := map[expansion]struct{}{}
	var pending []expansion
	rounds := 0
	for {
		rounds++
		l.Admit(repoRoot)
		_, err := d.resolveEntrySources(ctx, false)
		if err != nil {
			return err
		}
		if d.hasPins {
			d.discardPinnedWorkingTreeFiles(repoRoot)
		}
		kustomizations := d.loader.Kustomizations()
		for _, child := range kustomizations {
			ks := child.Obj.(*manifest.Kustomization)
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
			sourceRoot, _, external := loader.SourceView(d.cfg.Store, ks.SourceID(), repoRoot)
			if external && d.cfg.Store.GetObject(ks.SourceID()) == nil {
				next = append(next, key)
				continue
			}
			target := filepath.Join(sourceRoot, manifest.NormalizeClaimBase(ks.Path))
			if !safepath.Contains(sourceRoot, target) {
				continue
			}
			if err := d.loadAt(ctx, target, &total); err != nil {
				if !errors.Is(err, fs.ErrPermission) || key.source == nil || key.source.FS == nil {
					return err
				}
				name, _ := filepath.Rel(sourceRoot, target)
				if _, statErr := fs.Stat(key.source.FS, filepath.ToSlash(name)); !errors.Is(statErr, fs.ErrPermission) {
					return err
				}
			}
			added++
		}
		pending = next
		if added == 0 {
			missing, err := d.resolveEntrySources(ctx, true)
			if err != nil {
				return err
			}
			if missing != 0 {
				continue
			}
			roots := l.StandaloneRoots(repoRoot)
			if len(roots) == 0 {
				break
			}
			for _, root := range roots {
				if err := d.buildPackage(ctx, repoRoot, root); err != nil {
					return err
				}
			}
		}
	}
	l.PreferExisting = false
	d.warnBootstrapAliases(repoRoot)
	slog.Debug("discovery: loaded objects", "count", total, "rounds", rounds, "scan_root", d.cfg.Path, "source_root", repoRoot)
	return nil
}

func (d *discoverer) buildPackage(ctx context.Context, repoRoot, root string) error {
	input := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	docs, err := kustomize.Render(ctx, d.cfg.Trees, kustomize.RenderRequest{Source: input, SubPath: root}, d.cfg.RemoteRetry)
	if err != nil {
		return fmt.Errorf("discovery package %q build: %w", root, err)
	}
	built := []store.Child{}
	for _, doc := range manifest.FlattenLists(docs) {
		obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{WipeSecrets: d.cfg.WipeSecrets})
		if err != nil {
			return fmt.Errorf("discovery package %q document: %w", root, err)
		}
		if obj == nil {
			manifest.ReleaseDoc(doc)
			continue
		}
		built = append(built, store.Child{Obj: obj, Doc: authoritative})
	}
	packageRoot := filepath.Join(repoRoot, filepath.FromSlash(root))
	selected := packageRoot == d.cfg.Path
	d.loader.RegisterBuilt(packageRoot, built)
	for _, child := range built {
		obj := child.Obj
		if !selected && manifest.KindOf(obj).DiscoverySeed() {
			continue
		}
		record, _ := d.loader.Index.Lookup(obj.Named())
		d.documentOrigins = append(d.documentOrigins, documentOrigin{builder: record.Builder, object: obj})
		d.documents = append(d.documents, child.Doc)
	}
	return nil
}

// A broad --path scan can overlap a pinned subtree. Its working-tree objects
// must be discarded before the committed subtree supplies discovery metadata.
// Resolved sources must retain their objects and artifacts so a pinned root
// claim cannot replace its own source with a working-tree alias.
func (d *discoverer) discardPinnedWorkingTreeFiles(repoRoot string) {
	claims := d.loader.Claims(repoRoot)
	pinnedSources := make(map[manifest.NamedResource]struct{})
	pinnedKS := make(map[manifest.NamedResource]struct{})
	for _, child := range d.loader.Kustomizations() {
		ks := child.Obj.(*manifest.Kustomization)
		ref := ks.SourceID()
		if _, pinned, _ := loader.SourceView(d.cfg.Store, ks.SourceID(), repoRoot); pinned {
			pinnedSources[ref] = struct{}{}
			pinnedKS[ks.Named()] = struct{}{}
		}
	}
	var prefixes []manifest.KSClaim
	for _, c := range claims {
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

// The Loader deduplicates unchanged views; unavailable followed paths fail during reconciliation.
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
