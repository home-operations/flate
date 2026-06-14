// Package discovery owns flate's filesystem-to-store hydration phase:
// walking the user's working tree, expanding spec.path references,
// aliasing in-cluster-bootstrapped sources, rendering ResourceSets, and
// computing the structural-parent index. The output is everything the
// reconcile phase needs to start firing controllers — repo root,
// per-object source files, and the parent index.
//
// Splitting this out of the orchestrator turns a 750-line god-object
// into two ~350-line files with one clean interface between them. The
// load phase is independently testable (no controller wiring or
// task service required) and the orchestrator now reads as pure
// reconcile orchestration.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
	// SourceFiles maps each loaded resource to the repo-relative path
	// it was parsed from. Consumed by the change filter.
	SourceFiles map[manifest.NamedResource]string
	// SourceRefs maps each loaded consumer (HelmRelease / Kustomization)
	// to the source resources it references. Consumed by the change
	// filter's reverse edge so a changed source re-renders the
	// HelmReleases that chartRef it even when the source lives in a
	// separate Kustomization tree (and the HR never reached the Store
	// under DiscoveryOnly).
	SourceRefs map[manifest.NamedResource][]manifest.NamedResource
	// ParentOf maps each reconcilable resource (Kustomization or
	// HelmRelease) to its structural-parent Kustomization — the KS
	// whose spec.path is the deepest strict ancestor of the child's
	// source file. KS children honor it as a depwait dep so any
	// parent-render-time spec mutations (replacements: injecting
	// targetNamespace) are visible before the child renders;
	// HR children honor it so the first render reads the post-patch
	// spec (driftDetection / upgrade strategy / CRD policy overrides
	// applied at the cluster-KS level) instead of the pre-patch
	// file-loaded copy. Keyed by NamedResource so KS and HR entries
	// never collide. Empty when no parent enforcement applies.
	ParentOf map[manifest.NamedResource]manifest.NamedResource
	// SelfProduce attributes each ConfigMap to the Kustomization(s)
	// whose own render subtree emits it (bare-dir → subdir-base →
	// component graph, with namespace propagation). collectDeps uses it
	// to drop a self-produced postBuild.substituteFrom ConfigMap from
	// the dependency set — a KS can't wait on a CM only its own render
	// produces. Available in full mode, unlike the changed-only
	// producer index.
	SelfProduce *loader.SelfProduceIndex
	// Producers maps a target Secret to the in-repo ExternalSecret /
	// SealedSecret that declares it, seeded by the same self-produce walk.
	// The source + HR controllers consult it to skip a missing auth /
	// valuesFrom Secret that has a declared producer, without
	// --allow-missing-secrets. Augmented at render time by the HR
	// controller's EventObjectAdded listener. See manifest.ProducerIndex.
	Producers *manifest.ProducerIndex
	// Existence holds every file-loaded object the DiscoveryOnly
	// loader kept out of the Store: HRs, sources, CMs, Secrets, and
	// raw manifests. depwait's missing-dep fallback consults it to
	// resolve sibling-rendered substituteFrom CMs without
	// deadlocking the parent KS. The orchestrator passes a closure
	// over this index into the controllers' Waiter wiring.
	Existence *loader.ExistenceIndex
	// WipeSecrets reflects the loader's WipeSecrets setting. The
	// orchestrator forwards it to lazy-promotion so SOPS Secrets
	// stay wiped on demand the same way they were at file-load.
	WipeSecrets bool
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
	// GitRepository whose spec.url matches one of these is the cluster
	// pulling itself; its artifact is aliased to the local tree
	// (overrideSelfReferentialGitRepositories) so the offline render
	// resolves it. Supplied explicitly by SDK consumers rendering
	// extracted trees (no .git/config to read); empty ⇒ fall back to the
	// working tree's .git remotes, preserving local behavior.
	SelfURLs    []string
	Store       *store.Store
	WipeSecrets bool
	// ComponentCache, when non-nil, memoizes
	// manifest.ReadKustomizeComponents reads across discovery's
	// internal passes (parent-index build, orphan promotion, the
	// loader's FinalizeGenerators KSPathPrefixes) and any later
	// consumer that shares the same pointer (change.Filter,
	// finalize.detectOrphans). The orchestrator wires one cache per
	// Bootstrap; pass nil for standalone discovery callers (tests,
	// embedders) that don't need cross-consumer sharing.
	ComponentCache *manifest.ComponentCache
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
	// Render-driven discovery: only Kustomizations and the discovery-
	// meta pair (ResourceSet, RSIP) reach the Store from the file
	// walker. HRs, sources, CMs, Secrets, and raw manifests flow
	// through Existence — picked up later by KS render via
	// emitRenderedChildren, the orchestrator's orphan-promotion
	// sweep, or depwait's lazy-promotion fallback.
	l.Options.DiscoveryOnly = true
	l.Existence = loader.NewExistenceIndex()
	// Thread the shared component cache through to the loader so
	// FinalizeGenerators' KSPathPrefixes reads come from cache (and
	// land cache entries for the subsequent parent-index +
	// orphan-promotion passes below). nil is fine here — the loader
	// falls back to a per-call cache.
	l.ComponentCache = cfg.ComponentCache
	d := &discoverer{
		cfg:         cfg,
		loader:      l,
		sourceFiles: map[manifest.NamedResource]string{},
		sourceRefs:  map[manifest.NamedResource][]manifest.NamedResource{},
	}
	repoRoot, err := d.seedBootstrapSource()
	if err != nil {
		return nil, err
	}
	if err := d.loadManifests(ctx, repoRoot); err != nil {
		return nil, err
	}
	d.aliasBootstrapSources(repoRoot)
	d.applyNamespaces(repoRoot)
	// Resolve bare ${VAR} in Kustomization dependsOn against the
	// cluster's postBuild substitute values, now that the full KS set is
	// discovered (so the substitute union is complete and its conflict
	// check is sound). Without this a child KS's
	// `dependsOn: 0-${CLUSTER_NAME}-config` never matches the real
	// 0-biohazard-config the parent's render would have substituted.
	loader.ResolveDependsOnSubstitutions(d.cfg.Store)
	// Materialize configMapGenerator / secretGenerator entries the
	// file walker collected. The effective namespace comes from the
	// enclosing Flux Kustomization, which is only known now that
	// the full KS tree is loaded. Resolves the depwait gap where a
	// substituteFrom references a CM produced by a Component's
	// generator (issue #396).
	l.FinalizeGenerators(repoRoot)
	// Unified parent index over every reconcilable kind that uses a
	// parent gate. KS, HR and RS keys never collide because NamedResource
	// includes Kind; downstream controllers look up by their own id
	// and naturally filter to their own kind. Pass repoRoot — the
	// helpers read each KS's spec.path joined under this root to
	// follow `components:` entries; cfg.Path would misread when the
	// user pointed --path at a subdir below the actual repo root.
	// Compute the KS spec.path prefix list ONCE and reuse it across the
	// KS-parent index, the HR-parent index, and orphan promotion. Each
	// previously rebuilt the identical list (an O(KS) walk + sort +
	// component reads); the shared ComponentCache deduped the file reads
	// but not the iteration/sort/list construction.
	//
	// ResourceSet is included so a file-loaded RS knows its structural
	// parent KS at reconcile time — the RS controller gates on the parent
	// (parent-render-time spec mutations) and attributes its RawObject
	// children to that parent for `flate build` output. Without the
	// file-path entry the RS would only learn its parent once the parent
	// re-emitted it, racing the RS's own first render.
	prefixes := loader.KSPathPrefixesLocalOnly(d.cfg.Store, repoRoot, cfg.ComponentCache)
	parentOf := loader.BuildParentIndexFromPrefixes(prefixes, d.cfg.Store, d.sourceFiles, manifest.KindKustomization)
	maps.Copy(parentOf, loader.BuildParentIndexFromPrefixes(prefixes, d.cfg.Store, d.sourceFiles, manifest.KindHelmRelease))
	maps.Copy(parentOf, loader.BuildParentIndexFromPrefixes(prefixes, d.cfg.Store, d.sourceFiles, manifest.KindResourceSet))
	// Orphan promotion: every Existence entry whose file path is NOT
	// under any KS spec.path will never reach the Store through KS
	// render emission. Promote it now so standalone CRs (loose HR
	// at repo root, sources next to flux-system/kustomization.yaml,
	// etc.) keep working in DiscoveryOnly mode.
	d.promoteOrphans(prefixes)

	producers := &manifest.ProducerIndex{}
	return &Result{
		RepoRoot:    repoRoot,
		SourceFiles: d.sourceFiles,
		SourceRefs:  d.sourceRefs,
		ParentOf:    parentOf,
		SelfProduce: loader.BuildSelfProduceIndex(d.cfg.Store, repoRoot, producers, cfg.WipeSecrets),
		Producers:   producers,
		Existence:   l.Existence,
		WipeSecrets: cfg.WipeSecrets,
	}, nil
}

func (d *discoverer) applyNamespaces(repoRoot string) {
	// Stamp NamespaceTransformer-injected targetNamespace onto Flux KSes
	// first so ApplyNamespaceInheritance's projection sees a populated
	// targetNamespace and the leaf KS renders into the right namespace on
	// its first pass (issue #528).
	loader.StampTransformerTargetNamespaces(d.cfg.Store, d.sourceFiles, repoRoot)
	loader.ApplyNamespaceInheritanceWithRefs(d.cfg.Store, d.sourceFiles, d.sourceRefs, repoRoot)
	loader.ApplyDefaultNamespaces(d.cfg.Store, d.sourceFiles)
}

type discoverer struct {
	cfg         Config
	loader      *loader.Loader
	sourceFiles map[manifest.NamedResource]string
	sourceRefs  map[manifest.NamedResource][]manifest.NamedResource
}

// loadManifests scans cfg.Path, then iteratively follows each loaded
// Flux Kustomization's spec.path until a fixed point is reached.
// ResourceSets are loaded into the store by the file walker but expanded
// during the run by the ResourceSet controller (a first-class DAG node),
// not here — discovery only resolves KS spec.path references.
func (d *discoverer) loadManifests(ctx context.Context, repoRoot string) error {
	l := d.loader
	l.SourceRoot = repoRoot
	l.SourceFiles = d.sourceFiles
	l.SourceRefs = d.sourceRefs

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
	if err := d.loadAt(ctx, scanRoot, scanned, &total); err != nil {
		return err
	}
	// Apply namespaces once over the initially-scanned set so the
	// bootstrap-source alias and the first expansion pass see populated
	// namespaces. The fixed-point loop below intentionally does NOT
	// re-run applyNamespaces per discovered spec.path — that was an
	// O(N²) full-store rebuild on every newly-loaded KS. Namespace
	// inheritance is idempotent and order-independent, so the single
	// post-loop pass in Run (after the complete KS set is discovered)
	// stamps every loop-discovered object correctly in one walk.
	d.applyNamespaces(repoRoot)

	// Fixed-point expansion: each pass renders Kustomizations the prior
	// pass discovered. PreferExisting lets repeated AddObject re-emission
	// be a no-op so the loop terminates on convergence (no new objects
	// added). ResourceSets that emit child Kustomizations referencing new
	// spec.paths are handled at run time — the RS controller emits the
	// child KS via AddObject and the scheduler discovers it as a new node;
	// discovery no longer pre-expands RSes.
	l.PreferExisting = true
	ksExpanded := map[manifest.NamedResource]struct{}{}
	for {
		added := 0
		for _, ks := range store.ListAs[*manifest.Kustomization](d.cfg.Store, manifest.KindKustomization) {
			id := ks.Named()
			if _, seen := ksExpanded[id]; seen {
				continue
			}
			ksExpanded[id] = struct{}{}
			if ks.Path == "" {
				continue
			}
			target := filepath.Join(repoRoot, filepath.FromSlash(stripDotSlash(ks.Path)))
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
			if !pathUnderRoot(target, repoRoot) {
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
	slog.Debug("discovery: loaded objects", "count", total, "scan_root", scanRoot, "source_root", repoRoot)
	return nil
}

// loadAt scans dir if not already scanned, marks it, and accumulates
// the loaded object count.
func (d *discoverer) loadAt(ctx context.Context, dir string, scanned map[string]struct{}, total *int) error {
	if _, seen := scanned[dir]; seen {
		return nil
	}
	scanned[dir] = struct{}{}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil
	}
	n, err := d.loader.Load(ctx, dir)
	if err != nil {
		return err
	}
	*total += n
	return nil
}
