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
	Path        string
	Store       *store.Store
	WipeSecrets bool
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
	d := &discoverer{
		cfg:         cfg,
		loader:      l,
		sourceFiles: map[manifest.NamedResource]string{},
	}
	repoRoot, err := d.seedBootstrapSource()
	if err != nil {
		return nil, err
	}
	if err := d.loadManifests(ctx, repoRoot); err != nil {
		return nil, err
	}
	d.aliasBootstrapSources(repoRoot)
	loader.ApplyNamespaceInheritance(d.cfg.Store, d.sourceFiles, repoRoot)
	// Unified parent index over every reconcilable kind that uses a
	// parent gate. KS and HR keys never collide because NamedResource
	// includes Kind; downstream controllers look up by their own id
	// and naturally filter to their own kind.
	parentOf := mergeParents(
		loader.BuildParentIndexForKind(d.cfg.Store, d.sourceFiles, manifest.KindKustomization),
		loader.BuildParentIndexForKind(d.cfg.Store, d.sourceFiles, manifest.KindHelmRelease),
	)
	// Orphan promotion: every Existence entry whose file path is NOT
	// under any KS spec.path will never reach the Store through KS
	// render emission. Promote it now so standalone CRs (loose HR
	// at repo root, sources next to flux-system/kustomization.yaml,
	// etc.) keep working in DiscoveryOnly mode.
	d.promoteOrphans()

	return &Result{
		RepoRoot:    repoRoot,
		SourceFiles: d.sourceFiles,
		ParentOf:    parentOf,
		Existence:   l.Existence,
		WipeSecrets: cfg.WipeSecrets,
	}, nil
}

// mergeParents combines per-kind parent maps into one. Earlier
// arguments win on collision (which can't happen in practice — keys
// are NamedResource with distinct Kind components — but the rule is
// explicit so future callers don't accidentally clobber a KS parent
// with an HR-built rebuild).
func mergeParents(maps ...map[manifest.NamedResource]manifest.NamedResource) map[manifest.NamedResource]manifest.NamedResource {
	out := map[manifest.NamedResource]manifest.NamedResource{}
	for _, m := range maps {
		for k, v := range m {
			if _, exists := out[k]; exists {
				continue
			}
			out[k] = v
		}
	}
	return out
}

type discoverer struct {
	cfg         Config
	loader      *loader.Loader
	sourceFiles map[manifest.NamedResource]string
}

// loadManifests scans cfg.Path, then iteratively follows each loaded
// Flux Kustomization's spec.path until a fixed point is reached.
// Interleaved with KS expansion is ResourceSet rendering: a parent
// KS may emit a ResourceSet which itself emits child Kustomizations
// referencing new spec.paths the file walker hasn't visited yet.
func (d *discoverer) loadManifests(ctx context.Context, repoRoot string) error {
	l := d.loader
	l.SourceRoot = repoRoot
	l.SourceFiles = d.sourceFiles

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
	if err := d.loadAt(ctx, l, scanRoot, scanned, &total); err != nil {
		return err
	}

	// Fixed-point expansion: each pass renders Kustomizations the prior
	// pass discovered, plus ResourceSets that may emit further KSes.
	// PreferExisting lets repeated AddObject re-emission be a no-op so
	// the loop terminates on convergence (no new objects added).
	//
	// ResourceSets are re-rendered every iteration rather than memoized,
	// because a RS's inputsFrom selector may match RSIPs that only
	// arrive after a downstream Kustomization chain expands. Without
	// the retry, a RS whose RSIPs are produced by a child KS renders
	// to zero docs on first pass and never recovers — observed in
	// tholinka/home-ops where `dragonfly-acls` (a Permute RS) selects
	// RSIPs created by an unrelated `dragonfly/manual` component
	// applied through `renovate-operator-jobs-jobs`. Re-rendering is
	// safe: renderResourceSet skips already-present objects in the
	// store, so a steady-state RS contributes 0 new docs and the loop
	// converges via the `added == 0` exit.
	l.PreferExisting = true
	ksExpanded := map[manifest.NamedResource]struct{}{}
	for {
		added := 0
		for _, ks := range store.ListAs[*manifest.Kustomization](d.cfg.Store, manifest.KindKustomization) {
			id := ks.Named()
			if _, seen := ksExpanded[id]; seen {
				continue
			}
			if ks.Path == "" {
				ksExpanded[id] = struct{}{}
				continue
			}
			ksExpanded[id] = struct{}{}
			target := filepath.Join(repoRoot, filepath.FromSlash(stripDotSlash(ks.Path)))
			if _, seen := scanned[target]; seen {
				continue
			}
			if !pathUnderRoot(target, repoRoot) {
				continue
			}
			if err := d.loadAt(ctx, l, target, scanned, &total); err != nil {
				return err
			}
			added++
		}
		for _, rs := range store.ListAs[*manifest.ResourceSet](d.cfg.Store, manifest.KindResourceSet) {
			n, err := d.renderResourceSet(rs)
			if err != nil {
				return err
			}
			if n > 0 {
				added++
				total += n
			}
		}
		if added == 0 {
			break
		}
	}
	l.PreferExisting = false
	slog.Debug("discovery: loaded objects", "count", total, "scanRoot", scanRoot, "sourceRoot", repoRoot)
	return nil
}

// loadAt scans dir if not already scanned, marks it, and accumulates
// the loaded object count.
func (d *discoverer) loadAt(ctx context.Context, l *loader.Loader, dir string, scanned map[string]struct{}, total *int) error {
	if _, seen := scanned[dir]; seen {
		return nil
	}
	scanned[dir] = struct{}{}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil
	}
	n, err := l.Load(ctx, dir)
	if err != nil {
		return err
	}
	*total += n
	return nil
}

