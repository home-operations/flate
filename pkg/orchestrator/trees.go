package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// Tree is one cluster directory to render for comparison. RepoRoot is the
// source root that Kustomization spec.path values resolve against (the
// GitRepository artifact root); Path is the scan entry point and defaults
// to RepoRoot when empty (a cluster whose Flux entry is a subdir passes the
// subdir as Path and the root as RepoRoot). SelfURLs are the remote URL(s)
// the tree represents, for self-referential GitRepository aliasing. A
// consumer rendering extracted trees with no .git supplies all three
// explicitly; nothing here is inferred from a .git ancestor.
type Tree struct {
	RepoRoot string
	Path     string
	SelfURLs []string
}

// Rendered is one side of a RenderTrees comparison: the Orchestrator
// (already Stopped — its background reconcile loops are released, but its
// Store stays readable for diff doc gathering) paired with its render
// Result and that side's render error. Result is nil only on a fatal
// (Bootstrap) error; a per-resource render failure keeps Result non-nil
// (the failures are in Result.Failed) and is reported in Err.
type Rendered struct {
	*Orchestrator
	Result *Result
	Err    error
}

// RenderTrees renders two cluster directories for comparison — the engine
// behind `flate diff`, exposed so an SDK consumer (a PR-diff bot, a CI
// gate) doesn't re-implement the two-orchestrator dance.
//
// Each side reconciles in CHANGED-ONLY mode against the other (its
// PathOrig is set to the opposite tree's RepoRoot), so only resources
// whose source files differ between the two trees — plus their dependency
// closure — are rendered, not the whole cluster. Pairing the two scoped
// outputs (e.g. via diff.Changes) yields the same diff a full render
// would: an unchanged resource renders on neither side.
//
// Both sides share one source.Cache so a chart / OCI layer / git source
// fetched for one side is reused by the other (and concurrent slot
// allocation is serialized), and they run concurrently. cfg supplies the
// render tuning; cfg.Path / RepoRoot / PathOrig / SelfURLs are set from
// baseTree/headTree (any values on cfg are ignored), and cfg.SourceCache,
// when nil, is created from cfg.CacheDir (falling back to the OS tempdir).
// A consumer that renders many comparisons (one per PR) should pass its
// own long-lived cfg.SourceCache so artifacts persist across calls.
//
// Both orchestrators are Stopped before return; their Stores remain
// readable. err is non-nil if either side had any failure; callers that
// still want the partial diff check Rendered.Result != nil (nil only on a
// fatal Bootstrap error) and treat err as advisory. baseTree is the
// left/old side and headTree the right/new — matching diff.Changes(left,
// right). Each side reconciles changed-only against the OTHER tree's
// RepoRoot.
func RenderTrees(ctx context.Context, baseTree, headTree Tree, cfg Config) (base, head Rendered, err error) {
	if cfg.SourceCache == nil {
		root := cmp.Or(cfg.CacheDir, filepath.Join(os.TempDir(), "flate-cache"))
		cfg.SourceCache = source.NewCache(cacheroot.New(root))
	}
	baseCfg := cfg
	baseCfg.RepoRoot = baseTree.RepoRoot
	baseCfg.Path = cmp.Or(baseTree.Path, baseTree.RepoRoot)
	baseCfg.PathOrig = headTree.RepoRoot
	baseCfg.SelfURLs = baseTree.SelfURLs
	headCfg := cfg
	headCfg.RepoRoot = headTree.RepoRoot
	headCfg.Path = cmp.Or(headTree.Path, headTree.RepoRoot)
	headCfg.PathOrig = baseTree.RepoRoot
	headCfg.SelfURLs = headTree.SelfURLs

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		o, res, e := renderOne(gctx, baseCfg)
		base = Rendered{Orchestrator: o, Result: res, Err: e}
		// A fatal Bootstrap error yields a nil orchestrator — propagate
		// it so the errgroup cancels the sibling. Per-resource failures
		// keep the orchestrator non-nil; defer them so the caller still
		// gets a diff before flipping its exit code.
		if o == nil {
			return e
		}
		return nil
	})
	g.Go(func() error {
		o, res, e := renderOne(gctx, headCfg)
		head = Rendered{Orchestrator: o, Result: res, Err: e}
		if o == nil {
			return e
		}
		return nil
	})
	if e := g.Wait(); e != nil {
		return base, head, e
	}
	return base, head, joinTreeErrors(base.Err, head.Err)
}

// renderOne runs one orchestrator end to end: New → Render → Stop. Stop
// fires unconditionally so background loops are released even on the
// failure paths; the Store stays readable afterward. A nil Result (fatal
// Bootstrap error) drops the orchestrator so callers gate on it cleanly,
// mirroring the CLI's single-tree runner.
func renderOne(ctx context.Context, cfg Config) (*Orchestrator, *Result, error) {
	o, err := New(cfg)
	if err != nil {
		return nil, nil, err
	}
	res, err := o.Render(ctx)
	o.Stop()
	if res == nil {
		return nil, nil, err
	}
	return o, res, err
}

// joinTreeErrors composes the per-side render errors into one non-nil
// error when either side failed; both nil → nil.
func joinTreeErrors(base, head error) error {
	switch {
	case base != nil && head != nil:
		return errors.Join(
			errors.New("both trees had reconcile failures"),
			fmt.Errorf("base tree: %w", base),
			fmt.Errorf("head tree: %w", head),
		)
	case base != nil:
		return fmt.Errorf("base tree: %w", base)
	case head != nil:
		return fmt.Errorf("head tree: %w", head)
	}
	return nil
}
