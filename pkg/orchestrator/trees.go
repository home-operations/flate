package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
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
// Both sides share the common cache owner and run concurrently. Caller-injected
// cfg.Cache remains caller-owned; an internal owner closes after both sides stop.
// cfg.Path, RepoRoot, PathOrig and SelfURLs come from the supplied trees.
//
// Both orchestrators are Stopped before return; their Stores remain
// readable. err is non-nil if either side had any failure; callers that
// still want the partial diff check Rendered.Result != nil (nil only on a
// fatal Bootstrap error) and treat err as advisory. baseTree is the
// left/old side and headTree the right/new — matching diff.Changes(left,
// right). Each side reconciles changed-only against the OTHER tree's
// RepoRoot.
func RenderTrees(ctx context.Context, baseTree, headTree Tree, cfg Config) (base, head Rendered, err error) {
	if cfg.Cache == nil {
		cfg.Cache, err = sourcecache.New(ctx, cacheroot.New(cfg.CacheDir))
		if err != nil {
			return base, head, err
		}
		defer func() { err = errors.Join(err, cfg.Cache.Close()) }()
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

// treeErrors retains side attribution so Select can project resource failures
// without discarding outer errors from shared cache teardown.
type treeErrors struct {
	base error
	head error
}

func (e *treeErrors) Error() string {
	switch {
	case e.base != nil && e.head != nil:
		return "both trees had reconcile failures\nbase tree: " + e.base.Error() + "\nhead tree: " + e.head.Error()
	case e.base != nil:
		return "base tree: " + e.base.Error()
	default:
		return "head tree: " + e.head.Error()
	}
}

func (e *treeErrors) Unwrap() []error {
	var out []error
	if e.base != nil {
		out = append(out, fmt.Errorf("base tree: %w", e.base))
	}
	if e.head != nil {
		out = append(out, fmt.Errorf("head tree: %w", e.head))
	}
	return out
}

func joinTreeErrors(base, head error) error {
	if base == nil && head == nil {
		return nil
	}
	return &treeErrors{base: base, head: head}
}
