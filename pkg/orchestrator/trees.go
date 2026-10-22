package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// Tree is one cluster directory to render for comparison. RepoRoot is the
// source root that Kustomization spec.path values resolve against (the
// GitRepository artifact root); Path is the scan entry point and defaults
// to RepoRoot when empty (a cluster whose Flux entry is a subdir passes the
// subdir as Path and the root as RepoRoot). SelfURLs are the remote URL(s)
// the tree represents, for self-referential GitRepository aliasing. A
// consumer rendering extracted trees with no .git supplies the source root
// and scan path explicitly.
type Tree struct {
	RepoRoot string
	Path     string
	SelfURLs []string
}

// Rendered pairs a completed orchestrator and result with its error.
// A failed bootstrap retains only Result.Warnings and no Orchestrator;
// per-resource failures retain the completed result and readable Store.
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
// cfg.BaseInput and cfg.Input supply authoritative base and head bytes, respectively.
//
// Both orchestrators are Stopped before return; their Stores remain
// readable. err is non-nil if either side had any failure; callers that
// still want a partial diff require both Orchestrator and Result, and treat
// per-resource errors as advisory. baseTree is the
// left/old side and headTree the right/new — matching diff.Changes(left,
// right). Each side reconciles changed-only against the OTHER tree's
// RepoRoot.
func RenderTrees(ctx context.Context, baseTree, headTree Tree, cfg Config) (base, head Rendered, err error) {
	if cfg.Network == nil {
		cfg.Network = new(source.Network)
		defer cfg.Network.Close()
	}
	if cfg.Cache == nil {
		cfg.Cache, err = sourcecache.New(ctx, cfg.CacheDir)
		if err != nil {
			return base, head, err
		}
		defer func() { err = errors.Join(err, cfg.Cache.Close()) }()
	}
	inputs := [2]*store.SourceArtifact{cfg.BaseInput, cfg.Input}
	directoryInputs := inputs[0] == nil && inputs[1] == nil
	for i, side := range []*Tree{&baseTree, &headTree} {
		if inputs[i] == nil {
			path := cmp.Or(side.Path, side.RepoRoot)
			if path == "" {
				return base, head, fmt.Errorf("%w: tree path is required", manifest.ErrInput)
			}
			inputs[i], side.Path, err = baseline.AdmitDirectory(ctx, path, side.RepoRoot)
			if err != nil {
				return base, head, err
			}
		}
		if inputs[i].FS == nil || inputs[i].LocalPath == "" {
			return base, head, fmt.Errorf("%w: tree input content unavailable", manifest.ErrInput)
		}
		side.RepoRoot = inputs[i].LocalPath
	}
	if directoryInputs && baseTree.RepoRoot == headTree.RepoRoot {
		inputs[1] = inputs[0]
	}
	if cfg.ExternalChanges == nil && inputs[0] != inputs[1] {
		cfg.ExternalChanges, err = change.Detect(ctx, inputs[0], inputs[1])
		if err != nil {
			return base, head, err
		}
	}
	baseCfg := cfg
	baseCfg.RepoRoot = baseTree.RepoRoot
	baseCfg.Path = cmp.Or(baseTree.Path, baseTree.RepoRoot)
	baseCfg.PathOrig = headTree.RepoRoot
	baseCfg.SelfURLs = baseTree.SelfURLs
	baseCfg.Input, baseCfg.BaseInput = inputs[0], inputs[1]
	headCfg := cfg
	headCfg.RepoRoot = headTree.RepoRoot
	headCfg.Path = cmp.Or(headTree.Path, headTree.RepoRoot)
	headCfg.PathOrig = baseTree.RepoRoot
	headCfg.SelfURLs = headTree.SelfURLs
	headCfg.Input, headCfg.BaseInput = inputs[1], inputs[0]

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
	_ = g.Wait()
	return base, head, joinTreeErrors(base.Err, head.Err)
}

// renderOne preserves partial results after Render has joined teardown. A fatal
// Bootstrap error drops the orchestrator, matching the CLI's single-tree runner.
func renderOne(ctx context.Context, cfg Config) (*Orchestrator, *Result, error) {
	o, err := New(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	res, err := o.Render(ctx)
	if res == nil {
		return nil, &Result{Warnings: o.store.Warnings()}, err
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
