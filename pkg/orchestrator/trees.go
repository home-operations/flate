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
// PathOrig is set to the opposite tree), so only resources whose source
// files differ between basePath and headPath — plus their dependency
// closure — are rendered, not the whole cluster. Pairing the two scoped
// outputs (e.g. via diff.Changes) yields the same diff a full render
// would: an unchanged resource renders on neither side.
//
// Both sides share one source.Cache so a chart / OCI layer / git source
// fetched for one side is reused by the other (and concurrent slot
// allocation is serialized), and they run concurrently. cfg supplies the
// render tuning; cfg.Path and cfg.PathOrig are set by RenderTrees and any
// values there are ignored, and cfg.SourceCache, when nil, is created
// from cfg.CacheDir (falling back to the OS tempdir). A consumer that
// renders many comparisons (one per PR) should pass its own long-lived
// cfg.SourceCache so artifacts persist across calls.
//
// Both orchestrators are Stopped before return; their Stores remain
// readable. err is non-nil if either side had any failure; callers that
// still want the partial diff check Rendered.Result != nil (nil only on a
// fatal Bootstrap error) and treat err as advisory. base is the left/old
// side and head the right/new — matching diff.Changes(left, right).
func RenderTrees(ctx context.Context, basePath, headPath string, cfg Config) (base, head Rendered, err error) {
	if cfg.SourceCache == nil {
		root := cmp.Or(cfg.CacheDir, filepath.Join(os.TempDir(), "flate-cache"))
		cfg.SourceCache = source.NewCache(cacheroot.New(root))
	}
	baseCfg := cfg
	baseCfg.Path, baseCfg.PathOrig = basePath, headPath
	headCfg := cfg
	headCfg.Path, headCfg.PathOrig = headPath, basePath

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
