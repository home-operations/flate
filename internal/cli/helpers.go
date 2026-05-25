package cli

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/image"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// firstArg returns the first positional arg, or "" when none was given.
func firstArg(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

// sortRows orders rows by (namespace, name) so table output is
// deterministic across runs.
func sortRows(rows []map[string]string) {
	slices.SortFunc(rows, func(a, b map[string]string) int {
		return cmp.Or(
			cmp.Compare(a["namespace"], b["namespace"]),
			cmp.Compare(a["name"], b["name"]),
		)
	})
}

// collectImages returns the union of images extracted from every
// rendered Kustomization and HelmRelease document. Namespace scope on
// c is honored. Walks Result.Manifests directly (no Store
// GetArtifact + type-assertion dance).
func collectImages(o *orchestrator.Orchestrator, res *orchestrator.Result, c *commonFlags) map[string]struct{} {
	set := map[string]struct{}{}
	for id, docs := range res.Manifests {
		if id.Kind != manifest.KindKustomization && id.Kind != manifest.KindHelmRelease {
			continue
		}
		if !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		for _, doc := range docs {
			imgs, _ := image.Extract(doc)
			for _, img := range imgs {
				set[img] = struct{}{}
			}
		}
	}
	return set
}

// emitImageList writes a sorted image list — JSON / YAML when
// requested, otherwise one image per line.
func emitImageList(w io.Writer, imgs []string, out string) error {
	switch format.Output(out) {
	case format.OutputJSON:
		return format.JSON(w, imgs)
	case format.OutputYAML:
		return format.YAML(w, imgs)
	}
	for _, img := range imgs {
		if _, err := io.WriteString(w, img+"\n"); err != nil {
			return err
		}
	}
	return nil
}

// runDiffOrchestrators boots two orchestrators with each side's
// --path-orig pointing at the other, so both resolve the same symmetric
// change set and only render resources that differ between paths. Both
// sides are independent (separate task service, helm client, staging
// cache, store), so they run concurrently — wall time roughly halves on
// changed-only diffs.
// diffSide pairs an Orchestrator with its render Result. Diff
// commands need both — orchestrator for filter/object lookup, Result
// for the rendered docs feeding the diff.
type diffSide struct {
	O   *orchestrator.Orchestrator
	Res *orchestrator.Result
}

func runDiffOrchestrators(ctx context.Context, c *commonFlags, h *helmFlags) (diffSide, diffSide, error) {
	// diff REQUIRES a baseline — when neither --path-orig nor --base
	// is set, auto-detect via the merge-base ladder. resolveBaseline
	// with autoFallback=true preserves the existing "bare diff
	// figures it out" UX.
	if err := resolveBaseline(ctx, c, true); err != nil {
		return diffSide{}, diffSide{}, err
	}
	currentCfg := buildOrchCfg(*c, *h)
	origCfg := currentCfg
	origCfg.Path, origCfg.PathOrig = c.pathOrig, c.path

	// One source.Cache shared across both orchestrators. They write into
	// the same on-disk cache root; without a shared *Cache each side has
	// its own mutex, and concurrent first-time clones of the same
	// (url, ref) slot can race past the mkdir/Readdirnames check and
	// step on each other.
	cacheRoot := cmp.Or(currentCfg.CacheDir, filepath.Join(os.TempDir(), "flate-cache"))
	shared := source.NewCache(filepath.Join(cacheRoot, "sources"))
	currentCfg.SourceCache = shared
	origCfg.SourceCache = shared

	var (
		orig, current    diffSide
		origErr, currErr error
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		o, res, err := runOrchestratorCfg(gctx, currentCfg)
		current = diffSide{O: o, Res: res}
		currErr = err
		// Fatal Bootstrap errors return nil orchestrator — propagate
		// those so the errgroup cancels its sibling. Per-resource Run
		// failures keep the orchestrator non-nil; defer them so we
		// still produce a diff before the caller flips the exit code.
		if o == nil {
			return err
		}
		return nil
	})
	g.Go(func() error {
		o, res, err := runOrchestratorCfg(gctx, origCfg)
		orig = diffSide{O: o, Res: res}
		origErr = err
		if o == nil {
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return diffSide{}, diffSide{}, err
	}
	// Either side may have reconcile failures — return the combined
	// run-error alongside the orchestrators so the diff caller can
	// write its output, then flip the exit code.
	return orig, current, joinRunErrors(origErr, currErr)
}

// joinRunErrors composes the orig/current Run errors into a single
// non-nil error when either side had failures. Both nil → nil.
func joinRunErrors(orig, curr error) error {
	switch {
	case orig != nil && curr != nil:
		return fmt.Errorf("both snapshots had reconcile failures:\n  orig: %s\n  current: %s", orig, curr)
	case orig != nil:
		return fmt.Errorf("orig snapshot: %w", orig)
	case curr != nil:
		return fmt.Errorf("current snapshot: %w", curr)
	}
	return nil
}

// gatherArtifacts collects every rendered manifest produced by the
// Kustomizations or HelmReleases of the given kind, tagged with the
// parent that produced them. name optionally filters to a single
// resource. When c is non-nil the namespace scope from commonFlags +
// the orchestrator's change filter is honored.
//
// Reads res.Manifests for the rendered docs; falls back to the Store
// only to recover the producing object's spec.path (the diff header
// shows it for KS parents).
// gatherAllArtifacts is gatherArtifacts with a kind="" shortcut for
// the `diff all` command: when kind is empty, collect both
// Kustomization- and HelmRelease-rendered manifests in one pass.
// Each kind's docs are gathered separately so the diff header
// attribution (parent KS vs parent HR) stays accurate.
func gatherAllArtifacts(o *orchestrator.Orchestrator, res *orchestrator.Result, kind, name string, c *commonFlags) []diff.Doc {
	if kind != "" {
		return gatherArtifacts(o, res, kind, name, c)
	}
	out := gatherArtifacts(o, res, manifest.KindKustomization, name, c)
	return append(out, gatherArtifacts(o, res, manifest.KindHelmRelease, name, c)...)
}

func gatherArtifacts(o *orchestrator.Orchestrator, res *orchestrator.Result, kind, name string, c *commonFlags) []diff.Doc {
	var out []diff.Doc
	// Defensive re-drop of --skip-secrets / --skip-crds / --skip-kinds.
	// Orchestrator.Render already applies the same set to Result.Manifests
	// at the embed boundary (orchestrator.go:555), so this is a no-op for
	// CLI callers. It still pulls weight for SDK consumers who hand-build
	// a Result and pass it through the CLI helpers in tests / harnesses.
	// See #169.
	var skip []string
	if c != nil {
		skip = c.skipResourceKinds()
	}
	for id, docs := range res.Manifests {
		if id.Kind != kind {
			continue
		}
		if name != "" && id.Name != name {
			continue
		}
		if c != nil && !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		parent := diff.Parent{Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}
		if ks, ok := store.Get[*manifest.Kustomization](o.Store(), id); ok {
			parent.Path = strings.TrimPrefix(ks.Path, "./")
		}
		for _, m := range manifest.DropKinds(docs, skip) {
			out = append(out, diff.Doc{Manifest: m, Parent: parent})
		}
	}
	return out
}
