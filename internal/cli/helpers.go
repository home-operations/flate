package cli

import (
	"cmp"
	"context"
	"errors"
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
		if c := cmp.Compare(a["namespace"], b["namespace"]); c != 0 {
			return c
		}
		return cmp.Compare(a["name"], b["name"])
	})
}

// collectImages returns the union of images extracted from every
// rendered Kustomization and HelmRelease artifact. Namespace scope on
// c is honored.
func collectImages(o *orchestrator.Orchestrator, c *commonFlags) map[string]struct{} {
	set := map[string]struct{}{}
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, obj := range o.Store().ListObjects(kind) {
			id := obj.Named()
			if !c.includeNamespace(o.Filter(), id.Namespace) {
				continue
			}
			art, ok := o.Store().GetArtifact(id).(store.RenderedArtifact)
			if !ok {
				continue
			}
			for _, doc := range art.RenderedManifests() {
				imgs, _ := image.Extract(doc)
				for _, img := range imgs {
					set[img] = struct{}{}
				}
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
func runDiffOrchestrators(ctx context.Context, c *commonFlags, h *helmFlags) (*orchestrator.Orchestrator, *orchestrator.Orchestrator, error) {
	if c.pathOrig == "" {
		return nil, nil, errors.New("diff requires --path-orig")
	}
	currentCfg := buildOrchCfg(*c, *h)
	origCfg := currentCfg
	origCfg.Path, origCfg.PathOrig = c.pathOrig, c.path

	// One source.Cache shared across both orchestrators. They write into
	// the same on-disk cache root; without a shared *Cache each side has
	// its own mutex, and concurrent first-time clones of the same
	// (url, ref) slot can race past the mkdir/Readdirnames check and
	// step on each other.
	cacheRoot := currentCfg.CacheDir
	if cacheRoot == "" {
		cacheRoot = filepath.Join(os.TempDir(), "flate-cache")
	}
	shared := source.NewCache(filepath.Join(cacheRoot, "sources"))
	currentCfg.SourceCache = shared
	origCfg.SourceCache = shared

	var (
		orig, current     *orchestrator.Orchestrator
		origErr, currErr  error
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		o, err := runOrchestratorCfg(gctx, currentCfg)
		current = o
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
		o, err := runOrchestratorCfg(gctx, origCfg)
		orig = o
		origErr = err
		if o == nil {
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, nil, err
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
// stored Kustomization or HelmRelease artifacts of the given kind,
// tagged with the parent that produced them. name optionally filters
// to a single resource. When c is non-nil the namespace scope from
// commonFlags + the orchestrator's change filter is honored.
func gatherArtifacts(o *orchestrator.Orchestrator, kind, name string, c *commonFlags) []diff.Doc {
	var out []diff.Doc
	for _, obj := range o.Store().ListObjects(kind) {
		id := obj.Named()
		if name != "" && id.Name != name {
			continue
		}
		if c != nil && !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		parent := diff.Parent{Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}
		if ks, ok := obj.(*manifest.Kustomization); ok {
			parent.Path = strings.TrimPrefix(ks.Path, "./")
		}
		if a, ok := o.Store().GetArtifact(id).(store.RenderedArtifact); ok {
			for _, m := range a.RenderedManifests() {
				out = append(out, diff.Doc{Manifest: m, Parent: parent})
			}
		}
	}
	return out
}
