package cli

import (
	"cmp"
	"context"
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/image"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
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
	add := func(docs []map[string]any) {
		for _, doc := range docs {
			imgs, _ := image.Extract(doc)
			for _, img := range imgs {
				set[img] = struct{}{}
			}
		}
	}
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, obj := range o.Store().ListObjects(kind) {
			id := obj.Named()
			if !c.includeNamespace(o.Filter(), id.Namespace) {
				continue
			}
			switch art := o.Store().GetArtifact(id).(type) {
			case *store.KustomizationArtifact:
				add(art.Manifests)
			case *store.HelmReleaseArtifact:
				add(art.Manifests)
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
// change set and only render resources that differ between paths.
func runDiffOrchestrators(ctx context.Context, c *commonFlags, h *helmFlags) (orig, current *orchestrator.Orchestrator, err error) {
	if c.pathOrig == "" {
		return nil, nil, errors.New("diff requires --path-orig")
	}
	currentCfg := buildOrchCfg(*c, *h)
	if current, err = runOrchestratorCfg(ctx, currentCfg); err != nil && current == nil {
		return nil, nil, err
	}
	origCfg := currentCfg
	origCfg.Path, origCfg.PathOrig = c.pathOrig, c.path
	if orig, err = runOrchestratorCfg(ctx, origCfg); err != nil && orig == nil {
		return nil, nil, err
	}
	return orig, current, nil
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
		switch a := o.Store().GetArtifact(id).(type) {
		case *store.KustomizationArtifact:
			for _, m := range a.Manifests {
				out = append(out, diff.Doc{Manifest: m, Parent: parent})
			}
		case *store.HelmReleaseArtifact:
			for _, m := range a.Manifests {
				out = append(out, diff.Doc{Manifest: m, Parent: parent})
			}
		}
	}
	return out
}
