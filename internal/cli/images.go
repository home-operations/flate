package cli

import (
	"io"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/image"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

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
			imgs := image.Extract(doc)
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
