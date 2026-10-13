package cli

import (
	"fmt"
	"io"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/image"
	"github.com/home-operations/flate/pkg/orchestrator"
)

func collectImages(res *orchestrator.Result) map[string]struct{} {
	set := map[string]struct{}{}
	for _, docs := range res.Manifests {
		for _, doc := range docs {
			for _, img := range image.Extract(doc) {
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
		if _, err := fmt.Fprintln(w, img); err != nil {
			return err
		}
	}
	return nil
}
