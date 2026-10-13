package cli

import (
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
