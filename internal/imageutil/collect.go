// Package imageutil collects image references from prepared producer batches.
package imageutil

import (
	"github.com/home-operations/flate/pkg/image"
	"github.com/home-operations/flate/pkg/manifest"
)

// Collect extracts the image set from the supplied ordered producers.
func Collect(objects []manifest.BaseManifest, manifests map[manifest.NamedResource][]map[string]any) map[string]struct{} {
	set := map[string]struct{}{}
	for _, obj := range objects {
		for _, doc := range manifests[obj.Named()] {
			for _, img := range image.Extract(doc) {
				set[img] = struct{}{}
			}
		}
	}
	return set
}
