// Package imageutil collects image references from prepared document batches.
package imageutil

import (
	"github.com/home-operations/flate/pkg/image"
	"github.com/home-operations/flate/pkg/manifest"
)

// Collect extracts the image set from the supplied ordered document batches.
func Collect(manifests []manifest.DocumentBatch) map[string]struct{} {
	set := map[string]struct{}{}
	for _, batch := range manifests {
		for _, doc := range batch.Documents {
			for _, img := range image.Extract(doc) {
				set[img] = struct{}{}
			}
		}
	}
	return set
}
