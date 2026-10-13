package diff

import (
	"errors"
	"slices"

	"github.com/home-operations/flate/pkg/image"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Snapshot contains selected producer batches and their render availability.
// Objects, manifests, and status entries remain immutable during comparison.
type Snapshot struct {
	Objects     []manifest.BaseManifest
	Manifests   map[manifest.NamedResource][]map[string]any
	Unavailable map[manifest.NamedResource]store.StatusInfo
}

// Compare compares two selected snapshots in orig/current order. Selected errors
// remain errors even when the snapshots produce a usable partial diff.
func Compare(orig, current Snapshot, selectedErr error, opts Options) ([]byte, error) {
	left, right, suppressed := suppress(orig, current)
	opts.Suppressed = append(slices.Clone(opts.Suppressed), suppressed...)
	out, err := RenderDocs(selectedDocs(left, orig.Manifests), selectedDocs(right, current.Manifests), opts)
	return out, errors.Join(err, selectedErr)
}

// CompareImages returns the sorted images added between selected orig/current
// snapshots, plus removals when requested. Unavailable producer batches are
// withheld before extraction and disclosed as document counts.
func CompareImages(orig, current Snapshot, selectedErr error, includeRemoved bool) ([]string, []Suppression, error) {
	left, right, suppressed := suppress(orig, current)
	origImages := selectedImages(left, orig.Manifests)
	currentImages := selectedImages(right, current.Manifests)
	out := make([]string, 0, len(currentImages))
	for img := range currentImages {
		if _, ok := origImages[img]; !ok {
			out = append(out, img)
		}
	}
	if includeRemoved {
		for img := range origImages {
			if _, ok := currentImages[img]; !ok {
				out = append(out, img)
			}
		}
	}
	slices.Sort(out)
	return out, suppressed, selectedErr
}

func selectedImages(objects []manifest.BaseManifest, manifests map[manifest.NamedResource][]map[string]any) map[string]struct{} {
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
