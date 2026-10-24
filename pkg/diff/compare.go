package diff

import (
	"errors"
	"slices"

	"github.com/home-operations/flate/internal/imageutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Snapshot contains selected document batches and their render availability.
// Manifests and status entries remain immutable during comparison.
type Snapshot struct {
	Manifests   []manifest.DocumentBatch
	Unavailable map[manifest.NamedResource]store.StatusInfo
}

// Compare compares two selected snapshots in orig/current order. Selected errors
// remain errors even when the snapshots produce a usable partial diff; discovered
// warnings and structured suppressions remain available alongside those errors.
func Compare(orig, current Snapshot, selectedErr error, opts Options) ([]byte, []manifest.Warning, []Suppression, error) {
	left, right, suppressed := suppress(orig, current)
	opts.Suppressed = append(slices.Clone(opts.Suppressed), suppressed...)
	out, warnings, err := RenderDocs(selectedDocs(left), selectedDocs(right), opts)
	return out, warnings, opts.Suppressed, errors.Join(err, selectedErr)
}

// CompareImages returns the sorted images added between selected orig/current
// snapshots, plus removals when requested. Unavailable producer batches are
// withheld before extraction and disclosed as structured suppressions with document counts.
func CompareImages(orig, current Snapshot, selectedErr error, includeRemoved bool) ([]string, []Suppression, error) {
	left, right, suppressed := suppress(orig, current)
	origImages := imageutil.Collect(left)
	currentImages := imageutil.Collect(right)
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
