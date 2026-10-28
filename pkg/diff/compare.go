package diff

import (
	"errors"
	"slices"

	"github.com/home-operations/flate/internal/diffprep"
	"github.com/home-operations/flate/internal/imageutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Snapshot contains selected document batches and their render availability.
// Manifests and status entries remain immutable during comparison.
type Snapshot struct {
	Subjects    map[manifest.NamedResource]manifest.DiagnosticIdentity
	Manifests   []manifest.DocumentBatch
	Unavailable map[manifest.NamedResource]store.StatusInfo
	// ProducerKeys maps actual producers to their paired comparison identities.
	ProducerKeys map[manifest.NamedResource]manifest.NamedResource
}

// Compare compares two selected snapshots in orig/current order. Selected errors
// remain errors even when the snapshots produce a usable partial diff; discovered
// warnings and structured suppressions remain available alongside those errors.
func Compare(orig, current Snapshot, selectedErr error, opts Options) ([]byte, []manifest.Warning, []Suppression, error) {
	left, right, suppressed := diffprep.Suppress(diffprep.Snapshot(orig), diffprep.Snapshot(current))
	opts.subjects = [2]map[manifest.NamedResource]manifest.DiagnosticIdentity{orig.Subjects, current.Subjects}
	opts.Suppressed = append(slices.Clone(opts.Suppressed), publicSuppressions(suppressed)...)
	prepared := diffprep.Prepare(diffprep.Documents(left), diffprep.Documents(right), prepareOptions(opts))
	out, err := renderPrepared(prepared, opts)
	warnings := prepared.Warnings
	return out, warnings, opts.Suppressed, errors.Join(err, selectedErr)
}

// CompareImages returns the sorted images added between selected orig/current
// snapshots, plus removals when requested. Unavailable producer batches are
// withheld before extraction and disclosed as structured suppressions with document counts.
func CompareImages(orig, current Snapshot, selectedErr error, includeRemoved bool) ([]string, []Suppression, error) {
	left, right, suppressed := diffprep.Suppress(diffprep.Snapshot(orig), diffprep.Snapshot(current))
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
	return out, publicSuppressions(suppressed), selectedErr
}

func publicSuppressions(notes []diffprep.Suppression) []Suppression {
	out := slices.Grow([]Suppression(nil), len(notes))
	for _, n := range notes {
		out = append(out, Suppression{Parent: Parent(n.Parent), Subject: n.Subject, Side: Side(n.Side), Count: n.Count, Reason: n.Reason})
	}
	return out
}
