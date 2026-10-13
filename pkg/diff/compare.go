package diff

import (
	"errors"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/image"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

// Compare compares two selected snapshots in orig/current order. Selected errors
// remain errors even when the snapshots produce a usable partial diff.
func Compare(selected []*orchestrator.Result, selectedErr error, opts Options) ([]byte, error) {
	if err := validateSelected(selected, selectedErr); err != nil {
		return nil, err
	}
	left, right, suppressed := suppressSelected(selected)
	opts.Suppressed = append(slices.Clone(opts.Suppressed), suppressed...)
	out, err := RenderDocs(selectedDocs(left, selected[0].Manifests), selectedDocs(right, selected[1].Manifests), opts)
	return out, errors.Join(err, selectedErr)
}

// CompareImages returns the sorted images added between selected orig/current
// snapshots, plus removals when requested. Unavailable producer batches are
// withheld before extraction and disclosed as document counts.
func CompareImages(selected []*orchestrator.Result, selectedErr error, includeRemoved bool) ([]string, []Suppression, error) {
	if err := validateSelected(selected, selectedErr); err != nil {
		return nil, nil, err
	}
	left, right, suppressed := suppressSelected(selected)
	orig := selectedImages(left, selected[0].Manifests)
	current := selectedImages(right, selected[1].Manifests)
	out := make([]string, 0, len(current))
	for img := range current {
		if _, ok := orig[img]; !ok {
			out = append(out, img)
		}
	}
	if includeRemoved {
		for img := range orig {
			if _, ok := current[img]; !ok {
				out = append(out, img)
			}
		}
	}
	slices.Sort(out)
	return out, suppressed, selectedErr
}

func validateSelected(selected []*orchestrator.Result, selectedErr error) error {
	if len(selected) == 0 && selectedErr != nil {
		return selectedErr
	}
	if len(selected) != 2 {
		return errors.Join(fmt.Errorf("%w: comparison requires two selected results", manifest.ErrInput), selectedErr)
	}
	for _, result := range selected {
		if result == nil || result.Counts == nil {
			return errors.Join(fmt.Errorf("%w: comparison requires selected results", manifest.ErrInput), selectedErr)
		}
	}
	return nil
}

func suppressSelected(selected []*orchestrator.Result) ([]manifest.BaseManifest, []manifest.BaseManifest, []Suppression) {
	plans := selectedSuppressionPlans(selected[0].Unavailable, selected[1].Unavailable)
	return suppress(selected[0].Objects, selected[1].Objects, plans,
		func(obj manifest.BaseManifest, side Side) (Parent, int) {
			index := 0
			if side == SideCurrent {
				index = 1
			}
			return objectParent(obj), len(selected[index].Manifests[obj.Named()])
		})
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
