package imageutil

import (
	"maps"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCollect_SelectedBatches(t *testing.T) {
	t.Parallel()
	selected := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
	batches := []manifest.DocumentBatch{
		{Parent: selected, Documents: []map[string]any{{"spec": map[string]any{"template": map[string]any{"containers": []any{
			map[string]any{"image": "ghcr.io/demo/app:v1"}, map[string]any{"image": "ghcr.io/demo/app:v1"},
		}}}}}},
		{Documents: []map[string]any{{"data": map[string]any{"image": "ghcr.io/demo/sidecar:v2"}}}},
	}
	assert.Diff(t, slices.Sorted(maps.Keys(Collect(batches))), []string{"ghcr.io/demo/app:v1", "ghcr.io/demo/sidecar:v2"})
}
