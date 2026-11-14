package imageutil

import (
	"maps"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCollect_SelectedBatches(t *testing.T) {
	t.Parallel()
	selected := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
	batches := []manifest.DocumentBatch{
		testutil.DocumentBatch(
			selected,
			[]map[string]any{{"spec": map[string]any{"template": map[string]any{"containers": []any{
				map[string]any{"image": "ghcr.io/demo/app:v1"}, map[string]any{"image": "ghcr.io/demo/app:v1"},
			}}}}},
		),
		testutil.DocumentBatch(nil, []map[string]any{{
			"spec": map[string]any{"initContainers": []any{
				map[string]any{"image": "ghcr.io/demo/sidecar:v2"}, map[string]any{"image": "ghcr.io/demo/app:v1"},
			}},
			"data": map[string]any{"image": "ghcr.io/demo/config:v1", "record": "apiserver_request:burnrate1d"},
		}}),
	}
	assert.Diff(
		t,
		slices.Sorted(maps.Keys(Collect(batches))),
		[]string{"ghcr.io/demo/app:v1", "ghcr.io/demo/config:v1", "ghcr.io/demo/sidecar:v2"},
	)
}
