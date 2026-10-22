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
	other := &manifest.HelmRelease{Name: "other", Namespace: "apps"}
	batches := map[manifest.NamedResource][]map[string]any{
		selected.Named(): {{"spec": map[string]any{"template": map[string]any{"containers": []any{
			map[string]any{"image": "ghcr.io/demo/app:v1"}, map[string]any{"image": "ghcr.io/demo/app:v1"},
		}}}}, {"data": map[string]any{"image": "ghcr.io/demo/sidecar:v2"}}},
		other.Named(): {{"image": "ghcr.io/demo/unselected:v3"}},
	}
	assert.Diff(t, slices.Sorted(maps.Keys(Collect([]manifest.BaseManifest{selected}, batches))), []string{"ghcr.io/demo/app:v1", "ghcr.io/demo/sidecar:v2"})
}
