package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestEnsureMetadata_PreservesMap(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		metadata any
	}{
		{"missing", nil},
		{"non-map", "invalid"},
		{"nil map", map[string]any(nil)},
		{"existing", map[string]any{"name": "app"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"metadata": tc.metadata}
			md := EnsureMetadata(doc)
			md["namespace"] = "apps"
			assert.Equal(t, doc["metadata"].(map[string]any)["namespace"], any("apps"))
			if existing, ok := tc.metadata.(map[string]any); ok && existing != nil {
				assert.Equal(t, md["name"], any("app"))
				assert.Equal(t, existing["namespace"], any("apps"))
			}
		})
	}
}

func TestMergeStringMap_PreservesEntries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		in   map[string]string
		want map[string]any
	}{
		{"empty", nil, map[string]any{"keep": "value", "overwrite": "old"}},
		{"merge", map[string]string{"overwrite": "new", "add": "value"}, map[string]any{"keep": "value", "overwrite": "new", "add": "value"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			labels := map[string]any{"keep": "value", "overwrite": "old"}
			md := map[string]any{"labels": labels, "name": "app"}
			MergeStringMap(md, "labels", tc.in)
			assert.Diff(t, labels, tc.want)
			assert.Equal(t, md["name"], any("app"))
		})
	}
	md := map[string]any{}
	MergeStringMap(md, "labels", nil)
	assert.Equal(t, len(md), 0)
	MergeStringMap(md, "labels", map[string]string{"app": "demo"})
	assert.Diff(t, md["labels"], map[string]any{"app": "demo"})
}
