package manifest_test

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSecret_SnapshotOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		data, stringData map[string]any
	}{
		{"values", map[string]any{"token": "c2FuaXRpemVk", "bytes": []byte("owned"), "nested": map[string]any{"value": "kept"}}, map[string]any{"password": "sanitized"}},
		{"nil", nil, nil},
		{"empty", map[string]any{}, map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &manifest.Secret{Name: "credentials", Namespace: "apps", Data: tc.data, StringData: tc.stringData}
			before := &manifest.Secret{
				Name:       "credentials",
				Namespace:  "apps",
				Data:       manifest.DeepCopyMap(tc.data),
				StringData: manifest.DeepCopyMap(tc.stringData),
			}
			if tc.name == "values" {
				before.Data["bytes"] = []byte("owned")
			}
			doc := s.Snapshot()
			input := manifest.DeepCopyMap(doc)
			want := map[string]any{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata":   map[string]any{"name": "credentials", "namespace": "apps"},
				"data":       tc.data,
				"stringData": tc.stringData,
			}
			got := s.Snapshot()
			assert.Diff(t, got, want)
			for _, key := range []string{"data", "stringData"} {
				_, present := got[key]
				assert.Equal(t, present, true)
			}
			_, present := got["type"]
			assert.Equal(t, present, false)
			assert.Diff(t, doc, input)
			assert.Equal(t, manifest.Equal(s, before), true)

			got["immutable"] = false
			got["top"] = "new"
			got["metadata"].(map[string]any)["name"] = "changed"
			if tc.name == "values" {
				data := got["data"].(map[string]any)
				data["token"] = "changed"
				data["bytes"].([]byte)[0] = 'X'
				data["nested"].(map[string]any)["value"] = "changed"
				got["stringData"].(map[string]any)["password"] = "changed"
			}
			assert.Diff(t, doc, input)
			assert.Equal(t, manifest.Equal(s, before), true)
		})
	}
}
