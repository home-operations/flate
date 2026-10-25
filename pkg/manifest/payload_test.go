package manifest_test

import (
	"reflect"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOverlayPayload_Secret(t *testing.T) {
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
			before := &manifest.Secret{Name: "credentials", Namespace: "apps", Data: manifest.DeepCopyMap(tc.data), StringData: manifest.DeepCopyMap(tc.stringData)}
			if tc.name == "values" {
				before.Data["bytes"] = []byte("owned")
			}
			doc := map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"metadata": map[string]any{"name": "credentials", "namespace": "apps", "labels": map[string]any{"selected": "true"}, "annotations": map[string]any{"note": "authored"}},
				"data":     map[string]any{"stale": "c3RhbGU="}, "stringData": map[string]any{"stale": "stale"}, "type": "Opaque",
				"immutable": true,
			}
			input := manifest.DeepCopyMap(doc)
			want := manifest.DeepCopyMap(doc)
			want["data"], want["stringData"] = tc.data, tc.stringData
			delete(want, "type")

			got := manifest.OverlayPayload(s, doc)
			assert.Diff(t, got, want)
			for _, key := range []string{"data", "stringData"} {
				_, present := got[key]
				assert.Equal(t, present, true)
			}
			_, present := got["type"]
			assert.Equal(t, present, false)
			assert.Diff(t, doc, input)
			assert.Diff(t, s, before)

			got["immutable"] = false
			got["top"] = "new"
			if tc.name == "values" {
				data := got["data"].(map[string]any)
				data["token"] = "changed"
				data["bytes"].([]byte)[0] = 'X'
				data["nested"].(map[string]any)["value"] = "changed"
				got["stringData"].(map[string]any)["password"] = "changed"
			}
			assert.Diff(t, doc, input)
			assert.Diff(t, s, before)
		})
	}
}

func TestOverlayPayload_OtherKinds(t *testing.T) {
	t.Parallel()
	for _, obj := range []manifest.BaseManifest{
		&manifest.Kustomization{}, &manifest.HelmRelease{}, &manifest.HelmRepository{},
		&manifest.HelmChartSource{}, &manifest.GitRepository{}, &manifest.OCIRepository{},
		&manifest.ExternalArtifact{}, &manifest.Bucket{}, &manifest.ResourceSet{},
		&manifest.ResourceSetInputProvider{}, &manifest.ConfigMap{}, &manifest.RawObject{Kind: "Secret"},
	} {
		t.Run(reflect.TypeOf(obj).Elem().Name(), func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"type": "authored", "data": map[string]any{"value": "unchanged"}}
			before := manifest.DeepCopyMap(doc)
			got := manifest.OverlayPayload(obj, doc)
			assert.Equal(t, reflect.ValueOf(got).Pointer(), reflect.ValueOf(doc).Pointer())
			assert.Diff(t, got, before)
		})
	}
}
