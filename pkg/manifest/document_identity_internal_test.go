package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestDocIdentity_ReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  map[string]any
		want NamedResource
	}{
		{name: "nil"},
		{name: "empty", doc: map[string]any{}},
		{name: "wrong fields", doc: map[string]any{"apiVersion": 1, "kind": false, "metadata": "wrong"}},
		{name: "null metadata", doc: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": nil}, want: NamedResource{Kind: "ConfigMap"}},
		{name: "wrong metadata fields", doc: map[string]any{"apiVersion": "example.test/v1", "kind": "Widget", "metadata": map[string]any{"name": 1, "namespace": false}}, want: NamedResource{Group: "example.test", Kind: "Widget"}},
		{name: "core", doc: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "config", "namespace": "apps"}}, want: NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "config"}},
		{name: "group", doc: map[string]any{"apiVersion": "example.test/v1", "kind": "Widget", "metadata": map[string]any{"name": "widget", "namespace": "apps"}}, want: NamedResource{Group: "example.test", Kind: "Widget", Namespace: "apps", Name: "widget"}},
		{name: "version", doc: map[string]any{"apiVersion": "example.test/v2", "kind": "Widget", "metadata": map[string]any{"name": "widget", "namespace": "apps"}}, want: NamedResource{Group: "example.test", Kind: "Widget", Namespace: "apps", Name: "widget"}},
		{name: "permissive group", doc: map[string]any{"apiVersion": "example.test/v1/extra", "kind": "Widget", "metadata": map[string]any{"name": "widget"}}, want: NamedResource{Group: "example.test", Kind: "Widget", Name: "widget"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := DeepCopyMap(tc.doc)
			for range 2 {
				assert.Equal(t, DocIdentity(tc.doc), tc.want)
			}
			assert.Diff(t, tc.doc, before)
			assert.Equal(t, testing.AllocsPerRun(100, func() { DocIdentity(tc.doc) }), float64(0))
		})
	}
}
