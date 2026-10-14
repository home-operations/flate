package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestEnsureMetadata_PreservesMap(t *testing.T) {
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
	for _, tc := range []struct {
		name string
		in   map[string]string
		want map[string]any
	}{
		{"empty", nil, map[string]any{"keep": "value", "overwrite": "old"}},
		{"merge", map[string]string{"overwrite": "new", "add": "value"}, map[string]any{"keep": "value", "overwrite": "new", "add": "value"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

func TestStampNamespace(t *testing.T) {
	const ns = "storage"
	cases := []struct {
		name     string
		doc      map[string]any
		ns, want string
	}{
		{"omitted namespace", map[string]any{"kind": "ServiceAccount", "metadata": map[string]any{"name": "x"}}, ns, ns},
		{"missing metadata", map[string]any{"kind": "Deployment"}, ns, ns},
		{"explicit namespace", map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"namespace": "kube-system"}}, ns, "kube-system"},
		{"ClusterRole", map[string]any{"kind": "ClusterRole", "metadata": map[string]any{"name": "x"}}, ns, ""},
		{"CRD", map[string]any{"kind": "CustomResourceDefinition"}, ns, ""},
		{"Namespace", map[string]any{"kind": "Namespace", "metadata": map[string]any{"name": "x"}}, ns, ""},
		{"empty namespace", map[string]any{"kind": "ServiceAccount", "metadata": map[string]any{"name": "x"}}, "", ""},
		{"RuntimeClass", map[string]any{"kind": "RuntimeClass"}, ns, ns},
		{"foreign ClusterRole", map[string]any{"apiVersion": "foreign.example/v1", "kind": "ClusterRole"}, ns, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			StampNamespace(tc.doc, tc.ns)
			_, got := DocMetadata(tc.doc)
			assert.Equal(t, got, tc.want)
		})
	}
}

func TestStampNamespaces_MixedSlice(t *testing.T) {
	docs := []map[string]any{
		{"kind": "ServiceAccount", "metadata": map[string]any{"name": "sa"}},
		{"kind": "ClusterRole", "metadata": map[string]any{"name": "cr"}},
		{"kind": "Deployment", "metadata": map[string]any{"name": "d", "namespace": "explicit"}},
	}
	StampNamespaces(docs, "storage")
	for i, want := range []string{"storage", "", "explicit"} {
		_, ns := DocMetadata(docs[i])
		assert.Equal(t, ns, want)
	}
}
