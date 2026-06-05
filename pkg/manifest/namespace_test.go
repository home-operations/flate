package manifest

import "testing"

func TestStampNamespace(t *testing.T) {
	const ns = "storage"
	cases := []struct {
		name string
		doc  map[string]any
		want string // expected metadata.namespace after stamping ("" = absent)
	}{
		{
			name: "namespaced kind omitting namespace gets stamped",
			doc:  map[string]any{"kind": "ServiceAccount", "metadata": map[string]any{"name": "x"}},
			want: ns,
		},
		{
			name: "namespaced kind with no metadata block gets stamped",
			doc:  map[string]any{"kind": "Deployment"},
			want: ns,
		},
		{
			name: "explicit namespace preserved (even when it differs)",
			doc:  map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"namespace": "kube-system"}},
			want: "kube-system",
		},
		{
			name: "cluster-scoped ClusterRole never stamped",
			doc:  map[string]any{"kind": "ClusterRole", "metadata": map[string]any{"name": "x"}},
			want: "",
		},
		{
			name: "cluster-scoped CRD never stamped",
			doc:  map[string]any{"kind": "CustomResourceDefinition"},
			want: "",
		},
		{
			name: "cluster-scoped Namespace never stamped",
			doc:  map[string]any{"kind": "Namespace", "metadata": map[string]any{"name": "x"}},
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			StampNamespace(tc.doc, ns)
			if _, got := DocMetadata(tc.doc); got != tc.want {
				t.Errorf("namespace = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStampNamespace_EmptyNamespaceIsNoop(t *testing.T) {
	doc := map[string]any{"kind": "ServiceAccount", "metadata": map[string]any{"name": "x"}}
	StampNamespace(doc, "")
	if _, ns := DocMetadata(doc); ns != "" {
		t.Errorf("empty release namespace must be a no-op; got %q", ns)
	}
}

func TestStampNamespaces_MixedSlice(t *testing.T) {
	docs := []map[string]any{
		{"kind": "ServiceAccount", "metadata": map[string]any{"name": "sa"}},
		{"kind": "ClusterRole", "metadata": map[string]any{"name": "cr"}},
		{"kind": "Deployment", "metadata": map[string]any{"name": "d", "namespace": "explicit"}},
	}
	StampNamespaces(docs, "storage")
	if _, ns := DocMetadata(docs[0]); ns != "storage" {
		t.Errorf("namespaced SA should be stamped storage; got %q", ns)
	}
	if _, ns := DocMetadata(docs[1]); ns != "" {
		t.Errorf("cluster-scoped ClusterRole must stay namespace-less; got %q", ns)
	}
	if _, ns := DocMetadata(docs[2]); ns != "explicit" {
		t.Errorf("explicit namespace must be preserved; got %q", ns)
	}
}

func TestIsClusterScoped(t *testing.T) {
	for kind, want := range map[string]bool{
		"ClusterRole":              true,
		"CustomResourceDefinition": true,
		"ClusterSecretStore":       true, // ecosystem addition
		"ClusterPolicy":            true, // ecosystem addition
		"Deployment":               false,
		"ServiceAccount":           false,
		"ConfigMap":                false,
	} {
		if got := IsClusterScoped(map[string]any{"kind": kind}); got != want {
			t.Errorf("IsClusterScoped(%s) = %v, want %v", kind, got, want)
		}
	}
}
