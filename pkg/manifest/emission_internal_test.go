package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestEmissionPolicy_EmittedIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		parent    BaseManifest
		kind      string
		namespace string
		want      NamedResource
	}{
		{
			name: "internal namespace", parent: &ResourceSet{Namespace: "apps"}, kind: KindConfigMap,
			want: NamedResource{Kind: KindConfigMap, Namespace: "apps", Name: "config"},
		},
		{
			name: "explicit namespace", parent: &ResourceSet{Namespace: "apps"}, kind: KindConfigMap, namespace: "other",
			want: NamedResource{Kind: KindConfigMap, Namespace: "other", Name: "config"},
		},
		{
			name: "cluster scoped", parent: &ResourceSet{Namespace: "apps"}, kind: "Namespace",
			want: NamedResource{Kind: "Namespace", Name: "config"},
		},
		{
			name: "target namespace", parent: &Kustomization{TargetNamespace: "target"}, kind: KindConfigMap,
			want: NamedResource{Kind: KindConfigMap, Namespace: "target", Name: "config"},
		},
		{
			name: "raw namespace", parent: &RawObject{}, kind: KindConfigMap,
			want: NamedResource{Kind: KindConfigMap, Name: "config"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metadata := map[string]any{"name": "config"}
			if tc.namespace != "" {
				metadata["namespace"] = tc.namespace
			}
			doc := map[string]any{"apiVersion": "v1", "kind": tc.kind, "metadata": metadata}
			assert.Diff(t, tc.parent.EmissionPolicy().EmittedIdentity(doc), tc.want)
			assert.Equal(t, DocMetadata(doc).Namespace, tc.namespace)
		})
	}
}
