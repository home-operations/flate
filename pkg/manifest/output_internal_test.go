package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestDocumentBatch_IdentityAccessor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		parent     BaseManifest
		kind       string
		namespace  string
		documents  int
		identities []NamedResource
		want       NamedResource
		invalid    bool
	}{
		{name: "nil identities", kind: KindConfigMap, documents: 1, want: NamedResource{Kind: KindConfigMap, Name: "config"}},
		{name: "empty identities", kind: KindConfigMap, documents: 1, identities: []NamedResource{}, want: NamedResource{Kind: KindConfigMap, Name: "config"}},
		{name: "parent namespace", parent: &ResourceSet{Namespace: "apps"}, kind: KindConfigMap, documents: 1, want: NamedResource{Kind: KindConfigMap, Namespace: "apps", Name: "config"}},
		{name: "target namespace", parent: &Kustomization{TargetNamespace: "target"}, kind: KindConfigMap, documents: 1, want: NamedResource{Kind: KindConfigMap, Namespace: "target", Name: "config"}},
		{name: "explicit namespace", parent: &ResourceSet{Namespace: "apps"}, kind: KindConfigMap, namespace: "other", documents: 1, want: NamedResource{Kind: KindConfigMap, Namespace: "other", Name: "config"}},
		{name: "cluster scoped", parent: &ResourceSet{Namespace: "apps"}, kind: "Namespace", documents: 1, want: NamedResource{Kind: "Namespace", Name: "config"}},
		{name: "supplied", parent: &ResourceSet{Namespace: "report"}, kind: KindConfigMap, documents: 1, identities: []NamedResource{{Kind: KindSecret, Namespace: "emitter", Name: "actual"}}, want: NamedResource{Kind: KindSecret, Namespace: "emitter", Name: "actual"}},
		{name: "supplied zero", kind: KindConfigMap, documents: 1, identities: []NamedResource{{}}},
		{name: "short", documents: 2, identities: []NamedResource{{}}, invalid: true},
		{name: "long", documents: 1, identities: []NamedResource{{}, {}}, invalid: true},
		{name: "identities without documents", identities: []NamedResource{{}}, invalid: true},
		{name: "empty batch"},
		{name: "empty slices", identities: []NamedResource{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			batch := DocumentBatch{Parent: tc.parent, Identities: tc.identities}
			for range tc.documents {
				metadata := map[string]any{"name": "config"}
				if tc.namespace != "" {
					metadata["namespace"] = tc.namespace
				}
				batch.Documents = append(
					batch.Documents,
					map[string]any{"apiVersion": "v1", "kind": tc.kind, "metadata": metadata},
				)
			}
			before, err := json.Marshal(batch)
			assert.NoError(t, err)
			identity, err := batch.IdentityAccessor()
			if tc.invalid {
				assert.FatalIf(t, err == nil, "malformed batch accepted")
				assert.Equal(t, identity == nil, true)
				assert.Equal(t, errors.Is(err, ErrInput), true)
				assert.Equal(t, errors.Is(err, ErrFlux), true)
				assert.Equal(
					t,
					err.Error(),
					fmt.Sprintf("identity count %d does not match document count %d", len(tc.identities), tc.documents),
				)
			} else {
				assert.NoError(t, err)
				assert.FatalIf(t, identity == nil, "missing accessor")
				for i := range batch.Documents {
					assert.Equal(t, identity(i), tc.want)
				}
			}
			after, err := json.Marshal(batch)
			assert.NoError(t, err)
			assert.Diff(t, after, before)
		})
	}
}
