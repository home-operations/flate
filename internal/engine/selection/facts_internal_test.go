package selection

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOccurrenceFacts_IndexedReferenceViews(t *testing.T) {
	t.Parallel()
	relation := manifest.Reference{
		Kind: manifest.KindConfigMap, Name: "values",
		Use: "substituteFrom", NamespaceDefault: true,
	}
	gate := manifest.Reference{
		NamedResource: manifest.BootstrapSourceID,
		Gate:          manifest.GateArtifacts,
	}
	occurrence := OccurrenceFacts{
		Identity:     manifest.NamedResource{Namespace: "apps"},
		Refs:         []manifest.Reference{relation},
		ImplicitGate: &gate,
	}
	projected := relation
	projected.Namespace = "apps"
	for _, tc := range []struct {
		name string
		view manifest.ReferenceView
		want []manifest.Reference
	}{
		{name: "relations", view: manifest.ReferenceRelations, want: []manifest.Reference{projected}},
		{name: "all", view: manifest.ReferenceAll, want: []manifest.Reference{gate, projected}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var refs []manifest.Reference
			occurrence.VisitReferences(tc.view, func(ref manifest.Reference) bool {
				refs = append(refs, ref)
				return true
			})
			assert.FatalIf(t, len(refs) != len(tc.want), "got %d references, want %d", len(refs), len(tc.want))
			for i := range refs {
				assert.Equal(t, refs[i].NamedResource, tc.want[i].NamedResource)
				assert.Equal(t, refs[i].Use, tc.want[i].Use)
				assert.Equal(t, refs[i].Gate, tc.want[i].Gate)
			}
		})
	}
	assert.Equal(t, occurrence.Refs[0].Namespace, "")
}
