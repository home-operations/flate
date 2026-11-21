package selection

import (
	"hash/maphash"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCollectEmissions_EmitterMembership(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	emitter := &manifest.HelmChartSource{Name: "driver", Namespace: "apps"}
	documents := []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "required", "namespace": "apps"}},
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "sibling", "namespace": "apps"}},
	}
	decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(&Admission{
		Needed:   []manifest.NamedResource{manifest.DocIdentity(documents[0])},
		Selected: []manifest.NamedResource{parent.Named()},
	}, true)
	snapshot := Snapshot{Decision: decision, Emissions: []EmissionFacts{{
		Emitter: emitter,
		Parent:  parent,
		Documents: []DocumentFact{
			{Document: documents[0], Identity: manifest.DocIdentity(documents[0])},
			{Document: documents[1], Identity: manifest.DocIdentity(documents[1])},
		},
	}}}
	snapshot.CollectEmissions(nil)
	assert.Equal(t, len(snapshot.Manifests), 1)
	assert.Equal(t, snapshot.Manifests[0].Parent, manifest.BaseManifest(parent))
	assert.Diff(t, snapshot.Manifests[0].Documents, documents[:1])
	assert.Equal(t, len(snapshot.Emissions[0].Documents), 2)

	for _, tc := range []struct {
		name       string
		overlap    bool
		scoped     bool
		wantWinner string
	}{
		{name: "generated representative", overlap: true, wantWinner: "sync"},
		{name: "excluded representative", overlap: true, scoped: true, wantWinner: "authored"},
		{name: "discovery-only declaration", scoped: true, wantWinner: "authored"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			authored := &manifest.Kustomization{Name: "authored", Namespace: "apps"}
			generated := &manifest.Kustomization{Name: "sync", Namespace: "apps"}
			st := store.New()
			st.AddObject(authored)
			identity := manifest.DocIdentity(documents[0])
			child := &manifest.ConfigMap{Name: identity.Name, Namespace: identity.Namespace}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				identity: {Declaration: child},
			}}
			fact := func(producer *manifest.Kustomization) DocumentFact {
				doc := map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{"name": identity.Name, "namespace": identity.Namespace},
					"data":     map[string]any{"value": producer.Name},
				}
				return DocumentFact{Document: doc, Identity: identity, Child: identity, GeneratedOverlap: tc.overlap}
			}
			authoredFact, generatedFact := fact(authored), fact(generated)
			emissions := []EmissionFacts{{Emitter: authored, Parent: authored, Documents: []DocumentFact{
				{Document: documents[1], Identity: manifest.DocIdentity(documents[1])},
				authoredFact,
			}}}
			if tc.overlap {
				st.AddObject(generated)
				emissions = append(emissions, EmissionFacts{
					Emitter: generated, Parent: generated, Documents: []DocumentFact{generatedFact},
				})
			}
			snapshot := Snapshot{Store: st, Index: index, Complete: true, Emissions: emissions}
			snapshot.CollectEmissions(nil)
			if tc.overlap {
				assert.Equal(t, len(snapshot.Manifests), 2)
				assert.Diff(t, snapshot.Manifests[0].Documents, documents[1:])
				assert.Diff(t, snapshot.Manifests[1].Documents, []map[string]any{generatedFact.Document})
			}
			root := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}
			if tc.scoped {
				root = authored.Named()
			}
			got, errs := Project([]Snapshot{snapshot}, nil, Request{Roots: []manifest.NamedResource{root}})
			assert.NoError(t, errs.Selected)
			assert.NoError(t, errs.Remaining)
			want := []manifest.DocumentBatch{{
				Parent: authored, Documents: []map[string]any{documents[1]},
				Identities: []manifest.NamedResource{manifest.DocIdentity(documents[1])},
			}}
			if tc.wantWinner == generated.Name {
				want = append(want, manifest.DocumentBatch{
					Parent: generated, Documents: []map[string]any{generatedFact.Document},
					Identities: []manifest.NamedResource{identity},
				})
			} else {
				want[0].Documents = append(want[0].Documents, authoredFact.Document)
				want[0].Identities = append(want[0].Identities, identity)
			}
			assert.FatalIf(t, len(got[0].Manifests) != len(want), "unexpected batch count: %v", got[0].Manifests)
			for i := range want {
				batch := &got[0].Manifests[i]
				assert.Equal(t, batch.Parent.Named(), want[i].Parent.Named())
				assert.Diff(t, batch.Documents, want[i].Documents)
				assert.Diff(t, batch.Identities, want[i].Identities)
			}
			assert.Equal(t, st.GetObject(identity), nil)
			assert.Equal(t, len(snapshot.Emissions[0].Documents), 2)
		})
	}
}
