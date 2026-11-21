package selection

import (
	"hash/maphash"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestProject_ScopedEmitterOutputOwnership(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{})
	emitter := &manifest.ResourceSet{Name: "emitter", Namespace: "apps"}
	rival := &manifest.ResourceSet{Name: "winner", Namespace: "apps"}
	workload := &manifest.Kustomization{Name: "workload", Namespace: "apps"}
	sibling := &manifest.Kustomization{Name: "sibling", Namespace: "other"}
	shared := &manifest.ConfigMap{Name: "shared", Namespace: "apps"}
	for _, object := range []manifest.BaseManifest{parent, emitter, rival} {
		st.AddObject(object)
	}
	facts := make([]DocumentFact, 0, 3)
	children := make([]store.Child, 0, 3)
	for _, obj := range []manifest.BaseManifest{workload, sibling, shared} {
		doc := testutil.ObjectDocument(obj)
		children = append(children, store.Child{Obj: obj, Doc: doc})
		facts = append(facts, DocumentFact{Document: doc, Identity: obj.Named(), Child: obj.Named()})
	}
	st.RecordEmitted(emitter.Named(), &store.Emission{Children: children, Complete: true}, nil)
	winner := &manifest.ConfigMap{Name: shared.Name, Namespace: shared.Namespace}
	winner.Data = map[string]any{"value": "winner"}
	st.RecordEmitted(rival.Named(), &store.Emission{Children: []store.Child{{Obj: winner}}, Complete: true}, nil)
	assert.Equal(t, st.EmissionOwner(shared.Named()), rival.Named())

	output := &manifest.ConfigMap{Name: "rendered", Namespace: "apps"}
	doc := testutil.ObjectDocument(output)
	decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(&Admission{
		Selected: []manifest.NamedResource{emitter.Named(), workload.Named(), sibling.Named()},
		Needed:   []manifest.NamedResource{shared.Named()},
	}, true)
	snapshot := Snapshot{
		Store: st, Decision: decision, Complete: true,
		Emissions: []EmissionFacts{
			{Emitter: emitter, Parent: parent, ParentAttributed: true, ParentRecorded: true, Documents: facts},
			{Emitter: workload, Parent: workload, Documents: []DocumentFact{{Document: doc, Identity: output.Named()}}},
		},
	}
	for _, name := range []string{"", workload.Name} {
		root := testutil.KustomizationID("apps", name)
		results, failures := Project([]Snapshot{snapshot}, nil, Request{Roots: []manifest.NamedResource{root}})
		assert.NoError(t, failures.Selected)
		assert.NoError(t, failures.Remaining)
		want := map[manifest.NamedResource][]map[string]any{workload.Named(): {doc}}
		if name == "" {
			want[parent.Named()] = []map[string]any{facts[0].Document}
		}
		got := make(map[manifest.NamedResource][]map[string]any)
		for _, batch := range results[0].Manifests {
			got[batch.Parent.Named()] = batch.Documents
		}
		assert.Diff(t, got, want)
	}
}

func TestCollectEmissions_IdentityAlignment(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{Name: "report", Namespace: "reporting"}
	first := &manifest.ResourceSet{Name: "first", Namespace: "apps"}
	second := &manifest.ResourceSet{Name: "second", Namespace: "other"}
	documents := []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "z"}},
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}},
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "z"}},
	}
	identities := []manifest.NamedResource{
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "z"},
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "a"},
		{Kind: manifest.KindConfigMap, Namespace: "other", Name: "z"},
	}
	st := store.New()
	st.AddObject(parent)
	st.AddObject(first)
	st.AddObject(second)
	children := make([]store.Child, len(identities))
	for i, id := range identities {
		children[i] = store.Child{Obj: &manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace}, Doc: documents[i]}
	}
	st.RecordEmitted(first.Named(), &store.Emission{Children: children[:2], Complete: true}, nil)
	st.RecordEmitted(second.Named(), &store.Emission{Children: children[2:], Complete: true}, nil)
	for _, tc := range []struct {
		name        string
		selected    []manifest.NamedResource
		needed      []manifest.NamedResource
		counterpart []manifest.NamedResource
		want        []int
	}{
		{name: "unrestricted", want: []int{0, 1, 2}},
		{name: "selected emitter", selected: []manifest.NamedResource{first.Named()}, want: []int{0, 1}},
		{name: "required document", needed: identities[2:], want: []int{2}},
		{
			name: "counterpart selects reconciled emitter", needed: []manifest.NamedResource{first.Named()},
			counterpart: []manifest.NamedResource{first.Named()}, want: []int{0, 1},
		},
		{
			name: "counterpart selects reporting parent", needed: []manifest.NamedResource{first.Named(), parent.Named()},
			counterpart: []manifest.NamedResource{parent.Named()}, want: []int{0, 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var decision *Decision
			if tc.name != "unrestricted" {
				decision, _ = (&Decision{seed: maphash.MakeSeed()}).include(&Admission{
					Selected: tc.selected, Needed: tc.needed,
				}, true)
			}
			snapshot := Snapshot{Store: st, Decision: decision, Emissions: []EmissionFacts{
				{
					Emitter: first, Parent: parent, ParentAttributed: true, ParentRecorded: true,
					Documents: []DocumentFact{
						{Document: documents[0], Identity: identities[0], Child: identities[0]},
						{Document: documents[1], Identity: identities[1], Child: identities[1]},
					},
				},
				{
					Emitter: second, Parent: parent, ParentAttributed: true, ParentRecorded: true,
					Documents: []DocumentFact{
						{Document: documents[2], Identity: identities[2], Child: identities[2]},
					},
				},
			}}
			var other *Snapshot
			if len(tc.counterpart) != 0 {
				decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(&Admission{Selected: tc.counterpart}, true)
				other = &Snapshot{Decision: decision}
			}
			snapshot.CollectEmissions(other)
			assert.FatalIf(t, len(snapshot.Manifests) != 1, "projected batches: %v", snapshot.Manifests)
			batch := snapshot.Manifests[0]
			assert.Equal(t, batch.Parent, manifest.BaseManifest(parent))
			assert.Equal(t, len(batch.Documents), len(tc.want))
			assert.Equal(t, len(batch.Identities), len(tc.want))
			for i, position := range tc.want {
				assert.Diff(t, batch.Documents[i], documents[position])
				assert.Diff(t, batch.Identities[i], identities[position])
			}
			assert.Equal(t, len(snapshot.Emissions[0].Documents), 2)
			assert.Equal(t, len(snapshot.Emissions[1].Documents), 1)
		})
	}
}
