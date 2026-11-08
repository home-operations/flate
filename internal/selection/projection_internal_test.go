package selection

import (
	"hash/maphash"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestMatchesLabels_Matches(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Labels: map[string]string{"env": "prod"},
	}
	cases := []struct {
		name   string
		obj    manifest.BaseManifest
		labels map[string]string
		want   bool
	}{
		{name: "empty matches all", obj: ks, want: true},
		{name: "empty map matches all", obj: ks, labels: map[string]string{}, want: true},
		{name: "label match", obj: ks, labels: map[string]string{"env": "prod"}, want: true},
		{name: "label mismatch", obj: ks, labels: map[string]string{"env": "dev"}},
		{name: "nil"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			{
				got := matchesLabels(tc.obj, tc.labels)
				assert.ErrorIf(t, got != tc.want, "got %v want %v", got, tc.want)
			}
		})
	}
}

func TestSelect_EntryBatchSubjects(t *testing.T) {
	t.Parallel()
	run := Snapshot{Store: store.New()}
	entry := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID,
		"clusters/test",
		testutil.KustomizationID("apps", "captured"),
	)
	replacement := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID,
		"clusters/test",
		testutil.KustomizationID("apps", "replacement"),
	)
	ordinary := &manifest.Kustomization{Name: "ordinary", Namespace: "apps"}
	run.Store.AddObject(replacement)
	run.Store.AddObject(ordinary)
	side := selectionSide{run: run, result: &Result{
		Objects: []manifest.BaseManifest{ordinary},
		Manifests: []manifest.DocumentBatch{
			{Parent: entry},
			{Parent: ordinary},
			{},
		},
	}}
	side.prepareSubjects()
	assert.Diff(t, side.result.Subjects, map[manifest.NamedResource]manifest.DiagnosticIdentity{
		entry.Named(): {
			ID:       entry.Named(),
			Resource: testutil.KustomizationID("apps", "captured"),
			Path:     "clusters/test",
		},
	})
	assert.Equal(t, side.result.Manifests[0].Parent, manifest.BaseManifest(entry))
	assert.Equal(t, side.result.Manifests[1].Parent, manifest.BaseManifest(ordinary))
	assert.Equal(t, side.result.Manifests[2].Parent, nil)
}

func TestCollectManifests_SupportDeclarations(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	st.AddObject(owner)
	documents := []map[string]any{
		{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind":       "Kustomization",
			"metadata":   map[string]any{"name": "selected", "namespace": "apps"},
		},
		{
			"apiVersion": "source.toolkit.fluxcd.io/v1",
			"kind":       "GitRepository",
			"metadata":   map[string]any{"name": "support", "namespace": "apps"},
			"spec":       map[string]any{"url": "https://fixture.invalid/repo"},
		},
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "sibling", "namespace": "apps"}},
	}
	children := make([]store.Child, 0, len(documents))
	for i, doc := range documents {
		parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		assert.NoError(t, err)
		children = append(children, store.Child{Obj: parsed.Object, Doc: parsed.Document})
		documents[i] = parsed.Document
	}
	st.RecordEmitted(
		owner.Named(),
		&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(documents), Complete: true},
		nil,
	)
	decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(&Admission{
		Needed:   []manifest.NamedResource{owner.Named(), children[1].Obj.Named()},
		Selected: []manifest.NamedResource{children[0].Obj.Named()},
		Ordinary: []manifest.NamedResource{owner.Named(), children[1].Obj.Named()},
	}, true)
	snapshot := Snapshot{Store: st, Decision: decision}
	emission := EmissionFacts{Emitter: owner, Parent: owner}
	for _, doc := range documents {
		emission.Documents = append(
			emission.Documents,
			DocumentFact{Document: doc, Identity: manifest.DocIdentity(doc)},
		)
	}
	snapshot.Emissions = []EmissionFacts{emission}
	snapshot.CollectEmissions()
	assert.Equal(t, decision.Output(children[0].Obj.Named()), true)
	output := make(map[manifest.NamedResource][]map[string]any)
	for _, batch := range snapshot.Manifests {
		output[batch.Parent.Named()] = batch.Documents
	}

	assert.Diff(
		t,
		output,
		map[manifest.NamedResource][]map[string]any{owner.Named(): documents[:2]},
	)
}

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
	snapshot.CollectEmissions()
	assert.Equal(t, len(snapshot.Manifests), 1)
	assert.Equal(t, snapshot.Manifests[0].Parent, manifest.BaseManifest(parent))
	assert.Diff(t, snapshot.Manifests[0].Documents, documents[:1])
	assert.Equal(t, len(snapshot.Emissions[0].Documents), 2)
}

func TestProjectIndex_PreservesBorrowedReaders(t *testing.T) {
	t.Parallel()
	kept := testutil.KustomizationID("apps", "kept")
	excluded := testutil.KustomizationID("apps", "excluded")
	readers := map[string][]manifest.FileReader{
		"shared.yaml":   {{Reader: kept}, {Reader: excluded}},
		"excluded.yaml": {{Reader: excluded}},
	}
	index := &manifest.DiscoveryIndex{Readers: readers}
	assert.Equal(t, ProjectIndex(index, nil), index)
	decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(
		&Admission{Needed: []manifest.NamedResource{kept}},
		true,
	)
	projected := ProjectIndex(index, decision)
	assert.Diff(t, projected.Readers, map[string][]manifest.FileReader{"shared.yaml": {{Reader: kept}}})
	assert.Diff(t, index.Readers, readers)
	assert.Equal(t, len(index.Readers["shared.yaml"]), 2)
	assert.Equal(t, len(index.Readers["excluded.yaml"]), 1)
	projected.Readers["shared.yaml"][0].Exact = true
	assert.Equal(t, index.Readers["shared.yaml"][0].Exact, false)
	assert.Equal(t, ProjectIndex(projected, decision), projected)
}

func TestSnapshot_ChildrenByParent(t *testing.T) {
	t.Parallel()
	st := store.New()
	id := func(name string) manifest.NamedResource { return testutil.KustomizationID("apps", name) }
	parent, other, first, second := id("parent"), id("other"), id("a"), id("b")
	for _, identity := range []manifest.NamedResource{parent, other, first, second} {
		st.AddObject(&manifest.Kustomization{Name: identity.Name, Namespace: identity.Namespace})
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		first: {Parent: parent}, second: {Parent: parent},
	}}
	st.RecordEmitted(parent, &store.Emission{Children: []store.Child{{Obj: st.GetObject(second)}}}, nil)
	st.RecordEmitted(other, &store.Emission{Children: []store.Child{{Obj: st.GetObject(first)}}}, nil)
	snapshot := Snapshot{Store: st, Index: index}
	got := snapshot.childrenByParent()
	assert.Diff(t, got, map[manifest.NamedResource][]manifest.NamedResource{parent: {first, second}, other: {first}})
	got[parent][0] = other
	delete(got, other)
	assert.Diff(
		t,
		snapshot.childrenByParent(),
		map[manifest.NamedResource][]manifest.NamedResource{parent: {first, second}, other: {first}},
	)
}
