package selection

import (
	"encoding/json"
	"errors"
	"hash/maphash"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestProject_BatchIdentities(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		identities []manifest.NamedResource
	}{
		{name: "nil"},
		{name: "empty", identities: []manifest.NamedResource{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			entry := manifest.NewEntryKustomization(
				manifest.BootstrapSourceID,
				"clusters/test",
				manifest.NamedResource{},
			)
			doc := map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "kept", "namespace": "apps"},
			}
			st := store.New()
			st.AddObject(entry)
			runs := []Snapshot{{Store: st, Complete: true, Manifests: []manifest.DocumentBatch{{
				Parent: entry, Documents: []map[string]any{doc}, Identities: tc.identities,
			}}}}
			before, err := json.Marshal(runs[0].Manifests)
			assert.NoError(t, err)
			results, errs := Project(
				runs,
				nil,
				Request{Roots: []manifest.NamedResource{testutil.KustomizationID("", "")}},
			)
			assert.NoError(t, errs.Selected)
			assert.NoError(t, errs.Remaining)
			assert.Equal(t, len(results), 1)
			assert.Equal(t, len(results[0].Manifests), 1)
			assert.Diff(t, results[0].Manifests[0].Documents, []map[string]any{doc})
			assert.Diff(
				t,
				results[0].Manifests[0].Identities,
				[]manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "kept"}},
			)
			after, err := json.Marshal(runs[0].Manifests)
			assert.NoError(t, err)
			assert.Diff(t, after, before)
		})
	}
}

func TestProject_MixedBatchIdentities(t *testing.T) {
	t.Parallel()
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", manifest.NamedResource{})
	parent := &manifest.Kustomization{
		Name:            "report",
		Namespace:       "apps",
		TargetNamespace: "reporting",
		Labels:          map[string]string{"selected": "yes"},
	}
	kept := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      "kept",
			"namespace": "apps",
			"labels":    map[string]any{"selected": "yes"},
		},
	}
	excluded := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"name": "excluded", "namespace": "other"},
	}
	emitted := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "emitted"},
	}
	emitterID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "emitter", Name: "emitted"}
	st := store.New()
	st.AddObject(entry)
	st.AddObject(parent)
	runs := []Snapshot{{Store: st, Complete: true, Manifests: []manifest.DocumentBatch{
		{Parent: entry, Documents: []map[string]any{kept, excluded}},
		{Parent: parent, Documents: []map[string]any{emitted}, Identities: []manifest.NamedResource{emitterID}},
	}}}
	before, err := json.Marshal(runs[0].Manifests)
	assert.NoError(t, err)
	results, errs := Project(
		runs,
		nil,
		Request{
			Roots:  []manifest.NamedResource{testutil.KustomizationID("apps", "")},
			Labels: map[string]string{"selected": "yes"},
		},
	)
	assert.NoError(t, errs.Selected)
	assert.NoError(t, errs.Remaining)
	assert.Equal(t, len(results[0].Manifests), 2)
	assert.Diff(t, results[0].Manifests[0].Documents, []map[string]any{emitted})
	assert.Diff(t, results[0].Manifests[0].Identities, []manifest.NamedResource{emitterID})
	assert.Diff(t, results[0].Manifests[1].Documents, []map[string]any{kept})
	assert.Diff(
		t,
		results[0].Manifests[1].Identities,
		[]manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "kept"}},
	)
	after, err := json.Marshal(runs[0].Manifests)
	assert.NoError(t, err)
	assert.Diff(t, after, before)
}

func TestProject_BatchIdentityMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		targetNamespace string
		selectedSide    int
		supplied        bool
		count           int
	}{
		{name: "orig counterpart omitted", targetNamespace: "apps", selectedSide: 0, count: 2},
		{name: "current counterpart omitted", targetNamespace: "apps", selectedSide: 1, count: 2},
		{name: "supplied emitter under reporting parent", targetNamespace: "reporting", selectedSide: 1, supplied: true, count: 2},
		{name: "restricted membership", targetNamespace: "apps", count: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := &manifest.Kustomization{
				Name:            "report",
				Namespace:       "reporting",
				TargetNamespace: tc.targetNamespace,
			}
			documents := []map[string]any{
				{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "kept"}},
				{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "excluded"}},
			}
			identities := []manifest.NamedResource{
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "kept"},
				{Kind: manifest.KindSecret, Namespace: "apps", Name: "excluded"},
			}
			runs := make([]Snapshot, tc.count)
			for i := range runs {
				admission := &Admission{Needed: []manifest.NamedResource{parent.Named()}}
				if i == tc.selectedSide {
					admission.Selected = identities[:1]
				}
				decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(admission, true)
				st := store.New()
				st.AddObject(parent)
				batch := manifest.DocumentBatch{Parent: parent, Documents: documents, Identities: identities}
				if !tc.supplied && i == tc.selectedSide {
					batch.Identities = nil
				}
				runs[i] = Snapshot{
					Store:     st,
					Complete:  true,
					Decision:  decision,
					Manifests: []manifest.DocumentBatch{batch},
				}
			}
			before := make([][]byte, tc.count)
			for i := range runs {
				var err error
				before[i], err = json.Marshal(runs[i].Manifests)
				assert.NoError(t, err)
			}
			results, errs := Project(
				runs,
				nil,
				Request{Roots: []manifest.NamedResource{testutil.KustomizationID("apps", "")}},
			)
			assert.NoError(t, errs.Selected)
			assert.NoError(t, errs.Remaining)
			assert.Equal(t, len(results), tc.count)
			for i, result := range results {
				assert.Equal(t, len(result.Manifests), 1)
				assert.Diff(t, result.Manifests[0].Documents, documents[:1])
				assert.Diff(t, result.Manifests[0].Identities, identities[:1])
				after, err := json.Marshal(runs[i].Manifests)
				assert.NoError(t, err)
				assert.Diff(t, after, before[i])
			}
		})
	}
}

func TestProject_BatchIdentityValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		empty bool
	}{
		{name: "excluded malformed batch"},
		{name: "empty malformed batch", empty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := &manifest.Kustomization{Name: "excluded", Namespace: "other"}
			batch := manifest.DocumentBatch{Parent: parent, Identities: []manifest.NamedResource{{}, {}}}
			if !tc.empty {
				batch.Documents = []map[string]any{
					{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "config"}},
				}
			}
			st := store.New()
			st.AddObject(parent)
			runs := []Snapshot{{Store: st, Complete: true, Manifests: []manifest.DocumentBatch{batch}}}
			original := errors.New("original render error")
			results, errs := Project(
				runs,
				original,
				Request{Roots: []manifest.NamedResource{testutil.KustomizationID("apps", "")}},
			)
			for _, err := range []error{errs.Selected, errs.Remaining} {
				assert.Equal(t, errors.Is(err, original), true)
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			}
			assert.Equal(t, results == nil, true)
		})
	}
}

func TestProject_IncompleteBatchIdentityValidation(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{Name: "excluded", Namespace: "other"}
	batch := manifest.DocumentBatch{
		Parent: parent, Identities: []manifest.NamedResource{{}, {}},
		Documents: []map[string]any{
			{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "config"}},
		},
	}
	st := store.New()
	st.AddObject(parent)
	original := errors.New("original render error")
	runs := []Snapshot{
		{Store: st, Complete: true, Manifests: []manifest.DocumentBatch{batch}},
		{
			Store: store.New(), HasResult: true, Err: original,
			Warnings: []manifest.Warning{{
				Resource: testutil.KustomizationID("apps", "warned"),
				Category: manifest.WarnUnresolvedSubstitution,
				Message:  "input missing",
			}},
		},
	}
	results, errs := Project(
		runs, original, Request{Roots: []manifest.NamedResource{testutil.KustomizationID("apps", "")}},
	)
	for _, err := range []error{errs.Selected, errs.Remaining} {
		assert.Equal(t, errors.Is(err, original), true)
		assert.Equal(t, errors.Is(err, manifest.ErrInput), false)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), false)
		assert.Equal(t, err, original)
	}
	assert.Equal(t, len(results), 2)
	assert.Equal(t, len(results[0].Manifests), 0)
	assert.Equal(t, len(results[1].Manifests), 0)
	assert.Diff(t, results[1].Warnings, runs[1].Warnings)
}

func TestProject_AbsentBatchIdentityValidation(t *testing.T) {
	t.Parallel()
	parent := &manifest.Kustomization{Name: "excluded", Namespace: "other"}
	batch := manifest.DocumentBatch{
		Parent: parent, Identities: []manifest.NamedResource{{}, {}},
		Documents: []map[string]any{
			{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "config"}},
		},
	}
	st := store.New()
	st.AddObject(parent)
	valid := &manifest.Kustomization{Name: "valid", Namespace: "apps"}
	other := store.New()
	other.AddObject(valid)
	runs := []Snapshot{
		{Store: st, Complete: true, Absent: true, Manifests: []manifest.DocumentBatch{batch}},
		{
			Store:     other,
			Complete:  true,
			HasResult: true,
			Manifests: []manifest.DocumentBatch{{Parent: valid, Documents: batch.Documents}},
		},
	}
	results, errs := Project(
		runs, nil, Request{Roots: []manifest.NamedResource{testutil.KustomizationID("apps", "")}},
	)
	for _, err := range []error{errs.Selected, errs.Remaining} {
		assert.NoError(t, err)
	}
	assert.Equal(t, len(results), 2)
	assert.Equal(t, len(results[0].Manifests), 0)
	assert.Equal(t, len(results[1].Manifests), 1)
	assert.Diff(t, results[1].Manifests[0].Documents, batch.Documents)
}

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
	for _, tc := range []struct {
		name     string
		selected []manifest.NamedResource
		needed   []manifest.NamedResource
		want     []int
	}{
		{name: "unrestricted", want: []int{0, 1, 2}},
		{name: "selected emitter", selected: []manifest.NamedResource{first.Named()}, want: []int{0, 1}},
		{name: "required document", needed: identities[2:], want: []int{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var decision *Decision
			if tc.name != "unrestricted" {
				decision, _ = (&Decision{seed: maphash.MakeSeed()}).include(&Admission{
					Selected: tc.selected, Needed: tc.needed,
				}, true)
			}
			snapshot := Snapshot{Decision: decision, Emissions: []EmissionFacts{
				{Emitter: first, Parent: parent, Documents: []DocumentFact{
					{Document: documents[0], Identity: identities[0]},
					{Document: documents[1], Identity: identities[1]},
				}},
				{Emitter: second, Parent: parent, Documents: []DocumentFact{
					{Document: documents[2], Identity: identities[2]},
				}},
			}}
			snapshot.CollectEmissions()
			assert.Equal(t, len(snapshot.Manifests), 1)
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
