package selection

import (
	"encoding/json"
	"errors"
	"hash/maphash"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
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
			key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
			index := &manifest.DiscoveryIndex{
				ProducerKeys: map[manifest.NamedResource]manifest.NamedResource{entry.Named(): key},
			}
			runs := []Snapshot{{
				Store: st, Index: index, Complete: true,
				Entry: EntryIdentity{Producer: testutil.KustomizationID("apps", "stale"), Key: entry.Named()},
				Manifests: []manifest.DocumentBatch{{
					Parent: entry, Documents: []map[string]any{doc}, Identities: tc.identities,
				}},
			}}
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
			assert.Diff(t, maps.Collect(results[0].Bindings.All()), index.ProducerKeys)
			assert.Diff(
				t,
				maps.Collect(results[0].Bindings.All()),
				map[manifest.NamedResource]manifest.NamedResource{entry.Named(): key},
			)
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

func TestProject_NamedLookupFailureSides(t *testing.T) {
	t.Parallel()
	target := testutil.HelmReleaseID("apps", "target")
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "candidate", manifest.NamedResource{})
	pkg := manifest.DiagnosticFor(owner.Named(), owner)
	request := Request{Roots: []manifest.NamedResource{target}}
	const unresolved = "cannot establish membership"
	const failedRelease = "release failure"
	nonResource := errors.New("render diagnostic")
	runErr := errors.Join(&FailuresError{Message: "raw failure"}, nonResource)
	failed := Snapshot{
		Store: store.New(), Complete: true, HasResult: true, Err: runErr, ErrorIdentity: runErr,
		Failed: map[manifest.NamedResource]store.StatusInfo{target: store.FailureStatus(errors.New(failedRelease))},
	}
	unknown := Snapshot{
		Store: store.New(), Complete: true, HasResult: true,
		LookupEvidence: []LookupFailure{{Package: pkg, Path: "/entry", Cause: errors.New("bad candidate")}},
	}
	partial, absent := unknown, unknown
	partial.Complete, absent.Absent = false, true
	for _, tc := range []struct {
		name     string
		runs     []Snapshot
		err      error
		lookupAt int
	}{
		{name: "single without raw error", runs: []Snapshot{unknown}},
		{name: "orig unknown", runs: []Snapshot{unknown, failed}, err: runErr},
		{name: "current unknown", runs: []Snapshot{failed, unknown}, err: runErr, lookupAt: 1},
		{name: "partial lookup", runs: []Snapshot{partial, failed}, err: runErr, lookupAt: -1},
		{name: "absent lookup", runs: []Snapshot{failed, absent}, err: runErr, lookupAt: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			results, errs := Project(tc.runs, tc.err, request)
			assert.FatalIf(t, errs.Selected == nil, "missing projected failure")
			assert.Equal(t, errors.Is(errs.Remaining, nonResource), tc.err != nil)
			for i, result := range results {
				assert.Equal(t, result.LookupFailed, i == tc.lookupAt)
			}
			message := errs.Selected.Error()
			if tc.lookupAt < 0 {
				assert.Equal(t, strings.Contains(message, unresolved), false)
				if tc.runs[0].Complete {
					assert.Equal(t, strings.Count(message, failedRelease), 1)
				} else {
					assert.Equal(t, errs.Selected, tc.err)
				}
				return
			}
			lookup := results[tc.lookupAt]
			assert.Equal(t, lookup.Diagnostic(pkg.ID), pkg)
			assert.Equal(t, len(lookup.Failed), 1)
			assert.Equal(t, len(lookup.Causes), 1)
			assert.Equal(t, len(lookup.Unrelated), 0)
			assert.Equal(t, strings.Count(message, unresolved), 1)
			if len(tc.runs) != comparisonSides {
				return
			}
			labels := [2]string{"orig snapshot:", "current snapshot:"}
			for i, fragment := range []string{unresolved, failedRelease} {
				start := strings.Index(message, labels[(tc.lookupAt+i)%comparisonSides])
				end := strings.Index(message, fragment)
				other := strings.Index(message, labels[1-(tc.lookupAt+i)%comparisonSides])
				assert.FatalIf(t, start < 0 || end <= start || other > start && other < end,
					"wrong snapshot for %q: %s", fragment, message)
				assert.Equal(t, strings.Count(message, fragment), 1)
			}
		})
	}
}

func TestProject_UnresolvedLookupDoesNotPairDocuments(t *testing.T) {
	t.Parallel()
	target := &manifest.HelmRelease{Name: "target", Namespace: "apps"}
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "candidate", manifest.NamedResource{})
	doc := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "paired", "namespace": "apps"},
	}
	id := manifest.DocIdentity(doc)
	st := store.New()
	st.AddObject(target)
	known := Snapshot{
		Store: st, Complete: true,
		Decision:  &Decision{seed: maphash.MakeSeed()},
		Manifests: []manifest.DocumentBatch{{Parent: target, Documents: []map[string]any{doc}}},
	}
	unknown := Snapshot{
		Store: st, Complete: true,
		Entry: EntryIdentity{
			Producer: owner.Named(), Descendant: "entry", Documents: map[manifest.NamedResource]struct{}{id: {}},
		},
		Emissions: []EmissionFacts{{
			Emitter: owner, Parent: target, Documents: []DocumentFact{{Identity: id, Document: doc}},
		}},
	}
	failure := LookupFailure{
		Package: manifest.DiagnosticFor(owner.Named(), owner),
		Path:    "/entry",
		Cause:   errors.New("bad candidate"),
	}
	request := Request{Roots: []manifest.NamedResource{target.Named()}}
	for unresolved, label := range [comparisonSides]string{"orig snapshot", "current snapshot"} {
		t.Run(label, func(t *testing.T) {
			t.Parallel()
			var runs [comparisonSides]Snapshot
			runs[unresolved], runs[1-unresolved] = unknown, known
			results, errs := Project(runs[:], nil, request)
			assert.NoError(t, errs.Selected)
			batches := results[1-unresolved].Manifests
			assert.FatalIf(t, len(batches) != 1, "control batches = %v, want one matching batch", batches)
			assert.Diff(t, batches[0].Documents, []map[string]any{doc})

			runs[unresolved].LookupEvidence = []LookupFailure{failure}
			results, errs = Project(runs[:], nil, request)
			assert.Equal(t, results[unresolved].LookupFailed, true)
			assert.Equal(t, results[1-unresolved].LookupMatched, true)
			for _, result := range results {
				assert.Equal(t, len(result.Manifests), 0)
			}
			assert.FatalIf(t, errs.Selected == nil, "missing projected failure")
			message := errs.Selected.Error()
			for _, fragment := range []string{label + ":", "cannot establish membership", "/entry", "candidate"} {
				assert.FatalIf(t, !strings.Contains(message, fragment), "missing %q: %s", fragment, message)
			}
		})
	}
}

func TestProject_NamedLookupProofBeforeOutput(t *testing.T) {
	t.Parallel()
	target := &manifest.HelmRelease{Name: "target", Namespace: "apps"}
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "candidate", manifest.NamedResource{})
	st := store.New()
	st.AddObject(target)
	decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(new(Admission), true)
	snapshot := Snapshot{
		Store: st, Complete: true, Decision: decision,
		Index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			target.Named(): {BuilderOwner: owner.Named()},
		}},
		LookupEvidence: []LookupFailure{{
			Package: manifest.DiagnosticFor(owner.Named(), owner), Path: "/entry", Cause: errors.New("bad candidate"),
		}},
	}
	request := Request{Roots: []manifest.NamedResource{target.Named()}, Labels: map[string]string{"skip": "true"}}
	results, errs := Project([]Snapshot{snapshot}, nil, request)
	assert.NoError(t, errs.Selected)
	assert.Equal(t, results[0].LookupMatched, true)
	assert.Equal(t, results[0].LookupFailed, false)
	assert.Equal(t, results[0].Counts[target.Named().Kind], 0)
	assert.Equal(t, len(results[0].Objects), 0)

	snapshot.EntryScopes = &EntryScopes{owner.Named(): {}}
	results, errs = Project([]Snapshot{snapshot}, nil, request)
	assert.FatalIf(t, errs.Selected == nil, "pending placement established membership")
	assert.Equal(t, results[0].LookupMatched, false)
	assert.Equal(t, results[0].LookupFailed, true)
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
	snapshot.CollectEmissions(nil)
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

func TestProjectIndex_PreservesBorrowedReaders(t *testing.T) {
	t.Parallel()
	kept := testutil.KustomizationID("apps", "kept")
	excluded := testutil.KustomizationID("apps", "excluded")
	readers := map[string][]manifest.FileReader{
		"shared.yaml":   {{Reader: kept}, {Reader: excluded}},
		"excluded.yaml": {{Reader: excluded}},
	}
	key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).Named()
	index := &manifest.DiscoveryIndex{
		Readers:      readers,
		ProducerKeys: map[manifest.NamedResource]manifest.NamedResource{kept: key},
	}
	assert.Equal(t, ProjectIndex(index, nil), index)
	decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(
		&Admission{Needed: []manifest.NamedResource{kept}},
		true,
	)
	projected := ProjectIndex(index, decision)
	assert.Diff(t, projected.ProducerKeys, index.ProducerKeys)
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

func TestSnapshot_HiddenParentPredictions(t *testing.T) {
	t.Parallel()
	for _, blocked := range []bool{false, true} {
		name := "failed parent"
		if blocked {
			name = "blocked parent"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parent := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
			child := testutil.KustomizationID("apps", "child")
			st := store.New()
			failure := store.StatusInfo{Status: store.StatusFailed}
			if blocked {
				failure = store.FailureStatus(&manifest.DependencyFailedError{
					Parent: parent, Failed: []manifest.NamedResource{manifest.BootstrapSourceID},
				})
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				child: {File: "child.yaml", Parent: parent},
			}}
			facts := &solverFacts{roots: map[manifest.NamedResource]RootEvidence{
				child: {Occurrence: OccurrenceFacts{Declaration: OccurrenceID{File: "child.yaml"}}},
			}}
			snapshot := Snapshot{
				Store: st, Index: index, Facts: facts,
				Failed: map[manifest.NamedResource]store.StatusInfo{parent: failure},
			}
			assert.Equal(t, parent.ReportHidden(), true)
			assert.Equal(t, st.EmissionOwner(parent), manifest.NamedResource{})
			predictions := map[manifest.NamedResource][]manifest.Reference{}
			for id, references := range snapshot.Predictions() {
				predictions[id] = slices.Collect(references)
			}
			assert.Equal(t, len(predictions), 1)
			references := predictions[child]
			assert.FatalIf(
				t, len(references) != 1, "predicted child references = %v, want one materializer", references,
			)
			assert.Equal(t, references[0].NamedResource, parent)
			assert.Equal(t, references[0].Use, "materializer")
			assert.Equal(t, references[0].Gate, manifest.GateParents)
			assert.Equal(t, references[0].Optional, true)
			assert.Equal(t, st.GetDeclaration(child), nil)
		})
	}
}

func TestCollectEmissions_GeneratedRootCoverageOnBothSides(t *testing.T) {
	t.Parallel()
	root := &manifest.Kustomization{Name: "sync", Namespace: "apps"}
	literal := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "deploy/requested", manifest.NamedResource{})
	inside := testutil.KustomizationID("apps", "inside")
	sibling := testutil.KustomizationID("apps", "sibling")
	external := testutil.KustomizationID("apps", "external")
	generated := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "generated"}
	source := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "self"}
	otherSource := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "other"}
	entry := EntryIdentity{
		Producer:   root.Named(),
		Source:     source,
		Descendant: "deploy/requested",
		Key:        literal.Named(),
	}
	t.Run("follow accepted emitted children", func(t *testing.T) {
		t.Parallel()
		accepted := &manifest.Kustomization{Name: "accepted", Namespace: "apps"}
		rejected := &manifest.Kustomization{Name: "rejected", Namespace: "apps"}
		other := &manifest.Kustomization{Name: "other", Namespace: "apps"}
		indexed := &manifest.Kustomization{Name: "indexed", Namespace: "apps"}
		origin := manifest.Production{Source: source, File: "deploy/requested/package.yaml"}
		st := store.New()
		st.AddObject(root)
		st.AddObject(other)
		st.RecordEmitted(root.Named(), &store.Emission{Children: []store.Child{{Obj: accepted}, {Obj: indexed}}}, nil)
		st.RecordEmitted(other.Named(), &store.Emission{Children: []store.Child{{Obj: rejected}}}, nil)
		assert.Equal(t, st.EmissionOwner(accepted.Named()), root.Named())
		assert.Equal(t, st.EmissionOwner(rejected.Named()), other.Named())
		assert.Equal(t, root.EmissionPolicy().DocumentScope, false)

		acceptedDoc, indexedDoc := testutil.ObjectDocument(accepted), testutil.ObjectDocument(indexed)
		acceptedOutput := map[string]any{"value": "accepted output"}
		decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(&Admission{
			Selected: []manifest.NamedResource{root.Named(), accepted.Named(), rejected.Named()},
		}, true)
		childEntry := entry
		childEntry.Documents = map[manifest.NamedResource]struct{}{
			accepted.Named(): {}, indexed.Named(): {}, rejected.Named(): {},
		}
		snapshot := Snapshot{
			Store: st, Entry: childEntry, Decision: decision,
			Index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				indexed.Named(): {File: origin.File, Productions: []manifest.Production{origin}},
			}},
			Emissions: []EmissionFacts{
				{Emitter: root, Parent: root, Documents: []DocumentFact{
					{Identity: accepted.Named(), Child: accepted.Named(), Document: acceptedDoc},
					{Identity: indexed.Named(), Child: indexed.Named(), Document: indexedDoc},
					{Identity: indexed.Named(), Child: indexed.Named(), Document: indexedDoc},
					{Identity: rejected.Named(), Child: rejected.Named(), Document: testutil.ObjectDocument(rejected)},
				}},
				{Emitter: accepted, Parent: accepted, Documents: []DocumentFact{
					{Identity: generated, Document: acceptedOutput},
				}},
				{Emitter: rejected, Parent: rejected, Documents: []DocumentFact{
					{Identity: generated, Document: map[string]any{"value": "different owner's output"}},
				}},
			},
		}
		assert.Diff(t, snapshot.entryScope(), map[manifest.NamedResource]bool{
			root.Named(): true, accepted.Named(): true, indexed.Named(): true,
		})
		snapshot.CollectEmissions(nil)
		assert.FatalIf(t, len(snapshot.Manifests) != 2, "projected batches: %v", snapshot.Manifests)
		assert.Equal(t, snapshot.Manifests[0].Parent.Named(), root.Named())
		assert.Diff(t, snapshot.Manifests[0].Documents, []map[string]any{acceptedDoc, indexedDoc, indexedDoc})
		assert.Equal(t, snapshot.Manifests[1].Parent.Named(), accepted.Named())
		assert.Diff(t, snapshot.Manifests[1].Documents, []map[string]any{acceptedOutput})
	})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		inside: {
			File:        "deploy/requested/inside.yaml",
			Productions: []manifest.Production{{Source: source, File: "deploy/requested/inside.yaml"}},
		},
		sibling: {
			File:        "deploy/sibling/sibling.yaml",
			Productions: []manifest.Production{{Source: source, File: "deploy/sibling/sibling.yaml"}},
		},
		external: {
			File:        "deploy/requested/inside.yaml",
			Productions: []manifest.Production{{Source: otherSource, File: "deploy/requested/inside.yaml"}},
		},
	}}
	selectRoot, _ := (&Decision{seed: maphash.MakeSeed()}).include(
		&Admission{Selected: []manifest.NamedResource{root.Named()}},
		true,
	)
	reconcileRoot, _ := (&Decision{seed: maphash.MakeSeed()}).include(
		&Admission{Needed: []manifest.NamedResource{root.Named()}},
		true,
	)
	var runs [2]Snapshot
	for side, value := range []string{"before", "after"} {
		sideEntry := entry
		sideEntry.Documents = map[manifest.NamedResource]struct{}{inside: {}}
		runs[side] = Snapshot{
			Store:    store.New(),
			Index:    index,
			Entry:    sideEntry,
			Decision: reconcileRoot,
			Emissions: []EmissionFacts{{
				Emitter: root, Parent: root, Documents: []DocumentFact{
					{
						Identity: inside, Document: map[string]any{"value": value},
					},
					{
						Identity: sibling, Document: map[string]any{"value": value},
					},
					{
						Identity: external, Document: map[string]any{"value": value},
					},
					{
						Identity: inside, Document: map[string]any{"value": "foreign"},
					},
				},
			}},
		}
		runs[side].Emissions = append(runs[side].Emissions, EmissionFacts{
			Emitter: literal, Parent: literal,
			Documents: []DocumentFact{{Identity: inside, Document: map[string]any{"value": "unapplied entry"}}},
		})
	}
	second := &runs[1].Emissions[0].Documents
	*second = append(*second, DocumentFact{
		Identity: generated, Document: map[string]any{"value": "only on the second side"},
	})
	runs[1].Decision = selectRoot
	runs[1].Entry.Documents[generated] = struct{}{}
	for _, order := range [][2]int{{0, 1}, {1, 0}} {
		for _, side := range order {
			run := &runs[side]
			run.CollectEmissions(&runs[1-side])
			assert.Equal(t, run.Entry.Key, entry.Key)
			assert.FatalIf(t, len(run.Manifests) != 1, "side %d: %+v", side, run.Manifests)
			wantIDs := []manifest.NamedResource{inside, inside}
			wantDocs := []map[string]any{
				run.Emissions[0].Documents[0].Document,
				run.Emissions[0].Documents[3].Document,
			}
			if side == 1 {
				wantIDs = append(wantIDs, generated)
				wantDocs = append(wantDocs, (*second)[len(*second)-1].Document)
			}
			assert.Diff(t, run.Manifests[0].Identities, wantIDs)
			assert.Diff(t, run.Manifests[0].Documents, wantDocs)
		}
	}
}
