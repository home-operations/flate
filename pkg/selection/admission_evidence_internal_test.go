package selection

import (
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_NarrowerSuccessorKeepsCompleteEvidence(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	first := &manifest.Kustomization{Name: "first", Namespace: "apps", Path: "first"}
	second := &manifest.Kustomization{
		Name:      "second",
		Namespace: "apps",
		Path:      "second",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "values"}},
		},
	}
	input := manifest.NewConfigMap(
		manifest.NamedResource{Name: "values", Namespace: "apps"},
		map[string]any{"value": "before"},
	)
	st.AddObjects([]manifest.BaseManifest{first, second, input})
	content := testutil.ObjectDocument(second)
	st.AddDeclaration(store.Declaration{Obj: second, Doc: content})
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{first.Named(), second.Named()}})
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	proposal, err := session.prepareTest(t.Context(), PrepareRequest{Invocation: true, Preparation: &preparation})
	assert.NoError(t, err)
	assert.Equal(t, preparation.closure.states[second.Named()].initialKnown, true)
	_, capturedDoc, captured := proposal.reads.GetEmission(second.Named())
	assert.Diff(t, capturedDoc, content)
	captured.Finish()
	proposal.Retire()
	for range st.Sweep() {
	}
	var guard store.ReadSet
	guard.Init(st, first.Named(), nil)
	guard.RequireData(first.Named())
	defer guard.Close()
	proposal, err = session.prepareTest(t.Context(), PrepareRequest{Reads: &guard, Preparation: &preparation})
	assert.NoError(t, err)
	assert.Equal(t, preparation.closure.states[second.Named()].initialKnown, true)
	assert.Equal(t, slices.Contains(slices.Collect(proposal.reads.Inputs()), input.Named()), true)
	_, retainedDoc, retained := proposal.reads.GetEmission(second.Named())
	assert.Diff(t, retainedDoc, content)
	retained.Finish()
	proposal.Retire()
	for range st.Sweep() {
	}
	st.AddObject(manifest.NewConfigMap(input.Named(), map[string]any{"value": "after"}))
	proposal, err = session.prepareTest(t.Context(), PrepareRequest{Reads: &guard, Preparation: &preparation})
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, preparation.closure.states[first.Named()].initialKnown, true)
	assert.Equal(t, preparation.closure.states[second.Named()].initialKnown, false)
	assert.Equal(t, proposal.reads.Valid(), true)
}

func TestAdmission_UnknownDeclarationSkipsPreparedEvidence(t *testing.T) {
	t.Parallel()
	object := &manifest.ConfigMap{Name: "input", Namespace: "apps"}
	st := store.New()
	st.AddDeclaration(store.Declaration{Obj: object, Doc: testutil.ObjectDocument(object)})
	calls := 0
	closure := admissionClosure{
		objects: st,
		facts:   new(solverFacts),
		context: SourceContext{SideKnown: true, Side: manifest.TreeHead},
		states: map[manifest.NamedResource]admissionState{
			object.Named(): {productionsKnown: true},
		},
		proposal: &Admission{Request: PrepareRequest{Changes: change.NewSet(nil)}},
		declarationContents: func(map[string]any) store.DeclarationContent {
			calls++
			return store.DeclarationContent{Exact: "prepared"}
		},
	}
	assert.Equal(t, closure.declarationChanged(object.Named(), object), change.Comparison{})
	assert.Equal(t, calls, 0)
}

func TestAdmission_AuthoredDeclarationChangeOverridesOutput(t *testing.T) {
	t.Parallel()
	stores := [2]*store.Store{store.New(), store.New()}
	indexes := [2]*manifest.DiscoveryIndex{}
	object := &manifest.ConfigMap{Name: "input", Namespace: "apps"}
	id := object.Named()
	for side, st := range stores {
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
		document := testutil.ObjectDocument(object)
		document["untracked"] = side
		fingerprint := manifest.Fingerprint(document)
		delete(document, "untracked")
		st.AddDeclaration(store.Declaration{Obj: object, Doc: document})
		indexes[side] = &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Authored: id, File: "input.yaml", Declaration: object},
			},
			CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
			Declarations: map[manifest.NamedResource]map[string]manifest.FileDeclarations{
				manifest.BootstrapSourceID: {
					"input.yaml": {Present: true, Complete: true, Objects: map[manifest.NamedResource]string{
						id: fingerprint,
					}},
				},
			},
		}
	}
	changes, err := change.Qualify(t.Context(), change.NewSet([]string{"input.yaml"}), stores, manifest.WorkScope{})
	assert.NoError(t, err)
	changes, err = changes.WithDeclarations(t.Context(), change.DeclarationInputs{Stores: stores})
	assert.NoError(t, err)
	changes, err = changes.WithReaders(
		t.Context(),
		indexes,
		[2]map[manifest.NamedResource]inputprojection.Projection{},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	facts := &solverFacts{
		store: stores[1], changes: changes, index: *indexes[1],
		context: SourceContext{SideKnown: true, Side: manifest.TreeHead},
		sources: map[manifest.NamedResource]SourceFacts{manifest.BootstrapSourceID: {Paired: true}},
	}
	closure := solverClosure(t, stores[1], facts)
	closure.context = facts.context
	closure.proposal.Request.Changes = changes
	closure.states[id] = admissionState{productionsKnown: true}
	assert.Equal(t, closure.declarationChanged(id, object), change.Comparison{Known: true, Changed: true})
	assert.Equal(
		t,
		closure.outputChanged(id, new(manifest.Reference{NamedResource: id}), object),
		change.Comparison{Known: true},
	)
	assert.Equal(t, closure.declarationUnchanged(id, manifest.NamedResource{}, object), false)
}
