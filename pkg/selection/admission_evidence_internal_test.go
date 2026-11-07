package selection

import (
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
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
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{first.Named(), second.Named()}})
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	proposal, err := session.Prepare(t.Context(), PrepareRequest{Invocation: true, Preparation: &preparation})
	assert.NoError(t, err)
	assert.Equal(t, preparation.closure.states[second.Named()].initialKnown, true)
	proposal.Retire()
	for range st.Sweep() {
	}
	var guard store.ReadSet
	guard.Init(st, first.Named(), nil)
	guard.RequireData(first.Named())
	defer guard.Close()
	proposal, err = session.Prepare(t.Context(), PrepareRequest{Reads: &guard, Preparation: &preparation})
	assert.NoError(t, err)
	assert.Equal(t, preparation.closure.states[second.Named()].initialKnown, true)
	assert.Equal(t, slices.Contains(slices.Collect(proposal.reads.Inputs()), input.Named()), true)
	proposal.Retire()
	for range st.Sweep() {
	}
	st.AddObject(manifest.NewConfigMap(input.Named(), map[string]any{"value": "after"}))
	proposal, err = session.Prepare(t.Context(), PrepareRequest{Reads: &guard, Preparation: &preparation})
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, preparation.closure.states[first.Named()].initialKnown, true)
	assert.Equal(t, preparation.closure.states[second.Named()].initialKnown, false)
	assert.Equal(t, proposal.reads.Valid(), true)
}
