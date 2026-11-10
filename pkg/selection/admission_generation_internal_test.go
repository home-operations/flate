package selection

import (
	"fmt"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:gocognit,paralleltest,tparallel // Transition cases MUST share retained generation and evidence.
func TestAdmission_OneCapturePerGeneration(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	st.AddObject(parent)
	changes := change.NewSet(nil)
	session := newSolverSession(t, st, changes, Request{Roots: []manifest.NamedResource{parent.Named()}, Changed: true})
	_, retry := publishDecision(t, session, new(Admission{
		Needed: []manifest.NamedResource{parent.Named()}, Selected: []manifest.NamedResource{parent.Named()},
		Dirty: []manifest.NamedResource{parent.Named()}, Ordinary: []manifest.NamedResource{parent.Named()},
	}), 0)
	assert.Equal(t, retry, false)
	generation := make([]store.Child, 4)
	for i := range generation {
		generation[i].Obj = &manifest.ConfigMap{Name: fmt.Sprintf("sibling-%d", i), Namespace: "apps"}
	}
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	var evidence *store.ReadSet
	for i := range generation {
		var reads store.ReadSet
		reads.Init(st, parent.Named(), nil)
		proposal, err := session.prepareTest(t.Context(), PrepareRequest{
			Changes:    changes,
			Generation: generation, Owner: parent.Named(), Reads: &reads, Preparation: &preparation,
		})
		assert.NoError(t, err)
		if i == 0 {
			evidence = proposal.reads
		}
		assert.Equal(t, proposal.reads, evidence)
		assert.Equal(t, proposal.reused, i != 0)
		inputs := slices.Collect(proposal.reads.Inputs())
		assert.Equal(t, len(inputs), 5)
		for _, child := range generation {
			assert.Equal(t, slices.Contains(inputs, child.Obj.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Needed, child.Obj.Named()), true)
			assert.Equal(t, len(child.Productions), 0)
		}
		_, committed, err := session.Commit(t.Context(), proposal, Execution)
		assert.NoError(t, err)
		assert.Equal(t, committed, true)
		reads.Close()
	}
	for _, change := range []string{"new generation", "new declarations", "new input", "new source"} {
		t.Run(change, func(t *testing.T) {
			var guard store.ReadSet
			guard.Init(st, parent.Named(), nil)
			previousGeneration := generation
			switch change {
			case "new generation":
				generation = slices.Clone(generation)
			case "new declarations":
				generation = make([]store.Child, len(generation))
				for i := range generation {
					generation[i].Obj = &manifest.ConfigMap{Name: fmt.Sprintf("replacement-%d", i), Namespace: "apps"}
				}
			case "new input":
				guard.RequireData(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "late"})
			case "new source":
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
					LocalPath: root, FS: fstest.MapFS{}, Key: "replacement", Content: "replacement",
				})
			}
			proposal, err := session.prepareTest(t.Context(), PrepareRequest{
				Changes:    changes,
				Generation: generation, Owner: parent.Named(), Reads: &guard, Preparation: &preparation,
			})
			assert.NoError(t, err)
			if change == "new declarations" {
				for _, child := range previousGeneration {
					_, retained := preparation.closure.states[child.Obj.Named()]
					assert.Equal(t, retained, false)
				}
			}
			assert.Equal(t, proposal.reused, false)
			assert.Equal(t, proposal.reads == evidence, false)
			evidence = proposal.reads
			_, committed, err := session.Commit(t.Context(), proposal, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			guard.Retire()
		})
	}
	var stale store.ReadSet
	stale.Init(st, parent.Named(), nil)
	stale.GetObject(parent.Named())
	replacement := parent.Clone()
	replacement.Path = "changed"
	st.AddObject(replacement)
	proposal, err := session.prepareTest(t.Context(), PrepareRequest{
		Changes:    changes,
		Generation: generation, Owner: parent.Named(), Reads: &stale, Preparation: &preparation,
	})
	assert.NoError(t, err)
	assert.Equal(t, proposal.reused, false)
	_, committed, err := session.Commit(t.Context(), proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, false)
	stale.Retire()
}
