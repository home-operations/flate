package selection

import (
	"context"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestAdmission_ChangedPromotionsInvalidatePreparation(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "sources"}
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "."}
	st.AddObjects([]manifest.BaseManifest{source, parent})
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	session := newSolverSession(t, st, change.NewSet([]string{"one.yaml", "two.yaml"}), Request{Changed: true})
	facts := session.facts.(*solverFacts)
	facts.index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{
		source.Named(): {File: "source.yaml", Inputs: []string{"kustomization.yaml"}},
	}
	publishDecision(
		t,
		session,
		&Admission{
			Needed:   []manifest.NamedResource{source.Named(), parent.Named()},
			Ordinary: []manifest.NamedResource{source.Named()},
			Dirty:    []manifest.NamedResource{parent.Named()},
		},
		0,
	)
	var work manifest.WorkRecorder
	var caller store.ReadSet
	caller.Init(st, source.Named(), nil)
	caller.SetWork(&work)
	defer caller.Close()
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	generation := []store.Child{{Obj: source}}
	proposal, err := prepareSolver(
		t.Context(),
		t,
		session,
		PrepareRequest{Preparation: &preparation, Reads: &caller, Generation: generation},
	)
	assert.NoError(t, err)
	defer proposal.Retire()
	before := work.Scope().Admission().Counts
	assert.Diff(t, before, manifest.WorkCounts{Attempts: 1, Completed: 1, OwnerPrepares: 1, FileEntries: 2})
	assert.Equal(t, len(preparation.closure.states[source.Named()].productions), 0)
	published := make(chan struct{})
	tasks := task.NewBounded(2)
	tasks.Go(t.Context(), "publish source producer", func(context.Context) {
		publishDecision(
			t,
			session,
			&Admission{
				Needed:   []manifest.NamedResource{parent.Named()},
				Ordinary: []manifest.NamedResource{parent.Named()},
				Dirty:    []manifest.NamedResource{parent.Named()},
			},
			proposal.Revision,
		)
		st.RecordEmitted(parent.Named(), &store.Emission{Complete: true, Children: []store.Child{{Obj: source}}}, nil)
		close(published)
	})
	<-published
	tasks.BlockTillDone()
	assert.Equal(t, caller.Valid(), true)
	assert.Equal(t, proposal.Revision < session.Snapshot().version(), true)
	current := session.Snapshot()
	next, _ := current.include(proposal, true)
	assert.Equal(t, session.publish(proposal, current, next), false)
	proposal, committed, err := session.commitSolver(t.Context(), t, proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, slices.Contains(proposal.Needed, source.Named()), true)
	want := manifest.WorkCounts{Attempts: 2, Completed: 2, OwnerPrepares: 2, FileEntries: 4}
	assert.Diff(t, work.Scope().Admission().Counts, want)
	assert.Diff(t, preparation.closure.states[source.Named()].productions, st.Productions(source.Named(), &facts.index))
	var fresh store.ReadSet
	fresh.Init(st, source.Named(), nil)
	fresh.SetWork(&work)
	defer fresh.Close()
	proposal, err = prepareSolver(
		t.Context(),
		t,
		session,
		PrepareRequest{Preparation: &preparation, Reads: &fresh, Generation: []store.Child{{Obj: source}}},
	)
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Diff(t, work.Scope().Admission().Counts, want)
}
