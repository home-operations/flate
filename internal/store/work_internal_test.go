package store

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestWorkSnapshot_FinalAndRetired(t *testing.T) {
	t.Parallel()
	s := New()
	tree := manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}
	var root manifest.WorkRecorder
	s.SetWorkTree(tree, root.Scope())
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	var first, final, proof ReadSet
	first.Init(s, owner.Named(), nil)
	first.BeginExecution(nil)
	first.Work().
		Scope().
		Record(manifest.WorkKustomize, "first").
		Add(&manifest.WorkCounts{Attempts: 1, Failures: 1, PeakConcurrent: 2})
	s.Retire(&first)
	s.Retire(&first)
	first.Init(s, owner.Named(), nil)
	first.BeginExecution(nil)
	first.Work().Scope().Record(manifest.WorkKustomize, "second").Add(&manifest.WorkCounts{Attempts: 2, Completed: 2})
	first.Retire()
	first.Init(s, owner.Named(), nil)
	first.BeginExecution(nil)
	first.Work().Scope().Record(manifest.WorkKustomize, "third").Add(&manifest.WorkCounts{Attempts: 3, Completed: 3})
	final.Init(s, owner.Named(), nil)
	final.BeginExecution(nil)
	proof.Init(s, owner.Named(), nil)
	proof.SetWork(final.Work())
	proof.Work().Scope().Record(manifest.WorkPreparation, "proof").Add(&manifest.WorkCounts{FileEntries: 2})
	proof.Retire()
	final.Work().
		Scope().
		Record(manifest.WorkKustomize, "final").
		Add(&manifest.WorkCounts{Attempts: 1, Completed: 1, PeakConcurrent: 1})
	for _, input := range []string{"fifth", "sixth", "seventh", "eighth"} {
		first.Work().Scope().Record(manifest.WorkKustomize, input).Add(&manifest.WorkCounts{Attempts: 1, Completed: 1})
	}
	s.Retire(&first)
	s.Retire(&first)
	s.DeleteObject(owner.Named())
	got := s.WorkSnapshot()
	assert.Equal(
		t,
		sumWork(got),
		manifest.WorkCounts{Attempts: 12, Completed: 11, Failures: 1, FileEntries: 2, PeakConcurrent: 2},
	)
	for _, record := range got {
		assert.Equal(t, record.Key.Tree, tree)
		assert.Equal(t, record.Key.Owner, owner.Named())
	}
	inputs := map[string]manifest.WorkCounts{}
	for _, record := range got {
		if record.Key.Phase == manifest.WorkKustomize {
			inputs[record.Key.Input] = record.Counts
		}
	}
	assert.Diff(t, inputs, map[string]manifest.WorkCounts{
		"fifth":   {Attempts: 1, Completed: 1},
		"sixth":   {Attempts: 1, Completed: 1},
		"seventh": {Attempts: 1, Completed: 1},
		"eighth":  {Attempts: 1, Completed: 1},
		"first":   {Attempts: 1, Failures: 1, PeakConcurrent: 2},
		"second":  {Attempts: 2, Completed: 2},
		"third":   {Attempts: 3, Completed: 3},
		"final":   {Attempts: 1, Completed: 1, PeakConcurrent: 1},
	})
	detached := slices.Clone(got)
	final.Close()
	final.Init(s, owner.Named(), nil)
	s.AddObject(owner)
	root = manifest.WorkRecorder{}
	s.SetWorkTree(tree, root.Scope())
	final.BeginExecution(nil)
	final.Work().Scope().Record(manifest.WorkKustomize, "later").Add(&manifest.WorkCounts{Attempts: 4, Completed: 4})
	assert.Equal(t, sumWork(s.WorkSnapshot()).Attempts, uint64(5))
	assert.Diff(t, got, detached)
	final.Close()
}

func sumWork(work manifest.WorkSummary) manifest.WorkCounts {
	var counts manifest.WorkCounts
	for i := range work {
		record := &work[i]
		counts.Add(&record.Counts)
	}
	return counts
}
