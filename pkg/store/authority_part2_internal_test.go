package store

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func testReadRegistrationNoABACase2(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	s := New()
	obj := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(obj)
	child := emissionChild("child", "old")
	s.RecordEmitted(obj.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
	s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "old"})
	id := child.Obj.Named()
	if (*testCaseInput) == "producer" {
		id = obj.Named()
	}
	sh := s.shardFor(id)
	sh.mu.Lock()
	switch *testCaseInput {
	case "artifact":
		record := sh.artifacts[id]
		record.generation = math.MaxUint64
		sh.artifacts[id] = record
	case "producer":
		sh.slots[id].generation = math.MaxUint64
	case "bucket":
		bucket := sh.byName[kindID{id.Group, id.Kind}]
		bucket.generation = math.MaxUint64
		sh.byName[kindID{id.Group, id.Kind}] = bucket
	}
	sh.mu.Unlock()
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	switch *testCaseInput {
	case "artifact":
		reads.GetArtifact(id, ArtifactContent)
	case "producer":
		reads.GetObject(child.Obj.Named())
	case "bucket":
		reads.ListObjects(id.Group, id.Kind)
	}
	func() {
		defer func() {
			{
				assert.ErrorIf(t, recover() == nil, "%s", "exhaustion did not fail before mutation")
			}
		}()
		switch *testCaseInput {
		case "artifact":
			s.SetArtifact(id, &SourceArtifact{Key: "new"})
		case "producer":
			s.RecordEmitted(
				id,
				&Emission{Children: []Child{emissionChild("child", "new")}, Complete: true},
				nil,
			)
		case "bucket":
			s.AddObject(emissionChild("new", "new").Obj)
		}
	}()
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, s.GetObject(child.Obj.Named()), child.Obj)
	assert.Equal(t, s.GetArtifact(child.Obj.Named(), ArtifactContent).(*SourceArtifact).Key, "old")
	assert.Equal(t, s.GetObject(emissionChild("new", "new").Obj.Named()) == nil, true)
}

func testReadRegistrationNoABAExhaustion(t *testing.T) {
	t.Helper()

	s := New()
	a, b := emissionChild("exhausted", "a"), emissionChild("exhausted", "b")
	s.AddObject(a.Obj)
	sh := s.shardFor(a.Obj.Named())
	sh.mu.Lock()
	cell := sh.cells[a.Obj.Named()]
	cell.generation = math.MaxUint64
	sh.cells[a.Obj.Named()] = cell
	sh.mu.Unlock()
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	reads.GetObject(a.Obj.Named())
	defer reads.Retire()
	func() {
		defer func() {
			{
				assert.ErrorIf(t, recover() == nil, "%s", "exhaustion did not fail before mutation")
			}
		}()
		s.AddObject(b.Obj)
	}()
	assert.Equal(t, s.GetObject(a.Obj.Named()), a.Obj)
	assert.Equal(t, headsForTest(s, a.Obj.Named()).object, uint64(math.MaxUint64))
	assert.Equal(t, reads.Valid(), true)
}

func testFeasibilityPointerOnlyExistingSectionCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(p)
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{emissionChild("cut", "a")}, Complete: true}, nil)
	tasks := task.NewBounded(2)
	captured, replaced := make(chan struct{}), make(chan struct{})
	tasks.Go(t.Context(), "retry", func(context.Context) {
		sh := s.shardFor(p.Named())
		sh.mu.RLock()
		slot := sh.slots[p.Named()]
		previous, generation := slot.current.Load(), slot.generation
		sh.mu.RUnlock()
		_, _, proposed := s.GetEmission(p.Named())
		defer proposed.Finish()
		if (*testCaseInput) == "stable" {
			proposed.Children = []Child{emissionChild("cut", "proposed")}
		} else {
			proposed.Children = append(slices.Clone(proposed.Children), emissionChild("new", "b"))
		}
		prepared := make([]admission, len(proposed.Children))
		for i, child := range proposed.Children {
			prepared[i] = admission{child: child, targets: manifest.ProducerTargets(child.Obj)}
		}
		proposal := prepareEmission(
			&proposed,
			previous,
			prepared,
			make(map[manifest.NamedResource]int, len(prepared)),
		)
		close(captured)
		tasks.YieldSlot(func() { <-replaced })
		var result publicationResult
		if (*testCaseInput) == "stable" {
			result = s.commitStable(slot, previous, proposal, generation)
		} else {
			result = s.commitReplacement(p.Named(), previous, proposal, generation)
		}
		assert.Equal(t, result, publicationRetry)
	})
	changed := emissionChild("cut", "changed")
	tasks.Go(t.Context(), "replace", func(context.Context) {
		defer close(replaced)
		tasks.YieldSlot(func() { <-captured })
		s.RecordEmitted(p.Named(), &Emission{Children: []Child{changed}, Complete: true}, nil)
	})
	tasks.BlockTillDone()
	{
		assert.FatalIf(t, tasks.Failures() != 0, "%s", "publication task panicked")
	}
	_, _, retained := s.GetEmission(p.Named())
	defer retained.Finish()
	{
		assert.FatalIf(
			t,
			!slices.EqualFunc(retained.Children, []Child{changed}, childEqual),
			"%s",
			"retry changed the accepted replacement children",
		)
	}
	{
		assert.FatalIf(
			t,
			s.GetObject(emissionChild("new", "b").Obj.Named()) != nil,
			"%s",
			"retry installed a proposed target",
		)
	}
}
