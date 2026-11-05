package store

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

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
		childValues := proposed.Children
		for i := range childValues {
			child := childValues[i]
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
	assert.FatalIf(t, tasks.Failures() != 0, "%s", "publication task panicked")
	_, _, retained := s.GetEmission(p.Named())
	defer retained.Finish()

	assert.FatalIf(
		t,
		!slices.EqualFunc(retained.Children, []Child{changed}, Child.equal),
		"%s",
		"retry changed the accepted replacement children",
	)

	assert.FatalIf(
		t,
		s.GetObject(emissionChild("new", "b").Obj.Named()) != nil,
		"%s",
		"retry installed a proposed target",
	)
}
