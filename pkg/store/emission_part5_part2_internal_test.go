package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func testRecordEmittedNeverEmittedCoverageWithdrawalSimple(
	t *testing.T,
	testCaseInput *testRecordEmittedNeverEmittedCoverageWithdrawalSimpleInputs,
) {
	t.Helper()

	s, p, k, l := (*testCaseInput.fixture)(t, "k", "l", false)
	s.RecordEmitted(p, &Emission{Complete: true}, nil)
	(*testCaseInput.checkHidden)(t, s, k)
	(*testCaseInput.checkHidden)(t, s, l.Obj.Named())
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("reanchor/batch=%t", batch), func(t *testing.T) {
			t.Parallel()

			s, p, k, l := (*testCaseInput.fixture)(t, "k", "l", false)
			s.RecordEmitted(p, &Emission{Complete: true}, nil)
			(*testCaseInput.checkHidden)(t, s, k)
			(*testCaseInput.checkHidden)(t, s, l.Obj.Named())
			s.SetArtifact(l.Obj.Named(), &SourceArtifact{Key: "private"})
			s.UpdateStatus(l.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
			anchor := &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}
			if batch {
				s.AddObjects([]manifest.BaseManifest{anchor})
			} else {
				s.AddObject(anchor)
			}
			{
				assert.FatalIf(
					t,
					s.GetObject(k) != anchor || s.GetObject(l.Obj.Named()) != l.Obj,
					"%s",
					"external anchor did not restore eligible frozen descendants",
				)
			}
			{
				assert.ErrorIf(
					t,
					s.GetArtifact(l.Obj.Named(), ArtifactContent) != nil,
					"%s",
					"reanchoring restored a retained artifact",
				)
			}
			{
				_, ok := s.GetStatus(l.Obj.Named())
				assert.ErrorIf(t, ok, "%s", "reanchoring restored retained Ready")
			}
			sh := s.shardFor(l.Obj.Named())
			{
				assert.ErrorIf(
					t,
					sh.cells[l.Obj.Named()].retired || sh.cells[l.Obj.Named()].hidden,
					"%s",
					"restored frozen descendant remains retired or hidden",
				)
			}
			(*testCaseInput.seedRetained)(t, s, l.Obj.Named())
			{
				assert.FatalIf(t, !s.DeleteObject(k), "%s", "independent anchor was not removed")
			}
			(*testCaseInput.checkHidden)(t, s, k)
			(*testCaseInput.checkHidden)(t, s, l.Obj.Named())
		})
	}
}

type testRecordEmittedNeverEmittedCoverageWithdrawalCaseInputs struct {
	fixture     *func(t *testing.T, kName string, lName string, reverse bool) (*Store, manifest.NamedResource, manifest.NamedResource, Child)
	checkHidden *func(t *testing.T, s *Store, id manifest.NamedResource)
	workers     *int
	reverse     *bool
}

func testRecordEmittedNeverEmittedCoverageWithdrawalCase(
	t *testing.T,
	testCaseInput *testRecordEmittedNeverEmittedCoverageWithdrawalCaseInputs,
) {
	t.Helper()

	kName, lName := "k", "l"
	if *testCaseInput.reverse {
		kName, lName = "z", "a"
	}
	s, p, k, l := (*testCaseInput.fixture)(t, kName, lName, (*testCaseInput.reverse))
	tasks := task.NewBounded((*testCaseInput.workers))
	captured, observing, withdrawn := make(chan struct{}), make(chan struct{}), make(chan struct{})
	late := emissionChild("late", "private")
	tasks.Go(t.Context(), "late-publication", func(context.Context) {
		_, _, attempt := s.GetEmission(k)
		defer attempt.Finish()
		attempt.Children, attempt.Complete = []Child{late}, true
		close(captured)
		tasks.YieldSlot(func() { <-withdrawn })
		(*testCaseInput.checkHidden)(t, s, l.Obj.Named())
		s.RecordEmitted(k, &attempt, nil)
		s.SetArtifact(l.Obj.Named(), &SourceArtifact{Key: "late-private"})
		s.UpdateStatus(l.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	})
	tasks.Go(t.Context(), "withdrawal", func(context.Context) {
		tasks.YieldSlot(func() { <-captured; <-observing })
		s.RecordEmitted(p, &Emission{Complete: true}, nil)
		close(withdrawn)
	})
	tasks.Go(t.Context(), "observer", func(context.Context) {
		close(observing)
		for {
			select {
			case <-withdrawn:
				(*testCaseInput.checkHidden)(t, s, l.Obj.Named())
				return
			default:
				s.GetObject(l.Obj.Named())
				s.GetArtifact(l.Obj.Named(), ArtifactContent)
				s.GetStatus(l.Obj.Named())
			}
		}
	})
	tasks.BlockTillDone()
	{
		assert.FatalIf(t, tasks.Failures() != 0, "%s", "bounded task panicked")
	}
	for _, id := range []manifest.NamedResource{k, l.Obj.Named(), late.Obj.Named()} {
		(*testCaseInput.checkHidden)(t, s, id)
	}
	s.AddObject(&manifest.Kustomization{Name: k.Name, Namespace: k.Namespace})
	{
		assert.FatalIf(
			t,
			s.GetObject(late.Obj.Named()) != late.Obj || s.GetObject(l.Obj.Named()) != nil,
			"%s",
			"reanchoring must restore actual membership without restoring shadowed coverage",
		)
	}
	{
		assert.FatalIf(
			t,
			!s.shardFor(l.Obj.Named()).cells[l.Obj.Named()].retired,
			"%s",
			"shadowed static prediction was not retired",
		)
	}
	{
		assert.FatalIf(t, !s.DeleteObject(k), "%s", "late publisher anchor was not removed")
	}
	(*testCaseInput.checkHidden)(t, s, late.Obj.Named())
}
