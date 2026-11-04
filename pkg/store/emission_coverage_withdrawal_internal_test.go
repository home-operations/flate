package store

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func TestRecordEmitted_NeverEmittedCoverageWithdrawal(t *testing.T) {
	t.Parallel()

	seedRetained := func(t *testing.T, s *Store, id manifest.NamedResource) {
		t.Helper()
		s.SetArtifact(id, &SourceArtifact{Key: "static-l"})
		s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: ""})
		{
			assert.FatalIf(
				t,
				s.GetObject(id) == nil || s.GetArtifact(id, ArtifactContent) == nil,
				"%s",
				"fixture L not initially live with an artifact",
			)
		}
		{
			info, ok := s.GetStatus(id)
			assert.FatalIf(t, !ok || info.Status != StatusReady, "%s", "fixture L not initially Ready")
		}
	}
	fixture := func(t *testing.T, kName, lName string, reverse bool) (*Store, manifest.NamedResource, manifest.NamedResource, Child) {
		t.Helper()
		s := New()
		p, k := ownerID("p"), ownerID(kName)
		parent := &manifest.Kustomization{Name: p.Name, Namespace: p.Namespace}
		l := emissionChild(lName, "value")
		if reverse {
			s.AddObject(l.Obj)
			s.AddObject(parent)
		} else {
			s.AddObject(parent)
			s.AddObject(l.Obj)
		}
		s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			l.Obj.Named(): {
				Productions: []manifest.Production{
					{
						Owner:    k,
						Source:   manifest.BootstrapSourceID,
						Authored: l.Obj.Named(),
						Path:     "l.yaml",
						File:     "l.yaml",
					},
				},
			},
		}})
		s.RecordEmitted(
			p,
			&Emission{
				Children: []Child{{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}},
				Complete: true,
			},
			nil,
		)
		{
			assert.FatalIf(t, s.GetObject(k) == nil ||
				!slices.ContainsFunc(
					s.ListObjects("", ""),
					func(obj manifest.BaseManifest) bool { return obj.Named() == l.Obj.Named() },
				), "%s", "fixture K/L not initially visible")
		}
		seedRetained(t, s, l.Obj.Named())
		{
			assert.FatalIf(t, s.shardFor(k).slots[k].current.Load() != nil, "%s", "fixture K unexpectedly emitted")
		}
		return s, p, k, l
	}
	checkHidden := func(t *testing.T, s *Store, id manifest.NamedResource) {
		t.Helper()
		{
			assert.ErrorIf(t, s.GetObject(id) != nil, "%v object stays visible after withdrawal", id)
		}
		{
			assert.ErrorIf(t, slices.ContainsFunc(
				s.ListObjects("", ""),
				func(obj manifest.BaseManifest) bool { return obj.Named() == id },
			), "%v stays listed after withdrawal", id)
		}
		{
			assert.ErrorIf(t, s.GetArtifact(id, ArtifactContent) != nil, "%v artifact stays live after withdrawal", id)
		}
		{
			_, ok := s.GetStatus(id)
			assert.ErrorIf(t, ok, "%v status stays live after withdrawal", id)
		}
		sh := s.shardFor(id)
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		var view capture
		sh.captureLocked(id, new(sh.cells[id]), &view)
		child := view.child
		var owner manifest.NamedResource
		if view.owner != nil {
			owner = view.owner.id
		}
		{
			assert.ErrorIf(
				t,
				!sh.cells[id].hidden || !sh.cells[id].retired || child.Obj != nil ||
					owner != (manifest.NamedResource{}),
				"%v hidden, retirement and representative disagree",
				id,
			)
		}
		{
			slot := sh.slots[id]
			assert.ErrorIf(t, slot != nil && (slot.visible.Load() || !slot.hidden.Load()), "%v slot remains live", id)
		}
	}
	t.Run("simple", func(t *testing.T) {
		t.Parallel()
		testRecordEmittedNeverEmittedCoverageWithdrawalSimple(
			t,
			&testRecordEmittedNeverEmittedCoverageWithdrawalSimpleInputs{
				seedRetained: &seedRetained,
				fixture:      &fixture,
				checkHidden:  &checkHidden,
			},
		)
	},
	)
	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers=%d/reverse=%t", workers, reverse), func(t *testing.T) {
				t.Parallel()
				testRecordEmittedNeverEmittedCoverageWithdrawalCase(
					t,
					&testRecordEmittedNeverEmittedCoverageWithdrawalCaseInputs{
						fixture:     &fixture,
						checkHidden: &checkHidden,
						workers:     &workers,
						reverse:     &reverse,
					},
				)
			},
			)
		}
	}
}

type testRecordEmittedNeverEmittedCoverageWithdrawalSimpleInputs struct {
	seedRetained *func(t *testing.T, s *Store, id manifest.NamedResource)
	fixture      *func(t *testing.T, kName string, lName string, reverse bool) (*Store, manifest.NamedResource, manifest.NamedResource, Child)
	checkHidden  *func(t *testing.T, s *Store, id manifest.NamedResource)
}

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
