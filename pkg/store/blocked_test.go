package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestUpdateFailure_SnapshotIsolation(t *testing.T) {
	st := New()
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "consumer"}
	a := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "a"}
	b := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "b"}
	deps := []manifest.NamedResource{b, a}
	err := fmt.Errorf("reconcile: %w", &manifest.DependencyFailedError{Parent: id, Failed: deps})
	st.UpdateFailure(id, err)
	info, ok := st.GetStatus(id)
	if !ok || info.Status != StatusFailed || info.Message != err.Error() || !info.IsDependencyFailure() {
		t.Fatalf("failure snapshot = %+v, present = %v", info, ok)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		info, _ := st.GetStatus(id)
		_ = info.IsDependencyFailure()
	}); allocs != 0 {
		t.Fatalf("failure snapshot/predicate allocated %g times", allocs)
	}
	deps[0].Name = "mutated"
	got := info.BlockedBy()
	if !slices.Equal(got, []manifest.NamedResource{a, b}) {
		t.Fatalf("caller mutated blockers: %v", got)
	}
	got[0].Name = "mutated again"
	if info.BlockedBy()[0] != a {
		t.Fatal("reader mutated blockers")
	}
	st.UpdateStatus(id, StatusReady, "done")
	if !slices.Equal(info.BlockedBy(), []manifest.NamedResource{a, b}) {
		t.Fatal("later transition mutated the retained snapshot")
	}
	current, _ := st.GetStatus(id)
	if current.IsDependencyFailure() || current.BlockedBy() != nil {
		t.Fatalf("Ready retains failure provenance: %+v", current)
	}
}

func TestUpdateFailure_Transitions(t *testing.T) {
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "consumer"}
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "dep"}
	for _, tc := range []struct {
		name   string
		write  func(*Store)
		status Status
		failed bool
	}{
		{"primary error", func(st *Store) { st.UpdateFailure(id, manifest.ErrInput) }, StatusFailed, false},
		{"empty dependency error", func(st *Store) {
			st.UpdateFailure(id, &manifest.DependencyFailedError{Parent: id})
		}, StatusFailed, false},
		{"primary status", func(st *Store) { st.UpdateStatus(id, StatusFailed, "primary") }, StatusFailed, false},
		{"ready", func(st *Store) { st.UpdateStatus(id, StatusReady, "done") }, StatusReady, false},
		{"pending", func(st *Store) { st.UpdateStatus(id, StatusPending, "retrying") }, StatusPending, false},
		{"Ready condition", func(st *Store) {
			st.SetCondition(id, readyCondition(StatusFailed, "primary"))
		}, StatusFailed, false},
		{"Healthy condition", func(st *Store) {
			st.SetCondition(id, Condition{Type: ConditionHealthy, Status: metav1.ConditionTrue})
		}, StatusFailed, true},
		{"refire", func(st *Store) { st.Refire(id) }, StatusPending, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := New()
			st.AddObject(&manifest.HelmRelease{Name: id.Name})
			st.UpdateFailure(id, &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{dep}})
			if tc.name == "refire" {
				st.AddListener(EventObjectAdded, func(eventID manifest.NamedResource, _ any) {
					info, _ := st.GetStatus(eventID)
					if info.Status != StatusPending || info.IsDependencyFailure() || info.BlockedBy() != nil {
						t.Errorf("Refire object listener saw stale terminal state: %+v", info)
					}
				}, false)
			}
			tc.write(st)
			info, ok := st.GetStatus(id)
			if !ok || info.Status != tc.status || info.IsDependencyFailure() != tc.failed {
				t.Fatalf("transition snapshot = %+v, present = %v", info, ok)
			}
			if tc.failed {
				if !slices.Equal(info.BlockedBy(), []manifest.NamedResource{dep}) {
					t.Fatalf("Healthy update lost blockers: %v", info.BlockedBy())
				}
			} else if info.BlockedBy() != nil {
				t.Fatalf("transition retained blockers: %v", info.BlockedBy())
			}
			obj, conds := st.Snapshot(id)
			if obj == nil || !slices.EqualFunc(conds, st.GetConditions(id), conditionEqual) {
				t.Fatal("condition snapshot disagrees with GetConditions")
			}
			for _, cond := range conds {
				if cond.Type == ConditionReady && cond.Message != info.Message {
					t.Fatal("Ready condition disagrees with status snapshot")
				}
			}
			if tc.name == "refire" && info.Message != MsgRefetching {
				t.Fatalf("Refire message = %q", info.Message)
			}
		})
	}
}

func TestUpdateFailure_DeleteAndLateWrite(t *testing.T) {
	st := New()
	obj := &manifest.HelmRelease{Name: "consumer"}
	id := obj.Named()
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "dep"}
	err := &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{dep}}
	st.AddObject(obj)
	st.UpdateFailure(id, err)
	failed := st.FailedResources()[id]
	if !failed.IsDependencyFailure() || !slices.Equal(failed.BlockedBy(), err.Failed) {
		t.Fatal("FailedResources lost dependency provenance")
	}
	if !st.DeleteObject(id) {
		t.Fatal("delete did not remove the object")
	}
	st.AddObject(obj)
	if _, ok := st.GetStatus(id); ok {
		t.Fatal("re-added object inherited the deleted failure record")
	}
	st.DeleteObject(id)
	st.UpdateFailure(id, err)
	if len(st.FailedResources()) != 0 {
		t.Fatal("late terminal write resurrected an objectless failed resource")
	}
	if info, ok := st.GetStatus(id); !ok || !info.IsDependencyFailure() {
		t.Fatal("status-before-object support lost the late failure write")
	}
}

type fixedFailureMessage struct{ cause error }

func (fixedFailureMessage) Error() string   { return "dependencies failed" }
func (e fixedFailureMessage) Unwrap() error { return e.cause }

func TestUpdateFailure_StatusEvents(t *testing.T) {
	st := New()
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "consumer"}
	a := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "a"}
	b := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "b"}
	var events []StatusInfo
	st.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		info := payload.(StatusInfo)
		events = append(events, info)
		if deps := info.BlockedBy(); len(deps) > 0 {
			deps[0].Name = "listener mutation"
		}
	}, false)
	write := func(deps ...manifest.NamedResource) {
		st.UpdateFailure(id, fixedFailureMessage{&manifest.DependencyFailedError{Parent: id, Failed: deps}})
	}
	write(b, a)
	write(a, b)
	if len(events) != 1 {
		t.Fatalf("identical condition/provenance dispatched %d events", len(events))
	}
	write(b)
	if len(events) != 2 || !slices.Equal(events[1].BlockedBy(), []manifest.NamedResource{b}) {
		t.Fatalf("blocker-only transition = %+v", events)
	}
	var replay StatusInfo
	st.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		replay = payload.(StatusInfo)
	}, true)
	if !replay.IsDependencyFailure() || !slices.Equal(replay.BlockedBy(), []manifest.NamedResource{b}) {
		t.Fatalf("replay snapshot = %+v", replay)
	}
	st.UpdateStatus(id, StatusFailed, "dependencies failed")
	if len(events) != 3 || events[2].IsDependencyFailure() || events[2].BlockedBy() != nil {
		t.Fatalf("provenance-only clear did not dispatch: %+v", events)
	}
	if !slices.Equal(events[0].BlockedBy(), []manifest.NamedResource{a, b}) {
		t.Fatal("listener or later event mutated an earlier payload")
	}
}

func TestUpdateFailure_ConcurrentSnapshotsAndEvents(t *testing.T) {
	st := New()
	obj := &manifest.HelmRelease{Name: "consumer"}
	id := obj.Named()
	st.AddObject(obj)
	errs := make([]*manifest.DependencyFailedError, 2)
	for i := range errs {
		dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: fmt.Sprintf("dep-%d", i)}
		errs[i] = &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{dep}}
	}
	check := func(info StatusInfo) {
		if info.Status != StatusFailed {
			if info.IsDependencyFailure() || info.BlockedBy() != nil {
				t.Error("ordinary snapshot retained blockers")
			}
			return
		}
		if !info.IsDependencyFailure() {
			t.Errorf("terminal snapshot lost dependency provenance: %+v", info)
			return
		}
		for _, err := range errs {
			if info.Message == err.Error() && slices.Equal(info.BlockedBy(), err.Failed) {
				return
			}
		}
		t.Errorf("incoherent terminal snapshot: %+v, blockers = %v", info, info.BlockedBy())
	}
	st.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		check(payload.(StatusInfo))
	}, false)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, err := range errs {
		wg.Go(func() {
			<-start
			for range 200 {
				st.UpdateFailure(id, err)
				st.UpdateStatus(id, StatusPending, "retrying")
			}
		})
	}
	wg.Go(func() {
		<-start
		for range 400 {
			if info, ok := st.GetStatus(id); ok {
				check(info)
			}
			for _, info := range st.FailedResources() {
				check(info)
			}
		}
	})
	close(start)
	wg.Wait()
}
