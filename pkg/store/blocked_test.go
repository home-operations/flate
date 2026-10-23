package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestUpdateFailure_SnapshotIsolation(t *testing.T) {
	st := New()
	id := testutil.HelmReleaseID("", "consumer")
	a := testutil.HelmReleaseID("", "a")
	b := testutil.HelmReleaseID("", "b")
	deps := []manifest.NamedResource{b, a}
	err := fmt.Errorf("reconcile: %w", &manifest.DependencyFailedError{Parent: id, Failed: deps})
	st.UpdateFailure(id, err)
	info, ok := st.GetStatus(id)
	if !ok || info.Status != StatusFailed || info.Reason != ReasonFailed || info.Message != err.Error() || !info.IsDependencyFailure() {
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
	st.UpdateStatus(id, StatusReady, "", "done")
	if !slices.Equal(info.BlockedBy(), []manifest.NamedResource{a, b}) {
		t.Fatal("later transition mutated the retained snapshot")
	}
	current, _ := st.GetStatus(id)
	if current.Reason != ReasonSucceeded || current.IsDependencyFailure() || current.BlockedBy() != nil {
		t.Fatalf("Ready retains failure provenance: %+v", current)
	}
}

func TestUpdateFailure_Transitions(t *testing.T) {
	t.Parallel()
	id := testutil.HelmReleaseID("", "consumer")
	dep := testutil.HelmReleaseID("", "dep")
	failure := &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{dep}}
	empty := &manifest.DependencyFailedError{Parent: id}
	for _, tc := range []struct {
		name    string
		write   func(*Store)
		status  Status
		ready   metav1.ConditionStatus
		reason  string
		message string
		failed  bool
	}{
		{"primary error", func(st *Store) { st.UpdateFailure(id, manifest.ErrInput) },
			StatusFailed, metav1.ConditionFalse, ReasonFailed, manifest.ErrInput.Error(), false},
		{"empty dependency error", func(st *Store) { st.UpdateFailure(id, empty) },
			StatusFailed, metav1.ConditionFalse, ReasonFailed, empty.Error(), false},
		{"primary status", func(st *Store) { st.UpdateStatus(id, StatusFailed, "", "primary") },
			StatusFailed, metav1.ConditionFalse, ReasonFailed, "primary", false},
		{"ready", func(st *Store) { st.UpdateStatus(id, StatusReady, "", "done") },
			StatusReady, metav1.ConditionTrue, ReasonSucceeded, "done", false},
		{"pending", func(st *Store) { st.UpdateStatus(id, StatusPending, "", "retrying") },
			StatusPending, metav1.ConditionUnknown, ReasonReconciling, "retrying", false},
		{"Ready condition", func(st *Store) {
			st.SetCondition(id, readyCondition(StatusFailed, "", "primary"))
		}, StatusFailed, metav1.ConditionFalse, ReasonFailed, "primary", false},
		{"Healthy condition", func(st *Store) {
			st.SetCondition(id, Condition{Type: ConditionHealthy, Status: metav1.ConditionTrue})
		}, StatusFailed, metav1.ConditionFalse, ReasonFailed, failure.Error(), true},
		{"refire", func(st *Store) { st.Refire(id) },
			StatusPending, metav1.ConditionUnknown, string(ReasonRefetching), "re-fetching", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			hr := &manifest.HelmRelease{Name: id.Name}
			st.AddObject(hr)
			st.UpdateFailure(id, failure)
			if tc.name == "refire" {
				st.AddListener(EventObjectAdded, func(eventID manifest.NamedResource, _ any) {
					info, _ := st.GetStatus(eventID)
					if info.Status != StatusPending || info.Reason != ReasonRefetching || info.IsDependencyFailure() || info.BlockedBy() != nil {
						t.Errorf("Refire object listener saw stale terminal state: %+v", info)
					}
				}, false)
			}
			tc.write(st)
			info, ok := st.GetStatus(id)
			if !ok || info.Status != tc.status || info.Reason != StatusReason(tc.reason) || info.Message != tc.message || info.IsDependencyFailure() != tc.failed {
				t.Fatalf("transition snapshot = %+v, present = %v", info, ok)
			}
			if tc.failed {
				if !slices.Equal(info.BlockedBy(), []manifest.NamedResource{dep}) {
					t.Fatalf("Healthy update lost blockers: %v", info.BlockedBy())
				}
			} else if info.BlockedBy() != nil {
				t.Fatalf("transition retained blockers: %v", info.BlockedBy())
			}
			var initial Observation
			obj := st.Snapshot(&id, true, &initial)
			snapshot := initial.StatusInfo
			conds := initial.Conditions
			if obj != hr || snapshot != info {
				t.Fatalf("snapshot object/status = %v/%+v, want %v/%+v", obj, snapshot, hr, info)
			}
			var uncopiedObservation Observation
			uncopied := st.Snapshot(&id, false, &uncopiedObservation)
			status := uncopiedObservation.StatusInfo
			omitted := uncopiedObservation.Conditions
			if uncopied != obj || status != snapshot || omitted != nil {
				t.Fatalf("unrequested snapshot = %v/%+v/%v", uncopied, status, omitted)
			}
			want := []Condition{{Type: ConditionReady, Status: tc.ready, Reason: tc.reason, Message: tc.message}}
			if tc.name == "Healthy condition" {
				want = append(want, Condition{Type: ConditionHealthy, Status: metav1.ConditionTrue})
			}
			if !slices.EqualFunc(conds, want, conditionEqual) {
				t.Fatalf("conditions = %+v, want %+v", conds, want)
			}
			conds[0].Message = "caller mutation"
			var retainedObservation Observation
			st.Snapshot(&id, true, &retainedObservation)
			retained := retainedObservation.StatusInfo
			copied := retainedObservation.Conditions
			if retained != info || !slices.EqualFunc(copied, want, conditionEqual) {
				t.Fatal("caller mutated snapshot storage")
			}
		})
	}
}

func TestUpdateFailure_DeleteAndLateWrite(t *testing.T) {
	t.Parallel()
	st := New()
	obj := &manifest.HelmRelease{Name: "consumer"}
	id := obj.Named()
	dep := testutil.HelmReleaseID("", "dep")
	err := &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{dep}}
	st.AddObject(obj)
	st.UpdateFailure(id, err)
	failed := st.FailedResources()[id]
	if failed.Reason != ReasonFailed || !failed.IsDependencyFailure() || !slices.Equal(failed.BlockedBy(), err.Failed) {
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
	t.Parallel()
	st := New()
	id := testutil.HelmReleaseID("", "consumer")
	a := testutil.HelmReleaseID("", "a")
	b := testutil.HelmReleaseID("", "b")
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
	if replay.Reason != ReasonFailed || !replay.IsDependencyFailure() || !slices.Equal(replay.BlockedBy(), []manifest.NamedResource{b}) {
		t.Fatalf("replay snapshot = %+v", replay)
	}
	st.UpdateStatus(id, StatusFailed, "", "dependencies failed")
	if len(events) != 3 || events[2].IsDependencyFailure() || events[2].BlockedBy() != nil {
		t.Fatalf("provenance-only clear did not dispatch: %+v", events)
	}
	if !slices.Equal(events[0].BlockedBy(), []manifest.NamedResource{a, b}) {
		t.Fatal("listener or later event mutated an earlier payload")
	}
}

func TestUpdateFailure_ConcurrentSnapshotsAndEvents(t *testing.T) {
	t.Parallel()
	st := New()
	obj := &manifest.HelmRelease{Name: "consumer"}
	id := obj.Named()
	st.AddObject(obj)
	errs := make([]*manifest.DependencyFailedError, 2)
	for i := range errs {
		dep := testutil.HelmReleaseID("", fmt.Sprintf("dep-%d", i))
		errs[i] = &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{dep}}
	}
	check := func(info StatusInfo) {
		if info.Status != StatusFailed {
			if info.Reason != ReasonRefetching || info.IsDependencyFailure() || info.BlockedBy() != nil {
				t.Error("ordinary snapshot retained blockers")
			}
			return
		}
		if info.Reason != ReasonFailed || !info.IsDependencyFailure() {
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
	arrived := make(chan StatusInfo, 2)
	release := make(chan struct{})
	st.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		info := payload.(StatusInfo)
		if info.Status == StatusFailed {
			arrived <- info
			<-release
		}
	}, false)
	var wg sync.WaitGroup
	for _, err := range errs {
		wg.Go(func() { st.UpdateFailure(id, err) })
	}
	first, second := <-arrived, <-arrived
	check(first)
	check(second)
	info, _ := st.GetStatus(id)
	check(info)
	var initial Observation
	observed := st.Snapshot(&id, true, &initial)
	snapshot := initial.StatusInfo
	conditions := initial.Conditions
	check(snapshot)
	if observed != obj || len(conditions) != 1 || conditions[0].Message != snapshot.Message || conditions[0].Reason != string(snapshot.Reason) {
		t.Fatalf("incoherent object/status/conditions: %v/%+v/%+v", observed, snapshot, conditions)
	}
	for _, info := range st.FailedResources() {
		check(info)
	}
	st.Refire(id)
	check(first)
	check(second)
	info, _ = st.GetStatus(id)
	check(info)
	var updated Observation
	st.Snapshot(&id, true, &updated)
	snapshot = updated.StatusInfo
	conditions = updated.Conditions
	check(snapshot)
	if len(conditions) != 1 || conditions[0].Status != metav1.ConditionUnknown || conditions[0].Message != snapshot.Message {
		t.Fatalf("refire snapshot mismatch: %+v/%+v", snapshot, conditions)
	}
	close(release)
	wg.Wait()
}
