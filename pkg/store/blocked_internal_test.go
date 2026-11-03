package store

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestUpdateFailure_SnapshotIsolation(t *testing.T) {
	st := New()
	id := testutil.HelmReleaseID("", "consumer")
	a := testutil.HelmReleaseID("", "a")
	b := testutil.HelmReleaseID("", "b")
	deps := []manifest.NamedResource{b, a}
	err := fmt.Errorf("reconcile: %w", &manifest.DependencyFailedError{Parent: id, Failed: deps})
	st.UpdateFailure(id, err)
	info, ok := st.GetStatus(id)
	{
		assert.FatalIf(
			t,
			!ok || info.Status != StatusFailed || info.Reason != ReasonFailed || info.String() != err.Error() ||
				!info.IsDependencyFailure(),
			"failure snapshot = %+v, present = %v",
			info,
			ok,
		)
	}
	{
		allocs := testing.AllocsPerRun(100, func() {
			getStatusInfo, _ := st.GetStatus(id)
			_ = getStatusInfo.IsDependencyFailure()
		})
		assert.FatalIf(t, allocs != 0, "failure snapshot/predicate allocated %g times", allocs)
	}
	deps[0].Name = "mutated"
	got := info.BlockedBy()
	{
		assert.FatalIf(t, !slices.Equal(got, []manifest.NamedResource{a, b}), "caller mutated blockers: %v", got)
	}
	got[0].Name = "mutated again"
	{
		assert.FatalIf(t, info.BlockedBy()[0] != a, "%v", "reader mutated blockers")
	}
	st.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: "done"})
	{
		assert.FatalIf(t, !slices.Equal(info.BlockedBy(), []manifest.NamedResource{a, b}), "%v", "later transition mutated the retained snapshot")
	}
	current, _ := st.GetStatus(id)
	{
		assert.FatalIf(
			t,
			current.Reason != ReasonSucceeded || current.IsDependencyFailure() || current.BlockedBy() != nil,
			"Ready retains failure provenance: %+v",
			current,
		)
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
		{
			"primary error", func(st *Store) { st.UpdateFailure(id, manifest.ErrInput) },
			StatusFailed, metav1.ConditionFalse, ReasonFailed, manifest.ErrInput.Error(), false,
		},
		{
			"empty dependency error", func(st *Store) { st.UpdateFailure(id, empty) },
			StatusFailed, metav1.ConditionFalse, ReasonFailed, empty.Error(), false,
		},
		{
			"primary status", func(st *Store) { st.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "primary"}) },
			StatusFailed, metav1.ConditionFalse, ReasonFailed, "primary", false,
		},
		{
			"ready", func(st *Store) { st.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: "done"}) },
			StatusReady, metav1.ConditionTrue, ReasonSucceeded, "done", false,
		},
		{
			"pending", func(st *Store) {
				st.UpdateStatus(id, StatusInfo{Status: StatusPending, Reason: "", Message: "retrying"})
			},
			StatusPending, metav1.ConditionUnknown, ReasonReconciling, "retrying", false,
		},
		{"Ready condition", func(st *Store) {
			st.SetCondition(id, readyCondition(StatusFailed, "", "primary"))
		}, StatusFailed, metav1.ConditionFalse, ReasonFailed, "primary", false},
		{"Healthy condition", func(st *Store) {
			st.SetCondition(id, Condition{Type: ConditionHealthy, Status: metav1.ConditionTrue})
		}, StatusFailed, metav1.ConditionFalse, ReasonFailed, failure.Error(), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testUpdateFailureTransitionsCase(
				t,
				&testUpdateFailureTransitionsCaseInputs{id: &id, dep: &dep, failure: &failure, tc: &tc},
			)
		},
		)
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
	{
		assert.FatalIf(
			t,
			failed.Reason != ReasonFailed || !failed.IsDependencyFailure() ||
				!slices.Equal(failed.BlockedBy(), err.Failed),
			"%s",
			"FailedResources lost dependency provenance",
		)
	}
	{
		assert.FatalIf(t, !st.DeleteObject(id), "%s", "delete did not remove the object")
	}
	st.AddObject(obj)
	{
		_, ok := st.GetStatus(id)
		assert.FatalIf(t, ok, "%s", "re-added object inherited the deleted failure record")
	}
	st.DeleteObject(id)
	st.UpdateFailure(id, err)
	{
		assert.FatalIf(
			t,
			len(st.FailedResources()) != 0,
			"%s",
			"late terminal write resurrected an objectless failed resource",
		)
	}
	{
		info, ok := st.GetStatus(id)
		assert.FatalIf(
			t,
			!ok || !info.IsDependencyFailure(),
			"%s",
			"status-before-object support lost the late failure write",
		)
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
	initial, _ := st.GetStatus(id)
	st.UpdateFailure(
		id,
		&manifest.DependencyFailedError{
			Parent: id,
			Failed: []manifest.NamedResource{a, b},
			Reasons: map[manifest.NamedResource]manifest.DiagnosticReason{
				a: {Subject: manifest.DiagnosticFor(a, nil), Message: "nested propagation"},
			},
		},
	)
	{
		current, _ := st.GetStatus(id)
		assert.FatalIf(t, current != initial, "%s", "unchanged blockers replaced the first diagnostic")
	}
	{
		assert.FatalIf(t, len(events) != 1, "identical condition/provenance dispatched %d events", len(events))
	}
	write(b)
	{
		assert.FatalIf(
			t,
			len(events) != 2 || !slices.Equal(events[1].BlockedBy(), []manifest.NamedResource{b}),
			"blocker-only transition = %+v",
			events,
		)
	}
	var replay StatusInfo
	st.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		replay = payload.(StatusInfo)
	}, true)
	{
		assert.FatalIf(t, replay.Reason != ReasonFailed || !replay.IsDependencyFailure() ||
			!slices.Equal(replay.BlockedBy(), []manifest.NamedResource{b}), "replay snapshot = %+v", replay)
	}
	st.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "dependencies failed"})
	{
		assert.FatalIf(
			t,
			len(events) != 3 || events[2].IsDependencyFailure() || events[2].BlockedBy() != nil,
			"provenance-only clear did not dispatch: %+v",
			events,
		)
	}
	{
		assert.FatalIf(
			t,
			!slices.Equal(events[0].BlockedBy(), []manifest.NamedResource{a, b}),
			"%s",
			"listener or later event mutated an earlier payload",
		)
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
		subject := manifest.DiagnosticIdentity{
			ID:       dep,
			Resource: testutil.KustomizationID("flux-system", "sync"),
			Path:     "clusters/test",
		}
		errs[i] = &manifest.DependencyFailedError{
			Parent: id,
			Failed: []manifest.NamedResource{dep},
			Reasons: map[manifest.NamedResource]manifest.DiagnosticReason{
				dep: {
					Subject: subject,
					Causes:  []manifest.DiagnosticReason{{Subject: subject, Message: "nested failure"}},
				},
			},
		}
	}
	check := func(info StatusInfo) {
		if info.Status != StatusFailed {
			{
				assert.ErrorIf(
					t,
					info.Reason != StatusReason("CustomPending") || info.IsDependencyFailure() ||
						info.BlockedBy() != nil,
					"%s",
					"ordinary snapshot retained blockers",
				)
			}
			return
		}
		if info.Reason != ReasonFailed || !info.IsDependencyFailure() {
			t.Errorf("terminal snapshot lost dependency provenance: %+v", info)
			return
		}
		for _, err := range errs {
			if info.String() == err.Error() && slices.Equal(info.BlockedBy(), err.Failed) {
				reason := info.DependencyReason(manifest.DiagnosticFor(id, obj))
				if len(reason.Causes) != 1 || len(reason.Causes[0].Causes) != 1 {
					t.Error("concurrent snapshot lost nested context")
					return
				}
				reason.Causes[0].Causes[0].Message = "caller mutation"
				{
					assert.ErrorIf(t, strings.Contains(
						info.DependencyReason(manifest.DiagnosticFor(id, obj)).String(),
						"caller mutation",
					), "%s", "concurrent snapshot exposed retained slices")
				}
				return
			}
		}
		t.Errorf("incoherent terminal snapshot: %+v, blockers = %v", info, info.BlockedBy())
	}
	testUpdateFailureConcurrentSnapshotsAndEventsCompletion(t, &testUpdateFailureConcurrentSnapshotsAndEventsCompletionState{st: st, obj: obj, id: &id, errs: &errs, check: &check})
}

func TestUpdateFailure_NestedSyntheticSnapshots(t *testing.T) {
	t.Parallel()
	s := New()
	applier := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      "FluxInstance",
		Namespace: "flux-system",
		Name:      "sync",
	}
	root := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", applier)
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "sources"}
	repo := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	for _, obj := range []manifest.BaseManifest{root, repo, consumer} {
		s.AddObject(obj)
	}
	var events []manifest.NamedResource
	var retained []StatusInfo
	defer s.AddListener(EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		events = append(events, id)
		retained = append(retained, payload.(StatusInfo))
	}, false)()
	s.UpdateFailure(root.Named(), manifest.NewError(manifest.ErrInput, nil, "invalid package"))
	rootInfo, _ := s.GetStatus(root.Named())
	inner := &manifest.DependencyFailedError{
		Parent:  repo.Named(),
		Subject: manifest.DiagnosticFor(repo.Named(), repo),
		Failed:  []manifest.NamedResource{root.Named()},
		Reasons: map[manifest.NamedResource]manifest.DiagnosticReason{
			root.Named(): rootInfo.DependencyReason(manifest.DiagnosticFor(root.Named(), root)),
		},
	}
	s.UpdateFailure(repo.Named(), inner)
	middle, _ := s.GetStatus(repo.Named())
	outer := &manifest.DependencyFailedError{
		Parent: consumer.Named(),
		Failed: []manifest.NamedResource{repo.Named()},
		Reasons: map[manifest.NamedResource]manifest.DiagnosticReason{
			repo.Named(): middle.DependencyReason(manifest.DiagnosticFor(repo.Named(), repo)),
		},
	}
	s.UpdateFailure(consumer.Named(), outer)
	snapshot, _ := s.GetStatus(consumer.Named())
	if !strings.Contains(snapshot.String(), chart.Named().String()) ||
		!strings.Contains(snapshot.String(), applier.String()) ||
		!strings.Contains(snapshot.String(), `entry path "clusters/test"`) ||
		strings.Contains(snapshot.String(), root.Named().String()) ||
		strings.Contains(snapshot.String(), repo.Named().String()) {
		t.Fatalf("nested labels lost context: %s", snapshot.String())
	}
	{
		assert.FatalIf(
			t,
			!slices.Equal(snapshot.BlockedBy(), []manifest.NamedResource{repo.Named()}),
			"%s",
			"display changed blockers",
		)
	}
	detached := snapshot.DependencyReason(manifest.DiagnosticFor(consumer.Named(), consumer))
	detached.Causes[0].Causes[0].Message = "mutated"
	outer.Reasons[repo.Named()].Causes[0].Message = "caller mutation"
	again := snapshot.DependencyReason(manifest.DiagnosticFor(consumer.Named(), consumer))
	{
		assert.FatalIf(
			t,
			strings.Contains(again.String(), "mutation") || strings.Contains(again.String(), "mutated"),
			"%s",
			"nested snapshot aliases exposed causes",
		)
	}
	before := len(events)
	s.UpdateFailure(consumer.Named(), outer)
	{
		assert.FatalIf(t, len(events) != before, "%s", "unchanged blockers did not retain first reason")
	}
	{
		assert.FatalIf(
			t,
			!slices.Equal(events, []manifest.NamedResource{root.Named(), repo.Named(), consumer.Named()}),
			"event identities changed: %v",
			events,
		)
	}
	{
		assert.FatalIf(
			t,
			retained[2].DependencyReason(manifest.DiagnosticFor(consumer.Named(), consumer)).String() != again.String(),
			"%s",
			"event snapshot changed",
		)
	}
}

type testUpdateFailureTransitionsCaseInputs struct {
	id      *manifest.NamedResource
	dep     *manifest.NamedResource
	failure **manifest.DependencyFailedError
	tc      *struct {
		name    string
		write   func(*Store)
		status  Status
		ready   metav1.ConditionStatus
		reason  string
		message string
		failed  bool
	}
}

func testUpdateFailureTransitionsCase(t *testing.T, testCaseInput *testUpdateFailureTransitionsCaseInputs) {
	t.Helper()

	st := New()
	hr := &manifest.HelmRelease{Name: testCaseInput.id.Name}
	st.AddObject(hr)
	st.UpdateFailure((*testCaseInput.id), (*testCaseInput.failure))
	testCaseInput.tc.write(st)
	info, ok := st.GetStatus((*testCaseInput.id))
	{
		assert.FatalIf(
			t,
			!ok || info.Status != testCaseInput.tc.status || info.Reason != StatusReason(testCaseInput.tc.reason) ||
				info.String() != testCaseInput.tc.message ||
				info.IsDependencyFailure() != testCaseInput.tc.failed,
			"transition snapshot = %+v, present = %v",
			info,
			ok,
		)
	}
	if testCaseInput.tc.failed {
		if !slices.Equal(info.BlockedBy(), []manifest.NamedResource{(*testCaseInput.dep)}) {
			t.Fatalf("Healthy update lost blockers: %v", info.BlockedBy())
		}
	} else if info.BlockedBy() != nil {
		t.Fatalf("transition retained blockers: %v", info.BlockedBy())
	}
	var initial Observation
	obj, initial := st.Snapshot((*testCaseInput.id), true)
	snapshot := initial.StatusInfo
	conds := initial.Conditions
	{
		assert.FatalIf(
			t,
			obj != hr || snapshot != info,
			"snapshot object/status = %v/%+v, want %v/%+v",
			obj,
			snapshot,
			hr,
			info,
		)
	}
	var uncopiedObservation Observation
	uncopied, uncopiedObservation := st.Snapshot((*testCaseInput.id), false)
	status := uncopiedObservation.StatusInfo
	omitted := uncopiedObservation.Conditions
	{
		assert.FatalIf(
			t,
			uncopied != obj || status != snapshot || omitted != nil,
			"unrequested snapshot = %v/%+v/%v",
			uncopied,
			status,
			omitted,
		)
	}
	want := []Condition{
		{
			Type:    ConditionReady,
			Status:  testCaseInput.tc.ready,
			Reason:  testCaseInput.tc.reason,
			Message: testCaseInput.tc.message,
		},
	}
	if testCaseInput.tc.name == "Healthy condition" {
		want = append(want, Condition{Type: ConditionHealthy, Status: metav1.ConditionTrue})
	}
	{
		assert.FatalIf(t, !slices.Equal(conds, want), "conditions = %+v, want %+v", conds, want)
	}
	conds[0].Message = "caller mutation"
	var retainedObservation Observation
	_, retainedObservation = st.Snapshot((*testCaseInput.id), true)
	retained := retainedObservation.StatusInfo
	copied := retainedObservation.Conditions
	{
		assert.FatalIf(
			t,
			retained != info || !slices.EqualFunc(copied, want, conditionEqual),
			"%s",
			"caller mutated snapshot storage",
		)
	}
}

type testUpdateFailureConcurrentSnapshotsAndEventsCompletionState struct {
	st    *Store
	obj   *manifest.HelmRelease
	id    *manifest.NamedResource
	errs  *[]*manifest.DependencyFailedError
	check *func(info StatusInfo)
}

func testUpdateFailureConcurrentSnapshotsAndEventsCompletion(t *testing.T, scope *testUpdateFailureConcurrentSnapshotsAndEventsCompletionState) {
	t.Helper()
	scope.st.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		(*scope.check)(payload.(StatusInfo))
	}, false)
	arrived := make(chan StatusInfo, 2)
	release := make(chan struct{})
	scope.st.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		info := payload.(StatusInfo)
		if info.Status == StatusFailed {
			arrived <- info
			<-release
		}
	}, false)
	var wg sync.WaitGroup
	for _, err := range *scope.errs {
		wg.Go(func() { scope.st.UpdateFailure((*scope.id), err) })
	}
	first, second := <-arrived, <-arrived
	(*scope.check)(first)
	(*scope.check)(second)
	info, _ := scope.st.GetStatus((*scope.id))
	(*scope.check)(info)
	var initial Observation
	observed, initial := scope.st.Snapshot((*scope.id), true)
	snapshot := initial.StatusInfo
	conditions := initial.Conditions
	(*scope.check)(snapshot)
	{
		assert.FatalIf(t, observed != scope.obj || len(conditions) != 1 || conditions[0].Message != snapshot.Message ||
			conditions[0].Reason != string(
				snapshot.Reason,
			), "incoherent object/status/conditions: %v/%+v/%+v", observed, snapshot, conditions)
	}
	for _, info := range scope.st.FailedResources() {
		(*scope.check)(info)
	}
	scope.st.UpdateStatus((*scope.id), StatusInfo{Status: StatusPending, Reason: "CustomPending", Message: "reassessing"})
	(*scope.check)(first)
	(*scope.check)(second)
	info, _ = scope.st.GetStatus((*scope.id))
	(*scope.check)(info)
	var updated Observation
	_, updated = scope.st.Snapshot((*scope.id), true)
	snapshot = updated.StatusInfo
	conditions = updated.Conditions
	(*scope.check)(snapshot)
	{
		assert.FatalIf(t, len(conditions) != 1 || conditions[0].Status != metav1.ConditionUnknown ||
			conditions[0].Message != snapshot.Message, "refire snapshot mismatch: %+v/%+v", snapshot, conditions)
	}
	close(release)
	wg.Wait()
}
