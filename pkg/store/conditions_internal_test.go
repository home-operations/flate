package store

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_SetCondition_ReadyFiresStatusEvent(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("ns", "k")
	var events []StatusInfo
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		events = append(events, payload.(StatusInfo))
	}, false)
	cond := Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: string(ReasonSkipped), Message: "ok"}
	s.SetCondition(id, cond)
	var observation Observation
	_, observation = s.Snapshot(id, true)
	snapshot := observation.Conditions
	cond.Reason = "Custom"
	s.SetCondition(id, cond)
	s.SetCondition(id, cond)
	assert.Equal(t, len(events), 2)
	assert.Equal(t, events[0].Reason, ReasonSkipped)
	assert.Equal(t, events[1].Reason, StatusReason("Custom"))
	info, ok := s.GetStatus(id)
	{
		assert.FatalIf(t, !ok || info != events[1] || info.Outcome() != OutcomePassed, "projected status = %+v", info)
	}
	assert.Equal(t, snapshot[0].Reason, string(ReasonSkipped))
}

func TestStore_SetCondition_IdenticalIsNoOp(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("ns", "k")
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) { statusEvents++ }, false)

	cond := Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSucceeded}
	s.SetCondition(id, cond)
	s.SetCondition(id, cond)
	assert.Equal(t, statusEvents, 1) // identical SetCondition is a no-op
}

func TestStore_FailedResources(t *testing.T) {
	t.Parallel()
	s := New()
	assert.Equal(t, len(s.FailedResources()), 0) // empty store

	ks := &manifest.Kustomization{Name: "x", Namespace: ""}
	id := ks.Named()
	s.AddObject(ks)
	s.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "boom"})
	assert.Equal(t, len(s.FailedResources()), 1)

	// Phantom guard: a SetCondition that races after DeleteObject must
	// not resurrect a failure entry for an id that no longer exists.
	s.DeleteObject(id)
	s.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "phantom"}) // simulates the race
	assert.Equal(t, len(s.FailedResources()), 0)                                         // phantom entry must not leak
}

func TestStore_SetConditionLocked_StableSliceLength(t *testing.T) {
	t.Parallel()
	s := New()
	id := (&manifest.Kustomization{Name: "a", Namespace: "ns"}).Named()

	for i := range 100 {
		// Alternate status/message so every call is non-equal — the
		// no-op short-circuit must NOT kick in, forcing each call
		// through the overwrite branch.
		status := StatusPending
		if i%2 == 0 {
			status = StatusReady
		}
		s.UpdateStatus(id, StatusInfo{Status: status, Reason: "", Message: fmt.Sprintf("iter-%d", i)})
	}

	var observation Observation
	_, observation = s.Snapshot(id, true)
	conds := observation.Conditions
	assert.Equal(t, len(conds), 1)
}

func TestStore_SetConditionLocked_AppendsDistinctTypes(t *testing.T) {
	t.Parallel()
	s := New()
	id := (&manifest.Kustomization{Name: "a", Namespace: "ns"}).Named()

	s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: "ok"})
	healthy := Condition{
		Type:    ConditionHealthy,
		Status:  metav1.ConditionTrue,
		Reason:  "Healthy",
		Message: "healthy",
	}
	s.SetCondition(id, healthy)

	var before Observation
	_, before = s.Snapshot(id, true)
	conds := before.Conditions
	{
		got := len(conds)
		assert.FatalIf(t, got != 2, "conditions length = %d, want 2 (Ready + Healthy)", got)
	}
	want := []Condition{
		{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSucceeded, Message: "ok"},
		healthy,
	}
	assert.Diff(t, conds, want)

	s.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "boom"})
	assert.Diff(t, conds, want)
	conds[0].Message = "caller mutation"
	var after Observation
	_, after = s.Snapshot(id, true)
	conds = after.Conditions
	{
		got := len(conds)
		assert.ErrorIf(t, got != 2, "conditions length = %d after Ready transition, want 2", got)
	}
	want[0] = Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonFailed, Message: "boom"}
	assert.Diff(t, conds, want)
}
