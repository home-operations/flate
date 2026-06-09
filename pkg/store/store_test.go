package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func newCM(name, ns string) *manifest.ConfigMap {
	return &manifest.ConfigMap{Name: name, Namespace: ns}
}

func TestStore_AddObjectIdempotent(t *testing.T) {
	s := New()
	cm := newCM("a", "ns")
	id := cm.Named()

	count := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)

	s.AddObject(cm)
	s.AddObject(cm)
	assert.Equal(t, count, 1) // exactly 1 event for two identical adds
	if got := s.GetObject(id); got == nil {
		t.Errorf("GetObject after AddObject returned nil")
	}
}

// TestStore_AddObject_CloneTriggersUpdate guards the immutability
// contract: callers that need to "mutate" a stored object must clone
// it, mutate the clone, and re-AddObject. The store's reflect.DeepEqual
// dedup recognizes the clone as different and fires EventObjectAdded.
func TestStore_AddObject_CloneTriggersUpdate(t *testing.T) {
	s := New()
	cm := newCM("a", "ns")
	id := cm.Named()

	var seen int
	s.AddListener(EventObjectAdded, func(other manifest.NamedResource, _ any) {
		if other == id {
			seen++
		}
	}, false)

	s.AddObject(cm)
	clone := *cm
	clone.Data = map[string]any{"k": "v"}
	s.AddObject(&clone)

	assert.Equal(t, seen, 2) // initial + clone

	got, ok := s.GetObject(id).(*manifest.ConfigMap)
	if !ok {
		t.Fatalf("expected *manifest.ConfigMap, got %T", s.GetObject(id))
	}
	if got.Data["k"] != "v" {
		t.Errorf("store did not pick up the cloned value: %+v", got.Data)
	}
}

func TestStore_AddObjectsDispatchesAfterAllWrites(t *testing.T) {
	s := New()
	a := newCM("a", "ns")
	b := newCM("b", "ns")
	idA := a.Named()
	idB := b.Named()

	var sawBOnA bool
	s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == idA {
			sawBOnA = s.GetObject(idB) != nil
		}
	}, false)

	s.AddObjects([]manifest.BaseManifest{a, b})
	if !sawBOnA {
		t.Fatal("listener for first object did not see later batch sibling")
	}
}

func TestStore_AddListener_Flush(t *testing.T) {
	s := New()
	s.AddObject(newCM("a", "ns"))
	s.AddObject(newCM("b", "ns"))

	var got []string
	s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		got = append(got, id.Name)
	}, true)
	assert.Equal(t, len(got), 2) // both seeds replayed
}

// TestStore_Refire_ResetsStatusBeforeDispatch pins the contract that
// makes the issue #260 fix race-free: by the time the EventObjectAdded
// listener fires, the resource's recorded status MUST already be
// Pending. A consumer's depwait observing status between the two events
// must never see the stale Ready left by an initial PreGate skip.
func TestStore_Refire_ResetsStatusBeforeDispatch(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "src"}
	s.AddObject(&manifest.OCIRepository{Name: "src", Namespace: "flux-system"})
	s.UpdateStatus(id, StatusReady, MsgUnchanged) // simulate PreGate skip

	var observed StatusInfo
	s.AddListener(EventObjectAdded, func(eventID manifest.NamedResource, _ any) {
		if eventID != id {
			return
		}
		info, _ := s.GetStatus(eventID)
		observed = info
	}, false)
	s.Refire(id)

	if observed.Status != StatusPending {
		t.Errorf("listener saw status %+v on Refire; want Pending so depwaits block", observed)
	}
	if observed.Message != MsgRefetching {
		t.Errorf("listener saw message %q; want %q", observed.Message, MsgRefetching)
	}
}

// TestStore_Refire_NoopMissingID pins that Refire silently returns
// when id isn't in the store — used by callers that may speculatively
// refire ids that race a DeleteObject.
func TestStore_Refire_NoopMissingID(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "nope"}

	fired := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) { fired++ }, false)
	s.Refire(id) // must not panic, must not dispatch
	assert.Equal(t, fired, 0)
	if _, ok := s.GetStatus(id); ok {
		t.Errorf("Refire on missing id left status behind")
	}
}

func TestStore_UpdateStatus_Idempotent(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Namespace: "flux-system", Name: "apps"}

	count := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.UpdateStatus(id, StatusPending, "starting")
	s.UpdateStatus(id, StatusPending, "starting")
	s.UpdateStatus(id, StatusReady, "done")
	assert.Equal(t, count, 2)
	info, ok := s.GetStatus(id)
	if !ok || info.Status != StatusReady {
		t.Errorf("status: %+v ok=%v", info, ok)
	}
}

func TestStore_SetCondition_NonReadyFiresStatusEvent(t *testing.T) {
	// Non-Ready conditions DO fire EventStatusUpdated so CEL-based
	// ReadyExpr waiters can react to Healthy / per-target health
	// transitions. The Ready-derived StatusInfo payload doesn't change,
	// but listeners that need finer granularity (depwait's CEL path)
	// re-query GetConditions to see the change.
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Name: "k", Namespace: "ns"}
	s.UpdateStatus(id, StatusPending, "starting")
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) {
		statusEvents++
	}, false)
	s.SetCondition(id, Condition{Type: ConditionHealthy, Status: metav1.ConditionTrue, Reason: "Healthy"})
	assert.Equal(t, statusEvents, 1) // non-Ready SetCondition still fires
	if got := len(s.GetConditions(id)); got != 2 {
		t.Fatalf("expected 2 conditions, got %d", got)
	}
}

func TestStore_SetCondition_ReadyFiresStatusEvent(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Name: "k", Namespace: "ns"}
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		statusEvents++
		info, ok := payload.(StatusInfo)
		if !ok || info.Status != StatusReady {
			t.Errorf("expected Ready payload, got %+v", payload)
		}
	}, false)
	s.SetCondition(id, Condition{
		Type: ConditionReady, Status: metav1.ConditionTrue,
		Reason: ReasonSucceeded, Message: "ok",
	})
	assert.Equal(t, statusEvents, 1)
}

func TestStore_SetCondition_IdenticalIsNoOp(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Name: "k", Namespace: "ns"}
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) { statusEvents++ }, false)

	cond := Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSucceeded}
	s.SetCondition(id, cond)
	s.SetCondition(id, cond)
	assert.Equal(t, statusEvents, 1) // identical SetCondition is a no-op
}

func TestStore_FailedResources(t *testing.T) {
	s := New()
	assert.Equal(t, len(s.FailedResources()), 0) // empty store

	ks := &manifest.Kustomization{Name: "x", Namespace: ""}
	id := ks.Named()
	s.AddObject(ks)
	s.UpdateStatus(id, StatusFailed, "boom")
	assert.Equal(t, len(s.FailedResources()), 1)

	// Phantom guard: a SetCondition that races after DeleteObject must
	// not resurrect a failure entry for an id that no longer exists.
	s.DeleteObject(id)
	s.UpdateStatus(id, StatusFailed, "phantom")  // simulates the race
	assert.Equal(t, len(s.FailedResources()), 0) // phantom entry must not leak
}

// PR125 switched ListObjects(kind) to a byName-index walk when kind is
// non-empty. Verify it returns exactly the entries for that kind, that
// the empty-kind path still walks everything, and that DeleteObject
// keeps the secondary index consistent.
func TestStore_ListObjects_ByKindIndex(t *testing.T) {
	s := New()
	s.AddObject(newCM("a", "ns1"))
	s.AddObject(newCM("b", "ns2"))
	s.AddObject(&manifest.Secret{Name: "s", Namespace: "ns1"})

	assert.Equal(t, len(s.ListObjects(manifest.KindConfigMap)), 2)
	assert.Equal(t, len(s.ListObjects(manifest.KindSecret)), 1)
	assert.Equal(t, len(s.ListObjects("")), 3) // empty kind walks all
	s.DeleteObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns1", Name: "a"})
	assert.Equal(t, len(s.ListObjects(manifest.KindConfigMap)), 1) // index stays consistent
}

// TestStore_ListObjects_DeterministicOrder pins the sort contract:
// two ListObjects calls with concurrent concurrent adds must return
// objects in the same (Kind, Namespace, Name) order. Without the
// sort, Go map iteration is randomized and ownership tie-breaking in
// change/ownership.go can attribute the same file to different KS
// owners on different runs.
func TestStore_ListObjects_DeterministicOrder(t *testing.T) {
	s := New()
	// Add objects in non-alphabetical order so insertion order can't
	// accidentally hide a missing sort.
	objects := []*manifest.ConfigMap{
		{Name: "z", Namespace: "b"},
		{Name: "a", Namespace: "z"},
		{Name: "m", Namespace: "a"},
		{Name: "a", Namespace: "a"},
		{Name: "z", Namespace: "a"},
	}
	for _, obj := range objects {
		s.AddObject(obj)
	}

	first := s.ListObjects(manifest.KindConfigMap)
	second := s.ListObjects(manifest.KindConfigMap)

	if len(first) != len(second) {
		t.Fatalf("lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		a, b := first[i].Named(), second[i].Named()
		if a != b {
			t.Errorf("position %d differs between calls: %v vs %v", i, a, b)
		}
	}
	// Verify the order is actually sorted by (namespace, name) within
	// the same kind.
	for i := 1; i < len(first); i++ {
		prev, cur := first[i-1].Named(), first[i].Named()
		if prev.Compare(cur) > 0 {
			t.Errorf("unsorted at [%d]: %v > %v", i, prev, cur)
		}
	}
}

func TestStore_WatchReady_AlreadyReady(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	s.UpdateStatus(id, StatusReady, "")
	info, err := s.WatchReady(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("WatchReady: %v", err)
	}
	if info.Status != StatusReady {
		t.Errorf("status: %v", info.Status)
	}
}

func TestStore_WatchReady_TransitionsToReady(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}

	go s.UpdateStatus(id, StatusReady, "")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := s.WatchReady(ctx, id, nil); err != nil {
		t.Fatalf("WatchReady: %v", err)
	}
}

// TestStore_WatchReady_QuiesceStillPendingGivesUp: when the embedder's quiesce
// signal fires while the resource is still not terminal, WatchReady gives up
// with ErrQuiesced (the pool drained — no producer left to make it Ready).
func TestStore_WatchReady_QuiesceStillPendingGivesUp(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	s.UpdateStatus(id, StatusPending, "fetching")

	quiesce := make(chan struct{})
	close(quiesce) // already quiescent

	_, err := s.WatchReady(context.Background(), id, quiesce)
	if !errors.Is(err, ErrQuiesced) {
		t.Fatalf("err = %v; want ErrQuiesced", err)
	}
}

// TestStore_WatchReady_QuiesceRereadsReady: a Ready committed before the quiesce
// signal closes (RunWithStatus writes Ready before the producing task's
// decrActive) is observed by the quiesce arm's re-read, even if select picks
// quiesce over the pending wake.
func TestStore_WatchReady_QuiesceRereadsReady(t *testing.T) {
	for range 200 { // stress the wake/quiesce select ordering
		s := New()
		id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
		s.UpdateStatus(id, StatusPending, "fetching")
		quiesce := make(chan struct{})
		go func() {
			s.UpdateStatus(id, StatusReady, "") // terminal BEFORE quiesce closes
			close(quiesce)
		}()
		info, err := s.WatchReady(context.Background(), id, quiesce)
		if err != nil {
			t.Fatalf("err = %v; want Ready (quiesce arm must re-read the committed Ready)", err)
		}
		if info.Status != StatusReady {
			t.Fatalf("status = %v; want Ready", info.Status)
		}
	}
}

// TestStore_WatchReady_QuiesceRereadsFailed: a Failed committed before quiesce
// fast-fails via the quiesce re-read (no wasted grace window).
func TestStore_WatchReady_QuiesceRereadsFailed(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	s.UpdateStatus(id, StatusFailed, "boom")

	quiesce := make(chan struct{})
	close(quiesce)

	_, err := s.WatchReady(context.Background(), id, quiesce)
	var rfe *manifest.ResourceFailedError
	if !errors.As(err, &rfe) {
		t.Fatalf("err = %v; want *ResourceFailedError", err)
	}
	if rfe.Reason != "boom" {
		t.Errorf("reason = %q; want boom", rfe.Reason)
	}
}

func TestStore_WatchReady_FailedYieldsError(t *testing.T) {
	withFailedGrace(t, 50*time.Millisecond)
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	s.UpdateStatus(id, StatusFailed, "denied")

	_, err := s.WatchReady(context.Background(), id, nil)
	var rfe *manifest.ResourceFailedError
	if !errors.As(err, &rfe) {
		t.Fatalf("expected ResourceFailedError, got %v", err)
	}
	if rfe.Reason != "denied" {
		t.Errorf("reason: %q", rfe.Reason)
	}
}

// TestStore_WatchReady_FailedFlipsBeforeGrace asserts the race-fix
// behavior: a resource that briefly enters Failed before being
// re-reconciled to Ready (parent Kustomization re-emits a child with
// patched substituteFrom) does NOT propagate the transient failure
// to dependents.
func TestStore_WatchReady_FailedFlipsBeforeGrace(t *testing.T) {
	withFailedGrace(t, 200*time.Millisecond)
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Namespace: "ns", Name: "k"}
	s.UpdateStatus(id, StatusFailed, "transient")

	// Flip to Ready inside the grace window.
	go func() {
		time.Sleep(20 * time.Millisecond)
		s.UpdateStatus(id, StatusReady, "")
	}()

	info, err := s.WatchReady(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("expected nil err after Failed→Ready flip, got %v", err)
	}
	if info.Status != StatusReady {
		t.Errorf("status = %v, want Ready", info.Status)
	}
}

// TestStore_WatchReady_FailedFailedReady verifies multi-Failed-then-Ready:
// the grace timer arms on the first Failed and persists through subsequent
// Failed updates (does NOT reset). If Ready arrives before the original
// timer expires, dependents see Ready and proceed. Guards an invariant
// the grace-period comment in pkg/store/watch.go calls out.
func TestStore_WatchReady_FailedFailedReady(t *testing.T) {
	withFailedGrace(t, 300*time.Millisecond)
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Namespace: "ns", Name: "k"}
	s.UpdateStatus(id, StatusFailed, "first failure")

	go func() {
		time.Sleep(50 * time.Millisecond)
		s.UpdateStatus(id, StatusFailed, "second failure")
		time.Sleep(50 * time.Millisecond)
		s.UpdateStatus(id, StatusReady, "")
	}()

	info, err := s.WatchReady(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("expected nil err after Failed→Failed→Ready flip, got %v", err)
	}
	if info.Status != StatusReady {
		t.Errorf("status = %v, want Ready", info.Status)
	}
}

// TestStore_WatchReady_GraceExpiresOnSustainedFailed verifies the
// opposite: when Ready never arrives, the original grace window
// expires and WatchReady returns the most-recently-observed Failed.
func TestStore_WatchReady_GraceExpiresOnSustainedFailed(t *testing.T) {
	withFailedGrace(t, 50*time.Millisecond)
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Namespace: "ns", Name: "k"}
	s.UpdateStatus(id, StatusFailed, "first")
	go func() {
		time.Sleep(15 * time.Millisecond)
		s.UpdateStatus(id, StatusFailed, "second")
	}()

	info, err := s.WatchReady(context.Background(), id, nil)
	if err == nil {
		t.Fatalf("expected ResourceFailedError after grace expiration")
	}
	var rfe *manifest.ResourceFailedError
	if !errors.As(err, &rfe) {
		t.Fatalf("expected *ResourceFailedError, got %T", err)
	}
	if info.Status != StatusFailed {
		t.Errorf("status = %v, want Failed", info.Status)
	}
	// The latest reason should be reported, not the first.
	if rfe.Reason != "second" {
		t.Errorf("reason = %q, want %q (latest observed failure)", rfe.Reason, "second")
	}
}

// withFailedGrace temporarily shrinks FailedGrace for a single test
// and restores it on cleanup. Keeps unit-test wall time bounded
// without leaking the shorter value into other tests.
func withFailedGrace(t *testing.T, d time.Duration) {
	t.Helper()
	prev := FailedGrace
	FailedGrace = d
	t.Cleanup(func() { FailedGrace = prev })
}

func TestStore_WatchReady_ContextCancel(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.WatchReady(ctx, id, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestStore_WatchExists(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "ConfigMap", Namespace: "ns", Name: "a"}

	// Pre-existing: returns immediately.
	s.AddObject(newCM("a", "ns"))
	obj, err := s.WatchExists(context.Background(), id)
	if err != nil || obj == nil {
		t.Fatalf("WatchExists existing: %v %v", obj, err)
	}

	// Subscribe first, then add: WatchExists rechecks the store after
	// subscribing, so add-then-subscribe and subscribe-then-add both
	// resolve correctly.
	id2 := manifest.NamedResource{Kind: "ConfigMap", Namespace: "ns", Name: "b"}
	done := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = s.WatchExists(ctx, id2)
		close(done)
	}()
	s.AddObject(newCM("b", "ns"))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WatchExists never returned")
	}
}

func TestStore_AddListener_Unsubscribe(t *testing.T) {
	s := New()
	count := 0
	unsub := s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.AddObject(newCM("a", "ns"))
	unsub()
	s.AddObject(newCM("b", "ns"))
	assert.Equal(t, count, 1) // post-unsubscribe add is silent
}

func TestStore_SetArtifact(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "GitRepository", Name: "r"}
	art := &SourceArtifact{Kind: "GitRepository", URL: "https://example", LocalPath: "/tmp/x"}

	count := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.SetArtifact(id, art)
	s.SetArtifact(id, art) // idempotent
	assert.Equal(t, count, 1)
	got := s.GetArtifact(id)
	if got == nil {
		t.Errorf("GetArtifact: nil")
	}
}

// TestStore_SetArtifact_DistinctPointerDedup pins Item 7's primary
// dedup contract: a re-set with a DISTINCT POINTER but
// content-equal artifact is a no-op. The KS / HR re-emit case —
// rendered docs decode into fresh maps every reconcile — drives
// this in production.
func TestStore_SetArtifact_DistinctPointerDedup(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	makeArt := func() *KustomizationArtifact {
		return &KustomizationArtifact{
			Path: "./apps",
			Manifests: []map[string]any{
				{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "cm-1", "namespace": "default"},
					"data":       map[string]any{"k": "v"},
				},
			},
		}
	}

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.SetArtifact(id, makeArt())
	s.SetArtifact(id, makeArt()) // fresh pointer, identical content
	s.SetArtifact(id, makeArt()) // fresh pointer, identical content

	assert.Equal(t, events, 1) // content-equal re-sets dedup
}

// TestStore_SetArtifact_PointerIdentityShortCircuit pins Item 7's
// pointer-identity fast path: re-setting the SAME pointer skips
// reflection entirely. Source-controller refresh loops cache their
// own SourceArtifact and re-publish it on every tick; the
// short-circuit cuts that hot path from ~58 ns (reflect.DeepEqual on
// aliased pointers) to ~17 ns (the map lookup alone).
func TestStore_SetArtifact_PointerIdentityShortCircuit(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "r"}
	art := &SourceArtifact{Kind: "GitRepository", URL: "https://example", LocalPath: "/tmp/x"}

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.SetArtifact(id, art)
	s.SetArtifact(id, art) // same pointer
	s.SetArtifact(id, art) // same pointer

	assert.Equal(t, events, 1) // same-pointer re-set short-circuits
}

// TestStore_SetArtifact_DifferentContent pins the inverse: a
// re-set with DIFFERENT content fires an event.
func TestStore_SetArtifact_DifferentContent(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.SetArtifact(id, &KustomizationArtifact{Path: "./apps", Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-1"}},
	}})
	s.SetArtifact(id, &KustomizationArtifact{Path: "./apps", Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-2"}},
	}})

	assert.Equal(t, events, 2) // distinct content fires each time
}

func TestStore_ListenerPanicIsolated(t *testing.T) {
	s := New()
	other := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		panic("boom")
	}, false)
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		other++
	}, false)
	s.AddObject(newCM("a", "ns")) // should not crash
	assert.Equal(t, other, 1)     // sibling listener still ran
}

// AddListener with flush=true replays existing store contents into the
// new listener. A panic in the listener during replay must not crash
// the caller — controllers attach with flush=true in Start() and a
// crash here would tear down the whole reconcile pipeline before the
// orchestrator's Run loop ever ticked. Parity with the live `fire`
// path in TestStore_ListenerPanicIsolated.
func TestStore_ListenerPanicRecoveredDuringReplay(t *testing.T) {
	s := New()
	s.AddObject(newCM("a", "ns"))
	s.AddObject(newCM("b", "ns"))
	// The panicky listener attaches with flush=true → replay fires
	// against every existing object. Without panic recovery the test
	// would fail with a runtime panic.
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		panic("boom during replay")
	}, true)
	// Confirm the Store is still healthy after the recovered panic.
	other := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		other++
	}, true)
	assert.Equal(t, other, 2) // store healthy after recovered replay panic
}

func TestStore_Concurrency(_ *testing.T) {
	s := New()
	const N = 200

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range N {
			s.AddObject(newCM("a", "ns"))
		}
	}()
	go func() {
		defer wg.Done()
		for range N {
			_ = s.ListObjects("ConfigMap")
		}
	}()
	wg.Wait()
}

// TestStore_AddListener_FlushNoDoubleFire stresses the
// AddObject-vs-AddListener race that the fireUnderLock pattern closes.
// Without the fix, a flush=true listener registering while a sibling
// goroutine ran AddObject for the same id could observe the object
// TWICE: once via the live event (listener was added before the
// writer's listener-snapshot ran) and again via the replay snapshot
// (which captured the same object). The Coalescer hid this in
// production by deduping per-id reconciles, but a test counter
// surfaces it.
func TestStore_AddListener_FlushNoDoubleFire(t *testing.T) {
	const iterations = 200
	for range iterations {
		s := New()
		// Seed enough objects that the replay snapshot is non-trivial
		// and the race window is reachable.
		const seed = 8
		for i := range seed {
			s.AddObject(newCM(fmt.Sprintf("seed-%d", i), "ns"))
		}

		var (
			counts   sync.Map // id → atomic count
			racerID  = manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "racer"}
			racerObj = newCM("racer", "ns")
		)

		var startGate, done sync.WaitGroup
		startGate.Add(1)
		done.Add(2)

		go func() {
			defer done.Done()
			startGate.Wait()
			s.AddObject(racerObj)
		}()
		go func() {
			defer done.Done()
			startGate.Wait()
			s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
				v, _ := counts.LoadOrStore(id, new(atomic.Int64))
				v.(*atomic.Int64).Add(1)
			}, true)
		}()
		startGate.Done()
		done.Wait()

		v, _ := counts.LoadOrStore(racerID, new(atomic.Int64))
		if got := v.(*atomic.Int64).Load(); got != 1 {
			t.Fatalf("racer fired %d times on iteration; want exactly 1", got)
		}
	}
}

// TestStore_WatchExists_NoWedge stresses the subscribe-and-recheck
// race in WatchExists: a writer that lands between the initial
// GetObject and the listener registration must reach the waiter via
// the atomic register-and-read pair. Without subscribeWithObject's
// RLock-protected pairing, the listener can register after the
// writer's dispatch snapshot AND the recheck can see pre-write state,
// wedging the waiter until ctx expires.
//
// The test fires many parallel writer/waiter pairs; under -race, any
// regression that reintroduces the race shows up as either a timeout
// or a missing return.
func TestStore_WatchExists_NoWedge(t *testing.T) {
	const iters = 256
	for range iters {
		s := New()
		cm := newCM("a", "ns")
		id := cm.Named()

		var got manifest.BaseManifest
		var werr error
		var wg sync.WaitGroup
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		ready := make(chan struct{})
		wg.Go(func() {
			close(ready)
			got, werr = s.WatchExists(ctx, id)
		})

		<-ready
		s.AddObject(cm)
		wg.Wait()

		if werr != nil {
			t.Fatalf("WatchExists: %v (iter wedged — race regression)", werr)
		}
		if got == nil {
			t.Fatalf("WatchExists returned nil object")
		}
	}
}

// TestStore_AddRendered_DispatchesListeners pins the listener contract:
// AddRendered MUST fire EventObjectAdded so callers that listen for
// kinds promoted via render (e.g. valuesFrom invalidation watching
// rendered Secret docs) see the event. The original implementation
// skipped dispatch entirely with a "rendered docs are leaves" comment;
// adding any new listener for a render-output kind silently broke.
func TestStore_AddRendered_DispatchesListeners(t *testing.T) {
	s := New()
	var seen atomic.Int64
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		seen.Add(1)
	}, false)
	s.AddRendered(newCM("a", "ns"))
	assert.Equal(t, seen.Load(), int64(1)) // AddRendered must dispatch
}

// TestStore_AddListenerNoFlush_NoMissedConcurrentWrites pins that a
// listener registered with flush=false against a hot writer doesn't
// miss the next live event. Pre-fix, the non-flush branch took
// set.mu but not the store lock, so a writer could:
//  1. Lock its shard, mutate.
//  2. Capture set.snapshot() under set.mu (independent of the shard).
//  3. Release the shard.
//  4. Dispatch to the pre-add snapshot.
//
// while the registrar slid set.add(fn) between steps 2 and 4. fn
// silently misses the live event. Holding every shard's RLock
// around set.add closes the window (writers fire under their own
// shard's write lock, exclusive with the registrar's RLock). Test
// races N writers against AddListener and asserts the listener
// observes every AddObject made after its own goroutine acquired
// the lock.
func TestStore_AddListenerNoFlush_NoMissedConcurrentWrites(t *testing.T) {
	s := New()
	const writers = 64
	var (
		writerWG      sync.WaitGroup
		registerReady = make(chan struct{})
		registered    = make(chan struct{})
		seen          atomic.Int64
	)
	// Pre-populate so AddObject takes the "update existing" path
	// uniformly — keeps test deterministic.
	for i := range writers {
		s.AddObject(newCM("init"+strings.Repeat("x", i), "ns"))
	}
	writerWG.Go(func() {
		<-registerReady
		// Spin writes while the registrar runs. Some will land before
		// AddListener returns; some after.
		for i := range writers {
			s.AddObject(newCM("post"+strings.Repeat("x", i), "ns"))
		}
		<-registered // Make sure registrar is fully attached.
		// One more guaranteed-post-registration write that the
		// listener MUST observe.
		s.AddObject(newCM("definite-post", "ns"))
	})
	close(registerReady)
	s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.Name == "definite-post" {
			seen.Add(1)
		}
	}, false)
	close(registered)
	writerWG.Wait()
	if got := seen.Load(); got != 1 {
		t.Errorf("listener missed the post-registration definite-post event (saw %d, want 1) — non-flush race regression", got)
	}
}

// TestStore_ListenerSnapshotPool_NoLostEvents stresses the pooled
// listener-snapshot path under concurrent fires. With 10 goroutines
// firing 100 events each against a 50-listener set, every listener
// must observe every event — losing one means the pool aliasing
// is wrong (the dispatcher iterated a slice that had already been
// released and recycled by another fire). Run under -race for the
// full guarantee.
func TestStore_ListenerSnapshotPool_NoLostEvents(t *testing.T) {
	s := New()
	const (
		numListeners = 50
		numFirers    = 10
		eventsPer    = 100
	)
	totalEvents := int64(numFirers * eventsPer)
	wantPerListener := totalEvents

	counts := make([]atomic.Int64, numListeners)
	for i := range numListeners {
		s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
			counts[i].Add(1)
		}, false)
	}

	var wg sync.WaitGroup
	for g := range numFirers {
		wg.Go(func() {
			for n := range eventsPer {
				// Distinct names per (goroutine, event) so the
				// AddObject dedup never short-circuits — each call
				// must fire EventObjectAdded.
				name := fmt.Sprintf("g%d-n%d", g, n)
				s.AddObject(newCM(name, "ns"))
			}
		})
	}
	wg.Wait()

	for i := range numListeners {
		if got := counts[i].Load(); got != wantPerListener {
			t.Errorf("listener[%d] observed %d events, want %d (pool aliasing dropped a fire?)", i, got, wantPerListener)
		}
	}
}

// TestStore_SetConditionLocked_StableSliceLength asserts the in-place
// rebuild contract for setConditionLocked: mutating the same condition
// type N times leaves the conditions slice at length 1, not N. The
// pre-fix implementation rebuilt the slice from scratch on every
// transition (O(N) allocations across N transitions); the in-place
// overwrite drops every allocation past the first.
//
// Counting GetConditions length is sufficient — the contract is "one
// entry per unique Type, in-place updates do not grow the slice".
func TestStore_SetConditionLocked_StableSliceLength(t *testing.T) {
	s := New()
	id := newCM("a", "ns").Named()

	for i := range 100 {
		// Alternate status/message so every call is non-equal — the
		// no-op short-circuit must NOT kick in, forcing each call
		// through the overwrite branch.
		status := StatusPending
		if i%2 == 0 {
			status = StatusReady
		}
		s.UpdateStatus(id, status, fmt.Sprintf("iter-%d", i))
	}

	assert.Equal(t, len(s.GetConditions(id)), 1) // in-place overwrite, no growth
}

// TestStore_SetConditionLocked_AppendsDistinctTypes pins the
// fall-through-to-append path for new condition types: a Healthy
// transition on top of an existing Ready must produce a 2-entry
// slice, with both entries preserved across subsequent same-type
// updates. The in-place mutation rule applies per-Type, not globally.
func TestStore_SetConditionLocked_AppendsDistinctTypes(t *testing.T) {
	s := New()
	id := newCM("a", "ns").Named()

	s.UpdateStatus(id, StatusReady, "ok")
	s.SetCondition(id, metav1.Condition{
		Type:    ConditionHealthy,
		Status:  metav1.ConditionTrue,
		Reason:  "Healthy",
		Message: "healthy",
	})

	conds := s.GetConditions(id)
	if got := len(conds); got != 2 {
		t.Fatalf("conditions length = %d, want 2 (Ready + Healthy)", got)
	}

	// Mutating one in place must leave the other alone and keep length stable.
	s.UpdateStatus(id, StatusFailed, "boom")
	conds = s.GetConditions(id)
	if got := len(conds); got != 2 {
		t.Errorf("conditions length = %d after Ready transition, want 2", got)
	}
	var sawReady, sawHealthy bool
	for _, c := range conds {
		switch c.Type {
		case ConditionReady:
			sawReady = true
		case ConditionHealthy:
			sawHealthy = true
		}
	}
	if !sawReady || !sawHealthy {
		t.Errorf("expected both Ready and Healthy; sawReady=%v sawHealthy=%v", sawReady, sawHealthy)
	}
}

// TestStore_ShardedConcurrentDifferentKinds hammers two distinct Kinds
// (Kustomization, HelmRelease) from many goroutines concurrently and
// asserts the operations complete cleanly under -race. The KS and HR
// Kinds hash to different shards (verified manually: KS→14, HR→10), so
// the workload exercises the cross-Kind parallelism that justifies the
// shard refactor — under the old single-mutex design these goroutines
// would all serialize on the single global lock. The bug bar here is correctness, not
// timing: -race surfaces any shard-locking mistake (cross-shard
// dangling reads, missed unlock paths, deadlocks) as a data race or
// hang within the 30s test budget.
func TestStore_ShardedConcurrentDifferentKinds(t *testing.T) {
	s := New()
	const N = 50
	const workers = 8
	var wg sync.WaitGroup

	// 4 goroutines on Kustomizations, 4 on HelmReleases.
	for w := range workers {
		wg.Go(func() {
			isKS := w%2 == 0
			for i := range N {
				if isKS {
					ks := &manifest.Kustomization{Name: fmt.Sprintf("ks-%d-%d", w, i), Namespace: "ns"}
					s.AddObject(ks)
					s.UpdateStatus(ks.Named(), StatusReady, "ok")
					_ = s.GetObject(ks.Named())
				} else {
					hr := &manifest.HelmRelease{Name: fmt.Sprintf("hr-%d-%d", w, i), Namespace: "ns"}
					s.AddObject(hr)
					s.UpdateStatus(hr.Named(), StatusReady, "ok")
					_ = s.GetObject(hr.Named())
				}
			}
		})
	}
	wg.Wait()

	// Sanity: every object landed.
	if got := len(s.ListObjects(manifest.KindKustomization)); got != N*(workers/2) {
		t.Errorf("expected %d KSes, got %d", N*(workers/2), got)
	}
	if got := len(s.ListObjects(manifest.KindHelmRelease)); got != N*(workers/2) {
		t.Errorf("expected %d HRs, got %d", N*(workers/2), got)
	}
}

// TestStore_CrossKindOperationDoesntDeadlock pounds the cross-shard
// paths (ListObjects(""), FailedResources, AddListener(replay=true))
// from many goroutines while per-shard writers churn through different
// Kinds. The canonical-order lockAll/rLockAll rule prevents two
// cross-shard operations from deadlocking by taking shards in opposing
// orders. Any regression that locks shards out-of-order — e.g. a
// future lockAll variant that iterates from N-1 down to 0 — produces
// a hang within the 30s test timeout.
func TestStore_CrossKindOperationDoesntDeadlock(t *testing.T) {
	s := New()
	const goroutines = 32
	const ops = 30

	// Seed with a mix of kinds across multiple shards.
	for i := range 10 {
		s.AddObject(&manifest.Kustomization{Name: fmt.Sprintf("ks-%d", i), Namespace: "ns"})
		s.AddObject(&manifest.HelmRelease{Name: fmt.Sprintf("hr-%d", i), Namespace: "ns"})
		s.AddObject(newCM(fmt.Sprintf("cm-%d", i), "ns"))
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := range goroutines {
		wg.Go(func() {
			for i := range ops {
				switch (w + i) % 5 {
				case 0:
					_ = s.ListObjects("")
				case 1:
					_ = s.FailedResources()
				case 2:
					unsub := s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) {}, true)
					unsub()
				case 3:
					ks := &manifest.Kustomization{Name: fmt.Sprintf("ks-w%d-i%d", w, i), Namespace: "ns"}
					s.AddObject(ks)
					s.UpdateStatus(ks.Named(), StatusFailed, "boom")
				case 4:
					hr := &manifest.HelmRelease{Name: fmt.Sprintf("hr-w%d-i%d", w, i), Namespace: "ns"}
					s.AddObject(hr)
					s.SetArtifact(hr.Named(), &HelmReleaseArtifact{Manifests: nil, Fingerprint: "x"})
				}
			}
		})
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		// Success — no deadlock.
	case <-time.After(30 * time.Second):
		t.Fatal("cross-shard ops deadlocked (lockAll canonical-order regression?)")
	}
}
