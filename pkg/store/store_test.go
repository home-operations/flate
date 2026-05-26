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
	if count != 1 {
		t.Errorf("expected exactly 1 event for two identical adds, got %d", count)
	}
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

	if seen != 2 {
		t.Errorf("expected two AddObject events (initial + clone), got %d", seen)
	}
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
	if len(got) != 2 {
		t.Errorf("expected 2 replayed, got %d", len(got))
	}
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
	if fired != 0 {
		t.Errorf("Refire on missing id fired %d events; want 0", fired)
	}
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
	if count != 2 {
		t.Errorf("expected 2 status events, got %d", count)
	}
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
	if statusEvents != 1 {
		t.Errorf("non-Ready SetCondition fired %d events; want 1", statusEvents)
	}
	got := s.GetConditions(id)
	if len(got) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(got))
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
	if statusEvents != 1 {
		t.Errorf("Ready SetCondition fired %d events; want 1", statusEvents)
	}
}

func TestStore_SetCondition_IdenticalIsNoOp(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: "Kustomization", Name: "k", Namespace: "ns"}
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) { statusEvents++ }, false)

	cond := Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSucceeded}
	s.SetCondition(id, cond)
	s.SetCondition(id, cond)
	if statusEvents != 1 {
		t.Errorf("identical SetCondition fired %d events; want 1", statusEvents)
	}
}

// Mutate encodes the clone-then-AddObject pattern. Verify it clones
// (mutating the supplied pointer doesn't touch the canonical store),
// fires EventObjectAdded (so dependent controllers re-reconcile), and
// no-ops when the id isn't present.
func TestStore_Mutate(t *testing.T) {
	s := New()
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	orig := &manifest.Kustomization{
		Name: "a", Namespace: "ns",
		DependsOn: []manifest.DependencyRef{
			{NamedResource: manifest.NamedResource{Kind: manifest.KindKustomization, Name: "x"}},
		},
	}
	s.AddObject(orig)

	fired := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) { fired++ }, false)

	ok := Mutate(s, id, func(k *manifest.Kustomization) { k.DependsOn = nil })
	if !ok {
		t.Fatalf("Mutate returned false on a present id")
	}
	if fired != 1 {
		t.Errorf("expected EventObjectAdded to fire once on Mutate; got %d", fired)
	}
	// Original pointer unchanged (clone semantics).
	if len(orig.DependsOn) != 1 {
		t.Errorf("original pointer was mutated; clone failed: %v", orig.DependsOn)
	}
	// Store now holds the mutated clone.
	got, _ := s.GetObject(id).(*manifest.Kustomization)
	if got == nil || len(got.DependsOn) != 0 {
		t.Errorf("stored object not mutated: %v", got)
	}

	// Missing id is a no-op.
	if Mutate(s, manifest.NamedResource{Kind: manifest.KindKustomization, Name: "absent"},
		func(*manifest.Kustomization) {}) {
		t.Errorf("Mutate returned true for absent id")
	}
}

func TestStore_FailedResources(t *testing.T) {
	s := New()
	if len(s.FailedResources()) != 0 {
		t.Errorf("empty store should not have failures")
	}
	ks := &manifest.Kustomization{Name: "x", Namespace: ""}
	id := ks.Named()
	s.AddObject(ks)
	s.UpdateStatus(id, StatusFailed, "boom")
	if got := len(s.FailedResources()); got != 1 {
		t.Errorf("FailedResources count: %d, want 1", got)
	}

	// Phantom guard: a SetCondition that races after DeleteObject must
	// not resurrect a failure entry for an id that no longer exists.
	s.DeleteObject(id)
	s.UpdateStatus(id, StatusFailed, "phantom") // simulates the race
	if got := len(s.FailedResources()); got != 0 {
		t.Errorf("FailedResources after DeleteObject: %d, want 0 (phantom entry leaked)", got)
	}
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

	if got := len(s.ListObjects(manifest.KindConfigMap)); got != 2 {
		t.Errorf("ListObjects(KindConfigMap) = %d, want 2", got)
	}
	if got := len(s.ListObjects(manifest.KindSecret)); got != 1 {
		t.Errorf("ListObjects(KindSecret) = %d, want 1", got)
	}
	if got := len(s.ListObjects("")); got != 3 {
		t.Errorf("ListObjects(\"\") = %d, want 3", got)
	}
	s.DeleteObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns1", Name: "a"})
	if got := len(s.ListObjects(manifest.KindConfigMap)); got != 1 {
		t.Errorf("after Delete: ListObjects(KindConfigMap) = %d, want 1", got)
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
	if count != 1 {
		t.Errorf("expected 1 event after unsubscribe, got %d", count)
	}
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
	if count != 1 {
		t.Errorf("expected 1 artifact event, got %d", count)
	}
	got := s.GetArtifact(id)
	if got == nil {
		t.Errorf("GetArtifact: nil")
	}
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
	if other != 1 {
		t.Errorf("other listener should still have run, got %d", other)
	}
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
	if other != 2 {
		t.Errorf("post-panic listener should replay 2 objects, got %d", other)
	}
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
	if got := seen.Load(); got != 1 {
		t.Errorf("AddRendered fired %d events; want 1 (listener contract violated)", got)
	}
}

// TestStore_AddListenerNoFlush_NoMissedConcurrentWrites pins that a
// listener registered with flush=false against a hot writer doesn't
// miss the next live event. Pre-fix, the non-flush branch took
// set.mu but not s.mu, so a writer could:
//  1. Lock s.mu, mutate store.
//  2. Capture set.snapshot() under set.mu (independent of s.mu).
//  3. Release s.mu.
//  4. Dispatch to the pre-add snapshot.
//
// while the registrar slid set.add(fn) between steps 2 and 4. fn
// silently misses the live event. Holding s.mu around set.add
// closes the window. Test races N writers against AddListener and
// asserts the listener observes every AddObject made after its own
// goroutine acquired the lock.
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
