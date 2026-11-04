package store

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_AddListener_ReplayWithdrawnDeclaredSecret(t *testing.T) {
	t.Parallel()

	s := New()
	p, q := ownerID("p"), ownerID("q")
	for _, owner := range []manifest.NamedResource{p, q} {
		s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	}
	secret := &manifest.Secret{Name: "shared", Namespace: "apps"}
	external := &manifest.RawObject{
		Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "external", Namespace: "apps",
		Spec: map[string]any{"target": map[string]any{"name": secret.Name}},
	}
	accept(t, s, p, &Emission{Children: []Child{{Obj: secret}}, Complete: true})
	accept(t, s, q, &Emission{Children: []Child{{Obj: external}}, Complete: true})
	accept(t, s, p, &Emission{Complete: true})
	{
		assert.FatalIf(
			t,
			s.GetObject(secret.Named()) != nil || s.GetObject(external.Named()) != external,
			"%s",
			"withdrawal did not preserve only the effective ExternalSecret",
		)
	}
	assert.Equal(t, s.EmissionOwner(secret.Named()), manifest.NamedResource{})
	assert.Equal(t, s.EmissionOwner(external.Named()), q)
	proofs := s.Productions(secret.Named(), nil)
	{
		assert.FatalIf(
			t,
			len(proofs) != 1 || proofs[0].Owner != q || proofs[0].Declaration != external.Named(),
			"withdrawal lost declaration evidence: %v",
			proofs,
		)
	}
	inventory := s.ListObjects("", "")
	{
		assert.FatalIf(
			t,
			len(inventory) != 3,
			"effective inventory has %d objects, want two owners and ExternalSecret",
			len(inventory),
		)
	}
	replayed := make(map[manifest.NamedResource][]any)
	var notifications int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, payload any) {
		replayed[id] = append(replayed[id], payload)
		notifications++
	}, true)
	{
		assert.FatalIf(
			t,
			len(replayed) != len(inventory) || notifications != len(inventory),
			"replayed %d identities and %d notifications, want %d",
			len(replayed),
			notifications,
			len(inventory),
		)
	}
	for _, obj := range inventory {
		if payloads := replayed[obj.Named()]; len(payloads) != 1 || payloads[0] != obj {
			t.Fatalf("replay for %v: got %v, want effective object %v once", obj.Named(), payloads, obj)
		}
	}
	{
		assert.FatalIf(t, len(replayed[secret.Named()]) != 0, "%s", "replay revived the withdrawn Secret")
	}
	{
		got := s.ListObjects("", "")
		assert.FatalIf(t, !slices.Equal(got, inventory), "%s", "registration changed the effective inventory")
	}
	live := emissionChild("live", "value")
	s.AddObject(live.Obj)
	{
		assert.FatalIf(
			t,
			s.GetObject(live.Obj.Named()) != live.Obj,
			"%s",
			"store did not retain an object added after replay",
		)
	}
	{
		payloads := replayed[live.Obj.Named()]
		assert.FatalIf(t, len(payloads) != 1 || payloads[0] != live.Obj ||
			notifications != len(
				inventory,
			)+1, "live arrival: got %v and %d notifications, want one new notification", payloads, notifications)
	}
	unsub()
}

func TestStore_AddListener_Unsubscribe(t *testing.T) {
	t.Parallel()
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

func TestStore_ListenerPanicIsolated(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

		done.Go(func() {
			startGate.Wait()
			s.AddObject(racerObj)
		})
		done.Go(func() {
			startGate.Wait()
			s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
				v, _ := counts.LoadOrStore(id, new(atomic.Int64))
				v.(*atomic.Int64).Add(1)
			}, true)
		})
		startGate.Done()
		done.Wait()

		v, _ := counts.LoadOrStore(racerID, new(atomic.Int64))
		{
			got := v.(*atomic.Int64).Load()
			assert.FatalIf(t, got != 1, "racer fired %d times on iteration; want exactly 1", got)
		}
	}
}

func TestStore_AddListenerNoFlush_NoMissedConcurrentWrites(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			s := New()
			before, racing, registered := make(chan struct{}, workers), make(chan struct{}), make(chan struct{})
			var writers sync.WaitGroup
			var seen sync.Map
			for i := range workers {
				writers.Go(func() {
					s.AddObject(newCM(fmt.Sprintf("before-%d", i), "ns"))
					before <- struct{}{}
					<-racing
					s.AddObject(newCM(fmt.Sprintf("racing-%d", i), "ns"))
					<-registered
					s.AddObject(newCM(fmt.Sprintf("after-%d", i), "ns"))
				})
			}
			for range workers {
				<-before
			}
			close(racing)
			unsubscribe := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
				count, _ := seen.LoadOrStore(id, new(atomic.Int64))
				count.(*atomic.Int64).Add(1)
			}, false)
			defer unsubscribe()
			close(registered)
			writers.Wait()
			for i := range workers {
				id := newCM(fmt.Sprintf("after-%d", i), "ns").Named()
				count, ok := seen.Load(id)
				{
					assert.ErrorIf(
						t,
						!ok || count.(*atomic.Int64).Load() != 1,
						"post-registration delivery %s = %v, present=%t",
						id,
						count,
						ok,
					)
				}
				{
					_, ok := seen.Load(newCM(fmt.Sprintf("before-%d", i), "ns").Named())
					assert.ErrorIf(t, ok, "no-flush listener replayed pre-registration object %d", i)
				}
			}
		})
	}
}

func TestStore_ListenerSnapshots(t *testing.T) {
	t.Parallel()
	t.Run(
		"concurrent delivery",
		func(t *testing.T) { t.Parallel(); testStoreListenerSnapshotsConcurrentDelivery(t) },
	) // Distinct names per (goroutine, event) so the
	// AddObject dedup never short-circuits — each call
	// must fire EventObjectAdded.

	t.Run(
		"mutation during captured dispatch",
		func(t *testing.T) { t.Parallel(); testStoreListenerSnapshotsMutationDuringCapturedDispatch(t) },
	)
	t.Run(
		"registration during callback",
		func(t *testing.T) { t.Parallel(); testStoreListenerSnapshotsRegistrationDuringCallback(t) },
	)
}

func testStoreListenerSnapshotsConcurrentDelivery(t *testing.T) {
	t.Helper()

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
				name := fmt.Sprintf("g%d-n%d", g, n)
				s.AddObject(newCM(name, "ns"))
			}
		})
	}
	wg.Wait()

	for i := range numListeners {
		{
			got := counts[i].Load()
			assert.ErrorIf(
				t,
				got != wantPerListener,
				"listener[%d] observed %d events, want %d (listener snapshot lost an event?)",
				i,
				got,
				wantPerListener,
			)
		}
	}
}

func testStoreListenerSnapshotsMutationDuringCapturedDispatch(t *testing.T) {
	t.Helper()

	s := New()
	var seen []int
	first := s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen = append(seen, 1) }, false)
	second := s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen = append(seen, 2) }, false)
	third := s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen = append(seen, 3) }, false)
	obj := newCM("captured", "ns")
	shard := s.shardFor(obj.Named())
	shard.mu.Lock()
	captured := fireUnderLock(s, EventObjectAdded, obj.Named())
	shard.mu.Unlock()
	captured.payload = obj
	first()
	second()
	second()
	fourth := s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen = append(seen, 4) }, false)
	fifth := s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen = append(seen, 5) }, false)
	captured.run()
	assert.Diff(t, seen, []int{1, 2, 3})
	assert.Equal(t, len(captured.listeners.entries), 3)
	seen = nil
	s.AddObject(newCM("future", "ns"))
	assert.Diff(t, seen, []int{3, 4, 5})
	third()
	fourth()
	fifth()
	seen = nil
	s.AddObject(newCM("empty", "ns"))
	assert.Equal(t, len(seen), 0)
}

func testStoreListenerSnapshotsRegistrationDuringCallback(t *testing.T) {
	t.Helper()

	s := New()
	var seen []int
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var first, second Unsubscribe
	first = s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) {
		seen = append(seen, 1)
		close(entered)
		<-release
		first()
		second()
		s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen = append(seen, 4) }, false)
	}, false)
	second = s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen = append(seen, 2) }, false)
	s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen = append(seen, 3) }, false)
	obj := newCM("active", "ns")
	shard := s.shardFor(obj.Named())
	shard.mu.Lock()
	captured := fireUnderLock(s, EventObjectAdded, obj.Named())
	shard.mu.Unlock()
	before := make([]int64, len(captured.listeners.entries))
	for i, entry := range captured.listeners.entries {
		before[i] = entry.id
	}
	go func() {
		s.AddObject(obj)
		close(done)
	}()
	<-entered
	close(release)
	<-done
	assert.Diff(t, seen, []int{1, 2, 3})
	assert.Equal(t, len(captured.listeners.entries), 3)
	after := make([]int64, len(captured.listeners.entries))
	for i, entry := range captured.listeners.entries {
		after[i] = entry.id
	}
	assert.Diff(t, after, before)
	seen = nil
	s.AddObject(newCM("future", "ns"))
	assert.Diff(t, seen, []int{3, 4})
}
