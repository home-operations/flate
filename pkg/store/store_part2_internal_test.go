package store

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

func TestStore_ListObjects_ByKindIndex(t *testing.T) {
	t.Parallel()
	for _, core := range []bool{false, true} {
		for _, insertForeignFirst := range []bool{false, true} {
			for _, withdrawForeignFirst := range []bool{false, true} {
				t.Run(
					fmt.Sprintf(
						"core=%t/insertForeignFirst=%t/withdrawForeignFirst=%t",
						core,
						insertForeignFirst,
						withdrawForeignFirst,
					),
					func(t *testing.T) {
						t.Parallel()
						testStoreListObjectsByKindIndexCase(
							t,
							&testStoreListObjectsByKindIndexCaseInputs{
								core:                 &core,
								insertForeignFirst:   &insertForeignFirst,
								withdrawForeignFirst: &withdrawForeignFirst,
							},
						)
					},
				)
			}
		}
	}
}

// TestStore_ListObjects_DeterministicOrder pins the sort contract:
// two ListObjects calls with concurrent concurrent adds must return
// objects in the same (Kind, Group, Namespace, Name) order. Without the
// sort, Go map iteration is randomized and ownership tie-breaking in
// change/ownership.go can attribute the same file to different KS
// owners on different runs.
func TestStore_ListObjects_DeterministicOrder(t *testing.T) {
	t.Parallel()
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

	first := s.ListObjects("", manifest.KindConfigMap)
	second := s.ListObjects("", manifest.KindConfigMap)

	{
		assert.FatalIf(t, len(first) != len(second), "lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		a, b := first[i].Named(), second[i].Named()
		{
			assert.ErrorIf(t, a != b, "position %d differs between calls: %v vs %v", i, a, b)
		}
	}
	// Verify the order is actually sorted by (namespace, name) within
	// the same kind.
	for i := 1; i < len(first); i++ {
		prev, cur := first[i-1].Named(), first[i].Named()
		{
			assert.ErrorIf(t, prev.Compare(cur) > 0, "unsorted at [%d]: %v > %v", i, prev, cur)
		}
	}
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

func TestStore_SetArtifact(t *testing.T) {
	t.Parallel()
	s := New()
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: "GitRepository", Name: "r"}
	art := &SourceArtifact{Kind: "GitRepository", URL: "https://example", LocalPath: "/tmp/x"}

	count := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.SetArtifact(id, art)
	s.SetArtifact(id, art) // idempotent
	assert.Equal(t, count, 1)
	got := s.GetArtifact(id, ArtifactContent)
	{
		assert.ErrorIf(t, got == nil, "GetArtifact: nil")
	}
}

func TestStore_RecordEmitted_DistinctPointerDedup(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("flux-system", "apps")
	s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	makeArt := func() *Emission {
		return &Emission{
			Complete: true,

			Manifests: testutil.DocumentOccurrences([]map[string]any{
				{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "cm-1", "namespace": "default"},
					"data":       map[string]any{"k": "v"},
				},
			}),
		}
	}

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.RecordEmitted(id, makeArt(), nil)
	s.RecordEmitted(id, makeArt(), nil)
	s.RecordEmitted(id, makeArt(), nil)

	assert.Equal(t, events, 1)
}

// TestStore_SetArtifact_PointerIdentityShortCircuit pins Item 7's
// pointer-identity fast path: re-setting the SAME pointer skips
// reflection entirely. Source-controller refresh loops cache their
// own SourceArtifact and re-publish it on every tick; the
// short-circuit cuts that hot path from ~58 ns (reflect.DeepEqual on
// aliased pointers) to ~17 ns (the map lookup alone).
func TestStore_SetArtifact_PointerIdentityShortCircuit(t *testing.T) {
	t.Parallel()
	s := New()
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "r"}
	art := &SourceArtifact{Kind: "GitRepository", URL: "https://example", LocalPath: "/tmp/x"}

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.SetArtifact(id, art)
	s.SetArtifact(id, art) // same pointer
	s.SetArtifact(id, art) // same pointer

	assert.Equal(t, events, 1) // same-pointer re-set short-circuits
}

func TestStore_RecordEmitted_DifferentContent(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("flux-system", "apps")
	s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.RecordEmitted(id, &Emission{Complete: true, Manifests: testutil.DocumentOccurrences([]map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-1"}},
	})}, nil)
	s.RecordEmitted(id, &Emission{Complete: true, Manifests: testutil.DocumentOccurrences([]map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-2"}},
	})}, nil)

	assert.Equal(t, events, 2)
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

func TestStore_Concurrency(t *testing.T) {
	t.Parallel()
	s := New()
	const operations = 200

	var wg sync.WaitGroup
	wg.Go(func() {
		for range operations {
			s.AddObject(newCM("a", "ns"))
		}
	})
	wg.Go(func() {
		for range operations {
			_ = s.ListObjects("", "ConfigMap")
		}
	})
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

type testStoreListObjectsByKindIndexCaseInputs struct {
	core                 *bool
	insertForeignFirst   *bool
	withdrawForeignFirst *bool
}

func testStoreListObjectsByKindIndexCase(t *testing.T, testCaseInput *testStoreListObjectsByKindIndexCaseInputs) {
	t.Helper()

	s := New()
	var typed manifest.BaseManifest = &manifest.Kustomization{Name: "a", Namespace: "ns1"}
	if *testCaseInput.core {
		typed = newCM("a", "ns1")
	}
	id := typed.Named()
	foreign := &manifest.RawObject{
		APIVersion: "example.test/v1",
		Kind:       id.Kind,
		Namespace:  id.Namespace,
		Name:       id.Name,
	}
	pair := []manifest.BaseManifest{typed, foreign}
	if *testCaseInput.insertForeignFirst {
		pair[0], pair[1] = pair[1], pair[0]
	}
	s.AddObjects(pair)
	other := &manifest.Secret{Name: "s", Namespace: "ns1"}
	s.AddObject(other)
	for _, tt := range []struct {
		name, group, kind string
		want              []manifest.BaseManifest
	}{
		{"real", id.Group, id.Kind, []manifest.BaseManifest{typed}},
		{"foreign", "example.test", id.Kind, []manifest.BaseManifest{foreign}},
		{"other kind", "", manifest.KindSecret, []manifest.BaseManifest{other}},
		{"group without kind", "example.test", "", []manifest.BaseManifest{}},
	} {
		got := s.ListObjects(tt.group, tt.kind)
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			{
				diff := cmp.Diff(
					tt.want,
					got,
					cmp.AllowUnexported(manifest.Kustomization{}),
					cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
				)
				assert.FatalIf(t, diff != "", "listed objects changed (-want +got): %s", diff)
			}
		})
	}
	inventory := []manifest.BaseManifest{typed, foreign, other}
	slices.SortFunc(
		inventory,
		func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) },
	)
	{
		diff := cmp.Diff(
			s.ListObjects("", ""),
			inventory,
			cmp.AllowUnexported(manifest.Kustomization{}),
			cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "stored objects changed (-want +got): %s", diff)
	}
	pair = []manifest.BaseManifest{typed, foreign}
	if *testCaseInput.withdrawForeignFirst {
		pair[0], pair[1] = pair[1], pair[0]
	}
	for i, obj := range pair {
		assert.Equal(t, s.GetObject(obj.Named()), obj)
		assert.Equal(t, s.DeleteObject(obj.Named()), true)
		assert.Equal(t, s.GetObject(obj.Named()), nil)
		assert.Equal(t, len(s.ListObjects(obj.Named().Group, id.Kind)), 0)
		if i == 0 {
			remaining := pair[1]
			assert.Equal(t, s.GetObject(remaining.Named()), remaining)
			{
				diff := cmp.Diff(
					s.ListObjects(remaining.Named().Group, id.Kind),
					[]manifest.BaseManifest{remaining},
					cmp.AllowUnexported(manifest.Kustomization{}),
					cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
				)
				assert.FatalIf(t, diff != "", "stored objects changed (-want +got): %s", diff)
			}
		}
	}
	{
		diff := cmp.Diff(
			s.ListObjects("", ""),
			[]manifest.BaseManifest{other},
			cmp.AllowUnexported(manifest.Kustomization{}),
			cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "stored objects changed (-want +got): %s", diff)
	}
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
