package store

import (
	"log/slog"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// EventKind enumerates the three observable changes the Store dispatches.
type EventKind int

const (
	// EventObjectAdded fires when a new manifest is added (or when a
	// listener is registered with Flush=true, to replay existing state).
	EventObjectAdded EventKind = iota + 1
	// EventStatusUpdated fires when status transitions.
	EventStatusUpdated
	// EventArtifactUpdated fires when an artifact is stored.
	EventArtifactUpdated
)

// Listener receives store events. The payload type depends on EventKind:
//   - EventObjectAdded     → manifest.BaseManifest
//   - EventStatusUpdated   → StatusInfo
//   - EventArtifactUpdated → Artifact
//
// Listeners run synchronously on the goroutine that triggered the event,
// so they MUST NOT call back into the same Store with a blocking call.
type Listener func(id manifest.NamedResource, payload any)

// Unsubscribe removes a listener. It is safe to call from inside the
// listener.
type Unsubscribe func()

// OnObject registers fn for every EventObjectAdded with a typed
// payload. When replay is true, fn fires synchronously for every
// object already in the store before returning — useful when wiring
// a UI mid-render. Listeners MUST NOT block the dispatching goroutine.
func (s *Store) OnObject(fn func(manifest.NamedResource, manifest.BaseManifest), replay bool) Unsubscribe {
	return s.AddListener(EventObjectAdded, func(id manifest.NamedResource, p any) {
		obj, _ := p.(manifest.BaseManifest)
		fn(id, obj)
	}, replay)
}

// OnStatus registers fn for every EventStatusUpdated with the typed
// StatusInfo payload. Same blocking / replay semantics as OnObject.
func (s *Store) OnStatus(fn func(manifest.NamedResource, StatusInfo), replay bool) Unsubscribe {
	return s.AddListener(EventStatusUpdated, func(id manifest.NamedResource, p any) {
		info, _ := p.(StatusInfo)
		fn(id, info)
	}, replay)
}

// OnArtifact registers fn for every EventArtifactUpdated with the
// typed Artifact payload.
func (s *Store) OnArtifact(fn func(manifest.NamedResource, Artifact), replay bool) Unsubscribe {
	return s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, p any) {
		art, _ := p.(Artifact)
		fn(id, art)
	}, replay)
}

// --- Listener bus implementation ---

// AddListener registers a callback for the given event kind. When
// flush==true, the listener is immediately invoked with every matching
// object already in the store before the call returns. Replay order is
// unspecified (Go-map iteration); listeners that need a deterministic
// order must sort what they receive. Listener panics during replay
// are recovered, same as live dispatch. The returned Unsubscribe
// removes the listener.
//
// Lock strategy (sharded):
//   - flush=false: holds every shard's RLock during set.add so a
//     concurrent writer can't snapshot listeners (fireUnderLock) and
//     dispatch before fn is registered. We need RLocks on ALL shards
//     because a writer on any shard runs fireUnderLock under its own
//     shard's write lock — without holding every shard's RLock here,
//     a concurrent writer on shard X could snapshot listeners (not
//     yet including fn), release shard X's lock, and dispatch while
//     fn is being added. The cost (one rLockAll per AddListener) is
//     paid once at controller wiring time, not on the dispatch hot
//     path.
//   - flush=true: holds every shard's write lock across (register +
//     snapshot) so the pair is atomic with respect to writers. Without
//     the write locks, a concurrent AddObject on some shard could
//     update its map, snapshot listeners (already including fn via
//     set.add), and dispatch — while this goroutine replays the same
//     object from the post-update map snapshot, double-firing fn.
//     Exactly-one delivery is the invariant; the canonical-order
//     lockAll prevents deadlock between two simultaneous flush=true
//     calls.
func (s *Store) AddListener(event EventKind, fn Listener, flush bool) Unsubscribe {
	if event < 1 || int(event) > numEventKinds {
		panic("store: unknown event kind")
	}
	set := s.listeners[event]
	if !flush {
		// All-shard RLock closes the dispatcher-vs-add race uniformly
		// across shards. Writers acquire their single shard's write
		// lock to fire — RLocking every shard here forces the writer
		// to wait until our set.add completes, so the writer's
		// listener snapshot either pre-dates this add (and our fn
		// correctly misses that pre-existing event) or post-dates it
		// (and our fn is in the snapshot — correct).
		s.rLockAll()
		handle := set.add(fn)
		s.rUnlockAll()
		return func() { set.remove(handle) }
	}
	// flush=true: must hold every shard's write lock so the
	// (register + capture replay snapshot) pair is atomic with
	// respect to writers anywhere in the store.
	s.lockAll()
	handle := set.add(fn)
	pairs := s.snapshotForReplay(event)
	s.unlockAll()
	for _, p := range pairs {
		safeInvoke(fn, p.id, p.payload)
	}
	return func() { set.remove(handle) }
}

// idPayload is the replay tuple snapshotForReplay returns.
type idPayload struct {
	id      manifest.NamedResource
	payload any
}

// snapshotForReplay captures the existing-state replay for event.
// Caller MUST hold every shard's write lock (via lockAll) — the
// snapshot read walks every shard and must be atomic with respect to
// writers' map updates so the listener-snapshot they capture is
// consistent with the replay set returned here.
func (s *Store) snapshotForReplay(event EventKind) []idPayload {
	switch event {
	case EventObjectAdded:
		return collectReplay(s.shards[:], func(sh *shard) map[manifest.NamedResource]manifest.BaseManifest {
			return sh.objects
		}, func(obj manifest.BaseManifest) (any, bool) { return obj, true })
	case EventStatusUpdated:
		return collectReplay(s.shards[:], func(sh *shard) map[manifest.NamedResource][]Condition {
			return sh.conditions
		}, func(conds []Condition) (any, bool) { return statusInfoFromConditions(conds) })
	case EventArtifactUpdated:
		return collectReplay(s.shards[:], func(sh *shard) map[manifest.NamedResource]Artifact {
			return sh.artifacts
		}, func(art Artifact) (any, bool) { return art, true })
	}
	return nil
}

// collectReplay flattens one per-shard map (selected by pick) into a
// replay slice, applying conv to each value. conv returns (payload, ok)
// — entries for which ok is false are skipped (the EventStatusUpdated
// case drops ids whose conditions carry no Ready rollup). The result is
// pre-sized to the summed map lengths so the common all-keep case never
// re-grows. Caller MUST hold every shard's write lock (see
// snapshotForReplay).
func collectReplay[V any](shards []*shard, pick func(*shard) map[manifest.NamedResource]V, conv func(V) (any, bool)) []idPayload {
	total := 0
	for _, sh := range shards {
		total += len(pick(sh))
	}
	out := make([]idPayload, 0, total)
	for _, sh := range shards {
		for id, v := range pick(sh) {
			if payload, ok := conv(v); ok {
				out = append(out, idPayload{id, payload})
			}
		}
	}
	return out
}

// fireUnderLock is the race-safe dispatcher writers MUST use: it
// captures the listener snapshot under the caller's already-held
// shard lock and returns a closure the caller invokes AFTER releasing
// the lock. The pattern is:
//
//	sh.mu.Lock()
//	... mutate sh.objects / sh.conditions / sh.artifacts ...
//	dispatch := s.fireUnderLock(EventX, id, payload)
//	sh.mu.Unlock()
//	dispatch()
//
// Holding the shard lock while snapshotting listeners closes the
// AddListener-vs-writer race documented on AddListener: a
// flush=false add holds every shard's RLock, so the snapshot
// captured here cannot interleave with an in-progress add.
//
// When no listeners are registered for event, fireUnderLock returns
// a no-op closure with no allocation — AddRendered always dispatches
// (so the listener-contract gap is closed) and must stay cheap on
// the render hot path when nothing's listening.
//
// The snapshot slice is drawn from a sync.Pool keyed on capacity
// bucket and released back after dispatch via defer (so a panicking
// listener still returns it). The returned closure owns the slice
// exclusively — the dispatcher MUST NOT alias it past the closure
// call. listenerSet.snapshot is the only entry to the pool; never
// retain the result beyond fireUnderLock's returned closure.
func (s *Store) fireUnderLock(event EventKind, id manifest.NamedResource, payload any) func() {
	listeners := s.listeners[event].snapshot()
	if len(listeners) == 0 {
		return func() {}
	}
	return func() {
		defer releaseListenerSnapshot(listeners)
		for _, fn := range listeners {
			safeInvoke(fn, id, payload)
		}
	}
}

func safeInvoke(fn Listener, id manifest.NamedResource, payload any) {
	defer func() {
		if r := recover(); r != nil {
			// A panicking listener silently swallowed the event in
			// the past — the orchestrator would see a missing
			// status update with no diagnostic. Log at Error so
			// a CI run surfaces the panic instead of buried
			// "FAILED (no status reported)" downstream.
			slog.Error("store: listener panicked", "id", id.String(), "panic", r)
		}
	}()
	fn(id, payload)
}

// listenerSet is a copy-on-snapshot slice of listeners. add returns a
// handle (a stable id) used by remove to find the entry. We deliberately
// do not reuse handles after removal to avoid ABA bugs in long sessions.
type listenerSet struct {
	mu      sync.Mutex
	entries []listenerEntry
	nextID  int64
}

type listenerEntry struct {
	id int64
	fn Listener
}

func newListenerSet() *listenerSet { return &listenerSet{} }

func (l *listenerSet) add(fn Listener) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nextID++
	id := l.nextID
	l.entries = append(l.entries, listenerEntry{id: id, fn: fn})
	return id
}

func (l *listenerSet) remove(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = slices.DeleteFunc(l.entries, func(e listenerEntry) bool {
		return e.id == id
	})
}

// snapshot returns a copy of the current listener funcs so dispatch can
// iterate without holding the lock (and so listeners can mutate the set
// during dispatch without affecting the current pass).
//
// Returns nil (not a zero-length slice) when no listeners are
// registered so writers' fireUnderLock can short-circuit without
// allocating — AddRendered is on the render hot path, and the
// listener-contract guarantee shouldn't cost an allocation per
// rendered doc when nothing's listening for that kind.
//
// Non-empty snapshots are drawn from listenerSnapshotPools, bucketed
// by capacity (16/64/256/1024). Callers MUST hand the result to
// releaseListenerSnapshot exactly once after dispatch and MUST NOT
// retain any reference to the slice past that point — the slice is
// recycled and a future fire will overwrite its contents. See
// fireUnderLock for the canonical release pattern.
func (l *listenerSet) snapshot() []Listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return nil
	}
	bucket := poolBucket(len(l.entries))
	out := *listenerSnapshotPools[bucket].Get().(*[]Listener)
	// Reset to zero length; release nils out entries on Put so the
	// pooled slice carries no stale closure references, but the
	// length is whatever the previous fire grew it to. Truncate
	// here and let append below land each listener back.
	out = out[:0]
	for _, e := range l.entries {
		out = append(out, e.fn)
	}
	return out
}

// listenerSnapshotPools holds reusable []Listener backing arrays
// bucketed by capacity. Bucket boundaries (16/64/256/1024) cover the
// observed listener-fanout shape: most events fire against <16
// listeners (per-kind controller subscriptions); a few high-fanout
// fixtures (the orchestrator's parallel test harness) reach into
// the hundreds. A 4-bucket scheme keeps the pool footprint bounded
// while letting each fire land in a same-or-larger slot.
//
// The pool stores *[]Listener (pointer-to-slice) — sync.Pool
// internally retains values by interface{}, and stashing a bare
// []Listener would force an alloc on every Put for the slice
// header escape. Pointer-to-slice keeps the put alloc-free.
var listenerSnapshotPools = [4]sync.Pool{
	{New: func() any { s := make([]Listener, 0, 16); return &s }},
	{New: func() any { s := make([]Listener, 0, 64); return &s }},
	{New: func() any { s := make([]Listener, 0, 256); return &s }},
	{New: func() any { s := make([]Listener, 0, 1024); return &s }},
}

// poolBucket maps a listener count (or slice capacity at release
// time) to a listenerSnapshotPools index. Inputs above the largest
// bucket round down to the 1024-cap bucket — those slices grow
// past the pooled capacity on Get's append and the runtime
// reallocates a fresh backing array, but the pooled header is
// still recyclable for the next fire of comparable size.
func poolBucket(n int) int {
	switch {
	case n <= 16:
		return 0
	case n <= 64:
		return 1
	case n <= 256:
		return 2
	default:
		return 3
	}
}

// releaseListenerSnapshot hands snap back to the appropriate pool.
// No-op for nil (the empty-set fast path from snapshot). Callers MUST
// drop every reference to snap after calling this — a later fire
// will recycle the backing array and any retained alias would race
// with that fire's writes.
//
// Bucket selection uses capacity (not length), matching the size
// the slice was drawn at. A slice that grew past its original
// bucket via append still returns to whichever bucket its current
// cap matches; the bucket-down rule means an over-large slice
// lands in the largest pool slot, which is the correct home for
// the next fire that happens to need that much room.
func releaseListenerSnapshot(snap []Listener) {
	if snap == nil {
		return
	}
	// Clear listener references so the pool doesn't pin payload
	// closures (each Listener may close over arbitrary state).
	clear(snap)
	bucket := poolBucket(cap(snap))
	snap = snap[:0]
	listenerSnapshotPools[bucket].Put(&snap)
}
