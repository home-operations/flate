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
// Lock strategy:
//   - flush=false: holds s.mu.RLock() during set.add so a concurrent
//     writer can't snapshot listeners (fireUnderLock) and dispatch
//     before fn is registered. RLock is sufficient because the
//     non-flush path doesn't read or write store maps.
//   - flush=true: holds s.mu.Lock() across (register + snapshot) so
//     the pair is atomic with respect to writers. Without the write
//     lock, a concurrent AddObject could update the map, snapshot
//     listeners (already including fn via set.add), and dispatch —
//     while this goroutine replays the same object from the
//     post-update map snapshot, double-firing fn. Exactly-one
//     delivery is the invariant.
func (s *Store) AddListener(event EventKind, fn Listener, flush bool) Unsubscribe {
	if event < 1 || int(event) > numEventKinds {
		panic("store: unknown event kind")
	}
	set := s.listeners[event]
	if !flush {
		// The no-replay path needs only a read lock on s.mu. Writers
		// hold s.mu.Lock() while capturing their listener-set snapshot
		// (fireUnderLock), so holding s.mu.RLock() here is exclusive
		// with any concurrent writer's lock acquisition: the writer
		// either (a) completes its snapshot BEFORE this add (fn misses
		// that event — expected, the listener wasn't registered yet) or
		// (b) starts AFTER this add (fn is in the snapshot — correct).
		// Without any s.mu hold, a writer could snapshot listeners under
		// set.mu alone, release s.mu, and dispatch before fn lands —
		// silently missing the very event the caller registered for.
		// RLock is sufficient because we're only mutating set (which
		// has its own internal mutex), not s.objects/conditions/artifacts.
		s.mu.RLock()
		handle := set.add(fn)
		s.mu.RUnlock()
		return func() { set.remove(handle) }
	}
	// flush=true: must hold write lock so the (register + capture
	// replay snapshot) pair is atomic with respect to writers.
	s.mu.Lock()
	handle := set.add(fn)
	pairs := s.snapshotForReplay(event)
	s.mu.Unlock()
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
// Caller MUST hold s.mu (write lock) — the snapshot read must be
// atomic with respect to writers' map updates so the listener-snapshot
// they capture is consistent with the replay set returned here.
func (s *Store) snapshotForReplay(event EventKind) []idPayload {
	switch event {
	case EventObjectAdded:
		out := make([]idPayload, 0, len(s.objects))
		for id, obj := range s.objects {
			out = append(out, idPayload{id, obj})
		}
		return out
	case EventStatusUpdated:
		out := make([]idPayload, 0, len(s.conditions))
		for id, conds := range s.conditions {
			if info, ok := statusInfoFromConditions(conds); ok {
				out = append(out, idPayload{id, info})
			}
		}
		return out
	case EventArtifactUpdated:
		out := make([]idPayload, 0, len(s.artifacts))
		for id, art := range s.artifacts {
			out = append(out, idPayload{id, art})
		}
		return out
	}
	return nil
}

// fireUnderLock is the race-safe dispatcher writers MUST use: it
// captures the listener snapshot under the caller's already-held
// s.mu and returns a closure the caller invokes AFTER releasing the
// lock. The pattern is:
//
//	s.mu.Lock()
//	... mutate ...
//	dispatch := s.fireUnderLock(EventX, id, payload)
//	s.mu.Unlock()
//	dispatch()
//
// Holding s.mu while snapshotting listeners closes the
// AddListener-vs-writer race documented on AddListener.
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
	for i := range snap {
		snap[i] = nil
	}
	bucket := poolBucket(cap(snap))
	snap = snap[:0]
	listenerSnapshotPools[bucket].Put(&snap)
}
