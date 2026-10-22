package store

import (
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

// EventKind enumerates the three observable changes the Store dispatches.
type EventKind int

const (
	// EventObjectAdded fires when a new manifest is added (or when a
	// listener is registered with flush=true, to replay existing state).
	EventObjectAdded EventKind = iota + 1
	// EventStatusUpdated reports condition transitions and transient reconciliation progress.
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
		if e, ok := p.payload.(*Emission); ok && !emissionCurrent(e, p.input) {
			continue
		}
		safeInvoke(fn, p.id, p.payload)
	}
	return func() { set.remove(handle) }
}

// idPayload is the replay tuple snapshotForReplay returns.
type idPayload struct {
	id      manifest.NamedResource
	payload any
	input   Child
}

// snapshotForReplay captures the existing-state replay for event.
// Caller MUST hold every shard's write lock (via lockAll) — the
// snapshot read walks every shard and must be atomic with respect to
// writers' map updates so the listener-snapshot they capture is
// consistent with the replay set returned here.
func (s *Store) snapshotForReplay(event EventKind) []idPayload {
	switch event {
	case EventObjectAdded:
		return collectReplay(s.shards[:], func(sh *shard) map[manifest.NamedResource]objectCell {
			return sh.cells
		}, func(sh *shard, id manifest.NamedResource, _ objectCell) (any, bool) {
			obj := sh.objectLocked(id)
			return obj, obj != nil
		})
	case EventStatusUpdated:
		return collectReplay(s.shards[:], func(sh *shard) map[manifest.NamedResource]conditionRecord {
			return sh.conditions
		}, func(_ *shard, _ manifest.NamedResource, record conditionRecord) (any, bool) {
			return record.statusInfo()
		})
	case EventArtifactUpdated:
		return collectReplay(s.shards[:], func(sh *shard) map[manifest.NamedResource]artifactRecord {
			return sh.artifacts
		}, func(_ *shard, _ manifest.NamedResource, record artifactRecord) (any, bool) {
			return record.value, record.value != nil
		})
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
func collectReplay[V any](shards []*shard, pick func(*shard) map[manifest.NamedResource]V, conv func(*shard, manifest.NamedResource, V) (any, bool)) []idPayload {
	total := 0
	for _, sh := range shards {
		total += len(pick(sh))
	}
	out := make([]idPayload, 0, total)
	for _, sh := range shards {
		for id, v := range pick(sh) {
			if sh.derived && sh.cells[id].hidden {
				continue
			}
			payload, ok := conv(sh, id, v)
			if !ok {
				continue
			}
			var input Child
			if _, ok := payload.(*Emission); ok {
				var view capture
				sh.captureLocked(id, sh.cells[id], &view)
				input = view.child
			}

			out = append(out, idPayload{id: id, payload: payload, input: input})
		}
	}
	return out
}

var emptyListenerSnapshot listenerSnapshot

// fireUnderLock MUST capture the immutable listener set before the writer unlocks.
func fireUnderLock(s *Store, event EventKind, id manifest.NamedResource) eventDispatch {
	listeners := s.listeners[event].snapshot.Load()
	if len(listeners.entries) == 0 {
		return eventDispatch{}
	}
	return eventDispatch{id: id, listeners: listeners}
}

func safeInvoke(fn Listener, id manifest.NamedResource, payload any) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("store: listener panicked", "id", id.String(), "panic", r)
		}
	}()
	fn(id, payload)
}

// Listener handles are never reused, avoiding ABA bugs after removal.
// Only add/remove hold mu; published snapshots and their backing storage
// MUST remain immutable while dispatchers retain them.
type listenerSet struct {
	mu       sync.Mutex
	snapshot atomic.Pointer[listenerSnapshot]
	nextID   int64
}

type listenerSnapshot struct {
	entries []listenerEntry
}

type listenerEntry struct {
	id int64
	fn Listener
}

func newListenerSet() *listenerSet {
	l := &listenerSet{}
	l.snapshot.Store(&emptyListenerSnapshot)
	return l
}

func (l *listenerSet) add(fn Listener) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nextID++
	id := l.nextID
	entries := append(slices.Clone(l.snapshot.Load().entries), listenerEntry{id: id, fn: fn})
	l.snapshot.Store(&listenerSnapshot{entries: entries})
	return id
}

func (l *listenerSet) remove(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entries := l.snapshot.Load().entries
	i := slices.IndexFunc(entries, func(e listenerEntry) bool {
		return e.id == id
	})
	if i < 0 {
		return
	}
	l.snapshot.Store(&listenerSnapshot{entries: slices.Delete(slices.Clone(entries), i, i+1)})
}

type eventDispatch struct {
	id        manifest.NamedResource
	listeners *listenerSnapshot
	payload   any
}

func (e *eventDispatch) run() {
	if e.listeners != nil {
		for _, entry := range e.listeners.entries {
			safeInvoke(entry.fn, e.id, e.payload)
		}
	}
}

func dispatchEvents(events []eventDispatch) {
	slices.SortStableFunc(events, func(a, b eventDispatch) int { return a.id.Compare(b.id) })
	for _, event := range events {
		event.run()
	}
}
