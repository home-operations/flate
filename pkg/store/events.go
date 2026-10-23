package store

import (
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

// EventKind enumerates the observable changes the Store dispatches.
type EventKind int

const (
	// EventObjectAdded fires when a new manifest is added (or when a
	// listener is registered with flush=true, to replay existing state).
	EventObjectAdded EventKind = iota + 1
	// EventStatusUpdated fires when status transitions.
	EventStatusUpdated
	// EventArtifactUpdated fires when an artifact is stored.
	EventArtifactUpdated
	// EventWarningAdded fires once per newly recorded origin and complete warning payload.
	EventWarningAdded
)

// Listener receives store events. The payload type depends on EventKind:
//   - EventObjectAdded     → manifest.BaseManifest
//   - EventStatusUpdated   → StatusInfo
//   - EventArtifactUpdated → Artifact
//   - EventWarningAdded    → manifest.Warning with Count 1; id is the origin
//
// Listeners run synchronously on the goroutine that triggered the event,
// after its state lock is released. Payloads are immutable snapshots; listeners
// MUST copy Warning.Detail before modifying it.
type Listener func(id manifest.NamedResource, payload any)

// Unsubscribe removes a listener. It is safe to call from inside the
// listener.
type Unsubscribe func()

// AddListener registers a callback for the given event kind. With flush=true,
// it replays existing state before returning, in unspecified order. Warning
// replay delivers every origin/payload contribution, including zero origins,
// with Count 1. Derived conflicting-producer warnings are not contributions.
// Listener panics are recovered during both replay and live dispatch.
//
// Registration and replay capture MUST hold the same state lock as writers'
// listener snapshots: warnMu for warnings, all shard locks for other events.
// This prevents missed live events and double delivery between replay and live.
func (s *Store) AddListener(event EventKind, fn Listener, flush bool) Unsubscribe {
	if event < 1 || int(event) > numEventKinds {
		panic("store: unknown event kind")
	}
	set := &s.listeners[event]
	if event == EventWarningAdded {
		s.warnMu.Lock()
	} else if flush {
		s.lockAll()
	} else {
		s.rLockAll()
	}
	handle := set.add(fn)
	var pairs []idPayload
	if flush {
		pairs = s.snapshotForReplay(event)
	}
	if event == EventWarningAdded {
		s.warnMu.Unlock()
	} else if flush {
		s.unlockAll()
	} else {
		s.rUnlockAll()
	}
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

// snapshotForReplay requires warnMu for warnings or lockAll for other events.
func (s *Store) snapshotForReplay(event EventKind) []idPayload {
	switch event {
	case EventWarningAdded:
		var out []idPayload
		for _, record := range s.warnOrder {
			w := record.warning
			w.Count = 1
			for origin := range record.origins {
				out = append(out, idPayload{id: origin, payload: w})
			}
		}
		return out
	case EventObjectAdded:
		return collectReplay(s.shards[:], func(sh *shard) map[manifest.NamedResource]manifest.BaseManifest {
			return sh.objects
		}, func(obj manifest.BaseManifest) (any, bool) { return obj, true })
	case EventStatusUpdated:
		return collectReplay(s.shards[:], func(sh *shard) map[manifest.NamedResource]conditionRecord {
			return sh.conditions
		}, func(record conditionRecord) (any, bool) { return record.statusInfo() })
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
			if sh.hidden[id] {
				continue
			}
			var input Child
			if _, ok := any(v).(*Emission); ok {
				input, _ = sh.representativeLocked(id)
			}
			if eventObj, ok := any(v).(manifest.BaseManifest); ok {
				obj := sh.objectLocked(eventObj.Named())
				if obj == nil {
					continue
				}
				v = any(obj).(V)
			}
			if payload, ok := conv(v); ok {
				out = append(out, idPayload{id: id, payload: payload, input: input})
			}
		}
	}
	return out
}

// A generic no-op closure captures its dictionary and allocates.
var emptyDispatch = func() {}

var emptyListenerSnapshot listenerSnapshot

// fireUnderLock captures listeners under the caller's already-held state lock.
// Callers MUST invoke the returned closure after releasing that lock. Sharing
// the lock with AddListener makes registration atomic with respect to writers.
//
// When no listeners are registered for event, fireUnderLock returns
// a no-op closure without boxing the payload. AddObject dispatches changed objects
// and must stay cheap on the render hot path when nothing's listening.
//
// Published snapshots are immutable. An in-flight dispatch may still invoke
// an unsubscribed listener; registration affects only future snapshots.
func fireUnderLock[T any](s *Store, event EventKind, id manifest.NamedResource, payload T) func() {
	listeners := s.listeners[event].snapshot.Load()
	if len(listeners.entries) == 0 {
		return emptyDispatch
	}
	boxed := any(payload)
	return func() {
		for _, entry := range listeners.entries {
			safeInvoke(entry.fn, id, boxed)
		}
	}
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
	id  manifest.NamedResource
	run func()
}

func dispatchEvents(events []eventDispatch) {
	slices.SortStableFunc(events, func(a, b eventDispatch) int { return a.id.Compare(b.id) })
	for _, event := range events {
		event.run()
	}
}
