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
// The flush=true path holds s.mu across the (register + capture
// replay snapshot) pair. Without that serialization, a concurrent
// writer (AddObject / SetCondition / SetArtifact) could update its
// map, release s.mu, snapshot listeners (now including this fresh
// listener because set.add already ran), and dispatch the live event
// to fn — while THIS goroutine separately replays the same object
// from the post-update map snapshot. The result is a double-fire.
// Holding s.mu while we both register AND capture the replay state
// means writers either see the listener-registered state before
// dispatch (replay returns nothing new, live event fires once) or
// see the pre-registered state (live event misses this listener,
// replay fires once). Exactly-one delivery either way.
func (s *Store) AddListener(event EventKind, fn Listener, flush bool) Unsubscribe {
	set, ok := s.listeners[event]
	if !ok {
		panic("store: unknown event kind")
	}
	if !flush {
		handle := set.add(fn)
		return func() { set.remove(handle) }
	}
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
func (s *Store) fireUnderLock(event EventKind, id manifest.NamedResource, payload any) func() {
	set := s.listeners[event]
	if set == nil {
		return func() {}
	}
	listeners := set.snapshot()
	if len(listeners) == 0 {
		return func() {}
	}
	return func() { dispatch(listeners, id, payload) }
}

// dispatch invokes each listener with the payload, recovering panics.
func dispatch(listeners []Listener, id manifest.NamedResource, payload any) {
	for _, fn := range listeners {
		safeInvoke(fn, id, payload)
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
func (l *listenerSet) snapshot() []Listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return nil
	}
	out := make([]Listener, len(l.entries))
	for i := range l.entries {
		out[i] = l.entries[i].fn
	}
	return out
}
