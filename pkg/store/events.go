package store

import (
	"maps"
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
func (s *Store) AddListener(event EventKind, fn Listener, flush bool) Unsubscribe {
	set, ok := s.listeners[event]
	if !ok {
		panic("store: unknown event kind")
	}
	handle := set.add(fn)

	if flush {
		s.replayInto(event, fn)
	}
	return func() { set.remove(handle) }
}

// replayInto delivers existing state to a fresh listener so it can catch
// up. Called under no lock; takes its own RLock. Listener panics are
// recovered via safeInvoke — replay must NOT crash the controller that
// just attached the listener (parity with live `fire` dispatch).
func (s *Store) replayInto(event EventKind, fn Listener) {
	switch event {
	case EventObjectAdded:
		s.mu.RLock()
		snap := make(map[manifest.NamedResource]manifest.BaseManifest, len(s.objects))
		maps.Copy(snap, s.objects)
		s.mu.RUnlock()
		for id, obj := range snap {
			safeInvoke(fn, id, obj)
		}
	case EventStatusUpdated:
		s.mu.RLock()
		snap := make(map[manifest.NamedResource]StatusInfo, len(s.conditions))
		for id, conds := range s.conditions {
			if info, ok := statusInfoFromConditions(conds); ok {
				snap[id] = info
			}
		}
		s.mu.RUnlock()
		for id, info := range snap {
			safeInvoke(fn, id, info)
		}
	case EventArtifactUpdated:
		s.mu.RLock()
		snap := make(map[manifest.NamedResource]Artifact, len(s.artifacts))
		maps.Copy(snap, s.artifacts)
		s.mu.RUnlock()
		for id, art := range snap {
			safeInvoke(fn, id, art)
		}
	}
}

// fire dispatches an event to every registered listener. Listeners run
// synchronously on the calling goroutine. A panic in any listener is
// recovered to prevent one bad listener from breaking the dispatch.
func (s *Store) fire(event EventKind, id manifest.NamedResource, payload any) {
	set := s.listeners[event]
	if set == nil {
		return
	}
	for _, fn := range set.snapshot() {
		safeInvoke(fn, id, payload)
	}
}

func safeInvoke(fn Listener, id manifest.NamedResource, payload any) {
	defer func() { _ = recover() }()
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
func (l *listenerSet) snapshot() []Listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Listener, len(l.entries))
	for i := range l.entries {
		out[i] = l.entries[i].fn
	}
	return out
}
