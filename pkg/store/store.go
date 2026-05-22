package store

import (
	"maps"
	"reflect"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// Store is the central in-memory state container.
//
// # Immutability contract
//
// Objects passed to AddObject (and returned by GetObject / GetByName /
// ListObjects) are treated as IMMUTABLE after insertion. The store
// returns shared pointers rather than defensive copies for performance:
// rendering pipelines read millions of fields per reconcile, and
// cloning the full manifest tree on every read would dominate CPU.
//
// Callers that need to "modify" a stored object must:
//
//  1. Shallow-copy the struct (most manifest types are flat enough
//     that *clone := *orig works).
//  2. Mutate the copy's fields.
//  3. Re-AddObject the modified copy. AddObject's reflect.DeepEqual
//     dedup avoids spurious events, and the second-pass dispatch
//     reaches downstream controllers.
//
// Mutating an object after AddObject is a bug — concurrent readers
// will see torn state and AddObject's dedup will compare against
// a moving target. The loader (pkg/loader/inherit.go) and
// orchestrator (pkg/orchestrator/orchestrator.go) follow the
// clone-then-AddObject pattern; any new mutation site should too.
type Store struct {
	mu         sync.RWMutex
	objects    map[manifest.NamedResource]manifest.BaseManifest
	conditions map[manifest.NamedResource][]Condition
	artifacts  map[manifest.NamedResource]Artifact

	// byName is a secondary index keyed by kind, then "namespace/name".
	// It makes hot-path namespaced lookups (e.g. valuesFrom ConfigMap /
	// Secret resolution) O(1) instead of O(total objects of that kind).
	byName map[string]map[string]manifest.BaseManifest

	// rendered tracks IDs that were emitted by a parent Kustomization's
	// render output (in addition to or instead of the file walker's
	// static load). Used by the orchestrator to detect orphan
	// Kustomizations — files that exist on disk but no parent
	// kustomization.yaml ever references, so Flux would never
	// reconcile them either.
	rendered map[manifest.NamedResource]struct{}

	// listeners is keyed by EventKind. Each entry is a slice of
	// (id, listener) pairs. We use a slice + linear scan because:
	//   - listener counts are tiny (a handful per event)
	//   - removal preserves order
	//   - Unsubscribe identity is the slice index encoded in a closure
	listeners map[EventKind]*listenerSet
}

// New constructs an empty Store.
func New() *Store {
	return &Store{
		objects:    make(map[manifest.NamedResource]manifest.BaseManifest),
		conditions: make(map[manifest.NamedResource][]Condition),
		artifacts:  make(map[manifest.NamedResource]Artifact),
		byName:     make(map[string]map[string]manifest.BaseManifest),
		rendered:   make(map[manifest.NamedResource]struct{}),
		listeners: map[EventKind]*listenerSet{
			EventObjectAdded:     newListenerSet(),
			EventStatusUpdated:   newListenerSet(),
			EventArtifactUpdated: newListenerSet(),
		},
	}
}

// MarkRendered records that id was emitted by a parent Kustomization's
// render output (as opposed to coming only from the static file walk).
// The orchestrator's post-run phase uses this to distinguish real
// failures from orphaned-and-loaded-anyway resources.
func (s *Store) MarkRendered(id manifest.NamedResource) {
	s.mu.Lock()
	s.rendered[id] = struct{}{}
	s.mu.Unlock()
}

// WasRendered reports whether id was ever emitted by a parent's render.
func (s *Store) WasRendered(id manifest.NamedResource) bool {
	s.mu.RLock()
	_, ok := s.rendered[id]
	s.mu.RUnlock()
	return ok
}

func nameKey(namespace, name string) string { return namespace + "/" + name }

// AddObject inserts a manifest. Re-adding an equal object is a no-op.
// Re-adding a different object overwrites the existing entry AND still
// dispatches an ObjectAdded event (so newer values propagate).
func (s *Store) AddObject(obj manifest.BaseManifest) {
	id := obj.Named()
	s.mu.Lock()
	prev, exists := s.objects[id]
	if exists && reflect.DeepEqual(prev, obj) {
		s.mu.Unlock()
		return
	}
	s.objects[id] = obj
	inner, ok := s.byName[id.Kind]
	if !ok {
		inner = make(map[string]manifest.BaseManifest)
		s.byName[id.Kind] = inner
	}
	inner[nameKey(id.Namespace, id.Name)] = obj
	s.mu.Unlock()
	s.fire(EventObjectAdded, id, obj)
}

// AddRendered records a manifest produced by helm/kustomize rendering.
// Compared to AddObject it skips the reflect.DeepEqual dedup check and
// the listener dispatch — rendered docs are leaves that no controller
// listens for. The byName index is still updated so downstream
// valuesFrom / GetByName lookups see the new object. On the build/diff
// hot path this removes ~N×listeners closure invocations and the
// dispatch defer/recover stack per rendered manifest.
func (s *Store) AddRendered(obj manifest.BaseManifest) {
	id := obj.Named()
	s.mu.Lock()
	s.objects[id] = obj
	inner, ok := s.byName[id.Kind]
	if !ok {
		inner = make(map[string]manifest.BaseManifest)
		s.byName[id.Kind] = inner
	}
	inner[nameKey(id.Namespace, id.Name)] = obj
	s.mu.Unlock()
}

// GetObject returns the manifest for id, or nil if not present.
func (s *Store) GetObject(id manifest.NamedResource) manifest.BaseManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.objects[id]
}

// DeleteObject removes the object stored under id. Returns whether
// anything was removed. Status and artifact entries (if any) are also
// dropped so a re-add under a different id starts clean.
func (s *Store) DeleteObject(id manifest.NamedResource) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[id]; !ok {
		return false
	}
	delete(s.objects, id)
	delete(s.conditions, id)
	delete(s.artifacts, id)
	if inner := s.byName[id.Kind]; inner != nil {
		delete(inner, nameKey(id.Namespace, id.Name))
	}
	return true
}

// GetByName returns the object matching (kind, namespace, name), or nil
// when none is present. Hot-path callers (valuesFrom expansion, source
// resolution) should prefer this over filtering ListObjects.
func (s *Store) GetByName(kind, namespace, name string) manifest.BaseManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if inner := s.byName[kind]; inner != nil {
		return inner[nameKey(namespace, name)]
	}
	return nil
}

// ListObjects returns every stored manifest, optionally filtered by kind.
// An empty kind matches all objects.
func (s *Store) ListObjects(kind string) []manifest.BaseManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]manifest.BaseManifest, 0, len(s.objects))
	for id, obj := range s.objects {
		if kind != "" && id.Kind != kind {
			continue
		}
		out = append(out, obj)
	}
	return out
}

// UpdateStatus records a Ready-condition transition and dispatches a
// StatusUpdated event when the StatusInfo rollup changes. Internally
// the (status, message) pair is stored as a metav1.Condition so
// future callers (ReadyExpr CEL, SOPS detection, healthChecks) can
// see the rich state via GetConditions.
func (s *Store) UpdateStatus(id manifest.NamedResource, status Status, message string) {
	s.SetCondition(id, readyCondition(status, message))
}

// SetCondition upserts cond into the resource's condition list keyed
// by cond.Type. Dispatches a StatusUpdated event with the *StatusInfo
// rollup* (derived from Ready) on every observable condition change,
// not just Ready transitions. Listeners that only care about Ready
// can filter on the StatusInfo payload; CEL-based ReadyExpr watchers
// need the broader notification so a Healthy condition flip (for
// example) wakes them.
//
// An identical re-write of the same condition is a no-op (no event).
func (s *Store) SetCondition(id manifest.NamedResource, cond Condition) {
	s.mu.Lock()
	prev := s.conditions[id]

	updated := make([]Condition, 0, len(prev)+1)
	replaced := false
	for _, c := range prev {
		if c.Type == cond.Type {
			if conditionEqual(c, cond) {
				// Identical condition — nothing to do, including no event.
				s.mu.Unlock()
				return
			}
			updated = append(updated, cond)
			replaced = true
			continue
		}
		updated = append(updated, c)
	}
	if !replaced {
		updated = append(updated, cond)
	}
	s.conditions[id] = updated
	newInfo, _ := statusInfoFromConditions(updated)
	s.mu.Unlock()

	s.fire(EventStatusUpdated, id, newInfo)
}

// GetStatus returns the Ready-derived StatusInfo for id and whether
// a Ready condition was present.
func (s *Store) GetStatus(id manifest.NamedResource) (StatusInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return statusInfoFromConditions(s.conditions[id])
}

// GetConditions returns a copy of id's condition list. Empty for
// unknown ids.
func (s *Store) GetConditions(id manifest.NamedResource) []Condition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	conds := s.conditions[id]
	if len(conds) == 0 {
		return nil
	}
	out := make([]Condition, len(conds))
	copy(out, conds)
	return out
}

// conditionEqual reports whether two conditions carry the same
// observable state. LastTransitionTime is intentionally ignored — it
// is reset on every transition by the controller-runtime libraries
// and would otherwise prevent the no-op short-circuit from firing.
func conditionEqual(a, b Condition) bool {
	return a.Type == b.Type &&
		a.Status == b.Status &&
		a.Reason == b.Reason &&
		a.Message == b.Message &&
		a.ObservedGeneration == b.ObservedGeneration
}

// SetArtifact stores an artifact for id and dispatches an
// ArtifactUpdated event. Re-setting with a deep-equal value is a no-op.
func (s *Store) SetArtifact(id manifest.NamedResource, artifact Artifact) {
	s.mu.Lock()
	prev, exists := s.artifacts[id]
	if exists && reflect.DeepEqual(prev, artifact) {
		s.mu.Unlock()
		return
	}
	s.artifacts[id] = artifact
	s.mu.Unlock()
	s.fire(EventArtifactUpdated, id, artifact)
}

// GetArtifact returns the artifact for id, or nil if none was set.
func (s *Store) GetArtifact(id manifest.NamedResource) Artifact {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.artifacts[id]
}

// HasFailedResources reports whether any tracked Ready condition is False.
func (s *Store) HasFailedResources() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, conds := range s.conditions {
		if info, ok := statusInfoFromConditions(conds); ok && info.Status == StatusFailed {
			return true
		}
	}
	return false
}

// FailedResources returns every (id, info) currently in Failed state.
func (s *Store) FailedResources() map[manifest.NamedResource]StatusInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[manifest.NamedResource]StatusInfo)
	for id, conds := range s.conditions {
		if info, ok := statusInfoFromConditions(conds); ok && info.Status == StatusFailed {
			out[id] = info
		}
	}
	return out
}

// AddListener registers a callback for the given event kind. When
// flush==true, the listener is immediately invoked with every matching
// object already in the store before the call returns, in deterministic
// order. The returned Unsubscribe removes the listener.
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
// up. Called under no lock; takes its own RLock.
func (s *Store) replayInto(event EventKind, fn Listener) {
	switch event {
	case EventObjectAdded:
		s.mu.RLock()
		snap := make(map[manifest.NamedResource]manifest.BaseManifest, len(s.objects))
		maps.Copy(snap, s.objects)
		s.mu.RUnlock()
		for id, obj := range snap {
			fn(id, obj)
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
			fn(id, info)
		}
	case EventArtifactUpdated:
		s.mu.RLock()
		snap := make(map[manifest.NamedResource]Artifact, len(s.artifacts))
		maps.Copy(snap, s.artifacts)
		s.mu.RUnlock()
		for id, art := range snap {
			fn(id, art)
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
