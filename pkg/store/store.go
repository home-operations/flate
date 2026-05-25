package store

import (
	"reflect"
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
		listeners: map[EventKind]*listenerSet{
			EventObjectAdded:     newListenerSet(),
			EventStatusUpdated:   newListenerSet(),
			EventArtifactUpdated: newListenerSet(),
		},
	}
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
	s.setLocked(id, obj)
	dispatch := s.fireUnderLock(EventObjectAdded, id, obj)
	s.mu.Unlock()
	dispatch()
}

// setLocked is the single funnel for inserting an object into both
// the primary `objects` map and the secondary `byName` index. Three
// write paths (AddObject, AddRendered, future renames) must keep
// these two maps in sync; routing them through one helper makes the
// "forgot to update byName" drift class structurally impossible.
// Caller MUST hold s.mu (write lock).
func (s *Store) setLocked(id manifest.NamedResource, obj manifest.BaseManifest) {
	s.objects[id] = obj
	inner, ok := s.byName[id.Kind]
	if !ok {
		inner = make(map[string]manifest.BaseManifest)
		s.byName[id.Kind] = inner
	}
	inner[nameKey(id.Namespace, id.Name)] = obj
}

// deleteLocked is the symmetric remove for setLocked. Drops the
// object from objects + byName + conditions + artifacts (the full
// lifecycle wipe) and reports whether anything was present. Caller
// MUST hold s.mu (write lock).
func (s *Store) deleteLocked(id manifest.NamedResource) bool {
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

// Refire resets the resource's Ready condition to Pending and then
// dispatches EventObjectAdded for the existing object at id. Used to
// wake up listeners that short-circuited the first time — e.g. source
// controllers that PreGate-skipped a source whose consumer joined the
// change-filter keep set only at runtime (issue #260).
//
// The status reset is load-bearing, not cosmetic. Without it, a
// consumer's depwait may read the stale Ready/"unchanged" status the
// initial PreGate skip wrote, return immediately, and race ahead of
// the queued re-reconcile — producing an "artifact not found" failure
// while the actual fetch is still in flight. UpdateStatus completes
// before EventObjectAdded fires, so any depwait that reads status
// between the two events still sees Pending.
//
// No-op when id is not in the store. The object existence check, the
// status reset, and the event capture all run under one s.mu
// acquisition so a concurrent DeleteObject can't slip in and leave
// the listener dispatching against a stale object or the status map
// resurrected with a phantom Pending entry.
func (s *Store) Refire(id manifest.NamedResource) {
	s.mu.Lock()
	obj, ok := s.objects[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	updated, changed := s.setConditionLocked(id, readyCondition(StatusPending, MsgRefetching))
	dispatchObj := s.fireUnderLock(EventObjectAdded, id, obj)
	var dispatchStatus func()
	if changed {
		info, _ := statusInfoFromConditions(updated)
		dispatchStatus = s.fireUnderLock(EventStatusUpdated, id, info)
	}
	s.mu.Unlock()
	if dispatchStatus != nil {
		dispatchStatus()
	}
	dispatchObj()
}

// Cloneable is satisfied by manifest types that can be shallowly
// duplicated for safe mutation under the Store's immutability
// contract. Kustomization, HelmRelease implement this; new types that
// need post-load mutation should follow.
type Cloneable[T any] interface {
	Clone() T
}

// Mutate atomically replaces the store-owned object under id with the
// result of mutating a fresh clone. Encodes the documented
// immutability contract in one place: callers can't forget
// clone-then-AddObject. Returns false when no object of type T is
// stored under id (no-op).
//
// Use this for any post-load mutation — validateDependsOn pruning,
// namespace-inheritance rewrites, alias seeding. Listeners fire as
// they would on a fresh AddObject (intentionally — downstream
// controllers re-reconcile against the mutated spec).
func Mutate[T interface {
	manifest.BaseManifest
	Cloneable[T]
}](s *Store, id manifest.NamedResource, mutate func(T)) bool {
	obj, ok := s.GetObject(id).(T)
	if !ok {
		return false
	}
	cloned := obj.Clone()
	mutate(cloned)
	s.AddObject(cloned)
	return true
}

// AddRendered records a manifest produced by helm/kustomize rendering.
// Compared to AddObject it skips the reflect.DeepEqual dedup check —
// rendered docs change on every render and the dedup would never hit.
// Listener dispatch is unconditional: the listener-contract gap that
// previously existed (silent miss for any future kind with listeners,
// e.g. watching rendered Secret docs for valuesFrom invalidation) is
// closed by routing every write through fireUnderLock. The empty-set
// fast path in listenerSet.snapshot keeps the common "no listeners
// for this kind" case at one mutex pair, no allocations.
func (s *Store) AddRendered(obj manifest.BaseManifest) {
	id := obj.Named()
	s.mu.Lock()
	s.setLocked(id, obj)
	dispatch := s.fireUnderLock(EventObjectAdded, id, obj)
	s.mu.Unlock()
	dispatch()
}

// GetObject returns the manifest for id, or nil if not present.
func (s *Store) GetObject(id manifest.NamedResource) manifest.BaseManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.objects[id]
}

// Snapshot returns the manifest and conditions for id captured under
// a single RLock acquisition. Use this when a caller needs a
// consistent view of both — e.g. depwait's CEL projection reads
// status.conditions AND metadata.labels in one expression, and
// independent GetObject / GetConditions calls can mix a fresh object
// snapshot with stale conditions (or vice versa) if a writer
// interleaves. Returns nil object and nil conditions for unknown ids.
func (s *Store) Snapshot(id manifest.NamedResource) (manifest.BaseManifest, []Condition) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj := s.objects[id]
	conds := s.conditions[id]
	if len(conds) == 0 {
		return obj, nil
	}
	out := make([]Condition, len(conds))
	copy(out, conds)
	return obj, out
}

// DeleteObject removes the object stored under id. Returns whether
// anything was removed. Status and artifact entries (if any) are also
// dropped so a re-add under a different id starts clean.
func (s *Store) DeleteObject(id manifest.NamedResource) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteLocked(id)
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
// An empty kind matches all objects. The byName index is hit directly
// when kind is set — O(K) instead of O(N) — which matters on the
// orchestrator's per-pass list calls when one kind dominates the
// store (HelmReleases are typically the bulk).
func (s *Store) ListObjects(kind string) []manifest.BaseManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if kind != "" {
		inner := s.byName[kind]
		out := make([]manifest.BaseManifest, 0, len(inner))
		for _, obj := range inner {
			out = append(out, obj)
		}
		return out
	}
	out := make([]manifest.BaseManifest, 0, len(s.objects))
	for _, obj := range s.objects {
		out = append(out, obj)
	}
	return out
}

