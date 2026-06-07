package store

import (
	"cmp"
	"hash/fnv"
	"reflect"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// numEventKinds is the number of distinct EventKind values (1-based, so
// index 0 is unused). A fixed-size array indexed directly by EventKind
// removes the map lookup and bucket overhead on every dispatch path.
const numEventKinds = 3

// shardCount is the number of shards the per-Kind sharded state is
// distributed across. The Store's writers and readers route through
// shardFor(id) which hashes id.Kind via FNV-1a — operations on
// different Kinds proceed in parallel; operations on the same Kind
// still serialize on that Kind's shard.
//
// 16 is comfortably more than the live Kind set flate observes
// (Kustomization, HelmRelease, GitRepository, OCIRepository,
// HelmRepository, Bucket, ExternalArtifact, ConfigMap, Secret, plus
// the rendered-output kinds — well under 16). The cost of a few
// empty shards is a handful of unused 0-length map headers per
// Store; the benefit is FNV's near-perfect distribution leaves
// enough slack that two hot Kinds (KS + HR) almost always land in
// distinct shards.
const shardCount = 16

// shard owns the per-Kind state for the Kinds that hash to its
// index. The objects/conditions/artifacts maps are keyed by
// manifest.NamedResource, which includes Kind — so within a shard
// each is keyed by the same set of ids, and a Snapshot for one id
// touches only this shard.
//
// byName is the secondary index: kind → namespace/name → object.
// Sharding by Kind means each shard's byName only sees the Kinds
// that hash to it; lookups in GetByName/ListObjects(kind) route to
// exactly one shard.
type shard struct {
	mu         sync.RWMutex
	objects    map[manifest.NamedResource]manifest.BaseManifest
	conditions map[manifest.NamedResource][]Condition
	artifacts  map[manifest.NamedResource]Artifact

	// byName mirrors the per-shard slice of the Store-wide secondary
	// index. Keyed by Kind first so a single GetByName lookup is two
	// map indexes; sharding by Kind guarantees a shard only ever
	// holds Kinds that hash to it.
	byName map[string]map[string]manifest.BaseManifest
}

func newShard() *shard {
	return &shard{
		objects:    make(map[manifest.NamedResource]manifest.BaseManifest),
		conditions: make(map[manifest.NamedResource][]Condition),
		artifacts:  make(map[manifest.NamedResource]Artifact),
		byName:     make(map[string]map[string]manifest.BaseManifest),
	}
}

// Store is the central in-memory state container.
//
// # Sharded layout
//
// The Store's hot state is sharded across shardCount shards keyed by
// the FNV-1a hash of id.Kind. Reads and writes on different Kinds no
// longer serialize on a single global lock — the profile evidence
// from `buroa/k8s-gitops` had `store.SetCondition` + `UpdateStatus`
// contributing ~17% of total mutex delay on a warm 1.1s run, almost
// all of it cross-Kind contention (KS writers blocking HR writers
// blocking source-controller readers).
//
// Cross-shard operations (the ListObjects(""), FailedResources,
// AddListener(replay=true) iterate-everything paths) acquire every
// shard in ascending index order via lockAll/unlockAll. Holding all
// shards in canonical order is mandatory: any code path that locks
// two or more shards MUST do so via lockAll / rLockAll — taking
// them out-of-order or interleaving with shardFor's lock will
// deadlock.
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
	shards [shardCount]*shard

	// listeners is indexed by EventKind (1-based; index 0 unused). A
	// fixed-size array beats a map here: the set of kinds is closed,
	// the index arithmetic is O(1) with no hash, and the array itself
	// is inline in the Store struct rather than a separately-allocated
	// map header. Every dispatch path benefits.
	//
	// Listener sets stay global — they fan out to interested
	// observers irrespective of the resource's Kind, and the
	// listenerSet has its own internal mutex so the shard locks don't
	// serialize listener registration.
	listeners [numEventKinds + 1]*listenerSet
}

// New constructs an empty Store.
func New() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i] = newShard()
	}
	// Index 0 is the reserved unused slot (EventKind is 1-based); init
	// one listener set per real event kind so adding a kind can't drift
	// out of sync with a forgotten assignment here.
	for ev := EventObjectAdded; int(ev) <= numEventKinds; ev++ {
		s.listeners[ev] = newListenerSet()
	}
	return s
}

// shardFor returns the shard owning id's Kind. The hash is FNV-1a
// over id.Kind — short, well-distributed, stdlib (`hash/fnv`).
func (s *Store) shardFor(id manifest.NamedResource) *shard {
	return s.shards[kindShardIndex(id.Kind)]
}

// shardForKind is the kind-only sibling of shardFor for entrypoints
// that have a Kind string but no full NamedResource (GetByName,
// ListObjects).
func (s *Store) shardForKind(kind string) *shard {
	return s.shards[kindShardIndex(kind)]
}

// kindShardIndex returns the shard index for a Kind string. FNV-1a
// from stdlib `hash/fnv` is short, well-distributed, and avoids the
// "hand-rolled hash drifts subtly" risk a custom implementation
// would carry. shardCount is a power of two so the mask is fast.
func kindShardIndex(kind string) uint32 {
	h := fnv.New32a()
	// fnv32a.Write never returns an error.
	_, _ = h.Write([]byte(kind))
	return h.Sum32() & (shardCount - 1)
}

// lockAll acquires every shard's write lock in ascending index order.
// MUST be paired with unlockAll. Any cross-shard write path must
// route through this helper — taking shard locks in arbitrary order
// risks a deadlock if two goroutines lock two shards each in
// opposite orders.
func (s *Store) lockAll() {
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
}

// unlockAll releases every shard's write lock in descending index
// order. Symmetric to lockAll.
func (s *Store) unlockAll() {
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.Unlock()
	}
}

// rLockAll acquires every shard's read lock in ascending index order.
// MUST be paired with rUnlockAll. Same canonical-order rule as
// lockAll: any cross-shard read path that needs a consistent
// snapshot across shards routes through this.
func (s *Store) rLockAll() {
	for i := range s.shards {
		s.shards[i].mu.RLock()
	}
}

// rUnlockAll releases every shard's read lock in descending index
// order. Symmetric to rLockAll.
func (s *Store) rUnlockAll() {
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.RUnlock()
	}
}

func nameKey(namespace, name string) string { return namespace + "/" + name }

// AddObject inserts a manifest. Re-adding an equal object is a no-op.
// Re-adding a different object overwrites the existing entry AND still
// dispatches an ObjectAdded event (so newer values propagate).
//
// The equal-prev fast path reads under RLock and runs reflect.DeepEqual
// outside the write lock. Reflection on a typed CR is the slowest
// operation in this method; keeping it off the write path means
// re-emits (a common pattern when a parent KS re-renders unchanged
// children) don't block concurrent readers.
func (s *Store) AddObject(obj manifest.BaseManifest) {
	id := obj.Named()
	sh := s.shardFor(id)
	sh.mu.RLock()
	prev, exists := sh.objects[id]
	sh.mu.RUnlock()
	if exists && reflect.DeepEqual(prev, obj) {
		return
	}
	sh.mu.Lock()
	// Re-check under the write lock: a concurrent AddObject may have
	// landed the same object between our RUnlock and Lock. Without the
	// re-check we'd dispatch a redundant event for a write the previous
	// goroutine already did.
	if cur, ok := sh.objects[id]; ok && reflect.DeepEqual(cur, obj) {
		sh.mu.Unlock()
		return
	}
	sh.setLocked(id, obj)
	dispatch := s.fireUnderLock(EventObjectAdded, id, obj)
	sh.mu.Unlock()
	dispatch()
}

// AddObjects inserts multiple manifests, then dispatches their events
// after every changed object is visible in the store. Use this when a
// render emits a coherent sibling set whose listeners need the whole
// set before reacting.
//
// Sharded layout: each input object routes to its Kind's shard. We
// group writes by shard so each shard locks at most once, then drop
// every lock before dispatching — the "writes visible before any
// listener fires" guarantee holds within each shard, and listeners
// running outside any shard lock can see writes that landed in
// other shards before this call returned.
func (s *Store) AddObjects(objs []manifest.BaseManifest) {
	if len(objs) == 0 {
		return
	}
	// Bucket inputs by shard index so each shard locks at most once.
	var buckets [shardCount][]manifest.BaseManifest
	for _, obj := range objs {
		idx := kindShardIndex(obj.Named().Kind)
		buckets[idx] = append(buckets[idx], obj)
	}
	dispatches := make([]func(), 0, len(objs))
	for i := range buckets {
		if len(buckets[i]) == 0 {
			continue
		}
		sh := s.shards[i]
		sh.mu.Lock()
		for _, obj := range buckets[i] {
			id := obj.Named()
			if cur, ok := sh.objects[id]; ok && reflect.DeepEqual(cur, obj) {
				continue
			}
			sh.setLocked(id, obj)
			dispatches = append(dispatches, s.fireUnderLock(EventObjectAdded, id, obj))
		}
		sh.mu.Unlock()
	}
	for _, dispatch := range dispatches {
		dispatch()
	}
}

// setLocked is the single funnel for inserting an object into both
// the primary `objects` map and the secondary `byName` index. Three
// write paths (AddObject, AddRendered, future renames) must keep
// these two maps in sync; routing them through one helper makes the
// "forgot to update byName" drift class structurally impossible.
// Caller MUST hold sh.mu (write lock).
func (sh *shard) setLocked(id manifest.NamedResource, obj manifest.BaseManifest) {
	sh.objects[id] = obj
	inner, ok := sh.byName[id.Kind]
	if !ok {
		inner = make(map[string]manifest.BaseManifest)
		sh.byName[id.Kind] = inner
	}
	inner[nameKey(id.Namespace, id.Name)] = obj
}

// deleteLocked is the symmetric remove for setLocked. Drops the
// object from objects + byName + conditions + artifacts (the full
// lifecycle wipe) and reports whether anything was present. Caller
// MUST hold sh.mu (write lock).
func (sh *shard) deleteLocked(id manifest.NamedResource) bool {
	if _, ok := sh.objects[id]; !ok {
		return false
	}
	delete(sh.objects, id)
	delete(sh.conditions, id)
	delete(sh.artifacts, id)
	if inner := sh.byName[id.Kind]; inner != nil {
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
// status reset, and the event capture all run under one sh.mu
// acquisition so a concurrent DeleteObject can't slip in and leave
// the listener dispatching against a stale object or the status map
// resurrected with a phantom Pending entry. Single shard — both
// objects and conditions for id live in the same shard since both
// are keyed by id.Kind.
func (s *Store) Refire(id manifest.NamedResource) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	obj, ok := sh.objects[id]
	if !ok {
		sh.mu.Unlock()
		return
	}
	updated, changed := sh.setConditionLocked(id, readyCondition(StatusPending, MsgRefetching))
	dispatchObj := s.fireUnderLock(EventObjectAdded, id, obj)
	var dispatchStatus func()
	if changed {
		info, _ := statusInfoFromConditions(updated)
		dispatchStatus = s.fireUnderLock(EventStatusUpdated, id, info)
	}
	sh.mu.Unlock()
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
// Use this for any post-load mutation such as namespace-inheritance
// rewrites or alias seeding. Listeners fire as they would on a fresh
// AddObject (intentionally — downstream controllers re-reconcile
// against the mutated spec).
func Mutate[T interface {
	manifest.BaseManifest
	Cloneable[T]
}](s *Store, id manifest.NamedResource, mutate func(T)) bool {
	// Hold sh.mu across the whole clone-mutate-write so a concurrent
	// DeleteObject(id) or AddObject(newer) between Get and Add can't
	// resurrect deleted state or clobber a newer write with the
	// mutation of the previously-observed object.
	sh := s.shardFor(id)
	var dispatch func()
	ok := func() bool {
		sh.mu.Lock()
		defer sh.mu.Unlock()
		obj, isT := sh.objects[id].(T)
		if !isT {
			return false
		}
		cloned := obj.Clone()
		mutate(cloned)
		// Dedup parallel to AddObject — equal mutations no-op.
		if reflect.DeepEqual(sh.objects[id], cloned) {
			return true
		}
		sh.setLocked(id, cloned)
		dispatch = s.fireUnderLock(EventObjectAdded, id, cloned)
		return true
	}()
	if dispatch != nil {
		dispatch()
	}
	return ok
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
	sh := s.shardFor(id)
	sh.mu.Lock()
	sh.setLocked(id, obj)
	dispatch := s.fireUnderLock(EventObjectAdded, id, obj)
	sh.mu.Unlock()
	dispatch()
}

// GetObject returns the manifest for id, or nil if not present.
func (s *Store) GetObject(id manifest.NamedResource) manifest.BaseManifest {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.objects[id]
}

// Snapshot returns the manifest and conditions for id captured under
// a single RLock acquisition. Use this when a caller needs a
// consistent view of both — e.g. depwait's CEL projection reads
// status.conditions AND metadata.labels in one expression, and
// independent GetObject / GetConditions calls can mix a fresh object
// snapshot with stale conditions (or vice versa) if a writer
// interleaves. Returns nil object and nil conditions for unknown ids.
//
// Single-shard: both objects[id] and conditions[id] live in the
// same shard since both are keyed by id.Kind.
func (s *Store) Snapshot(id manifest.NamedResource) (manifest.BaseManifest, []Condition) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	obj := sh.objects[id]
	conds := sh.conditions[id]
	if len(conds) == 0 {
		return obj, nil
	}
	return obj, slices.Clone(conds)
}

// DeleteObject removes the object stored under id. Returns whether
// anything was removed. Status and artifact entries (if any) are also
// dropped so a re-add under a different id starts clean.
func (s *Store) DeleteObject(id manifest.NamedResource) bool {
	sh := s.shardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.deleteLocked(id)
}

// GetByName returns the object matching (kind, namespace, name), or nil
// when none is present. Hot-path callers (valuesFrom expansion, source
// resolution) should prefer this over filtering ListObjects.
func (s *Store) GetByName(kind, namespace, name string) manifest.BaseManifest {
	sh := s.shardForKind(kind)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if inner := sh.byName[kind]; inner != nil {
		return inner[nameKey(namespace, name)]
	}
	return nil
}

// ListObjects returns every stored manifest, optionally filtered by kind,
// sorted deterministically by (Kind, Namespace, Name). An empty kind
// matches all objects. The byName index is hit directly when kind is set
// — O(K) instead of O(N) — which matters on the orchestrator's per-pass
// list calls when one kind dominates the store (HelmReleases are
// typically the bulk).
//
// Sharded layout: when kind is set, the lookup routes to exactly one
// shard. When kind is empty, ALL shards are read-locked in canonical
// (ascending) order to capture a consistent global snapshot; the
// caller pays an N-shard read-lock cost only when truly iterating
// everything.
//
// Deterministic order is a correctness requirement: ownership
// tie-breaking in change/ownership.go (and any future tie-break) must
// produce the same winner across runs. Go map iteration is randomized,
// so without sorting the same file can be attributed to different
// KS owners on different runs.
func (s *Store) ListObjects(kind string) []manifest.BaseManifest {
	if kind != "" {
		sh := s.shardForKind(kind)
		sh.mu.RLock()
		inner := sh.byName[kind]
		out := make([]manifest.BaseManifest, 0, len(inner))
		for _, obj := range inner {
			out = append(out, obj)
		}
		sh.mu.RUnlock()
		// All results share the queried Kind (byName[kind] index), so
		// NamedResource.Compare's leading Kind comparison is always 0 here.
		// Sort by (Namespace, Name) directly — identical order, one fewer
		// string compare per element. This path runs per-kind in hot
		// orchestrator loops (cycles, finalize, render collection).
		slices.SortFunc(out, func(a, b manifest.BaseManifest) int {
			an, bn := a.Named(), b.Named()
			return cmp.Or(cmp.Compare(an.Namespace, bn.Namespace), cmp.Compare(an.Name, bn.Name))
		})
		return out
	}

	s.rLockAll()
	total := 0
	for _, sh := range s.shards {
		total += len(sh.objects)
	}
	out := make([]manifest.BaseManifest, 0, total)
	for _, sh := range s.shards {
		for _, obj := range sh.objects {
			out = append(out, obj)
		}
	}
	s.rUnlockAll()
	slices.SortFunc(out, func(a, b manifest.BaseManifest) int {
		return a.Named().Compare(b.Named())
	})
	return out
}
