package store

import (
	"cmp"
	"hash/fnv"
	"maps"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

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

type kindID struct {
	group, kind string
}

type nameID struct {
	namespace, name string
}

type authority struct {
	generation              uint64
	links                   *registrationHead
	rooted, retired, hidden bool
}

func (h authority) nextGeneration() authority {
	if h.generation == ^uint64(0) {
		panic("store: authority generation exhausted")
	}
	h.generation++
	if h.links != nil && (h.links.first.Load() != nil || h.links.detached.Load()) {
		h.links.generation.Store(h.generation)
	}

	return h
}

func (sh *shard) requireCapacityLocked(heads ...authority) {
	for _, h := range heads {
		if h.generation == ^uint64(0) {
			sh.mu.Unlock()
			panic("store: authority generation exhausted")
		}
	}
}

type objectCell struct {
	static    manifest.BaseManifest
	binding   *Input
	support   *Input
	excluded  bool
	preflight string
	authority
}

type objectBucket struct {
	names map[nameID]struct{}
	authority
}

type shard struct {
	spareHeads   []registrationHead
	headCapacity int
	pending      atomic.Pointer[registrationHead]
	absence      registrationHead
	derived      bool
	mu           sync.RWMutex
	cells        map[manifest.NamedResource]objectCell
	conditions   map[manifest.NamedResource]conditionRecord
	artifacts    map[manifest.NamedResource]artifactRecord
	byName       map[kindID]objectBucket
	members      map[manifest.NamedResource][]membership
	slots        map[manifest.NamedResource]*producerSlot
}

func newShard() *shard {
	sh := &shard{
		cells:      make(map[manifest.NamedResource]objectCell),
		conditions: make(map[manifest.NamedResource]conditionRecord),
		artifacts:  make(map[manifest.NamedResource]artifactRecord),
		byName:     make(map[kindID]objectBucket),
		members:    make(map[manifest.NamedResource][]membership),
		slots:      make(map[manifest.NamedResource]*producerSlot),
	}
	sh.absence.shard = sh
	return sh
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
// Cross-shard operations (the ListObjects("", ""), FailedResources,
// AddListener(replay=true) iterate-everything paths) acquire every
// shard in ascending index order via lockAll/unlockAll. Holding all
// shards in canonical order is mandatory: any code path that locks
// two or more shards MUST do so via lockAll / rLockAll — taking
// them out-of-order or interleaving with shardFor's lock will
// deadlock.
//
// # Immutability contract
//
// Objects passed to AddObject (and returned by GetObject / ListObjects)
// are treated as IMMUTABLE after insertion. The store
// returns shared pointers rather than defensive copies for performance:
// rendering pipelines read millions of fields per reconcile, and
// cloning the full manifest tree on every read would dominate CPU.
//
// Callers that need to "modify" a stored object must:
//
//  1. Shallow-copy the struct (most manifest types are flat enough
//     that *clone := *orig works).
//  2. Mutate the copy's fields.
//  3. Re-AddObject the modified copy. AddObject's content
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

	index    atomic.Pointer[manifest.DiscoveryIndex]
	coverage map[manifest.NamedResource]*contributor

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

	// warn collects render advisories (manifest.Warning) for Result.Warnings.
	// Kept off the shards: warnings are low-volume, some are render-global
	// (zero NamedResource) so they can't shard by id, and the orchestrator
	// reads them once at finalize. Its own mutex keeps advisory writes off the
	// shard locks' hot path. See warning.go.
	warnMu    sync.Mutex
	warnOrder []warningRecord
	warnIndex map[warningKey]int
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
	return s.shardForKind(id.Kind)
}

// shardForKind is the kind-only sibling of shardFor for entrypoints
// that have a Kind string but no full NamedResource (ListObjects).
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

type objectUpdate struct {
	obj                         manifest.BaseManifest
	binding                     *Input
	support                     *Input
	cell                        objectCell
	effective                   capture
	staticEqual, effectiveEqual bool
	membershipEqual             bool
	needsRoot                   bool
}

func (u *objectUpdate) compare() {
	u.staticEqual = u.cell.binding == u.binding && u.cell.support == u.support && objectEqual(u.cell.static, u.obj)
	u.membershipEqual = u.staticEqual || u.cell.static != nil && sameLabels(Child{Obj: u.cell.static}, Child{Obj: u.obj}, nil)
	u.effectiveEqual = u.staticEqual
	if u.effective.child.Obj != u.cell.static {
		u.effectiveEqual = objectEqual(u.effective.child.Obj, u.obj)
	}
}

// currentLocked MUST hold the effective producer's shard as well as the target's.
func (u objectUpdate) currentLocked(sh *shard) bool {
	return sh.cells[u.obj.Named()].generation == u.cell.generation &&
		(u.effective.owner == nil || u.effective.owner.current.Load() == u.effective.record)
}

// AddObject inserts a manifest. Equal content with unchanged authority is a no-op.
func (s *Store) AddObject(obj manifest.BaseManifest) { s.addObject(obj, nil, nil) }

func (s *Store) addObject(obj manifest.BaseManifest, binding, support *Input) {
	id := obj.Named()
	sh := s.shardFor(id)
	for {
		sh.mu.RLock()
		cell := sh.cells[id]
		needsRoot := !cell.rooted && (len(sh.members[id]) > 0 || sh.slots[id] != nil)
		if cell.static == obj && cell.binding == binding && cell.support == support && !cell.hidden && !cell.retired && !needsRoot {
			sh.mu.RUnlock()
			return
		}
		u := objectUpdate{obj: obj, binding: binding, support: support, cell: cell, needsRoot: needsRoot}
		if sh.derived {
			sh.captureLocked(id, cell, &u.effective)
		} else {
			u.effective.child.Obj = cell.static
		}
		sh.mu.RUnlock()
		u.compare()
		if u.staticEqual && u.effectiveEqual && !u.cell.hidden && !u.cell.retired && !u.needsRoot {
			return
		}
		sh.mu.Lock()
		if binding != nil && !binding.current() || support != nil && !support.productionCurrent() {
			sh.mu.Unlock()
			return
		}
		current := sh.cells[id]
		if current.generation != u.cell.generation {
			sh.mu.Unlock()
			continue
		}
		if u.cell.hidden || !u.cell.rooted && (len(sh.members[id]) > 0 || sh.slots[id] != nil) {
			sh.mu.Unlock()
			if s.addAnchor(u) {
				return
			}
			continue
		}
		u.cell.authority = current.authority
		dispatch := s.admitLocked(sh, id, &u)
		sh.mu.Unlock()
		dispatch.run()
		return
	}
}

func (s *Store) admitLocked(sh *shard, id manifest.NamedResource, u *objectUpdate) eventDispatch {
	cell := u.cell
	if u.staticEqual && u.effectiveEqual && !cell.retired {
		return eventDispatch{}
	}
	var admissions uint64
	if cell.generation == 0 {
		admissions++
	}
	var bucket objectBucket
	if !u.membershipEqual && sh.byName[kindID{id.Group, id.Kind}].links == nil {
		admissions++
	}
	if !sh.canAdmit(admissions) {
		sh.mu.Unlock()
		panic("store: authority generation exhausted")
	}
	if !u.membershipEqual {
		bucket = sh.byName[kindID{id.Group, id.Kind}]
		if bucket.links == nil {
			sh.admitAbsenceLocked(manifest.NamedResource{Group: id.Group, Kind: id.Kind}, bucketFacet)
			bucket.links = sh.newHeadLocked(bucket.generation)
		}
	}
	if cell.generation == ^uint64(0) || bucket.generation == ^uint64(0) {
		sh.requireCapacityLocked(cell.authority, bucket.authority)
	}
	if cell.generation == 0 {
		sh.admitAbsenceLocked(id, objectFacet)
		cell.links = sh.newHeadLocked(0)
	}
	cell.authority = cell.nextGeneration()
	if cell.links != nil {
		cell.authority = sh.enqueueLocked(cell.authority)
	}
	if !u.staticEqual {
		cell.static, cell.binding, cell.support = u.obj, u.binding, u.support
	}
	cell.retired = false
	sh.cells[id] = cell
	if !u.membershipEqual && bucket.update(nameID{id.Namespace, id.Name}, cell.static != nil, true, bucket.generation) {
		bucket.authority = sh.enqueueLocked(bucket.authority)
		sh.byName[kindID{id.Group, id.Kind}] = bucket
	}
	dispatch := fireUnderLock(s, EventObjectAdded, id)
	dispatch.payload = u.obj
	return dispatch
}

// AddObjects admits every changed sibling before dispatching any event.
func (s *Store) AddObjects(objs []manifest.BaseManifest) {
	if len(objs) == 0 {
		return
	}
	var buckets [shardCount][]objectUpdate
	for _, obj := range objs {
		i := kindShardIndex(obj.Named().Kind)
		buckets[i] = append(buckets[i], objectUpdate{obj: obj})
	}
	var anchors []objectUpdate
	dispatches := make([]eventDispatch, 0, len(objs))
	for i, bucket := range buckets {
		if len(bucket) == 0 {
			continue
		}
		slices.SortStableFunc(bucket, func(a, b objectUpdate) int { return a.obj.Named().Compare(b.obj.Named()) })
		compacted := bucket[:0]
		for j, u := range bucket {
			if j+1 == len(bucket) || u.obj.Named() != bucket[j+1].obj.Named() {
				compacted = append(compacted, u)
			}
		}
		bucket = compacted
		sh := s.shards[i]
		for {
			sh.mu.RLock()
			for j := range bucket {
				id := bucket[j].obj.Named()
				bucket[j].cell = sh.cells[id]
				sh.captureLocked(id, bucket[j].cell, &bucket[j].effective)
			}
			sh.mu.RUnlock()
			for j := range bucket {
				bucket[j].compare()
			}
			sh.mu.Lock()
			current := true
			for j := range bucket {
				u := &bucket[j]
				cell := sh.cells[u.obj.Named()]
				if cell.generation != u.cell.generation {
					current = false
					break
				}
				u.cell.authority = cell.authority
			}
			if !current {
				sh.mu.Unlock()
				continue
			}
			for _, u := range bucket {
				id := u.obj.Named()
				if u.cell.hidden || !u.cell.rooted && (len(sh.members[id]) > 0 || sh.slots[id] != nil) {
					anchors = append(anchors, u)
					continue
				}
				dispatches = append(dispatches, s.admitLocked(sh, id, &u))
			}
			sh.mu.Unlock()
			break
		}
	}
	for len(anchors) > 0 {
		var storage [4]regionState
		changed := newObjectRegion(len(anchors), storage[:])
		for _, u := range anchors {
			changed = changed.add(u.obj.Named(), true)
		}
		s.lockAll()
		current := true
		for _, u := range anchors {
			if !u.currentLocked(s.shardFor(u.obj.Named())) {
				current = false
				break
			}
		}
		if !current {
			s.unlockAll()
			for i := range anchors {
				u := &anchors[i]
				id := u.obj.Named()
				sh := s.shardFor(id)
				sh.mu.RLock()
				u.cell = sh.cells[id]
				sh.captureLocked(id, u.cell, &u.effective)
				u.needsRoot = !u.cell.rooted && (len(sh.members[id]) > 0 || sh.slots[id] != nil)
				sh.mu.RUnlock()
				u.compare()
			}
			anchors = slices.DeleteFunc(anchors, func(u objectUpdate) bool {
				return u.staticEqual && u.effectiveEqual && !u.cell.hidden && !u.cell.retired && !u.needsRoot
			})
			continue
		}
		region := s.affectedLocked(changed, manifest.NamedResource{})
		for _, u := range anchors {
			obj := u.obj
			if u.effectiveEqual && u.cell.static != nil {
				obj = u.cell.static
			}
			id := obj.Named()
			sh := s.shardFor(id)
			cell := sh.cells[id]
			cell.static, cell.rooted, cell.retired = obj, true, false
			sh.cells[id] = cell
		}
		for _, p := range s.reachabilityLocked(region) {
			dispatches = append(dispatches, p.event)
		}
		s.unlockAll()
		break
	}
	dispatchEvents(dispatches)
}

// update MUST run under the bucket's shard write lock.
func (b *objectBucket) update(name nameID, present, changed bool, expected uint64) bool {
	_, indexed := b.names[name]
	if indexed == present && !changed || !indexed && !present {
		return false
	}
	if b.generation == expected {
		b.authority = b.nextGeneration()
	}
	if !present {
		delete(b.names, name)
	} else if !indexed {
		if b.names == nil {
			b.names = make(map[nameID]struct{})
		}
		b.names[name] = struct{}{}
	}
	return true
}

func (sh *shard) deleteLocked(id manifest.NamedResource) bool {
	cell := sh.cells[id]
	if cell.static == nil {
		return false
	}
	bucket := sh.byName[kindID{id.Group, id.Kind}]
	sh.requireCapacityLocked(cell.authority, sh.conditions[id].stamp(), sh.artifacts[id].authority, bucket.authority)
	cell.authority = cell.nextGeneration()
	if cell.links != nil {
		cell.authority = sh.enqueueLocked(cell.authority)
	}
	cell.static, cell.rooted, cell.binding, cell.support = nil, false, nil, nil
	sh.cells[id] = cell
	sh.clearFacetsLocked(id)
	if bucket.update(nameID{id.Namespace, id.Name}, false, true, bucket.generation) {
		bucket.authority = sh.enqueueLocked(bucket.authority)
		sh.byName[kindID{id.Group, id.Kind}] = bucket
	}
	return true
}

// objectEqual MUST compare every RawObject field: Kind, APIVersion, Name,
// Namespace and Spec. The interface-identity shortcut assumes pointer
// implementations of BaseManifest. Reflection handles other typed manifests.
func objectEqual(a, b manifest.BaseManifest) bool {
	if a == b {
		return true
	}
	if prev, ok := a.(*manifest.RawObject); ok {
		cur, ok := b.(*manifest.RawObject)
		return ok && prev.Kind == cur.Kind && prev.APIVersion == cur.APIVersion &&
			prev.Name == cur.Name && prev.Namespace == cur.Namespace && specValueEqual(prev.Spec, cur.Spec)
	}
	return reflect.DeepEqual(a, b)
}

// Raw rendered specs contain JSON-shaped maps and slices. Comparing those
// directly avoids reflection's per-entry allocations on replayed charts.
func specValueEqual(a, b any) bool {
	switch v := a.(type) {
	case map[string]any:
		other, ok := b.(map[string]any)
		return ok && (v == nil) == (other == nil) && maps.EqualFunc(v, other, specValueEqual)
	case []any:
		other, ok := b.([]any)
		return ok && (v == nil) == (other == nil) && slices.EqualFunc(v, other, specValueEqual)
	case nil, string, bool, int, int64, float64:
		return a == b
	default:
		return reflect.DeepEqual(a, b)
	}
}

// GetObject returns the manifest for id, or nil if not present.
func (s *Store) GetObject(id manifest.NamedResource) manifest.BaseManifest {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.objectLocked(id)
}

// Observation holds the requested readiness facts from a coherent Snapshot.
type Observation struct {
	StatusInfo
	Conditions []Condition
	Statusful  bool
}

// Snapshot reads an effective object and its requested readiness observation.
func (s *Store) Snapshot(id manifest.NamedResource, conditions bool) (manifest.BaseManifest, Observation) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	obj := sh.objectLocked(id)
	hidden := sh.cells[id].hidden
	var observed Observation
	var record conditionRecord
	if !hidden {
		observed.Statusful = obj != nil && manifest.KindOf(obj).Status || obj == nil && id.KindInfo().Status
		if observed.Statusful || obj == nil || conditions {
			record = sh.conditions[id]
			observed.StatusInfo, _ = record.statusInfo()
			if conditions {
				observed.Conditions = slices.Clone(record.conditions)
			}
		}
	}
	sh.mu.RUnlock()
	record.project(observed.Conditions)
	return obj, observed
}

// DeleteObject removes static authority and its condition/artifact payloads.
func (s *Store) DeleteObject(id manifest.NamedResource) bool {
	sh := s.shardFor(id)
	sh.mu.Lock()
	if sh.cells[id].static == nil {
		sh.mu.Unlock()
		return false
	}
	if len(sh.members[id]) == 0 && sh.slots[id] == nil {
		removed := sh.deleteLocked(id)
		sh.mu.Unlock()
		return removed
	}
	sh.mu.Unlock()
	var storage [4]regionState
	changed := newObjectRegion(1, storage[:])
	changed = changed.add(id, true)
	s.lockAll()
	if sh.cells[id].static == nil {
		s.unlockAll()
		return false
	}
	region := s.affectedLocked(changed, manifest.NamedResource{})
	cell := sh.cells[id]
	cell.static, cell.rooted, cell.binding, cell.support = nil, false, nil, nil
	sh.cells[id] = cell
	sh.clearFacetsLocked(id)
	dispatches := s.reachabilityLocked(region)
	s.unlockAll()
	dispatchPublications(dispatches)
	return true
}

// ListObjects returns objects in the exact group/kind bucket, sorted by
// NamedResource.Compare. Empty group and kind returns the complete inventory.
// Single-bucket reads use one kind shard; inventory reads MUST use rLockAll
// to capture a consistent snapshot across shards.
func (s *Store) ListObjects(group, kind string) []manifest.BaseManifest {
	if group != "" || kind != "" {
		sh := s.shardForKind(kind)
		sh.mu.RLock()
		inner := sh.byName[kindID{group, kind}].names
		out := make([]manifest.BaseManifest, 0, len(inner))
		for name := range inner {
			if obj := sh.objectLocked(manifest.NamedResource{Group: group, Kind: kind, Namespace: name.namespace, Name: name.name}); obj != nil {
				out = append(out, obj)
			}
		}
		sh.mu.RUnlock()
		// Group and kind are identical within this bucket.
		slices.SortFunc(out, func(a, b manifest.BaseManifest) int {
			an, bn := a.Named(), b.Named()
			return cmp.Or(cmp.Compare(an.Namespace, bn.Namespace), cmp.Compare(an.Name, bn.Name))
		})
		return out
	}

	s.rLockAll()
	total := 0
	for _, sh := range s.shards {
		total += len(sh.cells)
	}
	out := make([]manifest.BaseManifest, 0, total)
	for _, sh := range s.shards {
		for id := range sh.cells {
			if obj := sh.objectLocked(id); obj != nil {
				out = append(out, obj)
			}
		}
	}
	s.rUnlockAll()
	slices.SortFunc(out, func(a, b manifest.BaseManifest) int {
		return a.Named().Compare(b.Named())
	})
	return out
}

func (s *Store) addAnchor(u objectUpdate) bool {
	id := u.obj.Named()
	sh := s.shardFor(id)
	var storage [4]regionState
	changed := newObjectRegion(1, storage[:])
	changed = changed.add(id, true)
	s.lockAll()
	if !u.currentLocked(sh) || u.binding != nil && !u.binding.current() || u.support != nil && !u.support.productionCurrent() {
		s.unlockAll()
		return false
	}
	region := s.affectedLocked(changed, manifest.NamedResource{})
	obj := u.obj
	if u.effectiveEqual && u.cell.static != nil {
		obj = u.cell.static
	}
	cell := sh.cells[id]
	cell.rooted, cell.static, cell.binding, cell.support = true, obj, u.binding, u.support
	sh.cells[id] = cell
	dispatches := s.reachabilityLocked(region)
	s.unlockAll()
	dispatchPublications(dispatches)
	return true
}

type objectPublication struct {
	event                  eventDispatch
	before, after          manifest.BaseManifest
	document, nextDocument map[string]any
	force                  bool
}

func dispatchPublications(publications []objectPublication) {
	slices.SortStableFunc(publications, func(a, b objectPublication) int { return a.event.id.Compare(b.event.id) })
	for _, p := range publications {
		if p.force || p.before == nil || p.after == nil || !objectEqual(p.before, p.after) ||
			p.document != nil && !specValueEqual(p.document, p.nextDocument) {
			p.event.run()
		}
	}
}
