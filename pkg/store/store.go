package store

import (
	"cmp"
	"hash/fnv"
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
	generation uint64
	links      *registrationHead
}

func (h authority) current() authority {
	if h.links != nil {
		h.generation = max(h.generation, h.links.stamp())
	}
	return h
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

func (sh *shard) requireCapacityLocked(heads ...authority) {
	for _, h := range heads {
		if h.current().generation == ^uint64(0) {
			sh.mu.Unlock()
			panic("store: authority generation exhausted")
		}
	}
}

type objectCell struct {
	authority

	reference *Declaration
	policy    manifest.DeclarationPolicy
	static    manifest.BaseManifest
	document  map[string]any
	// Direct writes use this revision; emitted values use the immutable producer record.
	publication                                              uint64
	binding                                                  *Input
	support                                                  *Input
	excluded, included, rooted, retired, hidden, independent bool
	reconcile, report                                        bool
	preflight                                                string
}

func (c *objectCell) bound() bool {
	return (c.binding == nil || c.binding.current()) && (c.support == nil || c.support.productionCurrent())
}

type objectBucket struct {
	authority

	names map[nameID]struct{}
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
	warnOrder []manifest.Warning
	warnIndex map[warningKey]struct{}
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
	policy                      manifest.DeclarationPolicy
	document                    map[string]any
	explicit                    bool
	binding                     *Input
	support                     *Input
	cell                        objectCell
	effective                   capture
	staticEqual, effectiveEqual bool
	membershipEqual             bool
	needsRoot                   bool
	independent                 bool
}

func (u *objectUpdate) compare() {
	equal := manifest.Equal(u.cell.static, u.obj)
	u.policy = manifest.DeclarationPolicy{Root: true, Reconcile: u.cell.reconcile, Report: u.cell.report}
	if u.cell.static == nil || u.cell.reconcile != manifest.KindOf(u.obj).Runnable {
		u.policy = (Child{Obj: u.obj}).Policy(u.obj.Named())
	}
	if !u.explicit {
		u.document = nil
		if equal {
			u.document = u.cell.document
		}
	}
	u.staticEqual = u.cell.binding == u.binding && u.cell.support == u.support && equal
	u.membershipEqual = u.staticEqual ||
		u.cell.static != nil && manifest.LabelsEqual(u.cell.static, u.obj, [2]map[string]any{}, nil)
	u.effectiveEqual = equal
	if u.effectiveEqual {
		u.effectiveEqual = reconcileInputEqual(u.obj, u.cell.document, u.document).equal
	}
	if u.effective.child.Obj != u.cell.static {
		u.effectiveEqual = manifest.Equal(u.effective.child.Obj, u.obj)
		if u.effectiveEqual {
			u.effectiveEqual = reconcileInputEqual(u.obj, u.effective.child.Doc, u.document).equal
		}
	}
}

// currentLocked MUST hold the effective producer's shard as well as the target's.
func (u *objectUpdate) currentLocked(sh *shard) bool {
	v := *u
	return sh.cells[v.obj.Named()].publication == v.cell.publication &&
		sh.cells[v.obj.Named()].generation == v.cell.generation &&
		(v.effective.owner == nil || v.effective.owner.current.Load() == v.effective.record)
}

// AddObject inserts a manifest. Equal content with unchanged authority is a no-op.
func (s *Store) AddObject(obj manifest.BaseManifest, docs ...map[string]any) {
	s.addObject(obj, nil, nil, docs...)
}

func (s *Store) addObject(obj manifest.BaseManifest, binding, support *Input, docs ...map[string]any) {
	if len(docs) > 1 {
		panic("multiple authoritative documents")
	}
	id := obj.Named()
	sh := s.shardFor(id)
	for {
		sh.mu.RLock()
		cell := sh.cells[id]
		needsRoot := !cell.rooted && (len(sh.members[id]) > 0 || sh.slots[id] != nil)
		if len(docs) == 0 && cell.static == obj && cell.binding == binding && cell.support == support && !cell.hidden &&
			!cell.retired &&
			!needsRoot {
			sh.mu.RUnlock()
			return
		}
		u := objectUpdate{
			obj:       obj,
			binding:   binding,
			support:   support,
			cell:      cell,
			needsRoot: needsRoot,
			explicit:  len(docs) != 0,
		}
		if u.explicit {
			u.document = docs[0]
		}
		if sh.derived {
			sh.captureLocked(id, new(cell), &u.effective)
		} else {
			u.effective.child.Obj, u.effective.child.Doc = cell.static, cell.document
		}
		sh.mu.RUnlock()
		u.compare()
		if u.staticEqual && u.effectiveEqual && (!u.explicit || manifest.ValueEqual(u.cell.document, u.document)) &&
			!u.cell.hidden &&
			!u.cell.retired &&
			!u.needsRoot {
			return
		}
		u.independent = declarationIndependent(s.index.Load(), id)
		if s.commitObjectUpdate(sh, id, &u) != publicationRetry {
			return
		}
	}
}

func (s *Store) admitLocked(sh *shard, id manifest.NamedResource, u *objectUpdate) eventDispatch {
	cell := u.cell
	if cell.publication == ^uint64(0) {
		sh.mu.Unlock()
		panic("store: publication revision exhausted")
	}
	cell.publication++
	if u.staticEqual && u.effectiveEqual && !cell.retired {
		cell.document = u.document
		sh.cells[id] = cell
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
	cell.authority = cell.current().nextGeneration()
	if cell.links != nil {
		cell.authority = sh.enqueueLocked(cell.authority)
	}
	if !u.staticEqual {
		cell.static, cell.binding, cell.support = u.obj, u.binding, u.support
		cell.reconcile, cell.report = u.policy.Reconcile, u.policy.Report
	}
	cell.document = u.document
	cell.retired, cell.independent, cell.policy = false, u.independent, u.policy
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
		buckets[i] = append(
			buckets[i],
			objectUpdate{obj: obj, independent: declarationIndependent(s.index.Load(), obj.Named())},
		)
	}
	var anchors []objectUpdate
	dispatches := make([]eventDispatch, 0, len(objs))
	for i, bucket := range buckets {
		if len(bucket) == 0 {
			continue
		}
		bucket = compactObjectBucket(bucket)
		sh := s.shards[i]
		lockObjectBucket(sh, bucket)
		{
			uValues2 := bucket
			for uIndex := range uValues2 {
				u := uValues2[uIndex]
				id := u.obj.Named()
				if u.cell.hidden || !u.cell.rooted && (len(sh.members[id]) > 0 || sh.slots[id] != nil) {
					anchors = append(anchors, u)
					continue
				}
				dispatches = append(dispatches, s.admitLocked(sh, id, &u))
			}
		}
		sh.mu.Unlock()
	}
	for len(anchors) > 0 {
		var storage [4]regionState
		changed := newObjectRegion(len(anchors), storage[:])
		changed = extendObjectAnchorRegion(anchors, changed)
		s.lockAll()
		current := s.objectAnchorsCurrentLocked(anchors)
		if !current {
			s.unlockAll()
			anchors = s.refreshObjectAnchors(anchors)
			continue
		}
		region := s.affectedLocked(changed, manifest.NamedResource{})
		s.installObjectAnchorsLocked(anchors)
		publications := s.reachabilityLocked(region)
		for i := range publications {
			p := publications[i]
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
	if sh.artifacts[id].value != nil && sh.artifacts[id].publication == ^uint64(0) {
		sh.mu.Unlock()
		panic("store: publication revision exhausted")
	}
	sh.requireCapacityLocked(
		cell.authority,
		sh.conditions[id].stamp(),
		sh.artifacts[id].authority,
		sh.artifacts[id].revisionHead,
		sh.artifacts[id].originHead,
		bucket.authority,
	)
	cell.authority = cell.current().nextGeneration()
	if cell.links != nil {
		cell.authority = sh.enqueueLocked(cell.authority)
	}
	cell.static, cell.rooted, cell.binding, cell.support, cell.document = nil, false, nil, nil, nil
	_, cell.policy = sh.captureLocked(id, new(cell), nil)
	sh.cells[id] = cell
	sh.clearFacetsLocked(id)
	if bucket.update(nameID{id.Namespace, id.Name}, cell.reference != nil, true, bucket.generation) {
		bucket.authority = sh.enqueueLocked(bucket.authority)
		sh.byName[kindID{id.Group, id.Kind}] = bucket
	}
	return true
}

// GetObject returns the manifest for id, or nil if not present.
//
//nolint:ireturn // The manifest contract requires polymorphic results.
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
//
//nolint:ireturn // The manifest contract requires polymorphic results.
func (s *Store) Snapshot(id manifest.NamedResource, conditions bool) (manifest.BaseManifest, Observation) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	cell := sh.cells[id]
	obj, _ := sh.captureLocked(id, new(cell), nil)
	var observed Observation
	var record conditionRecord
	if obj != nil || !cell.hidden {
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
		var dispatch eventDispatch
		if sh.cells[id].reference != nil {
			dispatch = fireUnderLock(s, EventObjectAdded, id)
			dispatch.payload = sh.objectLocked(id)
		}
		sh.mu.Unlock()
		dispatch.run()
		return removed
	}
	sh.mu.Unlock()
	var storage [4]regionState
	changed := newObjectRegion(1, storage[:])
	changed = changed.add(id, true, false)
	s.lockAll()
	if sh.cells[id].static == nil {
		s.unlockAll()
		return false
	}
	region := s.affectedLocked(changed, manifest.NamedResource{})
	cell := sh.cells[id]
	cell.static, cell.rooted, cell.binding, cell.support, cell.document = nil, false, nil, nil, nil
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
			if obj := sh.objectLocked(
				manifest.NamedResource{Group: group, Kind: kind, Namespace: name.namespace, Name: name.name},
			); obj != nil {
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

	out := s.Objects()
	slices.SortFunc(out, func(a, b manifest.BaseManifest) int {
		return a.Named().Compare(b.Named())
	})
	return out
}

// Objects snapshots visible objects in unspecified order without holding locks during use.
func (s *Store) Objects() []manifest.BaseManifest {
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
	return out
}

func (s *Store) addAnchor(uValue *objectUpdate) bool {
	u := *uValue
	id := u.obj.Named()
	sh := s.shardFor(id)
	var storage [4]regionState
	changed := newObjectRegion(1, storage[:])
	changed = changed.add(id, !u.effectiveEqual || u.cell.static == nil, u.independent)
	s.lockAll()
	if !new(u).currentLocked(sh) || u.binding != nil && !u.binding.current() ||
		u.support != nil && !u.support.productionCurrent() {
		s.unlockAll()
		return false
	}
	region := s.affectedLocked(changed, manifest.NamedResource{})
	obj := u.obj
	if u.effectiveEqual && u.cell.static != nil {
		obj = u.cell.static
	}
	cell := sh.cells[id]
	cell.rooted, cell.static, cell.binding, cell.support, cell.document = true, obj, u.binding, u.support, u.document
	cell.reconcile, cell.report = u.policy.Reconcile, u.policy.Report
	cell.publication++
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
	{
		pValues := publications
		for pIndex := range pValues {
			p := pValues[pIndex]
			changed := p.force || p.before == nil || p.after == nil || !manifest.Equal(p.before, p.after)
			if !changed && p.document != nil {
				comparison := reconcileInputEqual(p.after, p.document, p.nextDocument)
				changed = !comparison.equal
			}
			if changed {
				p.event.run()
			}
		}
	}
}

type documentInput interface {
	ReconcileInput(map[string]any) manifest.ReconcileMetadata
}

type reconcileComparison struct {
	equal    bool
	consumes bool
}

func reconcileInputEqual(obj manifest.BaseManifest, a, b map[string]any) reconcileComparison {
	input, ok := obj.(documentInput)
	sameAPI := a == nil || b == nil || manifest.DocAPIVersion(a) == manifest.DocAPIVersion(b)
	if !ok {
		return reconcileComparison{equal: sameAPI}
	}
	reconciliation := input.ReconcileInput(a)
	auid, adisabled, consumes := reconciliation.UID, reconciliation.Disabled, reconciliation.Consumes
	if !consumes {
		return reconcileComparison{equal: sameAPI}
	}
	reconciliation2 := input.ReconcileInput(b)
	buid, bdisabled := reconciliation2.UID, reconciliation2.Disabled
	return reconcileComparison{equal: sameAPI && auid == buid && adisabled == bdisabled, consumes: true}
}

//nolint:gocritic // Immutable value inputs preserve snapshot semantics.
func reconcileEqual(a, b Child) bool {
	comparison := reconcileInputEqual(a.Obj, a.Doc, b.Doc)
	if comparison.consumes {
		return manifest.Equal(a.Obj, b.Obj) && comparison.equal
	}
	return semanticEqual(a, b)
}

func artifactReadEqual(a, b Artifact, read ArtifactRead) bool {
	if a == b {
		return true
	}
	x, xok := a.(*SourceArtifact)
	y, yok := b.(*SourceArtifact)
	if xok || yok {
		return (x == nil || x.Content != "" || x.FS == nil) && (y == nil || y.Content != "" || y.FS == nil) &&
			x.Input(read) == y.Input(read)
	}
	return artifactValueEqual(a, b)
}

func (s *Store) commitObjectUpdate(sh *shard, id manifest.NamedResource, update *objectUpdate) publicationResult {
	u := *update
	sh.mu.Lock()
	if u.binding != nil && !u.binding.current() || u.support != nil && !u.support.productionCurrent() {
		sh.mu.Unlock()
		return publicationRejected
	}
	current := sh.cells[id]
	if current.publication != u.cell.publication || current.generation != u.cell.generation {
		sh.mu.Unlock()
		return publicationRetry
	}
	if u.cell.hidden || !u.cell.rooted && (len(sh.members[id]) > 0 || sh.slots[id] != nil) {
		sh.mu.Unlock()
		if s.addAnchor(new(u)) {
			return publicationAccepted
		}
		return publicationRetry
	}
	u.cell.authority = current.authority
	dispatch := s.admitLocked(sh, id, &u)
	sh.mu.Unlock()
	dispatch.run()
	return publicationAccepted
}

func compactObjectBucket(bucket []objectUpdate) []objectUpdate {
	slices.SortStableFunc(bucket, func(a, b objectUpdate) int { return a.obj.Named().Compare(b.obj.Named()) })
	compacted := bucket[:0]
	{
		uValues := bucket
		for j := range uValues {
			u := uValues[j]
			if j+1 == len(bucket) || u.obj.Named() != bucket[j+1].obj.Named() {
				compacted = append(compacted, u)
			}
		}
	}
	bucket = compacted
	return bucket
}

func (s *Store) refreshObjectAnchors(anchors []objectUpdate) []objectUpdate {
	for i := range anchors {
		u := &anchors[i]
		id := u.obj.Named()
		sh := s.shardFor(id)
		sh.mu.RLock()
		u.cell = sh.cells[id]
		sh.captureLocked(id, new(u.cell), &u.effective)
		u.needsRoot = !u.cell.rooted && (len(sh.members[id]) > 0 || sh.slots[id] != nil)
		sh.mu.RUnlock()
		u.compare()
	}
	anchors = slices.DeleteFunc(anchors, func(u objectUpdate) bool {
		return u.staticEqual && u.effectiveEqual && !u.cell.hidden && !u.cell.retired && !u.needsRoot
	})
	return anchors
}

func (s *Store) installObjectAnchorsLocked(anchors []objectUpdate) {
	{
		uValues5 := anchors
		for uIndex4 := range uValues5 {
			u := uValues5[uIndex4]
			obj := u.obj
			if u.effectiveEqual && u.cell.static != nil {
				obj = u.cell.static
			}
			id := obj.Named()
			sh := s.shardFor(id)
			cell := sh.cells[id]
			cell.static, cell.rooted, cell.retired, cell.document = obj, true, false, u.document
			cell.reconcile, cell.report = u.policy.Reconcile, u.policy.Report
			cell.publication++
			sh.cells[id] = cell
		}
	}
}

func objectBucketCurrentLocked(sh *shard, bucket []objectUpdate) bool {
	current := true
	for j := range bucket {
		u := &bucket[j]
		cell := sh.cells[u.obj.Named()]
		if cell.publication != u.cell.publication || cell.generation != u.cell.generation {
			current = false
			break
		}
		u.cell.authority = cell.authority
	}
	return current
}

func captureObjectBucket(sh *shard, bucket []objectUpdate) {
	sh.mu.RLock()
	for j := range bucket {
		id := bucket[j].obj.Named()
		bucket[j].cell = sh.cells[id]
		sh.captureLocked(id, new(bucket[j].cell), &bucket[j].effective)
	}
	sh.mu.RUnlock()
	for j := range bucket {
		bucket[j].compare()
	}
}

func (s *Store) objectAnchorsCurrentLocked(anchors []objectUpdate) bool {
	current := true
	{
		uValues4 := anchors
		for uIndex3 := range uValues4 {
			u := uValues4[uIndex3]
			if !new(u).currentLocked(s.shardFor(u.obj.Named())) {
				current = false
				break
			}
		}
	}
	return current
}

func extendObjectAnchorRegion(anchors []objectUpdate, changed objectRegion) objectRegion {
	{
		uValues3 := anchors
		for uIndex2 := range uValues3 {
			u := uValues3[uIndex2]
			changed = changed.add(u.obj.Named(), !u.effectiveEqual || u.cell.static == nil, u.independent)
		}
	}
	return changed
}

func lockObjectBucket(sh *shard, bucket []objectUpdate) {
	for {
		captureObjectBucket(sh, bucket)
		sh.mu.Lock()
		current := objectBucketCurrentLocked(sh, bucket)
		if !current {
			sh.mu.Unlock()
			continue
		}
		return
	}
}
