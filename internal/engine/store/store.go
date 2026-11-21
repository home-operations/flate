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
	// Borrowed stamps MUST advance after the last registration retires.
	if h.links != nil && (h.links.first.Load() != nil || h.links.detached.Load() || h.links.borrowed.Load()) {
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

	generatedDeclarations map[manifest.NamedResource]*Child
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

	reference            *Declaration
	policy               manifest.DeclarationPolicy
	classified, ordinary bool
	composed, operand    bool
	static               manifest.BaseManifest
	document             map[string]any
	warnings             []manifest.Warning
	// Direct writes use this revision; emitted values use the immutable producer record.
	publication                                              uint64
	binding                                                  *Input
	support                                                  *Input
	excluded, included, rooted, retired, hidden, independent bool
	reconcile, report                                        bool
	preflight                                                string
}

func (c *objectCell) visible(needsRoot bool) bool {
	return !c.hidden && !c.retired && !needsRoot
}

func (c *objectCell) stableAuthority(needsRoot bool) bool {
	return c.binding != nil || c.visible(needsRoot)
}

func (c *objectCell) bound() bool {
	return (c.binding == nil || c.binding.sourceCurrent()) && (c.support == nil || c.support.productionCurrent())
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
// the FNV-1a hash of id.Kind.
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
// a moving target. The loader (internal/engine/loader/inherit.go) and
// orchestrator (pkg/orchestrator/orchestrator.go) follow the
// clone-then-AddObject pattern; any new mutation site should too.
type Store struct {
	workTree  manifest.TreeID
	workScope manifest.WorkScope
	shards    [shardCount]*shard

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
	operand                     bool
	target, collection          *registration
	obj                         manifest.BaseManifest
	policy                      manifest.DeclarationPolicy
	document                    map[string]any
	warnings                    []manifest.Warning
	composed                    bool
	explicit                    bool
	binding                     *Input
	support                     *Input
	cell                        objectCell
	effective                   capture
	staticEqual, effectiveEqual bool
	membershipEqual             bool
	needsRoot                   bool
	independent                 bool
	classified                  bool
	reads                       *ReadSet
	inventory                   *registration

	generated, previousGenerated *Child
}

func (u *objectUpdate) sameStaticDeclaration(equal bool) bool {
	bindingEqual := u.cell.binding == u.binding
	if !bindingEqual && u.cell.binding != nil && u.binding != nil {
		bindingEqual = u.cell.binding.Ref.NamedResource == u.binding.Ref.NamedResource &&
			u.cell.binding.sourceCurrent() && u.binding.sourceCurrent()
	}
	supportEqual := u.cell.support == u.support
	if !supportEqual && u.cell.support != nil && u.support != nil {
		supportEqual = u.cell.support.Ref.NamedResource == u.support.Ref.NamedResource &&
			u.cell.support.productionCurrent() && u.support.productionCurrent()
	}
	sameEvidence := sameGeneratedEvidence(u.previousGenerated, u.generated)
	sameDeclaration := equal && u.cell.classified == u.classified && u.cell.operand == u.operand
	return bindingEqual && supportEqual && sameEvidence && sameDeclaration
}

func (u *objectUpdate) repeatsDeclaration(cell *objectCell, generated *Child) bool {
	if u.explicit {
		return false
	}
	sameEvidence := generated == u.generated
	sameDeclaration := cell.static == u.obj && cell.classified == u.classified &&
		warningsEqual(cell.warnings, u.warnings)
	return sameDeclaration && sameEvidence
}

func (u *objectUpdate) unchanged() bool {
	stableAuthority := u.cell.stableAuthority(u.needsRoot)
	return u.staticEqual && stableAuthority &&
		(!u.explicit || manifest.ValueEqual(u.cell.document, u.document))
}

func (u *objectUpdate) compare() {
	equal := manifest.Equal(u.cell.static, u.obj) && warningsEqual(u.cell.warnings, u.warnings)
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
	u.staticEqual = u.sameStaticDeclaration(equal)
	u.membershipEqual = u.staticEqual ||
		u.cell.static != nil && manifest.LabelsEqual(u.cell.static, u.obj, [2]map[string]any{}, nil)
	u.effectiveEqual = equal
	if u.effectiveEqual {
		u.effectiveEqual = reconcileInputEqual(u.obj, u.cell.document, u.document).equal
	}
	if u.effective.child.Obj != u.cell.static {
		u.effectiveEqual = manifest.Equal(u.effective.child.Obj, u.obj) &&
			warningsEqual(u.effective.child.Warnings, u.warnings)
		if u.effectiveEqual {
			u.effectiveEqual = reconcileInputEqual(u.obj, u.effective.child.Doc, u.document).equal
		}
	}
}

func (sh *shard) retainGeneratedDeclaration(id manifest.NamedResource, child *Child) {
	if child == nil {
		delete(sh.generatedDeclarations, id)
		return
	}
	if sh.generatedDeclarations == nil {
		sh.generatedDeclarations = make(map[manifest.NamedResource]*Child)
	}
	sh.generatedDeclarations[id] = child
}

func sameGeneratedEvidence(a, b *Child) bool {
	return a == b || a != nil && b != nil && slices.Equal(a.Productions, b.Productions)
}

func (u *objectUpdate) retainSupport() {
	if input := u.binding; input != nil {
		for kind := contentFacet; kind <= originFacet; kind++ {
			input.cell(kind).retain()
		}
	}
	if input := u.support; input != nil {
		input.cell(producerFacet).retain()
	}
}

// currentLocked MUST hold the effective producer's shard as well as the target's.
func (u *objectUpdate) currentLocked(sh *shard) bool {
	v := *u
	return sh.cells[v.obj.Named()].publication == v.cell.publication &&
		sh.cells[v.obj.Named()].generation == v.cell.generation &&
		(v.effective.owner == nil || v.effective.owner.current.Load() == v.effective.record)
}

// RetainOperand preserves reachability from the physical checkout across an emitted source replacement.
func (s *Store) RetainOperand(id manifest.NamedResource) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	if cell := sh.cells[id]; cell.static != nil && !cell.retired && cell.bound() {
		cell.operand = true
		sh.cells[id] = cell
	}
	sh.mu.Unlock()
}

// AddObject inserts a manifest. Equal content with unchanged authority is a no-op.
func (s *Store) AddObject(obj manifest.BaseManifest, docs ...map[string]any) {
	if len(docs) > 1 {
		panic("multiple authoritative documents")
	}
	update := objectUpdate{obj: obj, explicit: len(docs) != 0}
	if update.explicit {
		update.document = docs[0]
	}
	s.addObject(&update)
}

func (s *Store) addObject(update *objectUpdate) {
	obj, binding, support := update.obj, update.binding, update.support
	id := obj.Named()
	sh := s.shardFor(id)
	for {
		if !update.reads.Valid() {
			return
		}
		sh.mu.RLock()
		index := s.index.Load()
		cell := sh.cells[id]
		generated := sh.generatedDeclarations[id]
		supplied := binding != nil && cell.reference != nil && cell.reference.Obj == obj &&
			cell.reference.Origin == manifest.SuppliedDeclaration
		needsRoot := !cell.rooted && (len(sh.members[id]) > 0 || sh.slots[id] != nil)
		sameObject := update.repeatsDeclaration(&cell, generated)
		sameAuthority := cell.binding == binding && cell.support == support && cell.operand == update.operand
		stableAuthority := cell.stableAuthority(needsRoot)
		if supplied || sameObject && sameAuthority && stableAuthority {
			sh.mu.RUnlock()
			return
		}
		u := *update
		u.cell, u.needsRoot, u.previousGenerated = cell, needsRoot, generated
		u.effective.child.Obj, u.effective.child.Doc = cell.static, cell.document
		u.effective.child.Warnings = cell.warnings
		if sh.derived {
			sh.captureLocked(id, new(cell), &u.effective)
		}
		sh.mu.RUnlock()
		record, _ := index.Lookup(id)
		u.classified = update.classified &&
			(rawDeclarationAuthority(index, id, true) || preparedDeclaration(&record, obj))
		u.independent = rawDeclarationAuthority(index, id, false)
		u.compare()
		if u.unchanged() {
			return
		}
		if s.commitObjectUpdate(sh, id, &u, index) != publicationRetry {
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
	cell.operand, cell.rooted = u.operand, cell.rooted || u.operand
	if u.staticEqual && u.effectiveEqual && !cell.retired {
		cell.document, cell.warnings = u.document, u.warnings
		sh.cells[id] = cell
		return eventDispatch{}
	}
	var admissions uint64
	if cell.generation == 0 {
		admissions++
	}
	var bucket objectBucket
	if !u.membershipEqual {
		admissions++
	}
	if !sh.canAdmit(admissions) {
		sh.mu.Unlock()
		panic("store: authority generation exhausted")
	}
	if !u.membershipEqual {
		bucket = sh.byName[kindID{id.Group, id.Kind}]
		if bucket.links == nil {
			bucket.links = sh.newHeadLocked(bucket.generation)
		}
	}
	if cell.generation == ^uint64(0) || bucket.generation == ^uint64(0) {
		sh.requireCapacityLocked(cell.authority, bucket.authority)
	}
	u.consumeTarget()
	if cell.generation == 0 {
		sh.admitAbsenceLocked(id, objectFacet, u.reads)
		cell.links = sh.newHeadLocked(0)
	}
	cell.authority = cell.current().nextGeneration()
	if cell.links != nil {
		cell.authority = sh.enqueueLocked(cell.authority)
	}
	if !u.staticEqual {
		u.retainSupport()
		cell.static, cell.binding, cell.support = u.obj, u.binding, u.support
		sh.retainGeneratedDeclaration(id, u.generated)
		cell.composed = u.composed
		cell.reconcile, cell.report = u.policy.Reconcile, u.policy.Report
	}
	cell.document, cell.warnings = u.document, u.warnings
	cell.retired, cell.independent, cell.policy, cell.classified = false, u.independent, u.policy, u.classified
	sh.cells[id] = cell
	if !u.membershipEqual && bucket.update(nameID{id.Namespace, id.Name}, cell.static != nil, true, bucket.generation) {
		sh.admitAbsenceLocked(manifest.NamedResource{Group: id.Group, Kind: id.Kind}, bucketFacet, u.reads)
		u.advanceCollection(bucket.generation)
		bucket.authority = sh.enqueueLocked(bucket.authority)
		sh.byName[kindID{id.Group, id.Kind}] = bucket
	}
	dispatch := fireUnderLock(s, EventObjectAdded, id)
	dispatch.payload = u.obj
	return dispatch
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
	if !sh.canAdmit(1) {
		sh.mu.Unlock()
		panic("store: authority generation exhausted")
	}
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
	cell.static, cell.rooted, cell.binding, cell.support, cell.document, cell.warnings = nil, false, nil, nil, nil, nil
	cell.operand = false
	delete(sh.generatedDeclarations, id)
	_, cell.policy = sh.captureLocked(id, new(cell), nil)
	sh.cells[id] = cell
	sh.clearFacetsLocked(id)
	if bucket.update(nameID{id.Namespace, id.Name}, cell.reference != nil, true, bucket.generation) {
		sh.admitAbsenceLocked(manifest.NamedResource{Group: id.Group, Kind: id.Kind}, bucketFacet, nil)
		bucket.authority = sh.enqueueLocked(bucket.authority)
		sh.byName[kindID{id.Group, id.Kind}] = bucket
	}
	return true
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
	cell.static, cell.rooted, cell.binding, cell.support, cell.document, cell.warnings = nil, false, nil, nil, nil, nil
	cell.operand = false
	delete(sh.generatedDeclarations, id)
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

func (s *Store) addAnchor(uValue *objectUpdate, index *manifest.DiscoveryIndex) bool {
	u := *uValue
	id := u.obj.Named()
	sh := s.shardFor(id)
	var storage [4]regionState
	changed := newObjectRegion(1, storage[:])
	changed = changed.add(
		id,
		!u.effectiveEqual || u.cell.static == nil || u.classified != u.cell.classified,
		u.independent,
	)
	s.lockAll()
	if !u.reads.Valid() {
		s.unlockAll()
		return true
	}
	if s.index.Load() != index || !new(u).currentLocked(sh) || u.binding != nil && !u.binding.sourceCurrent() ||
		u.support != nil && !u.support.productionCurrent() {
		s.unlockAll()
		return false
	}
	u.consumeTarget()
	region := s.affectedLocked(changed, manifest.NamedResource{})
	obj := u.obj
	if u.effectiveEqual && u.cell.static != nil {
		obj = u.cell.static
	}
	cell := sh.cells[id]
	u.retainSupport()
	cell.operand = u.operand
	cell.rooted = u.binding == nil || u.operand
	cell.static, cell.binding, cell.support, cell.document, cell.classified = obj, u.binding, u.support, u.document, u.classified
	cell.reconcile, cell.report, cell.warnings = u.policy.Reconcile, u.policy.Report, u.warnings
	sh.retainGeneratedDeclaration(id, u.generated)
	cell.publication++
	sh.cells[id] = cell
	dispatches := s.reachabilityLocked(region)
	u.advanceCollection(sh.byName[kindID{id.Group, id.Kind}].generation)
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
				changed = !comparison.equal || !manifest.LabelsEqual(
					p.before,
					p.after,
					[2]map[string]any{p.document, p.nextDocument},
					manifest.IsOwnershipLabel,
				)
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
	if !warningsEqual(a.Warnings, b.Warnings) {
		return false
	}
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
		return x.SameMaterial(y, read)
	}
	return artifactValueEqual(a, b)
}
