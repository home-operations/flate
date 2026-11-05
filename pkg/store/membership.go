package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func (c *objectCell) ordinaryDemand() bool {
	return c.ordinary || !c.included && !c.excluded
}

type capture struct {
	capturePayload

	excluded    bool
	accepted    bool
	independent bool
	classified  bool
	ordinary    bool
	bound       bool
	binding     *Input
	support     *Input
	preflight   string
	owner       *producerSlot
	revision    uint64
	generation  uint64
	position    int
}

type capturePayload struct {
	child        Child
	record       *Emission
	payload      *SourceArtifact
	payloadInput *ArtifactInput
	payloadOwner *producerSlot
	source       *sourceBinding
	reference    *Declaration
}

func (c *capture) sourceBinding() sourceBinding {
	if c.source != nil {
		return *c.source
	}
	return sourceBinding{}
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (sh *shard) objectLocked(id manifest.NamedResource) manifest.BaseManifest {
	cell := sh.cells[id]
	if !sh.derived && cell.binding == nil && (cell.static != nil || cell.reference == nil) {
		return cell.static
	}
	obj, _ := sh.captureLocked(id, new(cell), nil)
	return obj
}

//nolint:ireturn // The manifest contract requires polymorphic results.
func (sh *shard) captureLocked(
	id manifest.NamedResource,
	cellValue *objectCell,
	view *capture,
) (manifest.BaseManifest, manifest.DeclarationPolicy) {
	cell := *cellValue
	if view != nil {
		*view = capture{
			classified:  cell.classified && !cell.retired && cell.bound(),
			ordinary:    cell.ordinaryDemand(),
			independent: cell.independent,
			accepted:    cell.rooted,
			generation:  cell.generation,
			excluded:    cell.excluded,
			preflight:   cell.preflight,
			binding:     cell.binding,
			support:     cell.support,
			reference:   cell.reference,
		}
	}
	var child Child
	if cell.hidden {
		if cell.reference != nil {
			child = cell.reference.child()
		}
	} else {
		child = sh.captureRepresentativeLocked(id, cellValue, view)
	}
	if view != nil {
		view.child = child
	}
	return child.Obj, child.policy
}

func (sh *shard) captureRepresentativeLocked(id manifest.NamedResource, cellValue *objectCell, view *capture) Child {
	cell := *cellValue
	policy := manifest.DeclarationPolicy{Root: true, Reconcile: cell.reconcile, Report: cell.report}
	obj := cell.static
	bound := cell.bound()
	if view != nil {
		view.bound = bound
	}
	if !bound {
		obj = nil
	}
	available := cell.rooted || !sh.derived && obj != nil
	if view == nil && bound && available {
		return Child{Obj: obj, Doc: cell.document, policy: policy}
	}
	chosen := sh.captureMembersLocked(id, view)
	if !cell.rooted && (cell.links == nil || !cell.links.retained.Load()) || !bound {
		if chosen != nil {
			record := chosen.slot.current.Load()
			child := &record.entries[chosen.position].child
			if view != nil {
				view.owner, view.record, view.revision, view.child, view.source = chosen.slot, record, record.publication(), *child, &record.entries[chosen.position].binding
				view.generation = max(cell.generation, record.entries[chosen.position].objectGeneration)
				view.position = chosen.position
			}
			return *child
		}
		if cell.retired {
			obj = nil
		}
	}
	if obj == nil && cell.reference != nil {
		return cell.reference.child()
	}
	return Child{Obj: obj, Doc: cell.document, policy: policy}
}

type regionState struct {
	id                                  manifest.NamedResource
	before                              manifest.BaseManifest
	document                            map[string]any
	payload                             *SourceArtifact
	cell                                objectCell
	live, static, direct, visible, seed bool
	independent, supportsProducer       bool
	producerGeneration                  uint64
	owner                               *producerSlot
	source                              *sourceBinding
	bucketGeneration                    uint64
}

type objectRegion struct {
	positions map[manifest.NamedResource]int
	states    []regionState
	reserve   int
}

func newObjectRegion(capacity int, storage []regionState) objectRegion {
	if capacity <= cap(storage) {
		return objectRegion{states: storage[:0], reserve: capacity}
	}
	return objectRegion{
		positions: make(map[manifest.NamedResource]int, capacity),
		states:    make([]regionState, 0, capacity),
	}
}

func (r objectRegion) position(id manifest.NamedResource) (int, bool) {
	if r.positions != nil {
		i, seen := r.positions[id]
		return i, seen
	}
	for i := range r.states {
		if r.states[i].id == id {
			return i, true
		}
	}
	return 0, false
}

func (r objectRegion) add(id manifest.NamedResource, direct, independent bool) objectRegion {
	if i, seen := r.position(id); seen {
		r.states[i].direct = r.states[i].direct || direct
		return r
	}
	if len(r.states) == cap(r.states) && r.positions == nil {
		capacity := max(r.reserve, 2*len(r.states)+1)
		r.positions = make(map[manifest.NamedResource]int, capacity)
		r.states = slices.Grow(r.states, capacity-len(r.states))
		for i := range r.states {
			r.positions[r.states[i].id] = i
		}
	}
	if r.positions != nil {
		r.positions[id] = len(r.states)
	}
	r.states = append(r.states, regionState{id: id, direct: direct, independent: independent})
	return r
}

func (s *Store) affectedLocked(region objectRegion, producer manifest.NamedResource) objectRegion {
	capacity := s.regionCapacityLocked(&region)
	region.reserve = capacity
	if region.positions != nil && capacity > cap(region.states) {
		positions := make(map[manifest.NamedResource]int, capacity)
		for i := range region.states {
			positions[region.states[i].id] = i
		}
		region.positions = positions
		region.states = slices.Grow(region.states, capacity-len(region.states))
	}
	region = s.expandRegionLocked(region)
	s.captureRegionLocked(&region, producer)
	return region
}

func (s *Store) staticAnchorLocked(id manifest.NamedResource, support objectRegion) bool {
	sh := s.shardFor(id)
	cell := sh.cells[id]
	if cell.binding != nil && !cell.binding.sourceCurrent() ||
		cell.support != nil && !cell.support.productionCurrent() {
		return false
	}
	if cell.rooted && cell.static != nil {
		return true
	}
	if cell.static == nil {
		return false
	}
	r, ok := s.index.Load().Lookup(id)
	if !ok {
		return true
	}
	if cell.retired && (bootstrapSeed(r.Productions) || cell.classified) {
		return false
	}
	if cell.classified {
		return true
	}
	return s.staticProductionLocked(id, support, &r)
}

func (s *Store) reachabilityLocked(region objectRegion) []objectPublication {
	queue := make([]int, 0, len(region.states))
	add := func(id manifest.NamedResource) {
		if i, inside := region.position(id); inside && !region.states[i].live {
			region.states[i].live = true
			queue = append(queue, i)
		}
	}
	s.seedReachabilityLocked(region, add)
	s.propagateReachabilityLocked(region, &queue, add)
	s.publishRegionVisibilityLocked(region)
	publications := make([]objectPublication, 0, len(region.states)+1)
	for i := range region.states {
		publications = s.publishRegionObjectLocked(&region.states[i], publications)
	}
	return publications
}

func (s *Store) publishRegionObjectLocked(
	stateValue *regionState,
	publications []objectPublication,
) []objectPublication {
	state := *stateValue
	id := state.id
	sh := s.shardFor(id)
	cell := sh.cells[id]
	generation, retired, hidden, policy := cell.generation, cell.retired, cell.hidden, cell.policy
	cell.generation = max(cell.generation, state.cell.generation)
	if cell.generation == 0 && cell.static == nil && len(sh.members[id]) == 0 && sh.slots[id] == nil {
		return publications
	}
	if cell.hidden && state.live {
		sh.clearFacetsLocked(id)
	}
	cell.retired, cell.hidden, cell.independent = !state.static, !state.live, state.independent
	if cell.links != nil && (cell.retired || cell.hidden || !cell.bound()) {
		cell.links.retained.Store(false)
	}
	var view capture
	sh.captureLocked(id, new(cell), &view)
	after := view.child

	s.advanceRegionObjectLocked(new(state), &cell, new(view))
	if cell.generation != generation || cell.retired != retired || cell.hidden != hidden ||
		cell.policy != policy {
		sh.cells[id] = cell
	}
	selectRegionHeadLocked(&cell, new(view))
	sh.updateRegionBucketLocked(new(state), new(after))
	// Direct removal MUST restore surviving emitted payloads after clearing static facets.
	publications = s.appendRegionArtifactLocked(new(state), new(cell), new(view), publications)
	if (state.direct || state.before != after.Obj) && (after.Obj != nil || state.visible || !cell.hidden) {
		event := fireUnderLock(s, EventObjectAdded, id)
		event.payload = after.Obj
		publications = append(
			publications,
			objectPublication{
				event:        event,
				before:       state.before,
				document:     state.document,
				after:        after.Obj,
				nextDocument: after.Doc,
				force:        state.seed,
			},
		)
	}
	return publications
}

// HasInventory reports whether a producer has published an inventory, including hidden output.
func (s *Store) HasInventory(id manifest.NamedResource) bool {
	sh := s.shardFor(id)
	sh.mu.RLock()
	slot := sh.slots[id]
	present := slot != nil && slot.current.Load() != nil
	sh.mu.RUnlock()
	return present
}

// HasGeneratedObjects reports whether retained inventory contains auxiliary declarations.
func (s *Store) HasGeneratedObjects(id manifest.NamedResource) bool {
	sh := s.shardFor(id)
	sh.mu.RLock()
	var inventory *Emission
	if slot := sh.slots[id]; slot != nil {
		inventory = slot.current.Load()
	}
	sh.mu.RUnlock()
	return inventory != nil &&
		slices.ContainsFunc(inventory.Children, func(child Child) bool { return child.Generated })
}

func (sh *shard) captureMembersLocked(id manifest.NamedResource, view *capture) *membership {
	var chosen *membership
	for i := range sh.members[id] {
		m := &sh.members[id][i]
		if m.declaration || !m.slot.visible.Load() {
			continue
		}
		if chosen == nil || m.slot.id.Compare(chosen.slot.id) > 0 {
			chosen = m
		}
		if view != nil {
			record := m.slot.current.Load()
			payload := record.entries[m.position].child.payload
			if payload != nil && (view.payloadOwner == nil || m.slot.id.Compare(view.payloadOwner.id) > 0) {
				view.payload, view.payloadOwner = payload, m.slot
				view.payloadInput = record.entries[m.position].input
			}
		}
	}
	return chosen
}

func (s *Store) staticProductionLocked(
	id manifest.NamedResource,
	support objectRegion,
	r *manifest.DiscoveryRecord,
) bool {
	{
		pValues7 := r.Productions
		for pIndex6 := range pValues7 {
			p := pValues7[pIndex6]
			if p.Owner == (manifest.NamedResource{}) || p.Owner == id {
				return true
			}
			slot := s.shardFor(p.Owner).slots[p.Owner]
			if i, inside := support.position(p.Owner); inside {
				if !support.states[i].live {
					continue
				}
			} else if slot != nil && slot.hidden.Load() {
				continue
			}
			if slot == nil || !slot.current.Load().shadows(new(s.contributionBinding(p.Owner))) {
				return true
			}
		}
	}
	return len(r.Productions) == 0
}

func (s *Store) regionCapacityLocked(region *objectRegion) int {
	capacity := len(region.states)
	{
		stateValues2 := region.states
		for stateIndex2 := range stateValues2 {
			state := stateValues2[stateIndex2]
			id := state.id
			if slot := s.shardFor(id).slots[id]; slot != nil {
				if e := slot.current.Load(); e != nil {
					capacity += len(e.entries)
					{
						aValues8 := e.entries
						for aIndex5 := range aValues8 {
							a := aValues8[aIndex5]
							capacity += len(a.targets)
						}
					}
				}
			}
			if c := s.coverage[id]; c != nil {
				capacity += len(c.records)
			}
		}
	}
	return capacity
}

func (s *Store) expandRegionLocked(region objectRegion) objectRegion {
	//nolint:intrange // add MUST extend the worklist during traversal.
	for i := 0; i < len(region.states); i++ {
		id := region.states[i].id
		if slot := s.shardFor(id).slots[id]; slot != nil {
			if e := slot.current.Load(); e != nil {
				{
					aValues9 := e.entries
					for aIndex6 := range aValues9 {
						a := aValues9[aIndex6]
						region = region.add(a.child.Obj.Named(), false, false)
						for _, t := range a.targets {
							region = region.add(t.NamedResource, false, false)
						}
					}
				}
			}
		}
		if c := s.coverage[id]; c != nil {
			{
				fValues3 := c.records
				for fIndex3 := range fValues3 {
					f := fValues3[fIndex3]
					region = region.add(f.id, false, f.independent)
				}
			}
		}
	}
	return region
}

func (s *Store) captureRegionLocked(region *objectRegion, producer manifest.NamedResource) {
	var admissions [shardCount]uint64
	if producer != (manifest.NamedResource{}) {
		admissions[kindShardIndex(producer.Kind)] = 2
	}
	for i := range region.states {
		state := &region.states[i]
		s.captureRegionStateLocked(state, &admissions)
	}
	for i, count := range admissions {
		if !s.shards[i].canAdmit(count) {
			s.unlockAll()
			panic("store: authority generation exhausted")
		}
	}
}

func (s *Store) captureRegionStateLocked(state *regionState, admissions *[shardCount]uint64) {
	id := state.id
	sh := s.shardFor(id)
	state.cell = sh.cells[id]
	// Metadata-only cells may predate preparation; frozen facts cannot disagree.
	state.independent = state.independent || state.cell.independent
	state.cell.authority = state.cell.current()
	if state.cell.generation == 0 {
		admissions[kindShardIndex(id.Kind)]++
	}
	artifact := sh.artifacts[id]
	for _, head := range []authority{artifact.authority, artifact.revisionHead, artifact.originHead} {
		if head.generation == 0 {
			admissions[kindShardIndex(id.Kind)]++
		}
	}
	admissions[kindShardIndex(id.Kind)]++
	state.bucketGeneration = sh.byName[kindID{id.Group, id.Kind}].generation
	if slot := sh.slots[id]; slot != nil {
		state.producerGeneration = slot.generation
		if slot.generation == ^uint64(0) {
			s.unlockAll()
			panic("store: authority generation exhausted")
		}
	}
	var view capture
	sh.captureLocked(id, new(state.cell), &view)
	before := view.child
	state.before, state.document, state.payload = before.Obj, before.Doc, view.payload
	state.owner, state.source = view.owner, view.source
	state.visible = !state.cell.hidden && (state.cell.static != nil || len(sh.members[id]) > 0)
	if state.cell.publication == ^uint64(0) || artifact.publication == ^uint64(0) ||
		state.cell.generation == ^uint64(0) ||
		sh.conditions[id].stamp().generation == ^uint64(0) ||
		artifact.generation == ^uint64(0) ||
		artifact.revisionHead.generation == ^uint64(0) ||
		artifact.originHead.generation == ^uint64(0) ||
		state.bucketGeneration == ^uint64(0) {
		s.unlockAll()
		panic("store: authority generation exhausted")
	}
}

func (s *Store) seedReachabilityLocked(region objectRegion, add func(manifest.NamedResource)) {
	{
		stateValues3 := region.states
		for stateIndex3 := range stateValues3 {
			state := stateValues3[stateIndex3]
			id := state.id
			reference := s.shardFor(id).cells[id].reference
			live := s.staticAnchorLocked(id, region) || reference != nil && reference.policy.Reconcile
			if !live {
				for _, m := range s.shardFor(id).members[id] {
					if _, inside := region.position(m.slot.id); !inside && m.slot.visible.Load() {
						live = true
						break
					}
				}
			}
			if live {
				add(id)
			}
		}
	}
}

func (s *Store) propagateReachabilityLocked(region objectRegion, queueValue *[]int, add func(manifest.NamedResource)) {
	//nolint:intrange // add MUST extend the worklist during traversal.
	for i := 0; i < len(*queueValue); i++ {
		id := region.states[(*queueValue)[i]].id
		var e *Emission
		if slot := s.shardFor(id).slots[id]; slot != nil {
			e = slot.current.Load()
		}
		if e != nil {
			e.visitEntryTargets(add)
		}
		if e.shadows(new(s.contributionBinding(id))) {
			continue
		}
		if c := s.coverage[id]; c != nil {
			{
				fValues4 := c.records
				for fIndex4 := range fValues4 {
					f := fValues4[fIndex4]
					if s.shardFor(f.id).cells[f.id].static != nil {
						add(f.id)
					}
				}
			}
		}
	}
}

func (s *Store) publishRegionVisibilityLocked(region objectRegion) {
	for i := range region.states {
		state := &region.states[i]
		id := state.id
		state.static = s.staticAnchorLocked(id, region)
		sh := s.shardFor(id)
		sh.derived = true
		if slot := sh.slots[id]; slot != nil {
			if (slot.visible.Load() != state.live || slot.hidden.Load() == state.live) &&
				slot.generation == state.producerGeneration {
				slot.authority = sh.advanceLocked(slot.authority)
			}
			slot.visible.Store(state.live)
			slot.hidden.Store(!state.live)
		}
	}
}

func (e *Emission) visitEntryTargets(add func(manifest.NamedResource)) {
	{
		aValues10 := e.entries
		for aIndex7 := range aValues10 {
			a := aValues10[aIndex7]
			add(a.child.Obj.Named())
			for _, t := range a.targets {
				add(t.NamedResource)
			}
		}
	}
}

func (s *Store) advanceRegionObjectLocked(stateValue *regionState, cell *objectCell, viewValue *capture) {
	state := *stateValue
	view := *viewValue
	after := view.child
	id := state.id
	sh := s.shardFor(id)
	referenceChanged := state.before != after.Obj && after.Origin == manifest.SuppliedDeclaration
	if state.before != nil && referenceChanged {
		sh.clearFacetsLocked(id)
	}
	cell.policy = after.policy
	sameDeclaration := reconcileEqual(Child{Obj: state.before, Doc: state.document}, after)
	sameAuthority := state.owner == view.owner && state.source.equal(view.source) && sameDeclaration
	sameVisibility := cell.retired == state.cell.retired && cell.hidden == state.cell.hidden &&
		cell.policy == state.cell.policy
	if state.direct && (cell.links == nil || !cell.links.retained.Load() || !sameAuthority || !sameVisibility) ||
		!sameAuthority ||
		cell.rooted != state.cell.rooted && state.cell.static == nil ||
		!sameVisibility {
		if cell.generation == 0 {
			sh.admitAbsenceLocked(id, objectFacet, nil)
		}
		cell.authority = sh.advanceLocked(cell.current())
	}
}

func selectRegionHeadLocked(cell *objectCell, viewValue *capture) {
	view := *viewValue
	if head := cell.links; head != nil &&
		(head.selected.Load() != view.owner || view.owner != nil && head.position.Load() != int64(view.position)) {
		head.generation.Store(max(cell.generation, head.stamp()))
		if view.owner != nil {
			head.position.Store(int64(view.position))
		}
		head.selected.Store(view.owner)
	}
}

func (sh *shard) updateRegionBucketLocked(stateValue *regionState, afterValue *Child) {
	state := *stateValue
	after := *afterValue
	id := state.id
	key := kindID{id.Group, id.Kind}
	bucket := sh.byName[key]
	if bucket.links == nil && after.Obj != nil {
		bucket.links = sh.newHeadLocked(bucket.generation)
	}
	if bucket.update(nameID{id.Namespace, id.Name}, after.Obj != nil, state.direct, state.bucketGeneration) {
		sh.admitAbsenceLocked(manifest.NamedResource{Group: id.Group, Kind: id.Kind}, bucketFacet, nil)
		bucket.authority = sh.enqueueLocked(bucket.authority)
		sh.byName[key] = bucket
	}
}

func (s *Store) appendRegionArtifactLocked(
	stateValue *regionState,
	cellValue *objectCell,
	viewValue *capture,
	publications []objectPublication,
) []objectPublication {
	state := *stateValue
	cell := *cellValue
	view := *viewValue
	id := state.id
	sh := s.shardFor(id)
	referenceChanged := state.before != view.child.Obj && view.child.Origin == manifest.SuppliedDeclaration
	if !state.direct && state.payload == view.payload && state.cell.hidden == cell.hidden && !referenceChanged {
		return publications
	}
	record := sh.artifacts[id]
	effective := sh.projectArtifactLocked(id, &view, new(record))
	if record.effective != effective {
		changed := sh.publishArtifactLocked(id, &record, effective)
		sh.artifacts[id] = record
		if changed && (!cell.hidden || state.payload != nil) {
			event := fireUnderLock(s, EventArtifactUpdated, id)
			event.payload = effective
			if effective == nil {
				event.payload = view.payload
			}
			publications = append(publications, objectPublication{event: event, force: true})
		}
	}
	return publications
}
