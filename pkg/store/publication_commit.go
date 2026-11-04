package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func (s *Store) commitStable(slot *producerSlot, prev, proposal *Emission, generation uint64) publicationResult {
	if !proposal.Reads.Valid() {
		return publicationRejected
	}
	ps := s.shardFor(slot.id)
	ps.mu.Lock()
	if !proposal.Reads.Valid() {
		ps.mu.Unlock()
		return publicationRejected
	}
	if slot.current.Load() != prev || slot.generation != generation {
		ps.mu.Unlock()
		return publicationRetry
	}
	if proposal.Reads != nil && ps.cells[slot.id].current().generation != proposal.inputHead.generation {
		ps.mu.Unlock()
		return publicationRejected
	}
	if proposal == prev {
		ps.mu.Unlock()
		return publicationAccepted
	}
	if proposal != prev && proposal.revision == prev.revision {
		return s.commitRevisionLocked(ps, slot, proposal)
	}
	checkStableCapacityLocked(ps, slot, proposal)
	for i := range proposal.entries {
		a := &proposal.entries[i]
		if a.objectChanged && a.objectHead != nil && a.objectHead.selected.Load() == slot {
			a.objectGeneration = a.objectHead.stamp() + 1
		}
	}
	slot.authority = ps.advanceLocked(slot.authority)
	slot.current.Store(proposal)
	{
		aValues2 := proposal.entries
		for aIndex2 := range aValues2 {
			a := aValues2[aIndex2]
			if a.objectChanged && a.objectHead != nil && a.objectHead.selected.Load() == slot {
				a.objectHead.shard.queue(a.objectHead)
			}
		}
	}
	artifact, events := s.stableEventsLocked(ps, slot, proposal)
	ps.mu.Unlock()
	dispatchEvents(events)
	artifact.run()
	return publicationAccepted
}

func (s *Store) commitReplacement(
	id manifest.NamedResource,
	prev, proposal *Emission,
	generation uint64,
) publicationResult {
	if !proposal.Reads.Valid() {
		return publicationRejected
	}
	var storage [4]regionState
	changed := s.changedTargets(prev, proposal, storage[:])
	if proposal.shadows(new(s.contributionBinding(id))) {
		{
			fValues2 := proposal.coverage
			for fIndex2 := range fValues2 {
				f := fValues2[fIndex2]
				changed = changed.add(f.id, true, f.independent)
			}
		}
	}
	independent := declarationIndependent(s.index.Load(), id)
	ps := s.shardFor(id)
	s.lockAll()
	slot := ps.slots[id]
	if !proposal.Reads.Valid() {
		s.unlockAll()
		return publicationRejected
	}
	if slot != nil && (slot.current.Load() != prev || slot.generation != generation) || slot == nil && prev != nil {
		s.unlockAll()
		return publicationRetry
	}
	if proposal.Reads != nil && ps.cells[id].current().generation != proposal.inputHead.generation {
		s.unlockAll()
		return publicationRejected
	}
	if slot != nil && slot.generation == ^uint64(0) || ps.artifacts[id].generation == ^uint64(0) ||
		ps.artifacts[id].publication == ^uint64(0) {
		s.unlockAll()
		panic("store: authority generation exhausted")
	}
	region := s.affectedLocked(changed, id)
	if slot == nil {
		slot = &producerSlot{id: id, links: ps.newHeadLocked(0)}
		ps.slots[id] = slot
		slot.visible.Store(ps.objectLocked(id) != nil)
		slot.hidden.Store(ps.cells[id].hidden)
	}
	s.retireMembersLocked(id, slot, proposal, &region)

	s.admitEntryHeadsLocked(proposal)
	slot.authority = ps.advanceLocked(slot.authority)
	slot.current.Store(proposal)
	s.hideProducerLocked(ps, id, slot, independent)
	s.publishMembersLocked(slot, proposal)
	publications := s.reachabilityLocked(region)
	publications = s.appendReplacementArtifactLocked(id, ps, slot, publications)
	s.unlockAll()
	dispatchPublications(publications)
	return publicationAccepted
}

func (s *Store) changedTargets(prev, proposal *Emission, storage []regionState) objectRegion {
	changed := newObjectRegion(len(proposal.entries), storage)
	add := func(entry admission, direct bool) {
		changed = changed.add(entry.child.Obj.Named(), direct, entry.independent)
		for _, target := range entry.targets {
			changed = changed.add(
				target.NamedResource,
				direct,
				declarationIndependent(s.index.Load(), target.NamedResource),
			)
		}
	}
	next := 0
	if prev != nil {
		{
			oldValues2 := prev.entries
			for oldIndex2 := range oldValues2 {
				old := oldValues2[oldIndex2]
				if next == len(proposal.entries) || old.child.Obj.Named() != proposal.entries[next].child.Obj.Named() {
					add(old, true)
					continue
				}
				entry := proposal.entries[next]
				direct := entry.objectChanged ||
					entry.changed &&
						!manifest.LabelsEqual(
							old.child.Obj,
							entry.child.Obj,
							[2]map[string]any{old.child.Doc, entry.child.Doc},
							ownershipLabel,
						) ||
					!old.binding.equal(&entry.binding) ||
					!slices.Equal(old.child.Productions, entry.child.Productions) ||
					!slices.EqualFunc(old.targets, entry.targets, func(a, b manifest.ProducerTarget) bool {
						return a.NamedResource == b.NamedResource && slices.Equal(a.DeclaredKeys, b.DeclaredKeys)
					})
				add(old, direct)
				add(entry, direct)
				next++
			}
		}
	}
	for _, entry := range proposal.entries[next:] {
		add(entry, true)
	}
	return changed
}

func (s *Store) commitRevisionLocked(ps *shard, slot *producerSlot, proposal *Emission) publicationResult {
	if ps.artifacts[slot.id].publication == ^uint64(0) {
		ps.mu.Unlock()
		panic("store: publication revision exhausted")
	}
	slot.current.Store(proposal)
	if slot.visible.Load() {
		record := ps.artifacts[slot.id]
		record.inventoryVisible = true
		var view capture
		ps.captureLocked(slot.id, new(ps.cells[slot.id]), &view)
		record.effective = ps.projectArtifactLocked(slot.id, &view, new(record))
		record.publication++
		ps.artifacts[slot.id] = record
	}
	ps.mu.Unlock()
	return publicationAccepted
}

func (s *Store) stableEventsLocked(ps *shard, slot *producerSlot, proposal *Emission) (eventDispatch, []eventDispatch) {
	var artifact eventDispatch
	var events []eventDispatch
	if slot.visible.Load() {
		record := ps.artifacts[slot.id]
		record.inventoryVisible = true
		var view capture
		ps.captureLocked(slot.id, new(ps.cells[slot.id]), &view)
		visible := ps.projectArtifactLocked(slot.id, &view, new(record))
		ps.publishArtifactLocked(slot.id, &record, visible)
		ps.artifacts[slot.id] = record
		artifact = fireUnderLock(s, EventArtifactUpdated, slot.id)
		artifact.payload = visible
		listeners := s.listeners[EventObjectAdded].snapshot.Load()
		if len(listeners.entries) > 0 {
			{
				aValues3 := proposal.entries
				for aIndex3 := range aValues3 {
					a := aValues3[aIndex3]
					if !a.changed {
						continue
					}
					events = append(
						events,
						eventDispatch{id: a.child.Obj.Named(), payload: a.child.Obj, listeners: listeners},
					)
					for _, t := range a.targets {
						events = append(events, eventDispatch{id: t.NamedResource, listeners: listeners})
					}
				}
			}
		}
	} else {
		events = nil
	}
	return artifact, events
}

func checkStableCapacityLocked(ps *shard, slot *producerSlot, proposal *Emission) {
	if slot.generation == ^uint64(0) || ps.artifacts[slot.id].publication == ^uint64(0) ||
		ps.artifacts[slot.id].generation == ^uint64(0) ||
		ps.artifacts[slot.id].generation == 0 && !ps.canAdmit(1) {
		ps.mu.Unlock()
		panic("store: authority generation exhausted")
	}
	{
		aValues := proposal.entries
		for aIndex := range aValues {
			a := aValues[aIndex]
			if a.objectChanged && a.objectHead != nil && a.objectHead.selected.Load() == slot &&
				a.objectHead.stamp() == ^uint64(0) {
				ps.mu.Unlock()
				panic("store: authority generation exhausted")
			}
		}
	}
}

func (s *Store) retireMembersLocked(
	id manifest.NamedResource,
	slot *producerSlot,
	proposal *Emission,
	region *objectRegion,
) {
	{
		stateValues := region.states
		for stateIndex := range stateValues {
			state := stateValues[stateIndex]
			target := state.id
			sh := s.shardFor(target)
			sh.members[target] = slices.DeleteFunc(
				sh.members[target],
				func(m membership) bool { return m.slot == slot },
			)
		}
	}
	// A publication MUST retain the bootstrap authority supporting its own declaration.
	for ancestor, inside := region.position(id); inside; {
		state := &region.states[ancestor]
		if state.before == nil || state.supportsProducer {
			break
		}
		state.supportsProducer = true
		if state.owner == nil {
			break
		}
		ancestor, inside = region.position(state.owner.id)
	}
	{
		entryValues4 := proposal.entries
		for entryIndex3 := range entryValues4 {
			entry := entryValues4[entryIndex3]
			target := entry.child.Obj.Named()
			r, _ := s.index.Load().Lookup(target)
			sh := s.shardFor(target)
			cell := sh.cells[target]
			if bootstrapSeed(r.Productions) && cell.static != nil && !cell.rooted && !cell.retired {
				position, _ := region.position(target)
				if region.states[position].supportsProducer {
					continue
				}
				cell.retired = true
				sh.cells[target] = cell
				region.states[position].seed = true
			}
		}
	}
}

func (s *Store) admitEntryHeadsLocked(proposal *Emission) {
	for i := range proposal.entries {
		entry := &proposal.entries[i]
		// An admitted object's head MUST remain stable for the store's lifetime.
		if entry.objectHead != nil {
			continue
		}
		target := s.shardFor(entry.child.Obj.Named())
		cell := target.cells[entry.child.Obj.Named()]
		if cell.links == nil {
			cell.links = target.newHeadLocked(cell.generation)
			target.cells[entry.child.Obj.Named()] = cell
		}
		entry.objectHead = cell.links
	}
}

func (s *Store) hideProducerLocked(ps *shard, id manifest.NamedResource, slot *producerSlot, independent bool) {
	if !slot.visible.Load() {
		cell := ps.cells[id]
		if !cell.hidden {
			ps.derived = true
			if cell.generation == 0 {
				ps.admitAbsenceLocked(id, objectFacet)
			}
			cell.authority = ps.advanceLocked(cell.current())
			cell.hidden, cell.independent = true, independent
			ps.cells[id] = cell
		}
		slot.hidden.Store(true)
	}
}

func (s *Store) publishMembersLocked(slot *producerSlot, proposal *Emission) {
	{
		aValues4 := proposal.entries
		for position := range aValues4 {
			a := aValues4[position]
			target := a.child.Obj.Named()
			sh := s.shardFor(target)
			sh.members[target] = append(sh.members[target], membership{slot: slot, position: position})
			for _, t := range a.targets {
				sh := s.shardFor(t.NamedResource)
				sh.members[t.NamedResource] = append(
					sh.members[t.NamedResource],
					membership{slot: slot, position: position, declaration: true},
				)
			}
		}
	}
}

func (s *Store) appendReplacementArtifactLocked(
	id manifest.NamedResource,
	ps *shard,
	slot *producerSlot,
	publications []objectPublication,
) []objectPublication {
	if slot.visible.Load() {
		record := ps.artifacts[id]
		record.inventoryVisible = true
		var view capture
		ps.captureLocked(id, new(ps.cells[id]), &view)
		visible := ps.projectArtifactLocked(id, &view, new(record))
		ps.publishArtifactLocked(id, &record, visible)
		ps.artifacts[id] = record
		event := fireUnderLock(s, EventArtifactUpdated, id)
		event.payload = visible
		publications = append(publications, objectPublication{event: event, force: true})
	}
	return publications
}
