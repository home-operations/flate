package store

import (
	"iter"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

type facet uint8

const (
	objectFacet facet = iota + 1
	conditionFacet
	contentFacet
	revisionFacet
	originFacet
	acquisitionFacet
	producerFacet
	bucketFacet
	reconciliationFacet
)

type headKey struct {
	id    manifest.NamedResource
	facet facet
}

type registrationHead struct {
	generation     atomic.Uint64
	ordinary       atomic.Bool
	first          atomic.Pointer[registration]
	selected       atomic.Pointer[producerSlot]
	position       atomic.Int64
	rendered       atomic.Pointer[RenderedOutput]
	assessments    [2]atomic.Pointer[acceptedAssessment]
	reconciliation atomic.Pointer[registrationHead]
	result         *registrationHead
	shard          *shard
	next           *registrationHead
	pending        atomic.Bool
	detached       atomic.Bool
	retained       atomic.Bool
	borrowed       atomic.Bool
	watch          *registration
}

func (h *registrationHead) stamp() uint64 {
	if h.result != nil {
		if accepted := h.result.assessments[0].Load(); accepted != nil {
			return accepted.revision
		}
		return 0
	}
	generation := h.generation.Load()
	if slot := h.selected.Load(); slot != nil {
		// A selected slot MUST already own a published record.
		record := slot.current.Load()
		position := int(h.position.Load())
		if position < len(record.entries) && record.entries[position].objectHead == h {
			return max(generation, record.entries[position].objectGeneration)
		}
	}
	return generation
}

const (
	demandUnread uint32 = iota
	demandQualification
	demandOrdinary
)

type registration struct {
	consumed   atomic.Bool
	ordinary   atomic.Uint32
	head       *registrationHead
	generation atomic.Uint64
	owner      *ReadSet
	next       atomic.Pointer[registration]
	absent     *Input
}

func (sh *shard) canAdmit(count uint64) bool {
	return sh.absence.first.Load() == nil && sh.absence.watch == nil || ^uint64(0)-sh.absence.generation.Load() >= count
}

func (sh *shard) admitAbsenceLocked(id manifest.NamedResource, kind facet, admission *ReadSet) {
	head := &sh.absence
	first := head.first.Load()
	if first == nil && head.watch == nil {
		return
	}
	generation := head.stamp()
	generation++
	head.generation.Store(generation)
	for _, chain := range [2]*registration{first, head.watch} {
		for cell := chain; cell != nil; cell = cell.next.Load() {
			matches := cell.absent.Ref.NamedResource == id && cell == cell.absent.cell(kind)
			if kind == bucketFacet && cell.absent.Ref.NamedResource == (manifest.NamedResource{}) {
				matches = true
			}
			if matches && cell.generation.Load() == 0 && (kind != bucketFacet || cell.owner != admission) {
				cell.generation.Store(generation)
				sh.queue(head)
			}
		}
	}
}

func (sh *shard) newHeadLocked(generation uint64) *registrationHead {
	if len(sh.spareHeads) == 0 {
		sh.headCapacity = max(minRegistrationHeadBlock, allocationGrowthFactor*sh.headCapacity)
		sh.spareHeads = make([]registrationHead, sh.headCapacity)
	}
	head := &sh.spareHeads[0]
	sh.spareHeads = sh.spareHeads[1:]
	head.shard = sh
	head.generation.Store(generation)
	return head
}

func (sh *shard) headLocked(key headKey) authority {
	switch key.facet {
	case objectFacet:
		return sh.cells[key.id].current()
	case conditionFacet:
		return sh.conditions[key.id].stamp()
	case contentFacet:
		return sh.artifacts[key.id].authority
	case revisionFacet:
		return sh.artifacts[key.id].revisionHead
	case originFacet:
		return sh.artifacts[key.id].originHead
	case acquisitionFacet:
		return sh.artifacts[key.id].acquisitionHead
	case producerFacet:
		return sh.slots[key.id].authority
	case bucketFacet:
		return sh.byName[kindID{key.id.Group, key.id.Kind}].authority
	case reconciliationFacet:
		if head := sh.cells[key.id].links; head != nil {
			if result := head.reconciliation.Load(); result != nil {
				return authority{links: result, generation: result.stamp()}
			}
		}
		return authority{}
	}
	panic("store: unknown authority facet")
}

func (sh *shard) enqueueLocked(head authority) authority {
	if head.links != nil {
		sh.queue(head.links)
	}
	return head
}

func (sh *shard) queue(head *registrationHead) {
	if (head.first.Load() != nil || head.detached.Load()) && head.pending.CompareAndSwap(false, true) {
		for {
			head.next = sh.pending.Load()
			if sh.pending.CompareAndSwap(head.next, head) {
				return
			}
		}
	}
}

func (sh *shard) advanceLocked(head authority) authority {
	if head.links == nil {
		head.links = sh.newHeadLocked(head.generation)
	}
	return sh.enqueueLocked(head.nextGeneration())
}

func (e *Emission) publication() uint64 {
	if e != nil {
		return e.revision
	}
	return 0
}

func (r *ReadSet) activate(input *Input, kind facet, generation uint64) {
	cell := r.registration(input, kind)
	cell.generation.Store(generation)
	cell.owner = r
	key := headKey{input.Ref.NamedResource, kind}
	sh := r.store.shardFor(key.id)
	sh.mu.RLock()
	if kind == producerFacet {
		slot := sh.slots[key.id]
		if slot == nil || slot.current.Load().publication() != input.producer {
			r.invalid.Store(true)
			sh.mu.RUnlock()
			return
		}
	}
	head := sh.headLocked(key)
	if kind == producerFacet {
		cell.generation.Store(head.generation)
	} else if head.generation != generation {
		r.invalid.Store(true)
	}
	cell.head = head.links
	if cell.head == nil {
		cell.head, cell.absent = &sh.absence, input
		cell.generation.Store(0)
	} else if cell.head.result == nil && cell.head.first.Load() == nil && !cell.head.detached.Load() {
		cell.head.generation.Store(head.generation)
	}
	cell.link()
	if kind == reconciliationFacet && cell.head.stamp() != generation {
		r.invalid.Store(true)
		sh.queue(cell.head)
	}
	sh.mu.RUnlock()
}

func (c *registration) link() {
	for {
		first := c.head.first.Load()
		c.next.Store(first)
		if c.head.first.CompareAndSwap(first, c) {
			return
		}
	}
}

func (c *registration) retain() {
	if c != nil && c.head != nil && c.absent == nil {
		c.head.borrowed.Store(true)
	}
}

func (c *registration) current() bool {
	if c == nil || c.head == nil {
		return true
	}
	if !c.ordinaryCurrent() {
		return false
	}
	if c.consumed.Load() {
		return true
	}
	if c.absent != nil {
		return c.generation.Load() == 0
	}
	if c.head.result != nil {
		return c.head.stamp() == c.generation.Load()
	}
	return c.head.generation.Load() == c.generation.Load()
}

func (c *registration) ordinaryCurrent() bool {
	demand := c.ordinary.Load()
	return demand == demandUnread || c.absent != nil || (demand == demandOrdinary) == c.head.ordinary.Load()
}

func (in *Input) objectCurrent() bool {
	cell := &in.object
	if in.view.owner != nil && cell.head != nil && cell.absent == nil {
		return cell.ordinaryCurrent() && cell.head.stamp() == cell.generation.Load()
	}
	return cell.current()
}

func (in *Input) current() bool {
	if !in.objectCurrent() || !in.production.Load().current() {
		return false
	}
	if details := in.details.Load(); details != nil {
		if details.inventory != nil {
			for i := range details.inventory {
				if !details.inventory[i].current() {
					return false
				}
			}
		}
		for i := range details.cells {
			if !details.cells[i].Load().current() {
				return false
			}
		}
	}
	return true
}

func (in *Input) sourceCurrent() bool {
	details := in.details.Load()
	if details == nil {
		return true
	}
	for kind := contentFacet; kind <= originFacet; kind++ {
		if !in.cell(kind).current() {
			return false
		}
	}
	return details.sourceGuard.Valid()
}

func (in *Input) productionCurrent() bool {
	cell := in.cell(producerFacet)
	return cell == nil || cell.head == nil ||
		cell.absent == nil && cell.head.generation.Load() == cell.generation.Load()
}

// Valid reports whether every consumed authority still has its original stamp.
func (r *ReadSet) Valid() bool {
	if r == nil {
		return true
	}
	if r.invalid.Load() {
		return false
	}
	for input := r.inputs; input != nil; input = input.next {
		if !input.current() ||
			input.view.bound &&
				(input.view.binding != nil && input.view.binding.Ref.NamedResource != r.id && !input.view.binding.sourceCurrent() ||
					input.view.support != nil && input.view.support.Ref.NamedResource != r.id && !input.view.support.productionCurrent()) {
			if !r.accepted.Load() {
				r.invalid.Store(true)
			}
			return false
		}
	}
	return true
}

// Retire releases only this execution's subscriptions at the next boundary.
func (r *ReadSet) Retire() {
	if r == nil || r.retired.Swap(true) {
		return
	}
	for input := r.inputs; input != nil; input = input.next {
		if details := input.details.Load(); details != nil {
			details.binding.Retire()
			details.gates = nil
			if details.inventory != nil {
				for i := range details.inventory {
					cell := &details.inventory[i]
					cell.head.shard.queue(cell.head)
				}
			}
		}
		for kind := objectFacet; kind <= reconciliationFacet; kind++ {
			cell := input.cell(kind)
			if cell == nil || cell.head == nil {
				continue
			}
			sh := cell.head.shard
			sh.queue(cell.head)
		}
	}
}

// Sweep delivers changed inputs and reclaims retired registrations.
// Only the invocation's structural boundary may call Sweep.
func (s *Store) Sweep() iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		deliver := true
		for _, sh := range s.shards {
			pending := sh.pending.Swap(nil)
			for pending != nil {
				pending = sh.sweepHead(pending, &deliver, yield)
			}
		}
	}
}

const (
	minRegistrationHeadBlock = 2
)

func (sh *shard) sweepHead(
	pending *registrationHead,
	deliver *bool,
	yield func(manifest.NamedResource) bool,
) *registrationHead {
	head := pending
	sh.mu.Lock()
	pending = head.next
	head.next = nil
	head.detached.Store(true)
	first, cutoff := head.first.Swap(nil), head.stamp()
	if head == &sh.absence {
		head.watch = first
	}
	head.pending.Store(false)
	sh.mu.Unlock()
	sweep := sweepRegistrations(first, cutoff, deliver, yield)
	live, tail, requeue := sweep.live, sweep.tail, sweep.requeue
	if tail == nil && head != &sh.absence {
		head.detached.Store(false)
		return pending
	}
	sh.mu.Lock()
	if tail != nil {
		tail.next.Store(head.first.Load())
		head.first.Store(live)
	}
	head.watch = nil
	head.detached.Store(false)
	if requeue {
		sh.queue(head)
	}
	sh.mu.Unlock()
	return pending
}

type registrationSweep struct {
	live    *registration
	tail    *registration
	requeue bool
}

func sweepRegistrations(
	first *registration,
	cutoff uint64,
	deliver *bool,
	yield func(manifest.NamedResource) bool,
) registrationSweep {
	var live, tail *registration
	requeue := false
	for cell := first; cell != nil; {
		next := cell.next.Load()
		changed := cell.generation.Load() != cutoff || !cell.ordinaryCurrent()
		if cell.absent != nil {
			generation := cell.generation.Load()
			changed = generation != 0 && generation <= cutoff
		}
		if !cell.owner.retired.Load() && !cell.consumed.Load() {
			if deliverRegistrationChange(cell, changed, deliver, yield) {
				requeue = true
			}
			if live == nil {
				live = cell
			} else {
				tail.next.Store(cell)
			}
			tail = cell
		} else if cell.absent == nil {
			cell.next.Store(nil)
		}
		cell = next
	}
	return registrationSweep{live: live, tail: tail, requeue: requeue}
}

func deliverRegistrationChange(
	cell *registration,
	changed bool,
	deliver *bool,
	yield func(manifest.NamedResource) bool,
) bool {
	requeue := false
	if changed && !cell.owner.delivered.Load() {
		if *deliver {
			completeRegistrationChange(cell, deliver, yield)
		} else {
			requeue = true
		}
	}
	return requeue
}

func completeRegistrationChange(cell *registration, deliver *bool, yield func(manifest.NamedResource) bool) {
	cell.owner.invalid.Store(true)
	if cell.owner.delivered.CompareAndSwap(false, true) {
		*deliver = yield(cell.owner.id)
	}
}
