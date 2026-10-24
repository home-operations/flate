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
	artifactFacet
	producerFacet
	bucketFacet
)

type headKey struct {
	id    manifest.NamedResource
	facet facet
}

type registrationHead struct {
	assessment atomic.Pointer[ReadSet]
	shard      *shard
	generation atomic.Uint64
	first      atomic.Pointer[registration]
	next       *registrationHead
	pending    atomic.Bool
	detached   atomic.Bool
	watch      *registration
}

type registration struct {
	head       *registrationHead
	generation atomic.Uint64
	owner      *ReadSet
	next       atomic.Pointer[registration]
	absent     *Input
}

func (sh *shard) canAdmit(count uint64) bool {
	return sh.absence.first.Load() == nil && sh.absence.watch == nil || ^uint64(0)-sh.absence.generation.Load() >= count
}

func (sh *shard) admitAbsenceLocked(id manifest.NamedResource, kind facet) {
	head := &sh.absence
	first := head.first.Load()
	if first == nil && head.watch == nil {
		return
	}
	generation := head.generation.Load()
	generation++
	head.generation.Store(generation)
	for _, chain := range [2]*registration{first, head.watch} {
		for cell := chain; cell != nil; cell = cell.next.Load() {
			if cell.absent.Ref.NamedResource == id && cell == &cell.absent.cells[kind-1] && cell.generation.Load() == 0 {
				cell.generation.Store(generation)
				sh.queue(head)
			}
		}
	}
}

func (sh *shard) newHeadLocked(generation uint64) *registrationHead {
	if len(sh.spareHeads) == 0 {
		sh.headCapacity = max(1, 2*sh.headCapacity)
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
		return sh.cells[key.id].authority
	case conditionFacet:
		return sh.conditions[key.id].stamp()
	case artifactFacet:
		return sh.artifacts[key.id].authority
	case producerFacet:
		return sh.slots[key.id].authority
	case bucketFacet:
		return sh.byName[kindID{key.id.Group, key.id.Kind}].authority
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
	cell := &input.cells[kind-1]
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
	} else if cell.head.first.Load() == nil && !cell.head.detached.Load() {
		cell.head.generation.Store(head.generation)
	}
	cell.link()
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

func (input *Input) current() bool {
	for i := range input.cells {
		cell := &input.cells[i]
		if cell.head == nil {
			continue
		}
		if cell.absent != nil {
			if cell.generation.Load() != 0 {
				return false
			}
		} else if cell.head.generation.Load() != cell.generation.Load() {
			return false
		}
	}
	return true
}

func (input *Input) productionCurrent() bool {
	cell := &input.cells[producerFacet-1]
	return cell.head == nil || cell.absent == nil && cell.head.generation.Load() == cell.generation.Load()
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
		if !input.current() || input.view.bound && (input.view.binding != nil && !input.view.binding.current() || input.view.support != nil && !input.view.support.productionCurrent()) {
			r.invalid.Store(true)
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
		input.gates = nil
		for i := range input.cells {
			cell := &input.cells[i]
			if cell.head == nil {
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
				head := pending
				sh.mu.Lock()
				pending = head.next
				head.next = nil
				head.detached.Store(true)
				first, cutoff := head.first.Swap(nil), head.generation.Load()
				if head == &sh.absence {
					head.watch = first
				}
				head.pending.Store(false)
				sh.mu.Unlock()
				var live, tail *registration
				requeue := false
				for cell := first; cell != nil; {
					next := cell.next.Load()
					changed := cell.generation.Load() != cutoff
					if cell.absent != nil {
						generation := cell.generation.Load()
						changed = generation != 0 && generation <= cutoff
					}
					if !cell.owner.retired.Load() {
						if changed && !cell.owner.invalid.Load() {
							if deliver {
								if cell.owner.invalid.CompareAndSwap(false, true) {
									deliver = yield(cell.owner.id)
								}
							} else {
								requeue = true
							}
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
				if tail == nil && head != &sh.absence {
					head.detached.Store(false)
					continue
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
			}
		}
	}
}
