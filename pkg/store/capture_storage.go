package store

import "github.com/home-operations/flate/pkg/manifest"

func (r *ReadSet) input(id manifest.NamedResource) *Input {
	if input := r.findInput(id); input != nil {
		r.last = input
		return input
	}
	return r.allocateInput(id)
}

func (r *ReadSet) details(input *Input) *inputDetails {
	if details := input.details.Load(); details != nil {
		return details
	}
	if len(r.spareDetails) == 0 {
		r.spareDetails = make([]inputDetails, max(1, allocationGrowthFactor*cap(r.spareDetails)))
	}
	i := len(r.spareDetails) - 1
	details := &r.spareDetails[i]
	r.spareDetails = r.spareDetails[:i]
	input.details.Store(details)
	return details
}

func (r *ReadSet) productionDetails(input *Input) *productionCapture {
	if captured := input.productions.Load(); captured != nil {
		return captured
	}
	if len(r.spareProductions) == 0 {
		r.spareProductions = make([]productionCapture, max(1, allocationGrowthFactor*cap(r.spareProductions)))
	}
	i := len(r.spareProductions) - 1
	captured := &r.spareProductions[i]
	r.spareProductions = r.spareProductions[:i]
	input.productions.Store(captured)
	return captured
}

func (in *Input) cell(kind facet) *registration {
	if kind == objectFacet {
		return &in.object
	}
	if kind == producerFacet {
		return in.production.Load()
	}
	if details := in.details.Load(); details != nil {
		index := kind - conditionFacet
		if kind > producerFacet {
			index--
		}
		return details.cells[index].Load()
	}
	return nil
}

func (r *ReadSet) registration(input *Input, kind facet) *registration {
	if cell := input.cell(kind); cell != nil {
		return cell
	}
	cell := &r.localRegistration
	if r.localRegistered {
		if len(r.spareRegistrations) == 0 {
			r.spareRegistrations = make([]registration, max(1, allocationGrowthFactor*cap(r.spareRegistrations)))
		}
		i := len(r.spareRegistrations) - 1
		cell = &r.spareRegistrations[i]
		r.spareRegistrations = r.spareRegistrations[:i]
	} else {
		r.localRegistered = true
	}
	if kind == producerFacet {
		input.production.Store(cell)
	} else {
		index := kind - conditionFacet
		if kind > producerFacet {
			index--
		}
		r.details(input).cells[index].Store(cell)
	}
	return cell
}

func (r *ReadSet) reserve(count int) {
	for spare := r.spare; spare != nil && count > 0; spare = spare.next {
		count--
	}
	remaining, capacity, allocations := count, r.capacity, 0
	for remaining > 0 {
		capacity = max(1, allocationGrowthFactor*capacity)
		remaining -= capacity
		allocations++
	}
	if allocations <= 1 {
		return
	}
	inputs := make([]Input, count)
	for i := range inputs {
		inputs[i].next, r.spare = r.spare, &inputs[i]
	}
	if r.byID == nil && allocations > maxLinearInputBlocks {
		r.indexInputs(count)
	}
}

func (r *ReadSet) indexInputs(spare int) {
	length := 0
	for input := r.inputs; input != nil; input = input.next {
		length++
	}
	index := make(map[manifest.NamedResource]*Input, length+spare)
	for input := r.inputs; input != nil; input = input.next {
		if index[input.Ref.NamedResource] == nil {
			index[input.Ref.NamedResource] = input
		}
	}
	r.byID = index
}

func (r *ReadSet) allocateInput(id manifest.NamedResource) *Input {
	var input *Input
	if r.local.derived == 0 {
		input = &r.local
	} else {
		if r.spare == nil {
			r.capacity = max(1, allocationGrowthFactor*r.capacity)
			block := make([]Input, r.capacity)
			for i := range block {
				block[i].next, r.spare = r.spare, &block[i]
			}
			if r.byID == nil && r.capacity >= 64 {
				r.indexInputs(r.capacity)
			}
		}
		input, r.spare = r.spare, r.spare.next
	}
	input.Ref.NamedResource, input.next, input.derived = id, r.inputs, initializedInput
	r.inputs = input
	if r.byID != nil {
		r.byID[id] = input
	}
	r.last = input
	return input
}

func (r *ReadSet) attachInput(input *Input) {
	if input.derived&initializedInput != 0 {
		return
	}
	if r.last != nil && r.last.Ref.NamedResource == input.Ref.NamedResource {
		r.last = input
	}
	input.next, r.inputs = r.inputs, input
	input.derived |= initializedInput
	if r.byID != nil {
		r.byID[input.Ref.NamedResource] = input
	}
}
