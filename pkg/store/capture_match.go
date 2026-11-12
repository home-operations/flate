package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func (r *ReadSet) matchesInput(input *Input, exact bool) bool {
	id := input.Ref.NamedResource
	details := input.details.Load()
	if input.derived&capturedObject != 0 {
		r.GetObject(id)
		if !reconcileEqual(r.input(id).view.child.input(), input.view.child.input()) {
			return false
		}
	}
	if input.ordinaryDemand && r.OrdinaryDemand(id) != input.view.ordinary {
		return false
	}
	if input.declaration && (r.GetDeclaration(id) == nil) != (new(input.view).declaration() == nil) {
		return false
	}
	if !r.matchesReadiness(id, input, details) {
		return false
	}
	if !r.matchesInputDetails(input, details) {
		return false
	}
	if input.derived&capturedSettlement != 0 {
		sh := r.store.shardFor(id)
		sh.mu.RLock()
		if slot := sh.slots[id]; slot != nil {
			r.sample(id, slot, slot.current.Load(), input.watchProducer)
		}
		sh.mu.RUnlock()
		r.activateProducers()
	}
	return !exact || r.matchesInputStamps(input)
}

func (r *ReadSet) matchesReadiness(id manifest.NamedResource, input *Input, details *inputDetails) bool {
	if input.derived&capturedConditions != 0 {
		var observed Observation
		_, observed = r.Snapshot(id, true)
		conditions := observed.Conditions
		if !slices.EqualFunc(conditions, details.conditions, conditionEqual) {
			return false
		}
	} else if input.derived&capturedStatus != 0 {
		status, ok := r.GetStatus(id)
		if ok != input.hasStatus || status.Status != details.status.Status ||
			status.Reason != details.status.Reason ||
			!sameMessage(
				status.Message,
				status.applyingObjects,
				details.status.Message,
				details.status.applyingObjects,
			) ||
			!slices.Equal(status.BlockedBy(), details.status.BlockedBy()) {
			return false
		}
	}
	return true
}

func (r *ReadSet) matchesInputDetails(input *Input, details *inputDetails) bool {
	id := input.Ref.NamedResource
	if input.derived&capturedArtifact != 0 &&
		!artifactReadEqual(details.artifact, r.GetArtifact(id, details.artifactRead), details.artifactRead) {
		return false
	}
	productions := input.productions.Load()
	if input.derived&capturedProductions != 0 {
		if !slices.Equal(productions.items, r.Productions(id, productions.index)) {
			return false
		}
	} else if productions != nil && productions.items != nil {
		r.UnsettledProducers(id)
	}
	if input.derived&capturedObjects != 0 &&
		!slices.EqualFunc(details.objects, r.ListObjects(id.Group, id.Kind), manifest.Equal) {
		return false
	}
	if input.acquisitionRead {
		r.GetAcquisition(id)
		if r.details(r.input(id)).acquisition != details.acquisition {
			return false
		}
	}
	return true
}

func (r *ReadSet) matchesInputStamps(input *Input) bool {
	id := input.Ref.NamedResource
	current := r.input(id)
	if current.producer != input.producer {
		return false
	}
	for kind := objectFacet; kind <= reconciliationFacet; kind++ {
		before, after := input.cell(kind), current.cell(kind)
		if before != nil && before.head != nil &&
			(after == nil || before.head != after.head || before.generation.Load() != after.generation.Load()) {
			return false
		}
	}
	return true
}
