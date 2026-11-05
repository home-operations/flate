package store

import (
	"iter"

	"github.com/home-operations/flate/pkg/manifest"
)

// ReleaseCapture MUST follow evaluation, replay and every output borrow.
func (r *ReadSet) ReleaseCapture() {
	if r == nil {
		return
	}
	r.produced = nil
	for _, first := range [3]*Input{r.inputs, r.spare, &r.local} {
		for input := first; input != nil; input = input.next {
			input.view.capturePayload = capturePayload{}
			if details := input.details.Load(); details != nil {
				details.inputPayload = inputPayload{}
			}
		}
	}
	r.localDetails.inputPayload = inputPayload{}
	for i := range r.spareDetails[:cap(r.spareDetails)] {
		r.spareDetails[:cap(r.spareDetails)][i].inputPayload = inputPayload{}
	}
}

// ReplacesInvalid reports whether this attempt supersedes a stale accepted assessment.
func (r *ReadSet) ReplacesInvalid() bool {
	r.GetObject(r.id)
	head := r.input(r.id).object.head
	if head == nil {
		return false
	}
	previous := head.assessments[0].Load()
	return previous != nil && previous.reads != nil &&
		(!previous.reads.Valid() || previous.reads.retired.Load() && !r.Matches(previous.reads))
}

// Admitted reports the captured changed-only disposition.
func (r *ReadSet) Admitted() bool { r.GetObject(r.id); return !r.input(r.id).view.excluded }

// Preflight returns the captured structural admission failure.
func (r *ReadSet) Preflight() string { r.GetObject(r.id); return r.input(r.id).view.preflight }

// RequiredInputs enumerates a completed origin's accepted required footprint.
func (s *Store) RequiredInputs(id manifest.NamedResource) iter.Seq2[manifest.NamedResource, bool] {
	sh := s.shardFor(id)
	sh.mu.RLock()
	head := sh.cells[id].links
	sh.mu.RUnlock()
	var reads *ReadSet
	if head != nil {
		if accepted := head.assessments[0].Load(); accepted != nil {
			reads = accepted.reads
		}
	}
	if reads == nil || !reads.Valid() {
		return func(func(manifest.NamedResource, bool) bool) {}
	}
	return reads.Required()
}

// OrdinaryDemand captures purpose so later promotion invalidates qualification settlement.
func (r *ReadSet) OrdinaryDemand(id manifest.NamedResource) bool {
	r.GetObject(id)
	input := r.input(id)
	input.ordinaryDemand = true
	demand := demandQualification
	if input.view.ordinary {
		demand = demandOrdinary
	}
	input.object.ordinary.Store(demand)
	if input.object.head != nil && !input.object.ordinaryCurrent() {
		r.invalid.Store(true)
		input.object.head.shard.queue(input.object.head)
	}
	return input.view.ordinary
}

// QualificationSettled MUST retain the original assessment's validity around capture matching.
func (r *ReadSet) QualificationSettled(id manifest.NamedResource) bool {
	r.GetObject(id)
	input := r.input(id)
	if input.object.head == nil || input.object.absent != nil {
		return false
	}
	previous := input.object.head.assessments[0].Load()
	if previous == nil || previous.kind != AssessmentQualificationSettled || !previous.reads.Valid() {
		return false
	}
	return r.Matches(previous.reads) && !r.OrdinaryDemand(id) && previous.reads.Valid()
}
