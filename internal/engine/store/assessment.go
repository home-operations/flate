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
			if productions := input.productions.Load(); productions != nil {
				*productions = productionCapture{}
			}
			if details := input.details.Load(); details != nil {
				details.inputPayload = inputPayload{}
				details.binding.ReleaseCapture()
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

// PreflightFailure keeps cycle origin separate from the failure's display text.
type PreflightFailure struct {
	Message         string
	DependencyCycle bool
}

// Preflight returns the captured structural admission failure.
func (r *ReadSet) Preflight(id manifest.NamedResource) PreflightFailure {
	r.GetObject(id)
	view := &r.input(id).view
	return PreflightFailure{Message: view.preflight, DependencyCycle: view.preflightCycle}
}

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

// AssessmentsWithToleratedCRDs MUST run after the scheduler drains.
func (s *Store) AssessmentsWithToleratedCRDs() []*ReadSet {
	return s.assessments(func(a *acceptedAssessment) bool {
		return (a.kind == AssessmentReconciled || a.kind == AssessmentReused) && a.reads.hasToleratedCRDs
	})
}

// ControllerDemand permits the controller body to establish a required outcome.
func (r *ReadSet) ControllerDemand(id manifest.NamedResource) bool {
	return r.captureDemand(id) != 0
}

// ReadinessDemand distinguishes an assessed prerequisite from ordinary rendering.
func (r *ReadSet) ReadinessDemand(id manifest.NamedResource) bool {
	return r.captureDemand(id)&uint8(demandReadiness) != 0
}

func (r *ReadSet) captureDemand(id manifest.NamedResource) uint8 {
	r.GetObject(id)
	input := r.input(id)
	input.demandCaptured = true
	input.object.demand.Store(uint32(input.view.demand) | demandCaptured)
	if input.object.head != nil && !input.object.demandCurrent() {
		r.invalid.Store(true)
		input.object.head.shard.queue(input.object.head)
	}
	return input.view.demand
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
	return r.Matches(previous.reads) && !r.ControllerDemand(id) && previous.reads.Valid()
}
