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
func (s *Store) RequiredInputs(id manifest.NamedResource) iter.Seq[manifest.NamedResource] {
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
		return func(func(manifest.NamedResource) bool) {}
	}
	return reads.Required()
}
