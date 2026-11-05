package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// AcquisitionState grants no content authority without valid captured evidence.
type AcquisitionState uint8

// Acquisition results describe credential settlement separately from source readiness.
const (
	AcquisitionPending AcquisitionState = iota
	AcquisitionPrepared
	AcquisitionSkipped
)

// AssessmentKind keeps entry bindings separate from reconciliation acceptance.
type AssessmentKind uint8

// Assessment dispositions select one of the two independent acceptance slots.
const (
	AssessmentReconciled AssessmentKind = iota
	AssessmentReused
	AssessmentEntryBinding
)

type acceptedAssessment struct {
	reads       *ReadSet
	kind        AssessmentKind
	revision    uint64
	acquisition AcquisitionState
	evidence    *ReadSet
	condition   uint64
}

// SetAcquisition stages settlement without changing the accepted result.
func (r *ReadSet) SetAcquisition(state AcquisitionState) {
	r.acquisition, r.hasAcquisition = state, true
}

// GetAcquisition freezes the accepted result for this capture.
func (r *ReadSet) GetAcquisition(id manifest.NamedResource) AcquisitionState {
	input := r.input(id)
	r.Capture(input)
	details := r.details(input)
	if input.acquisitionRead {
		return details.acquisitionState
	}
	input.acquisitionRead = true
	head := input.object.head
	if input.object.absent != nil || head == nil {
		return AcquisitionPending
	}
	if head.reconciliation.Load() == nil {
		head.reconciliation.CompareAndSwap(nil, &registrationHead{shard: head.shard, result: head})
	}
	accepted := head.assessments[0].Load()
	var revision uint64
	if accepted != nil {
		revision = accepted.revision
	}
	details.acquisition = accepted
	r.observe(input, reconciliationFacet, revision)
	if accepted == nil || accepted.evidence == nil || !accepted.evidence.Valid() ||
		!r.matches(accepted.evidence, true) ||
		!accepted.evidence.Valid() {
		return AcquisitionPending
	}
	state := accepted.acquisition
	if state == AcquisitionSkipped {
		status, ok := r.GetStatus(id)
		if !ok || status.Outcome() != OutcomeSkipped ||
			input.cell(conditionFacet).generation.Load() != accepted.condition {
			return AcquisitionPending
		}
	}
	details.acquisitionState = state
	return state
}

// Accept publishes only against the predecessor captured by this execution.
func (r *ReadSet) Accept(kind AssessmentKind) {
	r.GetObject(r.id)
	slot := 0
	if kind == AssessmentEntryBinding {
		slot = 1
	}
	head := r.originHead
	if head == nil || r.retired.Load() || !r.Valid() {
		return
	}
	if kind == AssessmentEntryBinding {
		r.ReleaseCapture()
	}
	previous := r.expected[slot]
	if head.assessments[slot].Load() != previous {
		r.invalid.Store(true)
		return
	}
	proposal := r.assessmentProposal(kind, previous, slot)
	if proposal == nil {
		return
	}
	if !r.Valid() || !head.assessments[slot].CompareAndSwap(previous, proposal) {
		r.invalid.Store(true)
		return
	}
	r.expected[slot] = proposal
	r.accepted.Store(true)
	if slot == 0 {
		if subscribers := head.reconciliation.Load(); subscribers != nil {
			head.shard.queue(subscribers)
		}
	}
	if previous != nil && previous.reads != r {
		r.store.Retire(previous.reads)
	}
}

func (r *ReadSet) assessmentProposal(kind AssessmentKind, previous *acceptedAssessment, slot int) *acceptedAssessment {
	var evidence *ReadSet
	var state AcquisitionState
	var condition uint64
	if slot == 0 && r.hasAcquisition {
		state = r.acquisition
		var valid bool
		condition, valid = r.acquisitionCondition(state)
		if !valid {
			return nil
		}
		if previous != nil && previous.reads == r && previous.kind == kind && previous.acquisition == state &&
			previous.condition == condition &&
			r.sameEvidence(previous.evidence) {
			return nil
		}
		evidence = r.freezeEvidence()
		if !evidence.Valid() {
			return nil
		}
	} else if previous != nil && previous.reads == r && previous.kind == kind {
		return nil
	}
	revision := uint64(1)
	if previous != nil {
		if previous.revision == ^uint64(0) {
			panic("store: authority generation exhausted")
		}
		revision = previous.revision + 1
	}
	proposal := &r.acceptedRecord
	if proposal.revision != 0 {
		proposal = new(acceptedAssessment)
	}
	*proposal = acceptedAssessment{
		reads:       r,
		kind:        kind,
		revision:    revision,
		acquisition: state,
		evidence:    evidence,
		condition:   condition,
	}
	return proposal
}

func (r *ReadSet) acquisitionCondition(state AcquisitionState) (uint64, bool) {
	if state != AcquisitionSkipped {
		return 0, true
	}
	status, ok := r.GetStatus(r.id)
	if !ok || status.Outcome() != OutcomeSkipped {
		return 0, false
	}
	return r.input(r.id).cell(conditionFacet).generation.Load(), true
}

func (r *ReadSet) sameEvidence(previous *ReadSet) bool {
	if previous == nil {
		return false
	}
	count := 0
	for input := r.inputs; input != nil; input = input.next {
		var before *Input
		for old := previous.inputs; old != nil; old = old.next {
			if old.Ref.NamedResource == input.Ref.NamedResource {
				before = old
				break
			}
		}
		if !sameInputEvidence(input, before) {
			return false
		}
		count++
	}
	for input := previous.inputs; input != nil; input = input.next {
		count--
	}
	return count == 0
}

func sameInputEvidence(current, before *Input) bool {
	if before == nil || current.derived != before.derived || current.acquisitionRead != before.acquisitionRead ||
		current.producer != before.producer ||
		current.watchProducer != before.watchProducer ||
		current.declaration != before.declaration ||
		current.required != before.required ||
		current.materializer != before.materializer ||
		current.gate != before.gate ||
		!sameReadinessGate(&current.Ref, &before.Ref) {
		return false
	}
	for kind := objectFacet; kind <= reconciliationFacet; kind++ {
		a, b := current.cell(kind), before.cell(kind)
		if a == nil || b == nil {
			if a != b {
				return false
			}
			continue
		}
		if a.head != b.head || a.generation.Load() != b.generation.Load() {
			return false
		}
	}
	a, b := current.details.Load(), before.details.Load()
	if a != nil && b != nil && (a.artifactRead != b.artifactRead || a.acquisition != b.acquisition) {
		return false
	}
	return true
}

func (r *ReadSet) freezeEvidence() *ReadSet {
	frozen := new(ReadSet)
	frozen.Init(r.store, r.id, nil)
	for input := r.inputs; input != nil; input = input.next {
		snapshot := frozen.input(input.Ref.NamedResource)
		snapshot.Ref, snapshot.view = input.Ref, input.view
		snapshot.producer, snapshot.settled, snapshot.watchProducer = input.producer, input.settled, input.watchProducer
		snapshot.gate, snapshot.materializer, snapshot.declaration = input.gate, input.materializer, input.declaration
		snapshot.required, snapshot.hasStatus, snapshot.derived = input.required, input.hasStatus, input.derived
		snapshot.acquisitionRead = input.acquisitionRead
		if details := input.details.Load(); details != nil {
			target := frozen.details(snapshot)
			target.productions = slices.Clone(details.productions)
			target.objects = slices.Clone(details.objects)
			target.artifact, target.artifactRead = details.artifact, details.artifactRead
			target.sourceGuard = details.sourceGuard
			target.conditions = slices.Clone(details.conditions)
			target.status = details.status
			target.acquisition, target.acquisitionState = details.acquisition, details.acquisitionState
		}
		for kind := objectFacet; kind <= reconciliationFacet; kind++ {
			cell := input.cell(kind)
			if cell == nil {
				continue
			}
			target := snapshot.cell(kind)
			if target == nil {
				continue
			}
			target.head = cell.head
			target.generation.Store(cell.generation.Load())
			if cell.absent != nil {
				target.absent = snapshot
			}
		}
	}
	return frozen
}
