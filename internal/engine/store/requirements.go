package store

import (
	"iter"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// Require retains each distinct readiness gate in the execution footprint.
//
//nolint:gocritic // The SDK preserves this value input and its snapshot boundary.
func (r *ReadSet) Require(ref manifest.DependencyRef) {
	r.require(new(ref), false)
}

// RequireMaterializer retains a supplier's status-aware readiness assessment.
func (r *ReadSet) RequireMaterializer(id manifest.NamedResource) {
	r.require(new(manifest.DependencyRef{NamedResource: id}), true)
}

func (r *ReadSet) require(refValue *manifest.DependencyRef, materializer bool) {
	ref := *refValue
	input := r.input(ref.NamedResource)
	input.required, input.qualification = true, false
	if !input.gate {
		input.Ref.ReadyExpr, input.gate = ref.ReadyExpr, true
		input.Ref.Artifact, input.Ref.ArtifactOnly, input.Ref.ChartSource = ref.Artifact, ref.ArtifactOnly, ref.ChartSource
		input.Ref.RequiresDeclaration = ref.RequiresDeclaration
		input.materializer = materializer
		return
	}
	if sameReadinessGate(new(input.Ref), new(ref)) && input.materializer == materializer {
		return
	}
	for gate := &r.details(input).gates; ; gate = &(*gate).next {
		if *gate == nil {
			*gate = &readinessGate{ref: ref, materializer: materializer}
			return
		}
		if sameReadinessGate(&(*gate).ref, &ref) && (*gate).materializer == materializer {
			return
		}
	}
}

// Gates enumerates the retained readiness assessments.
func (r *ReadSet) Gates() iter.Seq[Gate] {
	return func(yield func(Gate) bool) {
		for input := r.inputs; input != nil; input = input.next {
			if !input.gate {
				continue
			}
			if !yield(Gate{DependencyRef: input.Ref, Materializer: input.materializer}) {
				return
			}
			details := input.details.Load()
			if details == nil {
				continue
			}
			for gate := details.gates; gate != nil; gate = gate.next {
				if !yield(Gate{DependencyRef: gate.ref, Materializer: gate.materializer}) {
					return
				}
			}
		}
	}
}

// ClearToleratedCRDs starts a new pass over the retained gates.
func (r *ReadSet) ClearToleratedCRDs() {
	if !r.hasToleratedCRDs {
		return
	}
	r.hasToleratedCRDs = false
	for input := r.inputs; input != nil; input = input.next {
		input.toleratedCRD = false
	}
}

// RecordToleratedCRD requires a gate already retained for id.
func (r *ReadSet) RecordToleratedCRD(id manifest.NamedResource) {
	r.findInput(id).toleratedCRD = true
	r.hasToleratedCRDs = true
}

// ToleratedCRDs yields the missing declarations admitted in the latest gate pass.
func (r *ReadSet) ToleratedCRDs() iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		if !r.hasToleratedCRDs {
			return
		}
		for input := r.inputs; input != nil; input = input.next {
			if input.toleratedCRD && !yield(input.Ref.NamedResource) {
				return
			}
		}
	}
}

// Required enumerates the required inputs of the accepted execution.
func (r *ReadSet) Required() iter.Seq2[manifest.NamedResource, bool] {
	return func(yield func(manifest.NamedResource, bool) bool) {
		for input := r.inputs; input != nil; input = input.next {
			if input.required && !yield(input.Ref.NamedResource, input.qualification) {
				return
			}
		}
	}
}

// RequireData records a required semantic input without imposing readiness.
func (r *ReadSet) RequireData(id manifest.NamedResource) {
	input := r.input(id)
	input.required, input.qualification = true, false
}

// RequireOrdering retains readiness demand from the effective declaration.
func (r *ReadSet) RequireOrdering(id manifest.NamedResource) {
	r.RequireData(id)
	r.input(id).ordering = true
}

// AssessOrdering records that an empty ordering footprint is authoritative.
func (r *ReadSet) AssessOrdering() { r.orderingAssessed = true }

// OrderingAssessed distinguishes an empty assessment from one still in preparation.
func (r *ReadSet) OrderingAssessed() bool { return r != nil && r.orderingAssessed }

// RequiresOrdering identifies ordering among retained required inputs.
func (r *ReadSet) RequiresOrdering(id manifest.NamedResource) bool {
	input := r.findInput(id)
	return input != nil && input.ordering
}

// Ordering yields the effective declaration dependencies.
func (r *ReadSet) Ordering() iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		for input := r.inputs; input != nil; input = input.next {
			if input.ordering && !yield(input.Ref.NamedResource) {
				return
			}
		}
	}
}

// RequireQualification retains demand without authorizing an ordinary producer body.
func (r *ReadSet) RequireQualification(id manifest.NamedResource) {
	input := r.input(id)
	if !input.required {
		input.required, input.qualification = true, true
	}
}

// CarryWarnings MUST replace prior diagnostics, including an empty projection.
//
//nolint:gocritic // The SDK preserves this value input and its snapshot boundary.
func (r *ReadSet) CarryWarnings(previous RenderedOutput) {
	current := r.warnings
	{
		warningValues := previous.warnings
		for warningIndex := range warningValues {
			warning := warningValues[warningIndex]
			if warning.Category == manifest.WarnOmittedValues ||
				warning.Category == manifest.WarnUnresolvedSubstitution {
				continue
			}
			if slices.ContainsFunc(current, func(fresh manifest.Warning) bool {
				return fresh.Resource == warning.Resource && fresh.Category == warning.Category
			}) {
				continue
			}
			r.AddWarning(warning)
		}
	}
}

// AddWarning retains a render advisory in this execution's accepted bundle.
//
//nolint:gocritic // The SDK preserves this value input and its snapshot boundary.
func (r *ReadSet) AddWarning(warning manifest.Warning) {
	warning.Detail = slices.Clone(warning.Detail)
	warning.Related = slices.Clone(warning.Related)
	r.warnings = append(r.warnings, warning)
}

// DeclarationProducers retains observed omission without authorizing indexed bytes.
func (r *ReadSet) DeclarationProducers(id manifest.NamedResource) []manifest.NamedResource {
	result := productionResult{reads: r, captureTarget: true, retainOmitted: true, watch: true}
	r.store.visitProductions(id, nil, &result)
	var owners []manifest.NamedResource
	for i := range result.out {
		owner := r.Materializer(id, result.out[i])
		if owner != (manifest.NamedResource{}) && owner != id && !slices.Contains(owners, owner) {
			owners = append(owners, owner)
		}
	}
	return owners
}
