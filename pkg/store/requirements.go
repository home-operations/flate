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
	input.required = true
	if !input.gate {
		input.Ref.ReadyExpr, input.gate = ref.ReadyExpr, true
		input.Ref.Artifact, input.Ref.ArtifactOnly, input.Ref.ChartSource = ref.Artifact, ref.ArtifactOnly, ref.ChartSource
		input.materializer = materializer
		return
	}
	if sameReadinessGate(new(input.Ref), new(ref)) && input.materializer == materializer {
		return
	}
	for gate := &r.details(input).gates; ; gate = &(*gate).next {
		if *gate == nil {
			*gate = &readinessGate{
				expr:         ref.ReadyExpr,
				artifact:     ref.Artifact,
				artifactOnly: ref.ArtifactOnly,
				chartSource:  ref.ChartSource,
				materializer: materializer,
			}
			return
		}
		if sameReadinessGate(new(manifest.DependencyRef{
			NamedResource: ref.NamedResource,
			ReadyExpr:     (*gate).expr,
			Artifact:      (*gate).artifact,
			ArtifactOnly:  (*gate).artifactOnly,
			ChartSource:   (*gate).chartSource,
		}), new(ref),
		) &&
			(*gate).materializer == materializer {
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
				if !yield(
					Gate{
						NamedResource: input.Ref.NamedResource,
						ReadyExpr:     gate.expr,
						Artifact:      gate.artifact,
						ArtifactOnly:  gate.artifactOnly,
						ChartSource:   gate.chartSource,
						Materializer:  gate.materializer,
					},
				) {
					return
				}
			}
		}
	}
}

// Required enumerates the required inputs of the accepted execution.
func (r *ReadSet) Required() iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		for input := r.inputs; input != nil; input = input.next {
			if input.required && !yield(input.Ref.NamedResource) {
				return
			}
		}
	}
}

// RequireData records a required semantic input without imposing readiness.
func (r *ReadSet) RequireData(id manifest.NamedResource) { r.input(id).required = true }

// CarryWarnings reuses advisories after the caller verifies an equal consumed projection.
//
//nolint:gocritic // The SDK preserves this value input and its snapshot boundary.
func (r *ReadSet) CarryWarnings(previous RenderedOutput) {
	{
		warningValues := previous.warnings
		for warningIndex := range warningValues {
			warning := warningValues[warningIndex]
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
