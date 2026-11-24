package depwait

import (
	"context"
	"iter"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Prediction assesses captured references without promotion, publication, or execution authority.
func (w *Waiter) Prediction(
	ctx context.Context,
	reads *store.ReadSet,
	references iter.Seq[manifest.Reference],
	index *manifest.DiscoveryIndex,
) error {
	assessment := *w
	assessment.Existence = nil
	if assessment.Decision.EvaluationOnly(w.Parent) {
		assessment.orderingParent = assessment.Decision.OrderingDeclaration(reads, w.Parent, true)
	}
	for _, stage := range [...]manifest.GateStage{manifest.GateParents, manifest.GateInputs, manifest.GateArtifacts} {
		if stage == manifest.GateArtifacts {
			if err := assessment.predictData(ctx, reads, references, index); err != nil {
				return err
			}
		}
		for _, dep := range SelectGates(references, stage, 0, reads) {
			if stage == manifest.GateParents {
				reads.RequireMaterializer(dep.NamedResource)
			} else {
				reads.Require(dep)
			}
		}
		if err := assessment.Assess(ctx, reads, manifest.DrainForce, AssessGates); err != nil {
			return err
		}
	}
	return nil
}

func (w *Waiter) predictData(
	ctx context.Context,
	reads *store.ReadSet,
	references iter.Seq[manifest.Reference],
	index *manifest.DiscoveryIndex,
) error {
	var data DataAssessment
	for ref := range DataReferences(references) {
		data.Inputs = append(data.Inputs, DataDependency{
			Reference: ref, Input: reads.ReferenceFacts(ref.NamedResource, index, w.Parent),
		})
	}
	if err := data.Settle(ctx, w, reads, manifest.DrainForce); err != nil {
		return err
	}
	if len(data.Blocked) != 0 {
		return &BlockedError{Deps: data.Blocked}
	}
	return nil
}
