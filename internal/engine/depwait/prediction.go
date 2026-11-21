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
	for _, stage := range [...]manifest.GateStage{manifest.GateParents, manifest.GateInputs, manifest.GateArtifacts} {
		if stage == manifest.GateArtifacts {
			var data DataAssessment
			for ref := range DataReferences(references) {
				data.Inputs = append(data.Inputs, DataDependency{
					Reference: ref, Input: reads.ReferenceFacts(ref.NamedResource, index, w.Parent),
				})
			}
			if err := data.Settle(ctx, &assessment, reads, manifest.DrainForce); err != nil {
				return err
			}
			if len(data.Blocked) != 0 {
				return &BlockedError{Deps: data.Blocked}
			}
		}
		for _, dep := range assessment.SelectGates(references, stage, 0) {
			if stage == manifest.GateParents {
				reads.RequireMaterializer(dep.NamedResource)
			} else {
				reads.Require(dep)
			}
		}
		if err := assessment.Assess(ctx, reads, manifest.DrainForce); err != nil {
			return err
		}
	}
	return nil
}
