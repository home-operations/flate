package depwait

import (
	"context"
	"iter"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

// Prediction assesses captured references without promotion, publication, or execution authority.
func (w *Waiter) Prediction(
	ctx context.Context,
	reads *store.ReadSet,
	references iter.Seq[manifest.Reference],
) error {
	assessment := *w
	assessment.Existence = nil
	for ref := range references {
		if ref.Optional {
			continue
		}
		switch ref.Use {
		case "materializer":
			reads.RequireMaterializer(ref.NamedResource)
		case "substituteFrom", "valuesFrom":
			input := reads.ReferenceFacts(ref.NamedResource, nil, w.Parent)
			if w.Missing.Admission(&ref, &input) == selection.RequiredInput {
				reads.Require(ref.DependencyRef)
			}
		default:
			if ref.Gate != 0 {
				reads.Require(ref.DependencyRef)
			}
		}
	}
	return assessment.Assess(ctx, reads, manifest.DrainForce)
}
