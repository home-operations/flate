package depwait

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type errorPromotion struct{ cause error }

func (errorPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }
func (p errorPromotion) Promote(context.Context, selection.PrepareRequest, manifest.NamedResource) (bool, error) {
	return false, p.cause
}

func TestClassify_AdmissionErrorPrecedesAbsence(t *testing.T) {
	t.Parallel()
	cause := manifest.NewError(manifest.ErrInput, nil, "unavailable exact occurrence")
	w := Waiter{Store: store.New(), Existence: errorPromotion{cause}, AllowMissingCRDs: true}
	dep := manifest.DependencyRef{
		Group: "apiextensions.k8s.io",
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "examples.test",
	}
	for range 2 {
		{
			got := w.Classify(t.Context(), dep, manifest.DrainForce)
			assert.FatalIf(t, !errors.Is(got.Err, cause), "Classify error = %v, want %v", got.Err, cause)
		}
	}
}

func TestAvailable_AdmissionErrorPrecedesOptionalOmission(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "values", Namespace: "apps"}
	producer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "producer",
	}
	for _, materializer := range []bool{false, true} {
		t.Run(fmt.Sprintf("materializer=%t", materializer), func(t *testing.T) {
			t.Parallel()
			cause := manifest.NewError(manifest.ErrInput, nil, "admission failed")
			w := Waiter{Store: store.New(), Existence: errorPromotion{cause}}
			input := store.ReferenceFacts{Offline: true}
			if materializer {
				input.Materializers = []manifest.NamedResource{producer}
			}
			dep := DataDependency{NamedResource: id, Input: input, Optional: true, AllowMissing: true}
			got := w.Available(t.Context(), dep, manifest.DrainForce)
			assert.FatalIf(
				t,
				!errors.Is(got.Err, cause) || got.Omit,
				"Available = %+v, want original cause without omission",
				got,
			)
			w.Existence = errorPromotion{}
			got = w.Available(t.Context(), dep, manifest.DrainForce)
			assert.FatalIf(t, got.Err != nil || !got.Omit, "genuine absence = %+v, want optional omission", got)
		})
	}
}
