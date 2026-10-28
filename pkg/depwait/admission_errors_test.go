package depwait

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

type errorPromotion struct{ cause error }

func (errorPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }
func (p errorPromotion) Promote(context.Context, *store.ReadSet, manifest.NamedResource) (bool, error) {
	return false, p.cause
}

func TestClassify_AdmissionErrorPrecedesAbsence(t *testing.T) {
	t.Parallel()
	cause := fmt.Errorf("%w: unavailable exact occurrence", manifest.ErrInput)
	w := Waiter{Store: store.New(), Existence: errorPromotion{cause}, AllowMissingCRDs: true}
	dep := manifest.DependencyRef{Group: "apiextensions.k8s.io", Kind: manifest.KindCustomResourceDefinition, Name: "examples.test"}
	for range 2 {
		if got := w.Classify(t.Context(), dep, manifest.DrainForce); !errors.Is(got.Err, cause) {
			t.Fatalf("Classify error = %v, want %v", got.Err, cause)
		}
	}
}

func TestAvailable_AdmissionErrorPrecedesOptionalOmission(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "values", Namespace: "apps"}
	producer := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "producer"}
	for _, materializer := range []bool{false, true} {
		t.Run(fmt.Sprintf("materializer=%t", materializer), func(t *testing.T) {
			t.Parallel()
			cause := fmt.Errorf("%w: admission failed", manifest.ErrInput)
			w := Waiter{Store: store.New(), Existence: errorPromotion{cause}}
			input := values.Input{Offline: true}
			if materializer {
				input.Materializers = []manifest.NamedResource{producer}
			}
			dep := DataDependency{NamedResource: id, Input: input, Optional: true, AllowMissing: true}
			got := w.Available(t.Context(), dep, manifest.DrainForce)
			if !errors.Is(got.Err, cause) || got.Omit {
				t.Fatalf("Available = %+v, want original cause without omission", got)
			}
			w.Existence = errorPromotion{}
			got = w.Available(t.Context(), dep, manifest.DrainForce)
			if got.Err != nil || !got.Omit {
				t.Fatalf("genuine absence = %+v, want optional omission", got)
			}
		})
	}
}
