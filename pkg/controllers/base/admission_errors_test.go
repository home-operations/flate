package base

import (
	"context"
	"errors"
	"fmt"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

type admissionFailure struct{ cause error }

func (admissionFailure) IsFileIndexed(manifest.NamedResource) bool { return true }
func (p admissionFailure) Promote(context.Context, *store.ReadSet, manifest.NamedResource) (bool, error) {
	return false, p.cause
}

type countedDataLookup struct{ calls int }

func (p *countedDataLookup) Lookup(manifest.NamedResource) values.Input {
	p.calls++
	return values.Input{}
}

func TestDataInputs_AdmissionErrorPrecedesMissingPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		object       manifest.BaseManifest
		allowMissing bool
	}{
		{"helm_optional", &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "input", Optional: true}}}, false},
		{"helm_allow_missing", &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "input"}}}, true},
		{"substitute_optional", &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "input", Optional: true}}}}, false},
		{"substitute_secret", &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "input"}}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cause := errors.New("exact occurrence unavailable")
			st := store.New()
			st.AddObject(tc.object)
			c := New(st, nil)
			var missingKinds []string
			if tc.allowMissing {
				missingKinds = []string{manifest.KindSecret}
			}
			c.Configure(Options{Existence: admissionFailure{fmt.Errorf("%w: %w", manifest.ErrInput, cause)}, AllowMissingKinds: missingKinds})
			var execution Execution
			execution.Init(c, tc.object.Named())
			defer execution.Close()
			provider := &countedDataLookup{}
			inputs, err := execution.DataInputs(t.Context(), tc.object, provider)
			assert.Equal(t, errors.Is(err, cause), true)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, len(inputs), 0)
			assert.Equal(t, provider.calls, 0)
			assert.Equal(t, len(execution.Unreadable), 0)
		})
	}
}

func TestAwaitData_AdmissionErrorPreventsResume(t *testing.T) {
	t.Parallel()
	for _, materializer := range []bool{false, true} {
		t.Run(fmt.Sprintf("materializer=%t", materializer), func(t *testing.T) {
			t.Parallel()
			cause := fmt.Errorf("%w: exact input could not be read", manifest.ErrInput)
			st := store.New()
			owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			st.AddObject(owner)
			c := New(st, nil)
			c.Configure(Options{Existence: admissionFailure{cause}})
			var execution Execution
			execution.Init(c, owner.Named())
			defer execution.Close()
			input := values.Input{Offline: true}
			if materializer {
				input.Materializers = []manifest.NamedResource{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "producer"}}
			}
			resumed := false
			err := execution.AwaitData(t.Context(), DataWait{
				Inputs: []depwait.DataDependency{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input", Input: input, Optional: true, AllowMissing: true}},
				Resume: func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error { resumed = true; return nil },
			}, manifest.DrainForce)
			assert.Equal(t, errors.Is(err, cause), true)
			assert.Equal(t, resumed, false)
		})
	}
}
