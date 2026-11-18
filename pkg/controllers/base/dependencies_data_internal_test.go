package base

import (
	"context"
	"errors"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

func TestAwaitData_AllDependencyFailures(t *testing.T) {
	t.Parallel()
	first := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "first-vars"}
	second := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "second-vars"}
	absent := manifest.DiagnosticReason{
		Subject: manifest.DiagnosticFor(first, nil),
		Message: "not found; supply a readable document",
	}
	withProducers := absent
	for _, name := range []string{"first", "second", "third"} {
		producer := &manifest.Kustomization{Name: name, Namespace: "apps"}
		withProducers.Causes = append(withProducers.Causes, manifest.DiagnosticReason{
			Subject: manifest.DiagnosticFor(producer.Named(), producer), Message: "not ready",
		})
	}
	for _, tc := range []struct {
		name        string
		names       []string
		producers   [][]string
		wantIDs     []manifest.NamedResource
		wantReasons map[manifest.NamedResource]manifest.DiagnosticReason
	}{
		{
			name: "single", names: []string{"first-vars"},
			wantIDs: []manifest.NamedResource{first}, wantReasons: map[manifest.NamedResource]manifest.DiagnosticReason{first: absent},
		},
		{
			name: "several and duplicate", names: []string{"second-vars", "first-vars", "second-vars"},
			wantIDs: []manifest.NamedResource{first, second},
			wantReasons: map[manifest.NamedResource]manifest.DiagnosticReason{
				first: absent, second: {Subject: manifest.DiagnosticFor(second, nil), Message: "not found; supply a readable document"},
			},
		},
		{
			name: "overlapping forward", names: []string{"first-vars", "first-vars"},
			producers: [][]string{{"second", "first"}, {"third", "second"}},
			wantIDs:   []manifest.NamedResource{first}, wantReasons: map[manifest.NamedResource]manifest.DiagnosticReason{first: withProducers},
		},
		{
			name: "overlapping reverse", names: []string{"first-vars", "first-vars"},
			producers: [][]string{{"third", "second"}, {"second", "first"}},
			wantIDs:   []manifest.NamedResource{first}, wantReasons: map[manifest.NamedResource]manifest.DiagnosticReason{first: withProducers},
		},
	} {
		ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{}}
		hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
		for _, name := range tc.names {
			ks.PostBuild.SubstituteFrom = append(ks.PostBuild.SubstituteFrom, kustomizev1.SubstituteReference{
				Kind: manifest.KindConfigMap, Name: name,
			})
			hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindConfigMap, Name: name})
		}
		for _, owner := range []manifest.BaseManifest{ks, hr} {
			t.Run(owner.Named().Kind+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				st := store.New()
				st.AddObject(owner)
				var e Execution
				e.Init(New(st, task.NewBounded(2)), owner.Named())
				defer e.Close()
				inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads))
				assert.NoError(t, err)
				assert.Equal(t, len(inputs), len(tc.names))
				for i, producers := range tc.producers {
					for _, name := range producers {
						producer := &manifest.Kustomization{Name: name, Namespace: "apps"}
						st.AddObject(producer)
						st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
						inputs[i].Input.Materializers = append(inputs[i].Input.Materializers, producer.Named())
					}
				}
				resumed := false
				err = e.AwaitData(t.Context(), DataWait{
					Inputs: inputs,
					Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
						resumed = true
						return nil
					},
				}, manifest.DrainForce)
				assert.Equal(t, resumed, false)
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, errors.Is(err, manifest.ErrObjectNotFound), true)
				failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
				assert.FatalIf(t, !ok, "expected dependency failure, got %v", err)
				assert.Equal(t, failed.Parent, owner.Named())
				assert.Diff(t, failed.Subject, manifest.DiagnosticFor(owner.Named(), owner))
				assert.Diff(t, failed.Failed, tc.wantIDs)
				assert.Equal(t, len(failed.Reasons), len(tc.wantIDs))
				assert.Diff(t, failed.Reasons, tc.wantReasons)
				if tc.name == "several and duplicate" {
					assert.Equal(
						t,
						failed.Error(),
						"dependencies failed: ConfigMap/apps/first-vars: not found; supply a readable document; ConfigMap/apps/second-vars: not found; supply a readable document",
					)
				}
			})
		}
	}
}
