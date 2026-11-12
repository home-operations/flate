package base

import (
	"context"
	"errors"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestDataInputs_ProbeRequiresPromotion(t *testing.T) {
	t.Parallel()
	for _, object := range []manifest.BaseManifest{
		&manifest.Kustomization{
			Name: "consumer", Namespace: "apps",
			PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "input"},
			}},
		},
		&manifest.HelmRelease{
			Name: "consumer", Namespace: "apps",
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "input"}},
		},
	} {
		t.Run(object.Named().Kind, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.AddObject(object)
			input := &manifest.ConfigMap{
				Name:      "input",
				Namespace: "apps",
				Data:      map[string]any{"VALUE": "same", "values.yaml": "{}"},
			}
			calls := 0
			c := New(st, nil)
			c.Configure(Options{Existence: &gatePromotion{promote: func(id manifest.NamedResource) bool {
				assert.Equal(t, id, input.Named())
				calls++
				st.AddObject(input)
				return true
			}}})
			var probe Execution
			probe.InitProbe(c, object)
			defer probe.Close()
			inputs, err := probe.DataInputs(t.Context(), object, values.NewStoreProvider(&probe.Reads))
			assert.Equal(t, err != nil, true)
			assert.Equal(t, len(inputs), 0)
			assert.Equal(t, calls, 0)
			assert.Equal(t, st.GetObject(input.Named()) == nil, true)
			_, statusExists := st.GetStatus(object.Named())
			assert.Equal(t, statusExists, false)
			assert.Equal(t, len(st.Warnings()), 0)
			assert.Equal(t, probe.Reads.Valid(), true)

			var normal Execution
			normal.Init(c, object.Named())
			defer normal.Close()
			inputs, err = normal.DataInputs(t.Context(), object, values.NewStoreProvider(&normal.Reads))
			assert.NoError(t, err)
			assert.Equal(t, len(inputs), 1)
			assert.Equal(t, inputs[0].Input.Data, manifest.Data(input))
			assert.Equal(t, calls, 1)
			assert.Equal(t, probe.Reads.Valid(), false)
			resumed := false
			assert.NoError(t, normal.AwaitData(t.Context(), DataWait{
				Inputs: inputs,
				Resume: func(context.Context, manifest.DrainLevel, []depwait.DataDependency) error {
					resumed = true
					return nil
				},
			}, manifest.DrainNone))
			assert.Equal(t, resumed, true)
			probe.Close()
			probe.Init(c, object.Named())
			inputs, err = probe.DataInputs(t.Context(), object, values.NewStoreProvider(&probe.Reads))
			assert.NoError(t, err)
			assert.Equal(t, len(inputs), 1)
			assert.Equal(t, calls, 2)
		})
	}
}

func TestAwaitData_ProbeTracksMaterializerEvidence(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{
		Name: "consumer", Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "input", Optional: true},
		}},
	}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	source := (&manifest.GitRepository{Name: "repo", Namespace: "apps"}).Named()
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{owner, producer})
	st.SetArtifact(source, &store.SourceArtifact{Content: "before"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input: {Productions: []manifest.Production{{Owner: producer.Named(), Source: source}}},
	}})
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
	c := New(st, nil)
	assess := func() (*Execution, error) {
		e := new(Execution)
		e.InitProbe(c, owner)
		t.Cleanup(e.Close)
		inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads))
		assert.NoError(t, err)
		err = e.AwaitData(t.Context(), DataWait{
			Inputs: inputs,
			Resume: func(_ context.Context, _ manifest.DrainLevel, omitted []depwait.DataDependency) error {
				assert.Equal(t, len(omitted), 1)
				assert.Equal(t, omitted[0].NamedResource, input)
				return nil
			},
		}, manifest.DrainNone)
		return e, err
	}
	first, err := assess()
	blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
	assert.Equal(t, ok, true)
	assert.Diff(t, blocked.Deps, []manifest.NamedResource{producer.Named()})
	assert.Equal(t, first.Reads.Valid(), true)
	st.SetArtifact(source, &store.SourceArtifact{Content: "after"})
	assert.Equal(t, first.Reads.Valid(), false)

	second, err := assess()
	_, ok = errors.AsType[*depwait.ErrBlocked](err)
	assert.Equal(t, ok, true)
	assert.Equal(t, second.Reads.Valid(), true)
	st.RecordEmitted(producer.Named(), &store.Emission{Complete: true}, nil)
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
	assert.Equal(t, second.Reads.Valid(), false)
	settled, err := assess()
	assert.NoError(t, err)
	assert.Equal(t, settled.Reads.Valid(), true)
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
	assert.Equal(t, settled.Reads.Valid(), false)
	_, err = assess()
	blocked, ok = errors.AsType[*depwait.ErrBlocked](err)
	assert.Equal(t, ok, true)
	assert.Diff(t, blocked.Deps, []manifest.NamedResource{producer.Named()})
	_, statusExists := st.GetStatus(owner.Named())
	assert.Equal(t, statusExists, false)
	assert.Equal(t, len(st.Warnings()), 0)
	assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent) == nil, true)
}

func TestDataInputs_PreflightPrecedesCapture(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "input", Optional: true}},
	}}
	st.AddObject(owner)
	st.SetPreflight(owner.Named(), "captured admission failure")
	var e Execution
	e.Init(New(st, nil), owner.Named())
	defer e.Close()
	inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads))
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, len(inputs), 0)
	_, statusKnown := st.GetStatus(owner.Named())
	assert.Equal(t, statusKnown, false)
	st.AddObject(&manifest.ConfigMap{Name: "input", Namespace: owner.Namespace})
	assert.Equal(t, e.Reads.Valid(), true)
}

func TestDataInputs_ProbeRequiresRawProductionMaterialization(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "input", Optional: true}},
	}}
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input", Namespace: owner.Namespace}
	st.AddObject(owner)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input: {Productions: []manifest.Production{{Path: "input.yaml", Source: manifest.BootstrapSourceID}}},
	}})
	var e Execution
	e.InitProbe(New(st, nil), owner)
	defer e.Close()
	inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads))
	assert.Equal(t, errors.Is(err, errIncompleteHistory), true)
	assert.Equal(t, len(inputs), 0)
	assert.Equal(t, st.GetObject(input) == nil, true)
	_, statusKnown := st.GetStatus(owner.Named())
	assert.Equal(t, statusKnown, false)
}

func TestAwaitData_ReadableSupplyDoesNotRequireProducer(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{owner, producer})
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
	secret := &manifest.Secret{Name: "input", Namespace: "apps", StringData: map[string]any{"value": "supplied"}}
	st.AddDeclaration(store.Declaration{Obj: secret, Origin: manifest.SuppliedDeclaration})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		secret.Named(): {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}})
	var execution Execution
	execution.Init(New(st, nil), owner.Named())
	defer execution.Close()
	facts := execution.Reads.ReferenceFacts(secret.Named(), nil, owner.Named())
	assert.Diff(t, facts.Materializers, []manifest.NamedResource{producer.Named()})
	resumed := false
	assert.NoError(t, execution.AwaitData(t.Context(), DataWait{
		Inputs: []depwait.DataDependency{
			{NamedResource: secret.Named(), Key: "value", Use: "valuesFrom", Input: facts},
		},
		Resume: func(context.Context, manifest.DrainLevel, []depwait.DataDependency) error { resumed = true; return nil },
	}, manifest.DrainNone))
	assert.Equal(t, resumed, true)
	for required := range execution.Reads.Required() {
		assert.Equal(t, required == producer.Named(), false)
	}
}
