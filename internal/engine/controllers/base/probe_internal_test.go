package base

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestExecution_ProbeProviderLifetime(t *testing.T) {
	t.Parallel()
	t.Run("successor capture", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		controller := New(st, nil)
		owner := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{
			{Kind: manifest.KindConfigMap, Name: "input", ValuesKey: "first"},
			{Kind: manifest.KindSecret, Name: "absent", Optional: true},
			{Kind: manifest.KindConfigMap, Name: "input", ValuesKey: "last"},
		}}
		input := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{
			"first": "a: first", "last": "a: last",
		}}
		for _, object := range []manifest.BaseManifest{owner, input} {
			st.AddObject(object)
		}
		var previous, next Execution
		assert.Equal(t, previous.data == nil, true)
		first, _, err := previous.Probe(t.Context(), controller, owner, schedule.Result{})
		assert.NoError(t, err)
		defer previous.Close()
		assert.Equal(t, first.Assessment, &previous)
		provider := previous.Data()
		assert.Equal(t, previous.Data(), provider)
		history, results, err := next.Probe(t.Context(), controller, owner, schedule.Result{Assessment: &previous})
		assert.NoError(t, err)
		defer next.Close()
		assert.Equal(t, history.Assessment, &next)
		assert.Equal(t, history.Complete, false)
		assert.Equal(t, next.Data() == provider, false)
		assert.Equal(t, next.Data(), next.data)
		assert.Equal(t, len(results), 3)
		for position := range results {
			assert.Equal(t, results[position].Position(), position)
		}
		assert.Equal(t, results[0].Effect(), values.Used)
		assert.Equal(t, results[0].Value(), "a: first")
		assert.Equal(t, results[1].Effect(), values.Omitted)
		assert.Equal(t, results[2].Effect(), values.Used)
		assert.Equal(t, results[2].Value(), "a: last")
		previous.Retire()
		assert.Equal(t, previous.data == nil, true)
		assert.Equal(t, next.Reads.Valid(), true)
		st.AddObject(&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{
			"first": "a: changed", "last": "a: changed",
		}})
		assert.Equal(t, next.Reads.Valid(), false)
		_, statusExists := st.GetStatus(owner.Named())
		assert.Equal(t, statusExists, false)
		assert.Equal(t, st.HasInventory(owner.Named()), false)
		assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent) == nil, true)
		assert.Equal(t, len(st.Warnings()), 0)
		next.Retire()
		assert.Equal(t, next.data == nil, true)
	})
	t.Run("collection error", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		owner := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{
			{Kind: manifest.KindConfigMap, Name: "input", Optional: true},
		}}
		input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input", Namespace: "apps"}
		st.AddObject(owner)
		st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			input: {Productions: []manifest.Production{{Path: "input.yaml", Source: manifest.BootstrapSourceID}}},
		}})
		var run Execution
		history, results, err := run.Probe(t.Context(), New(st, nil), owner, schedule.Result{})
		defer run.Close()
		assert.Equal(t, err, errIncompleteHistory)
		assert.Equal(t, history.Assessment, &run)
		assert.Equal(t, len(results), 0)
		assert.Equal(t, run.Reads.Valid(), true)
		assert.Equal(t, st.GetObject(input) == nil, true)
		_, statusExists := st.GetStatus(owner.Named())
		assert.Equal(t, statusExists, false)
	})
	t.Run("waiting error", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		owner := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{
			{Kind: manifest.KindConfigMap, Name: "absent"},
		}}
		st.AddObject(owner)
		var run Execution
		history, results, err := run.Probe(t.Context(), New(st, nil), owner, schedule.Result{})
		defer run.Close()
		assert.FatalIf(t, err == nil, "%v", "waiting accepted a missing required input")
		pending, ok := errors.AsType[*pendingDataError](err)
		assert.FatalIf(t, !ok, "waiting error was reclassified: %T", err)
		assert.Equal[error](t, err, pending)
		assert.Equal(t, pending.execution, &run)
		assert.Equal(t, len(pending.assessment.Inputs), 1)
		assert.Equal(t, history.Assessment, &run)
		assert.Equal(t, len(results), 0)
		assert.Equal(t, run.Reads.Valid(), true)
		_, statusExists := st.GetStatus(owner.Named())
		assert.Equal(t, statusExists, false)
		assert.Equal(t, len(st.Warnings()), 0)
	})
}
