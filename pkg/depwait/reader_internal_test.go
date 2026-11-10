package depwait

import (
	"errors"
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

type structuralState struct{ stateReader }

func TestClassify_StructuralReaderPreflight(t *testing.T) {
	t.Parallel()
	s := store.New()
	parent := testutil.KustomizationID("apps", "consumer")
	s.SetPreflight(parent, "captured admission failure")
	var reads store.ReadSet
	reads.Init(s, parent, nil)
	defer reads.Retire()
	w := Waiter{
		Store: structuralState{&reads}, Parent: parent,
		Missing: NewMissingPolicy([]string{manifest.KindCustomResourceDefinition}),
	}
	got := w.Classify(t.Context(), manifest.DependencyRef{
		Group: "apiextensions.k8s.io", Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.test",
	}, manifest.DrainForce)
	assert.FatalIf(t, !errors.Is(got.Err, manifest.ErrInput), "classification = %+v", got)
	assert.Equal(t, got.Err.Error(), "captured admission failure")
}

func TestClassify_StructuralReaderPreservesAdmission(t *testing.T) {
	t.Parallel()
	s := store.New()
	parent := testutil.KustomizationID("apps", "consumer")
	data := &manifest.ConfigMap{Name: "input", Namespace: "apps"}
	var reads store.ReadSet
	reads.Init(s, parent, nil)
	defer reads.Retire()
	var preparation *selection.Preparation
	policy := NewMissingPolicy(nil)
	request := selection.PrepareRequest{
		Reads:       &reads,
		Preparation: &preparation,
		Owner:       parent,
		Invocation:  true,
		Missing:     policy,
	}
	promotion := &inputPromotion{store: s, data: data, materialize: true}
	w := Waiter{
		Store: structuralState{
			&reads,
		},
		Parent:    parent,
		Admission: request,
		Existence: promotion,
		Missing: NewMissingPolicy(
			nil),
	}
	s.SetPreflight(parent, "captured admission failure")
	reads.Retire()
	reads.Init(s, parent, nil)
	got := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: data.Named()}, manifest.DrainNone)
	assert.Equal(t, errors.Is(got.Err, manifest.ErrInput), true)
	assert.Equal(t, promotion.calls, 0)
	s.SetPreflight(parent, "")
	reads.Retire()
	reads.Init(s, parent, nil)
	got = w.Classify(t.Context(), manifest.DependencyRef{NamedResource: data.Named()}, manifest.DrainNone)
	assert.NoError(t, got.Err)
	assert.Equal(t, got.Kind, schedule.Terminal)
	assert.Equal(t, promotion.calls, 1)
	assert.Equal(t, promotion.request.Reads, request.Reads)
	assert.Equal(t, promotion.request.Preparation, request.Preparation)
	assert.Equal(t, promotion.request.Owner, request.Owner)
	assert.Equal(t, promotion.request.Invocation, request.Invocation)
	assert.Equal(t, promotion.request.Missing == policy, true)
}

func TestAvailable_StructuralReaderUnsettledProducers(t *testing.T) {
	t.Parallel()
	for _, level := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade, manifest.DrainForce} {
		t.Run(strconv.Itoa(int(level)), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			parent := testutil.KustomizationID("apps", "consumer")
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input", Namespace: "apps"}
			s.AddObject(producer)
			s.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				input: {Productions: []manifest.Production{{Owner: producer.Named()}}},
			}})
			var reads store.ReadSet
			reads.Init(s, parent, nil)
			defer reads.Retire()
			w := Waiter{
				Store: structuralState{&reads}, Parent: parent, Missing: NewMissingPolicy(
					nil),
			}
			got := w.Available(t.Context(), DataDependency{
				NamedResource: input, Use: "valuesFrom", Optional: true,
				Input: store.ReferenceFacts{Materializers: []manifest.NamedResource{producer.Named()}},
			}, level)
			assert.NoError(t, got.Err)
			if level < manifest.DrainForce {
				assert.Equal(t, got.Kind, schedule.Blocked)
				assert.Diff(t, got.Blocked, []manifest.NamedResource{producer.Named()})
				assert.Equal(t, got.Omit, false)
			} else {
				assert.Equal(t, got.Kind, schedule.Terminal)
				assert.Equal(t, got.Omit, true)
			}
		})
	}
}

func TestAvailable_StructuralReaderPreflightPrecedesOmission(t *testing.T) {
	t.Parallel()
	s := store.New()
	parent := testutil.KustomizationID("apps", "consumer")
	s.SetPreflight(parent, "captured admission failure")
	var reads store.ReadSet
	reads.Init(s, parent, nil)
	defer reads.Retire()
	w := Waiter{Store: structuralState{&reads}, Parent: parent, Missing: NewMissingPolicy(nil)}
	got := w.Available(t.Context(), DataDependency{
		Kind: manifest.KindSecret, Namespace: "apps", Name: "absent", Use: "valuesFrom", Optional: true,
	}, manifest.DrainNone)
	assert.Equal(t, errors.Is(got.Err, manifest.ErrInput), true)
	assert.Equal(t, got.Omit, false)
}
