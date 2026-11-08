package store

import (
	"maps"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadSet_QualificationSettlementDemand(t *testing.T) {
	t.Parallel()
	st := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	input := &manifest.ConfigMap{Name: "content", Namespace: "apps", Data: map[string]any{"value": "same"}}
	st.AddObjects([]manifest.BaseManifest{producer, input})
	st.SetIncluded(producer.Named(), true, false)
	var settled ReadSet
	settled.Init(st, producer.Named(), nil)
	defer settled.Close()
	settled.GetObject(input.Named())
	assert.Equal(t, settled.OrdinaryDemand(producer.Named()), false)
	settled.Accept(AssessmentQualificationSettled)
	var frontier ReadSet
	frontier.Init(st, ownerID("frontier"), nil)
	defer frontier.Retire()
	assert.Equal(t, frontier.QualificationSettled(producer.Named()), true)
	assert.Equal(t, frontier.Valid(), true)
	var rendered ReadSet
	rendered.Init(st, producer.Named(), nil)
	defer rendered.Close()
	rendered.GetObject(producer.Named())
	var late ReadSet
	late.Init(st, ownerID("late"), nil)
	defer late.Close()
	late.GetObject(producer.Named())
	st.SetIncluded(producer.Named(), true, true)
	assert.Equal(t, rendered.Valid(), true)
	assert.Equal(t, settled.Valid(), false)
	assert.Equal(t, frontier.Valid(), false)
	for range st.Sweep() {
	}
	assert.Equal(t, late.Valid(), true)
	assert.Equal(t, late.OrdinaryDemand(producer.Named()), false)
	assert.Equal(t, late.Valid(), false)
	assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{late.ID()})
	var fresh ReadSet
	fresh.Init(st, ownerID("fresh"), nil)
	defer fresh.Retire()
	assert.Equal(t, fresh.QualificationSettled(producer.Named()), false)
	assert.Equal(t, fresh.OrdinaryDemand(producer.Named()), true)
	assert.Equal(t, st.GetArtifact(producer.Named(), ArtifactContent), Artifact(nil))
	_, known := st.GetStatus(producer.Named())
	assert.Equal(t, known, false)
}

func TestReadSet_QualificationPurpose(t *testing.T) {
	t.Parallel()
	for _, demand := range []string{"qualification", "data", "readiness", "materializer"} {
		t.Run(demand, func(t *testing.T) {
			t.Parallel()
			st := New()
			id := ownerID("producer")
			var reads ReadSet
			reads.Init(st, ownerID("consumer"), nil)
			defer reads.Close()
			reads.RequireQualification(id)
			switch demand {
			case "data":
				reads.RequireData(id)
			case "readiness":
				reads.Require(manifest.DependencyRef{NamedResource: id})
			case "materializer":
				reads.RequireMaterializer(id)
			}
			reads.RequireQualification(id)
			assert.Diff(
				t,
				maps.Collect(reads.Required()),
				map[manifest.NamedResource]bool{id: demand == "qualification"},
			)
		})
	}
}

func TestReadSet_QualificationRejectsStaleCapture(t *testing.T) {
	t.Parallel()
	st := New()
	producer := &manifest.ResourceSet{Name: "producer", Namespace: "apps"}
	input := &manifest.ConfigMap{Name: "content", Namespace: "apps", Data: map[string]any{"value": "same"}}
	st.AddObjects([]manifest.BaseManifest{producer, input})
	st.SetIncluded(producer.Named(), true, false)
	var settled ReadSet
	settled.Init(st, producer.Named(), nil)
	defer settled.Close()
	settled.GetObject(input.Named())
	settled.OrdinaryDemand(producer.Named())
	settled.Accept(AssessmentQualificationSettled)
	st.AddObject(
		&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{"value": "replaced"}},
	)
	st.AddObject(input)
	var frontier ReadSet
	frontier.Init(st, ownerID("frontier"), nil)
	defer frontier.Close()
	assert.Equal(t, settled.Valid(), false)
	assert.Equal(t, frontier.Matches(&settled), true)
	assert.Equal(t, frontier.QualificationSettled(producer.Named()), false)
}
