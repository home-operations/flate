package store

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_ReceiptOnly(t *testing.T) {
	t.Parallel()
	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	st.AddObject(owner)
	child := emissionChild("output", "same")
	next := Emission{
		Children:  []Child{child},
		Manifests: testutil.DocumentOccurrences([]map[string]any{child.Doc}),
		Complete:  true,
		Render:    manifest.NewRenderReceipt("first", func(a, b any) bool { return a == b }),
	}
	st.RecordEmitted(owner.Named(), &next, nil)
	var observer ReadSet
	observer.Init(st, ownerID("observer"), nil)
	defer observer.Retire()
	before := observer.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	observer.GetObject(child.Obj.Named())
	var objects, artifacts atomic.Int32
	stopObjects := st.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { objects.Add(1) }, false)
	defer stopObjects()
	stopArtifacts := st.AddListener(EventArtifactUpdated, func(manifest.NamedResource, any) { artifacts.Add(1) }, false)
	defer stopArtifacts()
	next.Render = manifest.NewRenderReceipt("second", func(a, b any) bool { return a == b })
	st.RecordEmitted(owner.Named(), &next, nil)
	after := st.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	{
		assert.FatalIf(
			t,
			before == after || before.revision != after.revision || !observer.Valid() || objects.Load() != 0 ||
				artifacts.Load() != 0,
			"%s",
			"receipt-only replacement changed semantic publication",
		)
	}
	assert.Equal(t, before.Render.Value().(string), "first")
	assert.Equal(t, after.Render.Value().(string), "second")
	assert.Equal(t, next.Render.Value().(string), "second")
	{
		assert.FatalIf(t, next.Footprint() == nil, "%s", "receipt-only replacement lost the accepted caller handoff")
	}
}

func TestRecordEmitted_ConflictingProducers_SyntheticOwners(t *testing.T) {
	t.Parallel()
	for _, static := range []bool{false, true} {
		t.Run(fmt.Sprintf("static=%t", static), func(t *testing.T) {
			t.Parallel()
			testRecordEmittedConflictingProducersSyntheticOwnersCase(
				t,
				&static,
			)
		},
		)
	}
}

func testRecordEmittedConflictingProducersSyntheticOwnersCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	s := New()
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	first := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
	second := first.WithNamespace("other")
	s.AddObject(first)
	s.AddObject(second)
	target := &manifest.ConfigMap{Name: "target", Namespace: "apps"}
	if *testCaseInput {
		s.AddObject(target)
	}
	owners := []manifest.NamedResource{first.Named(), second.Named()}
	for i, owner := range owners {
		child := &manifest.ConfigMap{
			Name:      target.Name,
			Namespace: target.Namespace,
			Data:      map[string]any{"value": strconv.Itoa(i)},
		}
		s.RecordEmitted(owner, &Emission{Children: []Child{{Obj: child}}, Complete: true}, nil)
	}
	if *testCaseInput {
		s.AddObject(s.GetObject(target.Named()))
	}
	warnings := s.Warnings()
	{
		assert.FatalIf(t, len(warnings) != 1, "warnings=%+v", warnings)
	}
	warning := warnings[0]
	{
		assert.FatalIf(
			t,
			warning.Count != 1 || warning.Resource != target.Named() || warning.Subject.Resource != target.Named() ||
				len(warning.Detail) != 2 ||
				warning.Detail[0] != chart.Named().String() ||
				warning.Detail[1] != chart.Named().String(),
			"conflict display=%+v",
			warning,
		)
	}
	{
		assert.FatalIf(t, len(warning.Related) < 2 || warning.Related[0] != owners[0] || warning.Related[1] != owners[1], "raw contributors=%v", warning.Related)
	}
	winner := chart.Named().String()
	if *testCaseInput {
		winner = "static object"
	}
	{
		assert.FatalIf(t, warning.Message != "conflicting producers; using "+winner, "winner=%+v", warning)
	}
}
