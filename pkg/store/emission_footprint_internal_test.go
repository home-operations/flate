package store

import (
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_AcceptedFootprint(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"complete", "empty", "partial", "partial_reuse"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedAcceptedFootprintCase(t, &state)
		},
		)
	}
}

func testRecordEmittedAcceptedFootprintCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	st.AddObject(owner)
	emitterSource := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "sources",
		Name:      "emitter",
	}
	child := &manifest.Kustomization{
		Name:      "child",
		Namespace: "apps",
		Path:      "apps/red",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "content",
			Namespace: "sources",
		},
	}
	declaration := &manifest.RawObject{
		APIVersion: "objectbucket.io/v1alpha1",
		Kind:       "ObjectBucketClaim",
		Name:       "bucket",
		Namespace:  "apps",
	}
	covered := emissionChild("prediction", "raw")
	st.AddObject(covered.Obj)
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				covered.Obj.Named(): {
					Productions: []manifest.Production{{Owner: owner.Named(), Source: emitterSource}},
				},
			},
		},
	)
	_, _, next := st.GetEmission(owner.Named())
	defer next.Finish()
	next.Children, next.Render, next.Complete = []Child{
		{Obj: child},
		{Obj: declaration},
	}, manifest.NewRenderReceipt(
		"receipt",
		func(a, b any) bool { return a == b },
	), (*testCaseInput) != "partial"
	next.binding = sourceBinding{
		source:   emitterSource,
		artifact: &SourceArtifact{Key: "generation", Content: "generation"},
	}
	{
		assert.FatalIf(t, len(next.coverage) == 0, "%s", "fixture has no withdrawal coverage")
	}
	if (*testCaseInput) == "empty" {
		next.Children = nil
	}
	{
		assert.FatalIf(t, next.Footprint() != nil, "%s", "unpublished attempt exposed a footprint")
	}
	st.RecordEmitted(owner.Named(), &next, nil)
	if (*testCaseInput) == "partial_reuse" {
		next.Complete = false
		{
			assert.FatalIf(t, next.Footprint() != nil, "%s", "reused partial caller exposed complete history")
		}
		st.RecordEmitted(owner.Named(), &next, nil)
	}
	footprint := next.Footprint()
	if (*testCaseInput) == "partial" || (*testCaseInput) == "partial_reuse" {
		{
			assert.FatalIf(t, footprint != nil, "%s", "partial output exposed accepted history")
		}
		return
	}
	{
		assert.FatalIf(t, footprint == nil, "%s", "complete acceptance did not expose a footprint")
	}
	st.RecordEmitted(owner.Named(), &Emission{Complete: true, binding: next.binding}, nil)
	var ids []manifest.NamedResource
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{}
	//nolint:gocritic // Footprint's public iterator yields record values and cannot be indexed.
	for id, record := range footprint {
		ids = append(ids, id)
		records[id] = record
		{
			assert.FatalIf(t, id == covered.Obj.Named(), "%s", "withdrawal coverage became actual history")
		}
		{
			assert.FatalIf(
				t,
				record.Authored != (manifest.NamedResource{}) || record.Path != "" || record.File != "",
				"%s",
				"generated content acquired raw-file provenance",
			)
		}
	}
	{
		assert.FatalIf(
			t,
			!slices.IsSortedFunc(ids, manifest.NamedResource.Compare),
			"%s",
			"footprint identity order is unstable",
		)
	}
	if (*testCaseInput) == "empty" {
		assert.Equal(t, len(records), 0)
		return
	}
	assert.Equal(t, len(records), 4)
	assert.Diff(
		t,
		records[child.Named()].ContentRoots,
		map[manifest.NamedResource][]string{child.SourceID(): {"apps/red"}},
	)
	assert.Diff(t, records[child.Named()].Refs, []manifest.NamedResource{child.SourceID()})
	assert.Diff(
		t,
		records[child.Named()].Productions,
		[]manifest.Production{{Owner: owner.Named(), Source: emitterSource}},
	)
	secret := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Name:      declaration.Name,
		Namespace: declaration.Namespace,
	}
	assert.Diff(
		t,
		records[secret].Productions,
		[]manifest.Production{{Owner: owner.Named(), Declaration: declaration.Named(), Source: emitterSource}},
	)
	assert.Equal(t, next.Render.Value().(string), "receipt")
	{
		assert.FatalIf(
			t,
			st.GetArtifact(owner.Named(), ArtifactContent).(*Emission).Footprint() != nil,
			"%s",
			"stored inventory manufactured caller acceptance",
		)
	}
}
