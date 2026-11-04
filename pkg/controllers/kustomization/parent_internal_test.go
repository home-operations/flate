package kustomization

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_OptionalMaterializerAbsence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		status   store.Status
		required bool
	}{
		{name: "failed optional", status: store.StatusFailed},
		{name: "exhausted optional", status: store.StatusPending},
		{name: "required cause only", status: store.StatusFailed, required: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testReconcileOptionalMaterializerAbsenceCase(t, &tc)
		},
		)
	}
}

func TestReconcile_OwnerDoesNotEraseReadyExpr(t *testing.T) {
	t.Parallel()
	for _, expr := range []string{"false", "true"} {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()
			c, s, _ := newControllerWithFixture(t)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system"}
			child := &manifest.Kustomization{
				Name: "child", Namespace: owner.Namespace, Path: "./apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: owner.Name, ReadyExpr: expr}},
			}
			s.AddObject(owner)
			controllertest.Emit(
				t.Context(),
				c.Controller,
				owner.Named(),
				&store.Emission{Children: []store.Child{{Obj: child}}, Complete: true},
			)
			s.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			info := controllertest.DispatchToFixpoint(
				t.Context(),
				t,
				controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: child.Named(), ReconcileNode: c.ReconcileNode},
			)
			output, _ := s.GetArtifact(child.Named(), store.ArtifactContent).(*store.Emission)
			rendered := s.GetObject(
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "hello"},
			)
			if expr == "false" {
				assert.FatalIf(t, info.Status != store.StatusFailed || !info.IsDependencyFailure() ||
					!slices.Equal(info.BlockedBy(), []manifest.NamedResource{owner.Named()}) ||
					output != nil && len(output.Manifests) != 0 ||
					rendered != nil, "false owner gate permitted output: status=%+v artifact=%+v rendered=%v",
					info,
					output,
					rendered,
				)
			} else {
				assert.FatalIf(
					t,
					info.Status != store.StatusReady || output == nil || len(output.Manifests) != 1 || rendered == nil,
					"true owner gate prevented output: status=%+v artifact=%+v rendered=%v",
					info,
					output,
					rendered,
				)
			}
		})
	}
}

func testReconcileOptionalMaterializerAbsenceCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		status   store.Status
		required bool
	},
) {
	t.Helper()

	c, s, _ := newControllerWithFixture(t)
	producer := &manifest.Kustomization{Name: "optional-owner", Namespace: "flux-system"}
	s.AddObject(producer)
	s.UpdateStatus(
		producer.Named(),
		store.StatusInfo{Status: testCaseInput.status, Reason: "", Message: "optional materializer unavailable"},
	)
	optional := manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: producer.Namespace,
		Name:      "optional",
	}
	required := manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: producer.Namespace,
		Name:      "required",
	}
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			optional: {Productions: []manifest.Production{{Owner: producer.Named()}}},
		},
	}
	c.Store.SetDiscoveryIndex(index)
	ks := &manifest.Kustomization{
		Name:      "reader",
		Namespace: producer.Namespace,
		Path:      "./apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: optional.Kind, Name: optional.Name, Optional: true},
			},
		},
	}
	if testCaseInput.required {
		ks.PostBuild.SubstituteFrom = append(
			ks.PostBuild.SubstituteFrom,
			manifest.SubstituteReference{Kind: required.Kind, Name: required.Name},
		)
	}
	s.AddObject(ks)
	var assessment *base.Execution
	c.Tasks.Go(
		t.Context(),
		"reconcile",
		func(ctx context.Context) { assessment = c.ReconcileNode(ctx, ks.Named(), manifest.DrainNone) },
	)
	c.Tasks.BlockTillDone()
	defer assessment.Close()
	if testCaseInput.status == store.StatusPending || testCaseInput.required {
		assert.FatalIf(
			t,
			assessment.Outcome() != schedule.Blocked ||
				s.GetArtifact(ks.Named(), store.ArtifactContent) != nil,
			"%v",
			"reader did not wait for its unresolved input",
		)
		c.Tasks.Go(
			t.Context(),
			"settle",
			func(ctx context.Context) { assessment.Settle(ctx, manifest.DrainForce) },
		)
		c.Tasks.BlockTillDone()
	}
	info, _ := s.GetStatus(ks.Named())
	if testCaseInput.required {
		if info.Status != store.StatusFailed ||
			!slices.Equal(info.BlockedBy(), []manifest.NamedResource{required}) {
			t.Fatalf("required diagnostic causes=%+v blocked=%v", info, info.BlockedBy())
		}
	} else {
		art, _ := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
		assert.FatalIf(
			t,
			info.Status != store.StatusReady || art == nil || len(art.Manifests) != 1,
			"absent optional input prevented render: status=%+v artifact=%+v",
			info,
			art,
		)
	}
	for id := range assessment.Reads.Required() {
		assert.FatalIf(
			t,
			id == optional || id == producer.Named(),
			"optional input entered required footprint: %s",
			id,
		)
	}
}
