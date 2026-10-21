package kustomization

import (
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"slices"
	"testing"
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
			c, s, _ := newControllerWithFixture(t)
			producer := &manifest.Kustomization{Name: "optional-owner", Namespace: "flux-system"}
			s.AddObject(producer)
			s.UpdateStatus(producer.Named(), store.StatusInfo{Status: tc.status, Reason: "", Message: "optional materializer unavailable"})
			optional := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: producer.Namespace, Name: "optional"}
			required := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: producer.Namespace, Name: "required"}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{optional: {Productions: []manifest.Production{{Owner: producer.Named()}}}}}
			c.Store.SetDiscoveryIndex(index)
			ks := &manifest.Kustomization{Name: "reader", Namespace: producer.Namespace, Path: "./apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: optional.Kind, Name: optional.Name, Optional: true}}}}
			if tc.required {
				ks.PostBuild.SubstituteFrom = append(ks.PostBuild.SubstituteFrom, manifest.SubstituteReference{Kind: required.Kind, Name: required.Name})
			}
			s.AddObject(ks)
			assessment := c.ReconcileNode(t.Context(), ks.Named(), manifest.DrainNone)
			defer assessment.Close()
			if tc.status == store.StatusPending || tc.required {
				if assessment.Outcome() != schedule.Blocked || s.GetArtifact(ks.Named()) != nil {
					t.Fatal("reader did not wait for its unresolved input")
				}
				assessment.Settle(t.Context(), manifest.DrainForce)
			}
			info, _ := s.GetStatus(ks.Named())
			if tc.required {
				if info.Status != store.StatusFailed || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{required}) {
					t.Fatalf("required diagnostic causes=%+v blocked=%v", info, info.BlockedBy())
				}
			} else {
				art, _ := s.GetArtifact(ks.Named()).(*store.Emission)
				if info.Status != store.StatusReady || art == nil || len(art.Manifests) != 1 {
					t.Fatalf("absent optional input prevented render: status=%+v artifact=%+v", info, art)
				}
			}
			for id := range assessment.Reads.Required() {
				if id == optional || id == producer.Named() {
					t.Fatalf("optional input entered required footprint: %s", id)
				}
			}
		})
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
			c.Emit(owner.Named(), &store.Emission{Children: []store.Child{{Obj: child}}, Complete: true})
			s.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: child.Named(), ReconcileNode: c.ReconcileNode})
			output, _ := s.GetArtifact(child.Named()).(*store.Emission)
			rendered := s.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "hello"})
			if expr == "false" {
				if info.Status != store.StatusFailed || !info.IsDependencyFailure() || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{owner.Named()}) || output != nil && len(output.Manifests) != 0 || rendered != nil {
					t.Fatalf("false owner gate permitted output: status=%+v artifact=%+v rendered=%v", info, output, rendered)
				}
			} else if info.Status != store.StatusReady || output == nil || len(output.Manifests) != 1 || rendered == nil {
				t.Fatalf("true owner gate prevented output: status=%+v artifact=%+v rendered=%v", info, output, rendered)
			}
		})
	}
}
