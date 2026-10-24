package kustomization

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_SelfMetadataConverges(t *testing.T) {
	t.Parallel()
	for _, rerender := range []bool{false, true} {
		name := "initial"
		if rerender {
			name = "rerender"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, s, _ := newControllerWithFixture(t)
			dep := &manifest.Kustomization{Name: "dep", Namespace: "flux-system"}
			s.AddObject(dep)
			s.UpdateStatus(dep.Named(), store.StatusReady, "", "")
			ks := &manifest.Kustomization{
				Name: "apps", Namespace: "flux-system",
				Labels: map[string]string{"tier": "data"}, Path: "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
				},
			}
			s.AddObject(ks)
			var firstFingerprint string
			if rerender {
				info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode})
				if info.Status != store.StatusReady {
					t.Fatalf("setup: %+v", info)
				}
				firstFingerprint = s.GetArtifact(ks.Named()).(*store.Emission).Fingerprint
				ks = ks.Clone()
				ks.TargetNamespace = "elsewhere"
			}
			ks = ks.Clone()
			ks.DependsOn = []kustomizev1.DependencyReference{{Name: dep.Name, ReadyExpr: `self.metadata.labels['tier'] == 'data'`}}
			s.AddObject(ks)
			var attempts atomic.Int32
			var rendering, applying atomic.Bool
			s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
				if id != ks.Named() {
					return
				}
				info := payload.(store.StatusInfo)
				if info.Status == store.StatusPending && info.Message == "rendering" {
					rendering.Store(true)
				}
				if info.Status == store.StatusPending && strings.HasPrefix(info.Message, "applying ") {
					applying.Store(true)
				}
			}, false)
			info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
				Tasks: c.Tasks, Store: s, ID: ks.Named(),
				ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
					attempts.Add(1)
					return c.ReconcileNode(ctx, id, level)
				},
			})
			if info.Status != store.StatusReady || attempts.Load() > 2 {
				t.Fatalf("self-label render did not converge: status=%+v attempts=%d", info, attempts.Load())
			}
			emission, ok := s.GetArtifact(ks.Named()).(*store.Emission)
			if !ok || len(emission.Children) != 1 || emission.Children[0].Obj.Named().Name != "hello" {
				t.Fatalf("missing rendered ConfigMap: %+v", emission)
			}
			if !rendering.Load() || !applying.Load() {
				t.Fatalf("missing transient statuses: rendering=%v applying=%v", rendering.Load(), applying.Load())
			}
			if rerender && (emission.Fingerprint == firstFingerprint || emission.Children[0].Obj.Named().Namespace != "elsewhere") {
				t.Fatalf("genuine rerender did not replace output: %+v", emission)
			}
			t.Logf("ready with ConfigMap output in %d dispatches; rendering and applying observed", attempts.Load())
		})
	}
}
