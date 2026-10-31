package fluxinstance

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestReconcile_SyncLifecycle(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name     string
		preserve bool
	}{{"owned only", false}, {"authored collision", true}} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			preserveAuthored := scenario.preserve
			s := store.New()
			tasks := task.NewBounded(2)
			c := New(s, tasks, kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, tasks), true)
			c.Configure(base.Options{})
			c.StartLifecycle()
			t.Cleanup(tasks.BlockTillDone)
			obj := &manifest.RawObject{APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: "FluxInstance", Namespace: "control", Name: "flux", Spec: map[string]any{
				"distribution": map[string]any{"version": "2.x"},
				"sync":         map[string]any{"kind": "GitRepository", "url": "https://example.test/self", "ref": "main", "path": "./apps"},
				"kustomize":    map[string]any{"patches": []any{map[string]any{"target": map[string]any{"kind": "Kustomization"}, "patch": "[{\"op\":\"add\",\"path\":\"/spec/postBuild\",\"value\":{\"substitute\":{\"VALUE\":\"transformed\"}}}]"}}},
			}}
			s.AddObject(obj)
			root := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "control", Name: "control"}
			source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "control", Name: "control"}
			var first *base.Execution
			tasks.Go(t.Context(), "initial", func(ctx context.Context) { first = c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone) })
			tasks.BlockTillDone()
			defer first.Close()
			status, _ := s.GetStatus(obj.Named())
			assert.Equal(t, status.Status, store.StatusReady)
			for _, id := range []manifest.NamedResource{root, source} {
				assert.Equal(t, s.EmissionOwner(id), obj.Named())
				_, doc, attempt := s.GetEmission(id)
				attempt.Finish()
				assert.Equal(t, manifest.DocIdentity(doc), id)
				assert.Equal(t, doc["metadata"].(map[string]any)["labels"].(map[string]any)["app.kubernetes.io/version"], any("2.x"))
			}
			ks, ok := s.GetObject(root).(*manifest.Kustomization)
			if !ok || ks.PostBuild == nil || ks.PostBuild.Substitute["VALUE"] != "transformed" {
				t.Fatalf("transformed input not emitted: %+v", ks)
			}
			var replay *base.Execution
			tasks.Go(t.Context(), "replay", func(ctx context.Context) { replay = c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone) })
			tasks.BlockTillDone()
			defer replay.Close()
			assert.Equal(t, replay.Reused, true)
			if preserveAuthored {
				s.AddObject(ks)
			}
			next := obj.Clone()
			delete(next.Spec, "sync")
			s.AddObject(next)
			status = controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: tasks, Store: s, ID: next.Named(), ReconcileNode: c.ReconcileNode})
			assert.Equal(t, status.Status, store.StatusReady)
			for _, id := range []manifest.NamedResource{root, source} {
				if preserveAuthored && id == root {
					assert.Equal(t, s.GetObject(id), manifest.BaseManifest(ks))
				} else if s.GetObject(id) != nil {
					t.Fatalf("retired child survived: %s", id)
				}
			}
			output, ok := s.GetArtifact(next.Named(), store.ArtifactContent).(*store.Emission)
			if !ok || !output.Complete || len(output.Children) != 0 {
				t.Fatalf("empty sync emission: %+v", output)
			}
		})
	}
}

func TestController_Ownership(t *testing.T) {
	t.Parallel()
	c := New(store.New(), task.NewBounded(2), nil, true)
	for _, group := range []string{manifest.FluxOperatorDomain, "foreign.example"} {
		t.Run(group, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, c.Owns(manifest.NamedResource{Group: group, Kind: "FluxInstance"}), group == manifest.FluxOperatorDomain)
		})
	}
}
