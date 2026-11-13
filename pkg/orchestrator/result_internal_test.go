package orchestrator

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestResult_CloseAfterFinalProjection(t *testing.T) {
	t.Parallel()
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "failed"}[failed], func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			st.AddObject(owner)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				owner.Named(): {Declaration: owner},
			}}
			physical := loader.New(st)
			physical.Index = index
			catalog := physical.DetachCatalog()
			physical.Close()
			document := map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "result"},
			}
			completed := &Result{
				catalog:   catalog,
				Manifests: []manifest.DocumentBatch{{Parent: owner, Documents: []map[string]any{document}}},
			}
			defer completed.Close()
			run := Rendered{Orchestrator: &Orchestrator{store: st, index: index}, Result: completed}
			if failed {
				run.Err = errors.New("render failed")
			}
			for range 2 {
				projected, projection := Select([]Rendered{run}, run.Err, []manifest.NamedResource{owner.Named()}, nil)
				assert.Equal(t, errors.Is(projection.Selected, run.Err), true)
				assert.Equal(t, len(projected[0].Objects), 1)
				assert.Equal(t, projected[0].Objects[0], manifest.BaseManifest(owner))
				assert.Diff(t, projected[0].Manifests[0].Documents, []map[string]any{document})
				assert.Equal(t, catalog.CatalogObject(owner.Named()), manifest.BaseManifest(owner))
			}
			completed.Close()
			completed.Close()
			assert.Equal(t, completed.catalog == nil, true)
			assert.Equal(t, catalog.CatalogObject(owner.Named()), nil)
			assert.Equal(t, completed.Manifests[0].Parent, manifest.BaseManifest(owner))
			assert.Diff(t, completed.Manifests[0].Documents, []map[string]any{document})
			assert.Equal(t, st.GetObject(owner.Named()), manifest.BaseManifest(owner))
		})
	}
}
