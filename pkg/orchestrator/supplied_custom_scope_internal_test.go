package orchestrator

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testSuppliedCustomScope(t *testing.T) {
	t.Helper()
	for _, scope := range []string{"Namespaced", "Cluster"} {
		t.Run(scope, func(t *testing.T) {
			custom := map[string]any{
				"apiVersion": "sdk-scope.example/v1", "kind": "Widget",
				"metadata": map[string]any{"name": "external"},
			}
			definition := map[string]any{
				"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
				"metadata": map[string]any{"name": "widgets.sdk-scope.example"},
				"spec": map[string]any{
					"group": "sdk-scope.example", "scope": scope,
					"names": map[string]any{"kind": "Widget", "plural": "widgets"},
				},
			}
			_, err := RenderTrees(t.Context(), nil, Config{
				CacheDir: t.TempDir(), Concurrency: 2,
				Supply: []store.Declaration{{Doc: custom}, {Doc: definition}},
			})
			if scope == "Namespaced" {
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("expected input error for missing namespace, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("cluster-scoped supplied object rejected: %v", err)
			}
		})
	}
}
