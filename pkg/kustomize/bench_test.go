package kustomize

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

// BenchmarkRenderFlux_SmallKS measures the end-to-end RenderFlux call
// for a tiny single-resource kustomize tree. Reasonable upper bound on
// the per-KS cost in a real run.
func BenchmarkRenderFlux_SmallKS(b *testing.B) {
	src := b.TempDir()
	testutil.WriteFile(b, src, "kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(b, src, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: smoke
  namespace: ns
data:
  k: v
`)

	cache, err := NewStagingCache(b.TempDir(), 0)
	if err != nil {
		b.Fatalf("NewStagingCache: %v", err)
	}
	b.Cleanup(func() { _ = cache.Close() })

	rawSpec := map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "small", "namespace": "ns"},
		"spec":       map[string]any{"path": "./"},
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := RenderFlux(ctx, cache, src, ".", "", rawSpec)
		if err != nil {
			b.Fatalf("RenderFlux: %v", err)
		}
		if len(out) == 0 {
			b.Fatalf("empty render")
		}
	}
}

// BenchmarkRenderFlux_DeepComponentStack measures RenderFlux against a
// KS whose kustomize tree pulls in 5 nested Components — the real cost
// pattern when a flux-system overlay stacks multiple shared component
// fragments.
func BenchmarkRenderFlux_DeepComponentStack(b *testing.B) {
	src := b.TempDir()
	const depth = 5

	// Root kustomization that pulls in level-0 as a component, which
	// chains down through ../level-1 ... level-(depth-1).
	testutil.WriteFile(b, src, "kustomization.yaml", fmt.Sprintf(
		"resources:\n  - cm.yaml\ncomponents:\n  - ./components/level-%d\n", 0))
	testutil.WriteFile(b, src, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: root
  namespace: ns
data:
  k: v
`)
	for i := range depth {
		base := fmt.Sprintf("components/level-%d", i)
		body := "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources:\n  - ./cm.yaml\n"
		if i < depth-1 {
			body += fmt.Sprintf("components:\n  - ../level-%d\n", i+1)
		}
		testutil.WriteFile(b, src, base+"/kustomization.yaml", body)
		testutil.WriteFile(b, src, base+"/cm.yaml", fmt.Sprintf(
			`apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-%d
  namespace: ns
data:
  k: v
`, i))
	}

	cache, err := NewStagingCache(b.TempDir(), 0)
	if err != nil {
		b.Fatalf("NewStagingCache: %v", err)
	}
	b.Cleanup(func() { _ = cache.Close() })

	rawSpec := map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "deep", "namespace": "ns"},
		"spec":       map[string]any{"path": "./"},
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := RenderFlux(ctx, cache, src, ".", "", rawSpec)
		if err != nil {
			b.Fatalf("RenderFlux: %v", err)
		}
		if len(out) == 0 {
			b.Fatalf("empty render")
		}
	}
}

// BenchmarkStagingCache_MultiKSPerRoot measures concurrent Stage calls
// for different subPaths under the SAME source root. The first call
// pays the copy; every subsequent call should be a sync.OnceValues
// hit. Uses b.RunParallel because the operation is genuinely
// concurrent — multiple KSes rooted at the same source race here.
func BenchmarkStagingCache_MultiKSPerRoot(b *testing.B) {
	src := b.TempDir()
	// Synthesize a tree wide enough that the initial copy is non-trivial,
	// so the first-call cost on a single iteration doesn't dominate the
	// rest of the parallel workload.
	for i := range 40 {
		testutil.WriteFile(b, src, fmt.Sprintf("apps/app-%d/kustomization.yaml", i),
			"resources:\n- cm.yaml\n")
		testutil.WriteFile(b, src, fmt.Sprintf("apps/app-%d/cm.yaml", i), fmt.Sprintf(
			"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app-%d\n  namespace: ns\ndata:\n  k: v\n", i))
	}

	cache, err := NewStagingCache(b.TempDir(), 0)
	if err != nil {
		b.Fatalf("NewStagingCache: %v", err)
	}
	b.Cleanup(func() { _ = cache.Close() })

	// Warm the cache so the bench measures the warm-stage hit, not the
	// one-shot initial copy.
	ctx := context.Background()
	if _, err := cache.Stage(ctx, src, ""); err != nil {
		b.Fatalf("warm Stage: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var ctr atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// All workers share the same source root; subPath
			// distinction is what RenderFlux would pass through
			// downstream — here we measure the stage-lookup cost
			// alone since that's the synchronization point.
			ctr.Add(1)
			path, err := cache.Stage(ctx, src, "")
			if err != nil {
				b.Fatalf("Stage: %v", err)
			}
			if !strings.HasPrefix(path, "/") {
				b.Fatalf("unexpected stage path: %q", path)
			}
		}
	})
}
