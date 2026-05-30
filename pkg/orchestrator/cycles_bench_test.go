package orchestrator

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// BenchmarkFailDependsOnCycles_Incremental simulates the post-Bootstrap
// hot path: the orchestrator's store listener fires per-id when a KS
// or HR lands, and the cycle-detection step must touch only that id's
// edges. Pre-Phase-2.6 every fire re-ran a full O(N+E) DFS over every
// Kustomization in the store; on a 1000-object Bootstrap that turned
// cycle detection into O(N(N+E)) — the dominant orchestrator cost on
// big repos.
//
// The bench builds an acyclic 1000-object chain (each KS depends on
// its successor — every chain is a happy path through the graph), then
// measures one full listener-replay: 1000 calls to
// updateDependencyGraphFor, one per added KS. The b.Loop hot region
// re-runs the replay against a pre-warmed store so we measure the
// graph-update path, not store mutations.
//
// Compare to baseline via:
//
//	git stash
//	go test -run=^$ -bench='BenchmarkFailDependsOnCycles' -benchmem \
//	  -count=5 ./pkg/orchestrator
//	git stash pop
//	go test -run=^$ -bench='BenchmarkFailDependsOnCycles' -benchmem \
//	  -count=5 ./pkg/orchestrator
//
// Phase 2.6 target: ≥10× improvement.
func BenchmarkFailDependsOnCycles_Incremental(b *testing.B) {
	const N = 1000
	o := &Orchestrator{store: store.New()}

	ids := make([]manifest.NamedResource, N)
	for i := range N {
		ids[i] = manifest.NamedResource{
			Kind: manifest.KindKustomization, Namespace: "ns",
			Name: fmt.Sprintf("ks-%05d", i),
		}
	}
	// Build an acyclic chain: ks-i depends on ks-(i+1). The last
	// object has no deps. Single linear DAG keeps the bench
	// deterministic and matches the worst case for forward-reach
	// (each new edge's target sits at the far end of the chain).
	for i := range N {
		var deps []manifest.NamedResource
		if i+1 < N {
			deps = []manifest.NamedResource{ids[i+1]}
		}
		o.store.AddObject(makeKS(ids[i].Name, ids[i].Namespace, deps...))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// One full Bootstrap-style replay: every KS/HR add fires
		// the listener once. We measure the cost of running that
		// listener N times against the pre-built store state.
		for _, id := range ids {
			o.updateDependencyGraphFor(id)
		}
	}
}

// BenchmarkFailDependsOnCycles_Bootstrap measures the one-shot
// Bootstrap full-rebuild path (failDependsOnCycles called once at
// startup). This is the cost of the rebuild that happens BEFORE any
// listener fires; the incremental path takes over after.
func BenchmarkFailDependsOnCycles_Bootstrap(b *testing.B) {
	const N = 1000
	o := &Orchestrator{store: store.New()}

	ids := make([]manifest.NamedResource, N)
	for i := range N {
		ids[i] = manifest.NamedResource{
			Kind: manifest.KindKustomization, Namespace: "ns",
			Name: fmt.Sprintf("ks-%05d", i),
		}
	}
	for i := range N {
		var deps []manifest.NamedResource
		if i+1 < N {
			deps = []manifest.NamedResource{ids[i+1]}
		}
		o.store.AddObject(makeKS(ids[i].Name, ids[i].Namespace, deps...))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		o.failDependsOnCycles()
	}
}
