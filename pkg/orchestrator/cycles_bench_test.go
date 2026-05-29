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

// BenchmarkFailDependsOnCycles_Baseline reproduces the pre-Phase-2.6
// listener cost: a full per-kind DFS on every store event. Each
// iteration runs the legacy findDependencyCycles N times — one per
// simulated EventObjectAdded — to model the old O(N(N+E)) listener
// fire pattern. The Incremental bench above measures the replacement.
//
// findDependencyCycles is retained for detectDependsOnCycles callers
// (tests, future tooling); using it here lets the baseline live in
// the same binary as the new code so the delta is measured against
// an apples-to-apples N+E.
func BenchmarkFailDependsOnCycles_Baseline(b *testing.B) {
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
		// One full listener replay: N store events, each running
		// the per-kind full DFS once (the pre-Phase-2.6 listener
		// body). Both kinds are walked per event because the old
		// listener fired failDependsOnCycles which walked both.
		for range N {
			_ = findDependencyCycles(o.store, manifest.KindKustomization)
			_ = findDependencyCycles(o.store, manifest.KindHelmRelease)
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
