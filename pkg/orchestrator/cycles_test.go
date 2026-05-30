package orchestrator

import (
	"fmt"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func makeKS(name, ns string, deps ...manifest.NamedResource) *manifest.Kustomization {
	refs := make([]manifest.DependencyRef, len(deps))
	for i, d := range deps {
		refs[i] = manifest.DependencyRef{NamedResource: d}
	}
	return &manifest.Kustomization{
		Name: name, Namespace: ns,
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./" + name},
		DependsOn:         refs,
	}
}

// TestFailDependsOnCycles_RecordsPreflightFailures verifies the runtime
// behavior the user cares about: cycle members fail before render, while
// their Flux dependsOn graph remains intact.
func TestFailDependsOnCycles_RecordsPreflightFailures(t *testing.T) {
	idA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}
	// `extra` is an acyclic dep — it must not be marked failed.
	idExtra := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "extra"}

	o := &Orchestrator{store: store.New()}
	o.store.AddObject(makeKS("a", "ns", idB, idExtra))
	o.store.AddObject(makeKS("b", "ns", idA))
	o.store.AddObject(makeKS("extra", "ns"))

	o.failDependsOnCycles()

	a, _ := o.store.GetObject(idA).(*manifest.Kustomization)
	b, _ := o.store.GetObject(idB).(*manifest.Kustomization)
	if a == nil || b == nil {
		t.Fatal("post-Bootstrap objects went missing")
	}
	if len(a.DependsOn) != 2 || len(b.DependsOn) != 1 {
		t.Fatalf("cycle detection must not rewrite dependsOn: a=%+v b=%+v", a.DependsOn, b.DependsOn)
	}
	for _, id := range []manifest.NamedResource{idA, idB} {
		msg, ok := o.preflightFailure(id)
		if !ok || !strings.Contains(msg, "dependency cycle detected") {
			t.Fatalf("%s preflight failure = %q, %v", id, msg, ok)
		}
	}
	if msg, ok := o.preflightFailure(idExtra); ok {
		t.Fatalf("acyclic dependency was marked failed: %q", msg)
	}
}

func TestFailDependsOnCycles_ClearsResolvedCycle(t *testing.T) {
	idA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}

	o := &Orchestrator{store: store.New()}
	o.store.AddObject(makeKS("a", "ns", idB))
	o.store.AddObject(makeKS("b", "ns", idA))
	o.failDependsOnCycles()
	if msg, ok := o.preflightFailure(idA); !ok || !strings.Contains(msg, "dependency cycle detected") {
		t.Fatalf("initial cycle was not recorded: %q, %v", msg, ok)
	}

	o.store.AddObject(makeKS("a", "ns"))
	o.failDependsOnCycles()
	for _, id := range []manifest.NamedResource{idA, idB} {
		if msg, ok := o.preflightFailure(id); ok {
			t.Fatalf("resolved cycle member still has preflight failure %s: %q", id, msg)
		}
	}
}

func TestFailDependsOnCycles_RefiresResolvedMembers(t *testing.T) {
	idA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}

	o := &Orchestrator{store: store.New()}
	o.store.AddObject(makeKS("a", "ns", idB))
	o.store.AddObject(makeKS("b", "ns", idA))
	o.failDependsOnCycles()

	seen := map[manifest.NamedResource]int{}
	o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		seen[id]++
	}, false)

	o.store.AddObject(makeKS("a", "ns"))
	o.failDependsOnCycles()

	if seen[idB] == 0 {
		t.Fatal("resolved cycle member b was not refired after preflight failure cleared")
	}
}

// TestFailDependsOnCycles_RenderEmittedCycle verifies the runtime
// cycle-detection listener: when a parent KS's render emits two
// children whose dependsOn closes a loop, the orchestrator's
// post-Bootstrap listener re-runs failDependsOnCycles and records
// preflight failures. Bootstrap's one-shot pass never saw the emitted
// nodes.
//
// The test invokes failDependsOnCycles directly (mirrors what the
// Run-time listener does) AFTER adding the emit'd children to the
// store. End-to-end coverage of the listener wiring sits in the
// e2e suite — but pinning the load-bearing behavior here keeps the
// listener honest if it ever gets refactored.
func TestFailDependsOnCycles_RenderEmittedCycle(t *testing.T) {
	idA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}

	o := &Orchestrator{store: store.New()}
	// Bootstrap-time graph: just one acyclic KS. No cycles to break.
	o.store.AddObject(makeKS("acyclic", "ns"))
	o.failDependsOnCycles()

	// Now simulate the render-emit: a parent KS adds two children
	// whose dependsOn closes a loop. Bootstrap's pass missed these.
	o.store.AddObject(makeKS("a", "ns", idB))
	o.store.AddObject(makeKS("b", "ns", idA))
	// Re-run the cycle detector (the post-Bootstrap listener fires
	// this on every KS/HR add).
	o.failDependsOnCycles()

	a, _ := o.store.GetObject(idA).(*manifest.Kustomization)
	b, _ := o.store.GetObject(idB).(*manifest.Kustomization)
	if a == nil || b == nil {
		t.Fatal("emitted children went missing")
	}
	for _, id := range []manifest.NamedResource{idA, idB} {
		msg, ok := o.preflightFailure(id)
		if !ok || !strings.Contains(msg, "dependency cycle detected") {
			t.Fatalf("%s preflight failure = %q, %v", id, msg, ok)
		}
	}
}

// TestUpdateDependencyGraphFor_LargeAcyclicAddThenCycle stresses the
// incremental hot path: feed the graph 10k acyclic edges via the
// per-event listener API, assert no false positive, then add a single
// cycle-introducing edge and assert it's flagged. Mirrors the post-
// Bootstrap listener: every store add hits updateDependencyGraphFor
// with exactly one id, no batched DFS.
func TestUpdateDependencyGraphFor_LargeAcyclicAddThenCycle(t *testing.T) {
	const N = 10000
	o := &Orchestrator{store: store.New()}

	// Chain: ks-0 → ks-1 → ks-2 → ... → ks-(N-1). Pure DAG.
	ids := make([]manifest.NamedResource, N)
	for i := range N {
		ids[i] = manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: fmt.Sprintf("ks-%05d", i)}
	}
	for i := range N {
		var deps []manifest.NamedResource
		if i+1 < N {
			deps = []manifest.NamedResource{ids[i+1]}
		}
		o.store.AddObject(makeKS(ids[i].Name, ids[i].Namespace, deps...))
		o.updateDependencyGraphFor(ids[i])
	}
	if got := o.depGraph.Failures(); len(got) != 0 {
		t.Fatalf("acyclic chain reported cycles: %v", got)
	}
	for _, id := range ids {
		if msg, ok := o.preflightFailure(id); ok {
			t.Fatalf("acyclic %s flagged: %q", id, msg)
		}
	}

	// Close the chain: ks-(N-1) → ks-0 creates one huge cycle.
	o.store.AddObject(makeKS(ids[N-1].Name, ids[N-1].Namespace, ids[0]))
	o.updateDependencyGraphFor(ids[N-1])

	failures := o.depGraph.Failures()
	if len(failures) != N {
		t.Fatalf("cycle-closing edge should flag every chain member; got %d / %d", len(failures), N)
	}
	for _, id := range ids {
		if msg, ok := o.preflightFailure(id); !ok || !strings.Contains(msg, "dependency cycle detected") {
			t.Fatalf("%s preflight after cycle close = %q, %v", id, msg, ok)
		}
	}
}

// TestUpdateDependencyGraphFor_BreaksCycle exercises the remove path:
// stand up a cycle via the incremental API, then rewrite one id's
// dependsOn to break it; every previously-failed member must clear.
func TestUpdateDependencyGraphFor_BreaksCycle(t *testing.T) {
	idA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}
	idC := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "c"}

	o := &Orchestrator{store: store.New()}
	o.store.AddObject(makeKS("a", "ns", idB))
	o.updateDependencyGraphFor(idA)
	o.store.AddObject(makeKS("b", "ns", idC))
	o.updateDependencyGraphFor(idB)
	o.store.AddObject(makeKS("c", "ns", idA))
	o.updateDependencyGraphFor(idC)

	for _, id := range []manifest.NamedResource{idA, idB, idC} {
		if msg, ok := o.preflightFailure(id); !ok || !strings.Contains(msg, "dependency cycle detected") {
			t.Fatalf("%s pre-break = %q, %v", id, msg, ok)
		}
	}

	// Rewrite c to drop its dep on a; cycle dissolves.
	o.store.AddObject(makeKS("c", "ns"))
	o.updateDependencyGraphFor(idC)

	for _, id := range []manifest.NamedResource{idA, idB, idC} {
		if msg, ok := o.preflightFailure(id); ok {
			t.Fatalf("%s post-break still failed: %q", id, msg)
		}
	}
}

// TestUpdateDependencyGraphFor_HelmRelease verifies the same
// incremental update works for HelmRelease objects — the listener
// dispatches both kinds through one path.
func TestUpdateDependencyGraphFor_HelmRelease(t *testing.T) {
	idX := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "x"}
	idY := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "y"}

	o := &Orchestrator{store: store.New()}
	o.store.AddObject(&manifest.HelmRelease{
		Name: "x", Namespace: "ns",
		DependsOn: []manifest.DependencyRef{{NamedResource: idY}},
	})
	o.updateDependencyGraphFor(idX)
	o.store.AddObject(&manifest.HelmRelease{
		Name: "y", Namespace: "ns",
		DependsOn: []manifest.DependencyRef{{NamedResource: idX}},
	})
	o.updateDependencyGraphFor(idY)

	for _, id := range []manifest.NamedResource{idX, idY} {
		if msg, ok := o.preflightFailure(id); !ok || !strings.Contains(msg, "dependency cycle detected") {
			t.Fatalf("HR %s preflight = %q, %v", id, msg, ok)
		}
	}
}
