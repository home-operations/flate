package orchestrator

import (
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

// TestDetectDependsOnCycles_Simple locks the headline case: a two-
// node KS cycle is reported as a closed loop, deterministic across
// runs (sort + sort = same output every invocation).
func TestDetectDependsOnCycles_Simple(t *testing.T) {
	idA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}

	o := &Orchestrator{store: store.New()}
	o.store.AddObject(makeKS("a", "ns", idB))
	o.store.AddObject(makeKS("b", "ns", idA))

	cycles := o.detectDependsOnCycles()
	if len(cycles) == 0 {
		t.Fatal("expected at least one cycle; got none")
	}
	got := formatCyclePath(cycles[0])
	if !strings.Contains(got, "a") || !strings.Contains(got, "b") {
		t.Errorf("cycle path missing nodes: %q", got)
	}
	// Closed loop: first id repeats at the end.
	if cycles[0][0] != cycles[0][len(cycles[0])-1] {
		t.Errorf("cycle should close on itself; got %v", cycles[0])
	}
}

// TestDetectDependsOnCycles_NoCycleNoOutput confirms acyclic graphs
// produce no false positives.
func TestDetectDependsOnCycles_NoCycleNoOutput(t *testing.T) {
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}
	idC := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "c"}

	o := &Orchestrator{store: store.New()}
	o.store.AddObject(makeKS("a", "ns", idB, idC)) // a → {b, c}
	o.store.AddObject(makeKS("b", "ns", idC))      // b → c
	o.store.AddObject(makeKS("c", "ns"))           // c has no deps

	if cycles := o.detectDependsOnCycles(); len(cycles) != 0 {
		t.Errorf("expected no cycles on a DAG; got %v", cycles)
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
