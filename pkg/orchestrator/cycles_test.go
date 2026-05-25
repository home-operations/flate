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

// TestBreakDependsOnCycles_StripsCycleEdges verifies the runtime
// behavior the user cares about: after Bootstrap, KSes that were in
// a cycle have their cycle-closing dependsOn entries dropped. Their
// reconcile won't hang 30s waiting on a peer that can't become Ready.
func TestBreakDependsOnCycles_StripsCycleEdges(t *testing.T) {
	idA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}
	// `extra` is an acyclic dep — it must SURVIVE the strip.
	idExtra := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "extra"}

	o := &Orchestrator{store: store.New()}
	o.store.AddObject(makeKS("a", "ns", idB, idExtra))
	o.store.AddObject(makeKS("b", "ns", idA))
	o.store.AddObject(makeKS("extra", "ns"))

	o.breakDependsOnCycles()

	a, _ := o.store.GetObject(idA).(*manifest.Kustomization)
	b, _ := o.store.GetObject(idB).(*manifest.Kustomization)
	if a == nil || b == nil {
		t.Fatal("post-Bootstrap objects went missing")
	}
	// a should no longer depend on b (cycle edge), but should still
	// depend on extra (acyclic).
	for _, d := range a.DependsOn {
		if d.NamedResource == idB {
			t.Errorf("a→b cycle edge not stripped from a.DependsOn: %+v", a.DependsOn)
		}
	}
	hasExtra := false
	for _, d := range a.DependsOn {
		if d.NamedResource == idExtra {
			hasExtra = true
		}
	}
	if !hasExtra {
		t.Errorf("a→extra acyclic dep was stripped along with the cycle edge: %+v", a.DependsOn)
	}
	// b should have lost its only edge.
	for _, d := range b.DependsOn {
		if d.NamedResource == idA {
			t.Errorf("b→a cycle edge not stripped: %+v", b.DependsOn)
		}
	}
}

// TestBreakDependsOnCycles_RenderEmittedCycle verifies the runtime
// cycle-detection listener: when a parent KS's render emits two
// children whose dependsOn closes a loop, the orchestrator's
// post-Bootstrap listener re-runs breakDependsOnCycles and strips
// the cycle edges. Pre-fix, Bootstrap's one-shot pass never saw the
// emitted nodes and every cycle member burned its full per-dep
// timeout waiting on a peer that would never become Ready.
//
// The test invokes breakDependsOnCycles directly (mirrors what the
// Run-time listener does) AFTER adding the emit'd children to the
// store. End-to-end coverage of the listener wiring sits in the
// e2e suite — but pinning the load-bearing behavior here keeps the
// listener honest if it ever gets refactored.
func TestBreakDependsOnCycles_RenderEmittedCycle(t *testing.T) {
	idA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}

	o := &Orchestrator{store: store.New()}
	// Bootstrap-time graph: just one acyclic KS. No cycles to break.
	o.store.AddObject(makeKS("acyclic", "ns"))
	o.breakDependsOnCycles()

	// Now simulate the render-emit: a parent KS adds two children
	// whose dependsOn closes a loop. Bootstrap's pass missed these.
	o.store.AddObject(makeKS("a", "ns", idB))
	o.store.AddObject(makeKS("b", "ns", idA))
	// Re-run the cycle detector (the post-Bootstrap listener fires
	// this on every KS/HR add).
	o.breakDependsOnCycles()

	a, _ := o.store.GetObject(idA).(*manifest.Kustomization)
	b, _ := o.store.GetObject(idB).(*manifest.Kustomization)
	if a == nil || b == nil {
		t.Fatal("emitted children went missing")
	}
	if len(a.DependsOn) != 0 {
		t.Errorf("a's cycle edge not stripped post-emit: %+v", a.DependsOn)
	}
	if len(b.DependsOn) != 0 {
		t.Errorf("b's cycle edge not stripped post-emit: %+v", b.DependsOn)
	}
}
