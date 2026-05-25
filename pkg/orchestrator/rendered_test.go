package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

// TestRenderedSet_RecordsAndQueriesParent locks the foundation of
// render-driven discovery: each child carries an explicit parent KS
// reference that downstream consumers (parent index, change filter,
// orphan detection, RS extension attribution) can query when the
// child has no source file.
func TestRenderedSet_RecordsAndQueriesParent(t *testing.T) {
	r := newRenderedSet()
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster"}
	child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "demo"}

	if r.has(child) {
		t.Fatal("expected child to be absent before MarkRendered")
	}
	if _, ok := r.ParentOf(child); ok {
		t.Fatal("ParentOf should return ok=false for unknown id")
	}

	r.MarkRendered(parent, child)

	if !r.has(child) {
		t.Error("expected child to be present after MarkRendered")
	}
	got, ok := r.ParentOf(child)
	if !ok {
		t.Fatal("ParentOf should return ok=true after MarkRendered")
	}
	if got != parent {
		t.Errorf("ParentOf = %v, want %v", got, parent)
	}
}

// TestRenderedSet_FirstWriterWins pins the attribution semantic:
// when a child id is rendered by two parents in the same run (rare,
// but possible if a child KS is referenced from multiple parent
// paths), the FIRST emitter owns the child. The first-write-wins
// guard exists so PR #361's fingerprint-dedup replay — which
// re-runs MarkRendered on every reconcile of every parent — doesn't
// silently swap attribution to whichever parent reconciled most
// recently (breaking detectOrphans / ParentOf / RS-extension queries
// downstream).
func TestRenderedSet_FirstWriterWins(t *testing.T) {
	r := newRenderedSet()
	child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "n", Name: "demo"}
	parentA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "a"}
	parentB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "b"}

	r.MarkRendered(parentA, child)
	r.MarkRendered(parentB, child)

	got, _ := r.ParentOf(child)
	if got != parentA {
		t.Errorf("ParentOf = %v, want first writer %v", got, parentA)
	}
}
