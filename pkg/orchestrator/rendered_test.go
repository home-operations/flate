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
		t.Fatal("expected child to be absent before MarkRenderedBatch")
	}
	if _, ok := r.ParentOf(child); ok {
		t.Fatal("ParentOf should return ok=false for unknown id")
	}

	r.MarkRenderedBatch(parent, []manifest.NamedResource{child})

	if !r.has(child) {
		t.Error("expected child to be present after MarkRenderedBatch")
	}
	got, ok := r.ParentOf(child)
	if !ok {
		t.Fatal("ParentOf should return ok=true after MarkRenderedBatch")
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

	r.MarkRenderedBatch(parentA, []manifest.NamedResource{child})
	r.MarkRenderedBatch(parentB, []manifest.NamedResource{child})

	got, _ := r.ParentOf(child)
	if got != parentA {
		t.Errorf("ParentOf = %v, want first writer %v", got, parentA)
	}
}

// TestRenderedSet_MarkRenderedBatch_AttributesEveryChild asserts the
// batched form records parent linkage for every child in the slice
// under a single lock acquisition — same observable end-state as N
// successive MarkRendered calls. Locks the contract the KS emit
// loop relies on when it accumulates rendered children and flushes
// once instead of paying N r.mu acquisitions.
func TestRenderedSet_MarkRenderedBatch_AttributesEveryChild(t *testing.T) {
	r := newRenderedSet()
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster"}
	a := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "a"}
	b := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "b"}
	c := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "c"}

	r.MarkRenderedBatch(parent, []manifest.NamedResource{a, b, c})

	for _, child := range []manifest.NamedResource{a, b, c} {
		got, ok := r.ParentOf(child)
		if !ok {
			t.Errorf("ParentOf(%v) ok=false after MarkRenderedBatch", child)
			continue
		}
		if got != parent {
			t.Errorf("ParentOf(%v) = %v, want %v", child, got, parent)
		}
	}
}

// TestRenderedSet_MarkRenderedBatch_HonorsSelfEdgeAndFirstWriterWins
// guards the two invariants the per-call MarkRendered enforces:
// self-edges are dropped (parent KS whose spec.path covers its own
// definition emits itself during render — see the Zariel/home-ops
// cluster KS pattern) and first-write-wins on duplicates (fingerprint
// dedup replays render emissions; later parents must NOT clobber
// initial attribution). The batched form must inherit both, else the
// emit-loop switch silently regresses orphan detection / ParentOf
// for re-emitted children.
func TestRenderedSet_MarkRenderedBatch_HonorsSelfEdgeAndFirstWriterWins(t *testing.T) {
	r := newRenderedSet()
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster"}
	other := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "other"}
	child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "demo"}

	// Self-edge (parent == child) must be silently dropped.
	r.MarkRenderedBatch(parent, []manifest.NamedResource{parent, child})
	if _, ok := r.ParentOf(parent); ok {
		t.Error("self-edge should have been dropped, but ParentOf(parent) returned ok=true")
	}
	if got, ok := r.ParentOf(child); !ok || got != parent {
		t.Errorf("ParentOf(child) = (%v, %v), want (%v, true)", got, ok, parent)
	}

	// Re-batching child under a different parent must not overwrite.
	r.MarkRenderedBatch(other, []manifest.NamedResource{child})
	if got, _ := r.ParentOf(child); got != parent {
		t.Errorf("ParentOf(child) = %v after second batch, want first writer %v", got, parent)
	}
}
