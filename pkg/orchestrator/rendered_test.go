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

// TestRenderedSet_LastWriterWins documents the overwrite semantic:
// when a child id is rendered by two parents in the same run (rare,
// but theoretically possible if a child KS is referenced from
// multiple parent paths), the most recent emission wins. This
// matches kustomize's semantics and the AddObject DeepEqual gate's
// "last write wins" pattern.
func TestRenderedSet_LastWriterWins(t *testing.T) {
	r := newRenderedSet()
	child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "n", Name: "demo"}
	parentA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "a"}
	parentB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "b"}

	r.MarkRendered(parentA, child)
	r.MarkRendered(parentB, child)

	got, _ := r.ParentOf(child)
	if got != parentB {
		t.Errorf("ParentOf = %v, want last writer %v", got, parentB)
	}
}
