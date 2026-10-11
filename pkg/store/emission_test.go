package store

import (
	"fmt"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_EmittedParent_RecordsAndQueriesParent(t *testing.T) {
	r := New()
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster"}
	child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "demo"}

	if _, ok := r.EmittedParent(child); ok {
		t.Fatal("EmittedParent should return ok=false for unknown id")
	}

	r.RecordEmitted(parent, []manifest.NamedResource{child})
	r.AddObject(&manifest.HelmRelease{Name: child.Name, Namespace: child.Namespace})
	r.DeleteObject(child)

	got, ok := r.EmittedParent(child)
	if !ok {
		t.Fatal("EmittedParent should return ok=true after RecordEmitted")
	}
	if got != parent {
		t.Errorf("EmittedParent = %v, want %v", got, parent)
	}
}

func TestStore_EmittedParent_FirstWriterWins(t *testing.T) {
	r := New()
	child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "n", Name: "demo"}
	parentA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "a"}
	parentB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "b"}

	r.RecordEmitted(parentA, []manifest.NamedResource{child})
	r.RecordEmitted(parentB, []manifest.NamedResource{child})

	got, _ := r.EmittedParent(child)
	if got != parentA {
		t.Errorf("EmittedParent = %v, want first writer %v", got, parentA)
	}
}

func TestStore_RecordEmitted_AttributesEveryChild(t *testing.T) {
	r := New()
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster"}
	a := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "a"}
	b := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "b"}
	c := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "c"}

	r.RecordEmitted(parent, []manifest.NamedResource{a, b, c})

	for _, child := range []manifest.NamedResource{a, b, c} {
		got, ok := r.EmittedParent(child)
		if !ok {
			t.Errorf("EmittedParent(%v) ok=false after RecordEmitted", child)
			continue
		}
		if got != parent {
			t.Errorf("EmittedParent(%v) = %v, want %v", child, got, parent)
		}
	}
}

func TestStore_RecordEmitted_HonorsSelfEdgeAndFirstWriterWins(t *testing.T) {
	r := New()
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster"}
	other := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "other"}
	child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "demo"}

	// Self-edge (parent == child) must be silently dropped.
	r.RecordEmitted(parent, []manifest.NamedResource{parent, child})
	if _, ok := r.EmittedParent(parent); ok {
		t.Error("self-edge should have been dropped, but EmittedParent(parent) returned ok=true")
	}
	if got, ok := r.EmittedParent(child); !ok || got != parent {
		t.Errorf("EmittedParent(child) = (%v, %v), want (%v, true)", got, ok, parent)
	}

	// Re-batching child under a different parent must not overwrite.
	r.RecordEmitted(other, []manifest.NamedResource{child})
	if got, _ := r.EmittedParent(child); got != parent {
		t.Errorf("EmittedParent(child) = %v after second batch, want first writer %v", got, parent)
	}
}

func TestStore_EmittedChildrenByParent_Concurrent(t *testing.T) {
	r := New()
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "parent"}
	var wg sync.WaitGroup
	for worker := range 2 {
		wg.Go(func() {
			for i := range 100 {
				child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: fmt.Sprintf("%d-%d", worker, i)}
				r.RecordEmitted(parent, []manifest.NamedResource{child})
				r.EmittedChildrenByParent()
			}
		})
	}
	wg.Wait()
	snapshot := r.EmittedChildrenByParent()
	assert.Equal(t, len(snapshot[parent]), 200)
	snapshot[parent][0] = manifest.NamedResource{}
	delete(snapshot, parent)
	fresh := r.EmittedChildrenByParent()
	assert.Equal(t, len(fresh[parent]), 200)
	for _, child := range fresh[parent] {
		if child.Name == "" {
			t.Fatal("snapshot aliases rendered state")
		}
	}
}
