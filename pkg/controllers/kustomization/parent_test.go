package kustomization

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestCollectDeps_AppendsStructuralParent locks the contract that
// when ParentOf records an enclosing KS, that parent is appended to
// the dependency list so the reconcile waits for the parent's Ready
// before rendering. See #102.
func TestCollectDeps_AppendsStructuralParent(t *testing.T) {
	parent := manifest.NamedResource{
		Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps",
	}
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}

	c := New(store.New(), nil, nil, false)
	c.Configure(Options{ParentOf: map[manifest.NamedResource]manifest.NamedResource{
		child.Named(): parent,
	}})
	deps := c.collectDeps(child)
	for _, d := range deps {
		if d.NamedResource == parent {
			return
		}
	}
	t.Errorf("parent %+v missing from deps %+v", parent, deps)
}

// TestCollectDeps_NoParentNoExtraDep guards against ParentOf-less
// controllers (e.g. unit-test setups) panicking on a nil map.
func TestCollectDeps_NoParentNoExtraDep(t *testing.T) {
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}
	c := New(store.New(), nil, nil, false) // ParentOf nil
	deps := c.collectDeps(child)
	if len(deps) != 0 {
		t.Errorf("expected no deps for KS without sourceRef/dependsOn/parent; got %+v", deps)
	}
}
