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
	c.Configure(Options{ParentOf: mapResolver(map[manifest.NamedResource]manifest.NamedResource{
		child.Named(): parent,
	})})
	deps := c.collectDeps(child)
	for _, d := range deps {
		if d.NamedResource == parent {
			return
		}
	}
	t.Errorf("parent %+v missing from deps %+v", parent, deps)
}

// TestCollectDeps_NoParentNoExtraDep guards against ParentOf-less
// controllers (e.g. unit-test setups) panicking on a nil resolver.
func TestCollectDeps_NoParentNoExtraDep(t *testing.T) {
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}
	c := New(store.New(), nil, nil, false) // ParentOf nil
	deps := c.collectDeps(child)
	if len(deps) != 0 {
		t.Errorf("expected no deps for KS without sourceRef/dependsOn/parent; got %+v", deps)
	}
}

// TestCollectDeps_AppendsSubstituteFromConfigMap locks the
// substituteFrom→ConfigMap depwait edge: every non-Optional ref
// becomes a real dependency. Without this, KS-A would race the CM
// that KS-B's render emits, and Prepare would silently expand with
// empty values for any var that should have come from KS-B's CM.
func TestCollectDeps_AppendsSubstituteFromConfigMap(t *testing.T) {
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
			{Kind: manifest.KindConfigMap, Name: "maybe-missing", Optional: true},
		},
	}
	c := New(store.New(), nil, nil, false)
	deps := c.collectDeps(ks)

	wantID := manifest.NamedResource{
		Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings",
	}
	var found bool
	for _, d := range deps {
		if d.NamedResource == wantID {
			found = true
		}
		if d.Kind == manifest.KindConfigMap && d.Name == "maybe-missing" {
			t.Errorf("Optional substituteFrom ref must not gate reconcile; got %+v", d)
		}
	}
	if !found {
		t.Errorf("expected substituteFrom dep %+v in deps %+v", wantID, deps)
	}
}

// TestCollectDeps_SubstituteFromSkipsSecrets locks the
// SOPS/ExternalSecret carve-out: substituteFrom Secret refs are
// resolved by values.ExpandPostBuildSubstituteReference at Prepare
// time and gracefully degrade on missing entries. Adding a depwait
// edge for them would regress every offline render against a Flux
// repo using secret-substitute patterns (e.g. cnpg-objectstore,
// cloudflare-tunnel-substitute) because those Secrets live in
// cluster state flate cannot materialize.
func TestCollectDeps_SubstituteFromSkipsSecrets(t *testing.T) {
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindSecret, Name: "cluster-secrets"},
		},
	}
	c := New(store.New(), nil, nil, false)
	if deps := c.collectDeps(ks); len(deps) != 0 {
		t.Errorf("Secret substituteFrom refs must NOT gate reconcile; got %+v", deps)
	}
}

// TestCollectDeps_SubstituteFromIgnoresMalformedRefs covers the
// defensive branches in collectDeps: refs with an unknown Kind or
// empty Name must be skipped rather than producing meaningless
// depwait edges that would never resolve.
func TestCollectDeps_SubstituteFromIgnoresMalformedRefs(t *testing.T) {
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: "Junk", Name: "x"},
			{Kind: manifest.KindConfigMap, Name: ""},
		},
	}
	c := New(store.New(), nil, nil, false)
	if deps := c.collectDeps(ks); len(deps) != 0 {
		t.Errorf("expected no deps from malformed substituteFrom refs; got %+v", deps)
	}
}

// mapResolver wraps a static parent map into the func resolver shape
// the controllers consume. Used by tests that want to verify
// behavior on a known parent index without standing up the full
// orchestrator wiring (which combines this with the renderedSet).
func mapResolver(m map[manifest.NamedResource]manifest.NamedResource) func(manifest.NamedResource) (manifest.NamedResource, bool) {
	return func(id manifest.NamedResource) (manifest.NamedResource, bool) {
		parent, ok := m[id]
		return parent, ok
	}
}
