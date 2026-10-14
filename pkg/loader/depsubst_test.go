package loader

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func ksWithDeps(name, ns string, sub map[string]any, depNames ...string) *manifest.Kustomization {
	deps := make([]kustomizev1.DependencyReference, len(depNames))
	for i, dn := range depNames {
		deps[i] = kustomizev1.DependencyReference{
			Namespace: ns, Name: dn,
		}
	}
	return &manifest.Kustomization{
		Name: name, Namespace: ns,
		PostBuildSubstitute: sub,
		DependsOn:           deps,
	}
}

func firstDepName(t *testing.T, s *store.Store, ns, name string) string {
	t.Helper()
	ks, ok := s.GetByName[*manifest.Kustomization](manifest.KindKustomization, ns, name)
	if !ok {
		t.Fatalf("Kustomization %s/%s missing from store", ns, name)
	}
	for ref := range manifest.References(ks) {
		if ref.Use == "dependsOn" {
			return ref.Name
		}
	}
	t.Fatalf("Kustomization %s/%s has no dependsOn", ns, name)
	return ""
}

// A bare ${VAR} in a child KS's dependsOn (no default) resolves to the
// real KS name once a discovered KS supplies the value — the rook-ceph
// `0-${CLUSTER_NAME}-config` case.
func TestResolveDependsOnSubstitutions_ResolvesBareVar(t *testing.T) {
	s := store.New()
	s.AddObject(ksWithDeps("0-biohazard-config", "flux-system", map[string]any{"CLUSTER_NAME": "biohazard"}))
	s.AddObject(ksWithDeps("rook-ceph-app", "flux-system", nil, "0-${CLUSTER_NAME}-config"))

	ResolveDependsOnSubstitutions(s)

	assert.Equal(t, firstDepName(t, s, "flux-system", "rook-ceph-app"), "0-biohazard-config")
	// Idempotent: a second pass is a no-op (resolved name has no ${).
	ResolveDependsOnSubstitutions(s)
	assert.Equal(t, firstDepName(t, s, "flux-system", "rook-ceph-app"), "0-biohazard-config")
}

// A var no KS supplies leaves envsubst.Eval in its error path, so the
// reference is kept verbatim rather than collapsed to empty.
func TestResolveDependsOnSubstitutions_UnsetVarStaysLiteral(t *testing.T) {
	s := store.New()
	s.AddObject(ksWithDeps("app", "flux-system", nil, "0-${UNKNOWN}-config"))

	ResolveDependsOnSubstitutions(s)

	assert.Equal(t, firstDepName(t, s, "flux-system", "app"), "0-${UNKNOWN}-config")
}

// A var declared with conflicting values across Kustomizations (a
// multi-cluster repo's per-cluster CLUSTER_NAME) is dropped from the
// union and left literal — never resolved to one cluster's value.
func TestResolveDependsOnSubstitutions_ConflictingVarDropped(t *testing.T) {
	s := store.New()
	s.AddObject(ksWithDeps("cfg-a", "prod", map[string]any{"CLUSTER_NAME": "prod"}))
	s.AddObject(ksWithDeps("cfg-b", "staging", map[string]any{"CLUSTER_NAME": "staging"}))
	s.AddObject(ksWithDeps("app", "flux-system", nil, "0-${CLUSTER_NAME}-config"))

	ResolveDependsOnSubstitutions(s)

	assert.Equal(t, firstDepName(t, s, "flux-system", "app"), "0-${CLUSTER_NAME}-config")
}

// A KS whose dependsOn carries no template is left untouched (no clone /
// no store churn), and a literal-but-equal redeclaration of the same var
// across KSes is not a conflict.
func TestResolveDependsOnSubstitutions_NoTemplateUntouchedAndAgreeingVarOK(t *testing.T) {
	s := store.New()
	s.AddObject(ksWithDeps("cfg-a", "flux-system", map[string]any{"CLUSTER_NAME": "biohazard"}))
	s.AddObject(ksWithDeps("cfg-b", "flux-system", map[string]any{"CLUSTER_NAME": "biohazard"})) // agrees, not a conflict
	s.AddObject(ksWithDeps("plain", "flux-system", nil, "0-biohazard-config"))
	s.AddObject(ksWithDeps("templated", "flux-system", nil, "0-${CLUSTER_NAME}-config"))

	ResolveDependsOnSubstitutions(s)

	assert.Equal(t, firstDepName(t, s, "flux-system", "plain"), "0-biohazard-config")
	assert.Equal(t, firstDepName(t, s, "flux-system", "templated"), "0-biohazard-config")
	withDeps := testing.AllocsPerRun(100, func() { ResolveDependsOnSubstitutions(s) })
	for _, ks := range s.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
		clone := ks.Clone()
		clone.DependsOn = nil
		s.AddObject(clone)
	}
	withoutDeps := testing.AllocsPerRun(100, func() { ResolveDependsOnSubstitutions(s) })
	assert.Equal(t, withDeps, withoutDeps)
}

func TestResolveDependsOnSubstitutions_PreservesAuthoredNamespaceAndStoredSpec(t *testing.T) {
	st := store.New()
	st.AddObject(ksWithDeps("settings", "apps", map[string]any{"APP": "backend", "NS": "shared"}))
	original := ksWithDeps("consumer", "apps", nil, "${APP}")
	original.DependsOn[0].Namespace = "${NS}"
	st.AddObject(original)
	ResolveDependsOnSubstitutions(st)
	updated, _ := st.Get[*manifest.Kustomization](original.Named())
	if updated == original || updated.DependsOn[0].Name != "backend" || updated.DependsOn[0].Namespace != "shared" {
		t.Fatalf("resolved dependency = %+v", updated.DependsOn)
	}
	if original.DependsOn[0].Name != "${APP}" || original.DependsOn[0].Namespace != "${NS}" {
		t.Fatalf("stored original spec mutated: %+v", original.DependsOn)
	}
}
