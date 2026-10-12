package loader

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

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

	if got := firstDepName(t, s, "flux-system", "rook-ceph-app"); got != "0-biohazard-config" {
		t.Errorf("dependsOn name = %q, want 0-biohazard-config", got)
	}
	// Idempotent: a second pass is a no-op (resolved name has no ${).
	ResolveDependsOnSubstitutions(s)
	if got := firstDepName(t, s, "flux-system", "rook-ceph-app"); got != "0-biohazard-config" {
		t.Errorf("second pass changed result: %q", got)
	}
}

// A var no KS supplies leaves envsubst.Eval in its error path, so the
// reference is kept verbatim rather than collapsed to empty.
func TestResolveDependsOnSubstitutions_UnsetVarStaysLiteral(t *testing.T) {
	s := store.New()
	s.AddObject(ksWithDeps("app", "flux-system", nil, "0-${UNKNOWN}-config"))

	ResolveDependsOnSubstitutions(s)

	if got := firstDepName(t, s, "flux-system", "app"); got != "0-${UNKNOWN}-config" {
		t.Errorf("unset var must stay literal; got %q", got)
	}
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

	if got := firstDepName(t, s, "flux-system", "app"); got != "0-${CLUSTER_NAME}-config" {
		t.Errorf("conflicting var must be dropped (stay literal); got %q", got)
	}
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

	if got := firstDepName(t, s, "flux-system", "plain"); got != "0-biohazard-config" {
		t.Errorf("literal dependsOn must be untouched; got %q", got)
	}
	if got := firstDepName(t, s, "flux-system", "templated"); got != "0-biohazard-config" {
		t.Errorf("agreeing var must still resolve; got %q", got)
	}
}

func TestResolveDependsOnSubstitutions_PreservesAuthoredNamespaceAndStoredSpec(t *testing.T) {
	st := store.New()
	st.AddObject(ksWithDeps("settings", "apps", map[string]any{"APP": "backend", "NS": "shared"}))
	original := &manifest.Kustomization{
		Name: "consumer", Namespace: "apps",
		DependsOn: []kustomizev1.DependencyReference{
			{Name: "${APP}", ReadyExpr: "dep.metadata.name == 'backend'"},
			{Name: "database", Namespace: "${NS}"},
		},
	}
	st.AddObject(original)
	ResolveDependsOnSubstitutions(st)
	updated, ok := st.Get[*manifest.Kustomization](original.Named())
	if !ok || updated == original {
		t.Fatal("resolved Kustomization was not replaced")
	}
	if got := original.DependsOn; got[0].Name != "${APP}" || got[0].Namespace != "" || got[1].Namespace != "${NS}" {
		t.Fatalf("stored original spec mutated: %+v", got)
	}
	if got := updated.DependsOn; got[0].Name != "backend" || got[0].Namespace != "" || got[1].Namespace != "shared" {
		t.Fatalf("authored dependency projection = %+v", got)
	}
	var refs []manifest.DependencyRef
	for ref := range manifest.References(updated) {
		if ref.Use == "dependsOn" {
			refs = append(refs, ref)
		}
	}
	if len(refs) != 2 || refs[0].Namespace != "apps" || refs[1].Namespace != "shared" || refs[0].ReadyExpr != "dep.metadata.name == 'backend'" {
		t.Fatalf("effective dependency references = %+v", refs)
	}
}
