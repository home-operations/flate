package loader

import (
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func cmID(ns string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ns, Name: "cluster-settings"}
}

// The bjw-s/onedr0p topology: a root KS with a BARE spec.path (no
// kustomization.yaml) whose per-namespace subdir bases each stamp their own
// `namespace:` and pull in a shared substitutions component defining a
// namespace-LESS ConfigMap. The index must attribute that ConfigMap — in
// EACH group's resolved namespace — back to the root KS whose own render
// emits it, resolving the bare-dir → subdir-base → component → namespace
// chain that no path-prefix index can see.
func TestBuildSelfProduceIndex_BareDirComponentNamespace(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kubernetes/apps/flux-system/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\ncomponents:\n  - ../../components/substitutions\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/default/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: default\ncomponents:\n  - ../../components/substitutions\n")
	testutil.WriteFile(t, dir, "kubernetes/components/substitutions/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources:\n  - ./cluster-settings.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/components/substitutions/cluster-settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cluster-settings\ndata:\n  CLUSTER_NAME: home\n")

	s := store.New()
	clusterApps := &manifest.Kustomization{
		Name:              "cluster-apps",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./kubernetes/apps"},
	}
	s.AddObject(clusterApps)

	idx := BuildSelfProduceIndex(s, dir)

	// Produced in EVERY group's namespace (the namespace transformer stamps
	// the component's namespace-less ConfigMap per base) — not just the first.
	for _, ns := range []string{"flux-system", "default"} {
		if got := idx.ProducedBy(cmID(ns)); !slices.Contains(got, clusterApps.Named()) {
			t.Errorf("ProducedBy(ConfigMap/%s/cluster-settings) = %v, want to contain cluster-apps", ns, got)
		}
	}
	// A namespace no base stamps is not attributed.
	if got := idx.ProducedBy(cmID("kube-system")); len(got) != 0 {
		t.Errorf("ProducedBy(ConfigMap/kube-system/cluster-settings) = %v, want empty", got)
	}
}

// A substituteFrom ConfigMap defined OUTSIDE the KS's own render subtree —
// i.e. produced by a different KS — must NOT be attributed to this KS, so its
// dependency edge survives and a real failure stays loud. Here the root KS's
// spec.path holds only its own kustomization with no component, so it produces
// nothing.
func TestBuildSelfProduceIndex_NonProducerNotAttributed(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources: []\n")

	s := store.New()
	ks := &manifest.Kustomization{
		Name:              "cluster-apps",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./kubernetes/apps"},
	}
	s.AddObject(ks)

	idx := BuildSelfProduceIndex(s, dir)
	if got := idx.ProducedBy(cmID("flux-system")); len(got) != 0 {
		t.Errorf("ProducedBy = %v, want empty (KS produces no cluster-settings)", got)
	}
}

// Nil index is safe — collectDeps falls back to always-add (the pre-index
// behavior) when no repoRoot produced an index.
func TestSelfProduceIndex_NilSafe(t *testing.T) {
	var idx *SelfProduceIndex
	if got := idx.ProducedBy(cmID("flux-system")); got != nil {
		t.Errorf("nil index ProducedBy = %v, want nil", got)
	}
}
