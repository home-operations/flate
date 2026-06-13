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

	idx := BuildSelfProduceIndex(s, dir, nil)

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

	idx := BuildSelfProduceIndex(s, dir, nil)
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

func secretID(ns, name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindSecret, Namespace: ns, Name: name}
}

// The discovery-time producer scan rides the same self-produce walk: an in-repo
// ExternalSecret / SealedSecret under a KS spec.path is recorded as
// target-Secret → producer, with the SAME effective-namespace resolution the
// ConfigMap path uses — an enclosing `namespace:` wins over the file's own.
func TestBuildSelfProduceIndex_RecordsProducers(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nresources:\n  - ./es.yaml\n  - ./sealed.yaml\n  - ./explicit.yaml\n  - ./obc.yaml\n")
	// ExternalSecret, no namespace in file → inherits `secure`; explicit target.
	testutil.WriteFile(t, dir, "apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app-creds\nspec:\n  target:\n    name: app-values\n")
	// SealedSecret with spec.template.metadata.name.
	testutil.WriteFile(t, dir, "apps/sealed.yaml",
		"apiVersion: bitnami.com/v1alpha1\nkind: SealedSecret\nmetadata:\n  name: db\nspec:\n  template:\n    metadata:\n      name: db-secret\n")
	// ExternalSecret carrying its own namespace — the transformer (secure)
	// overrides it, exactly as kustomize does.
	testutil.WriteFile(t, dir, "apps/explicit.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: own\n  namespace: ignored\nspec:\n  target:\n    name: own-values\n")
	// ObjectBucketClaim: its provisioner materializes a Secret AND a ConfigMap
	// both named after the claim, in the (transformer-resolved) namespace.
	testutil.WriteFile(t, dir, "apps/obc.yaml",
		"apiVersion: objectbucket.io/v1alpha1\nkind: ObjectBucketClaim\nmetadata:\n  name: media-bucket\nspec:\n  bucketName: media\n  storageClassName: ceph-bucket\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	})

	producers := &manifest.ProducerIndex{}
	BuildSelfProduceIndex(s, dir, producers)

	want := map[manifest.NamedResource]string{ // target → producer name
		secretID("secure", "app-values"): "app-creds",
		secretID("secure", "db-secret"):  "db",
		secretID("secure", "own-values"): "own",
		// The OBC produces BOTH a Secret and a ConfigMap named after the claim.
		secretID("secure", "media-bucket"):                                        "media-bucket",
		{Kind: manifest.KindConfigMap, Namespace: "secure", Name: "media-bucket"}: "media-bucket",
	}
	for target, prodName := range want {
		got, ok := producers.Producer(target)
		if !ok {
			t.Errorf("Producer(%v) missing; want %q", target, prodName)
			continue
		}
		if got.Name != prodName {
			t.Errorf("Producer(%v).Name = %q, want %q", target, got.Name, prodName)
		}
	}
}

// Caveat-as-test: the scan reads the producer's RAW target name; it does not
// apply a kustomize namePrefix/nameSuffix (the walker ignores those
// directives), so a prefixed target is recorded — and would be looked up —
// under the un-prefixed name. Producer-inference then misses the real
// (prefixed) Secret and the consumer falls back to fail-loud / the flag.
func TestBuildSelfProduceIndex_NamePrefixNotFollowed(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nnamePrefix: prod-\nresources:\n  - ./es.yaml\n")
	testutil.WriteFile(t, dir, "apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app-creds\nspec:\n  target:\n    name: app-values\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	})

	producers := &manifest.ProducerIndex{}
	BuildSelfProduceIndex(s, dir, producers)

	if _, ok := producers.Producer(secretID("secure", "app-values")); !ok {
		t.Error("producer not recorded under its raw target name")
	}
	// The materialized Secret would be prod-app-values; the scan does NOT
	// record that — pinning the documented namePrefix coverage gap.
	if _, ok := producers.Producer(secretID("secure", "prod-app-values")); ok {
		t.Error("scan unexpectedly followed namePrefix; the caveat test is stale")
	}
}
