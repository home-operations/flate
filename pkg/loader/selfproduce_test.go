package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func cmID(ns string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ns, Name: "cluster-settings"}
}

func TestFinalizeIndex_EmptyPathRequiresSource(t *testing.T) {
	for _, tc := range []struct {
		name, path, source string
		produces           bool
	}{
		{name: "stub"},
		{name: "source_root", source: "flux-system", produces: true},
		{name: "explicit_root", path: "./", produces: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cluster-settings}\n")
			st := store.New()
			ks := &manifest.Kustomization{
				Name: "root", Namespace: "flux-system", Path: tc.path,
				SourceKind: manifest.KindGitRepository, SourceName: tc.source, SourceNamespace: "flux-system",
			}
			st.AddObject(ks)
			l := loaderFixture(st, nil)
			l.Options.WipeSecrets = false
			idx := rendererFixture(t, l, root, nil)
			var want []manifest.NamedResource
			if tc.produces {
				want = []manifest.NamedResource{ks.Named()}
			}
			if got := renderers(idx, cmID("apps")); !slices.Equal(got, want) {
				t.Fatalf("ProducedBy = %v, want %v", got, want)
			}
			if got := ownersForTest(idx, "apps/cm.yaml"); !slices.Equal(got, want) {
				t.Fatalf("OwnersOfFile = %v, want %v", got, want)
			}
		})
	}
}

// The bjw-s/onedr0p topology: a root KS with a BARE spec.path (no
// kustomization.yaml) whose per-namespace subdir bases each stamp their own
// `namespace:` and pull in a shared substitutions component defining a
// namespace-LESS ConfigMap. The index must attribute that ConfigMap — in
// EACH group's resolved namespace — back to the root KS whose own render
// emits it, resolving the bare-dir → subdir-base → component → namespace
// chain that no path-prefix index can see.
func TestFinalizeIndex_BareDirComponentNamespace(t *testing.T) {
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
		Name:      "cluster-apps",
		Namespace: "flux-system",
		Path:      "./kubernetes/apps",
	}
	s.AddObject(clusterApps)

	idx := rendererFixture(t, loaderFixture(s, nil), dir, nil)

	// Produced in EVERY group's namespace (the namespace transformer stamps
	// the component's namespace-less ConfigMap per base) — not just the first.
	for _, ns := range []string{"flux-system", "default"} {
		if got := renderers(idx, cmID(ns)); !slices.Contains(got, clusterApps.Named()) {
			t.Errorf("ProducedBy(ConfigMap/%s/cluster-settings) = %v, want to contain cluster-apps", ns, got)
		}
	}
	// A namespace no base stamps is not attributed.
	if got := renderers(idx, cmID("kube-system")); len(got) != 0 {
		t.Errorf("ProducedBy(ConfigMap/kube-system/cluster-settings) = %v, want empty", got)
	}
}

// A substituteFrom ConfigMap defined OUTSIDE the KS's own render subtree —
// i.e. produced by a different KS — must NOT be attributed to this KS, so its
// dependency edge survives and a real failure stays loud. Here the root KS's
// spec.path holds only its own kustomization with no component, so it produces
// nothing.
func TestFinalizeIndex_NonProducerNotAttributed(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources: []\n")

	s := store.New()
	ks := &manifest.Kustomization{
		Name:      "cluster-apps",
		Namespace: "flux-system",
		Path:      "./kubernetes/apps",
	}
	s.AddObject(ks)

	idx := rendererFixture(t, loaderFixture(s, nil), dir, nil)
	if got := renderers(idx, cmID("flux-system")); len(got) != 0 {
		t.Errorf("ProducedBy = %v, want empty (KS produces no cluster-settings)", got)
	}
}

// Nil index is safe — collectDeps falls back to always-add (the pre-index
// behavior) when no repoRoot produced an index.
func TestDiscoveryIndex_NilSafe(t *testing.T) {
	var idx *manifest.DiscoveryIndex
	if got := declarationOwnersForTest(idx, manifest.NamedResource{}); got != nil {
		t.Errorf("nil index OwnersOfProducer = %v, want nil", got)
	}
	if got := renderers(idx, cmID("flux-system")); got != nil {
		t.Errorf("nil index ProducedBy = %v, want nil", got)
	}
	if _, ok := emissionParentForTest(idx, "apps/base/app-a/ks.yaml"); ok {
		t.Errorf("nil index EmissionParentByFile = ok, want absent")
	}
	if got := ownersForTest(idx, "apps/base/postgres/cluster.yaml"); got != nil {
		t.Errorf("nil index OwnersOfFile = %v, want nil", got)
	}
}

// TestFinalizeIndex_OwnersOfFileResourcesEscape reproduces #833: an
// overlay's kustomization.yaml pulls a resource in from outside its own
// spec.path via a `resources:` entry. OwnersOfFile must attribute every
// file the walk reads — both kustomization.yamls and the leaf resource —
// back to the owning top-level Kustomization.
func TestFinalizeIndex_OwnersOfFileResourcesEscape(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprintf("owners_%d", count), func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "apps/homelab/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres\n")
			testutil.WriteFile(t, dir, "apps/base/postgres/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cluster.yaml\n")
			testutil.WriteFile(t, dir, "apps/base/postgres/cluster.yaml",
				"apiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: postgres\n")

			s := store.New()
			var want []manifest.NamedResource
			for i := range count {
				ks := &manifest.Kustomization{Name: fmt.Sprintf("apps-%d", i), Namespace: "flux-system", Path: "./apps/homelab"}
				s.AddObject(ks)
				want = append(want, ks.Named())
			}

			idx := rendererFixture(t, loaderFixture(s, nil), dir, nil)

			for _, file := range []string{
				"apps/homelab/kustomization.yaml",
				"apps/base/postgres/kustomization.yaml",
				"apps/base/postgres/cluster.yaml",
			} {
				if got := idx.Readers[file]; !slices.Equal(got, want) {
					t.Errorf("Readers[%s] = %v, want %v", file, got, want)
				}
			}
			filter := change.NewFilter(change.NewSet([]string{"apps/base/postgres/cluster.yaml"}), idx, s)
			for _, id := range want {
				if !filter.ShouldReconcile(id) {
					t.Errorf("changed shared base did not select %s", id)
				}
			}
		})
	}
}

// TestFinalizeIndex_OwnersOfFileDirectFileEscape: an overlay lists an
// existing resource file directly, by a path escaping its own directory. Even
// though the escape-rejecting policy keeps it from being opened, it must still
// be attributed to the owning KS — the direct-file twin of the dir escape in
// #833. Otherwise changed-only mode omits the KS when that file is the only edit.
func TestFinalizeIndex_OwnersOfFileDirectFileEscape(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/homelab/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres/cluster.yaml\n")
	testutil.WriteFile(t, dir, "apps/base/postgres/cluster.yaml",
		"apiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: postgres\n")

	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps/homelab"}
	s.AddObject(ks)

	idx := rendererFixture(t, loaderFixture(s, nil), dir, nil)

	if got := ownersForTest(idx, "apps/base/postgres/cluster.yaml"); !slices.Contains(got, ks.Named()) {
		t.Errorf("OwnersOfFile(escaped file cluster.yaml) = %v, want to contain apps", got)
	}
}

// TestFinalizeIndex_OwnersOfFileDanglingResource: a resources: entry
// whose file is missing from this tree (deleted, entry left behind) must
// still be attributed to the Kustomization that references it. Otherwise
// changed-only mode finds no owner for the deleted path and silently skips
// the broken reference instead of rendering it and failing loud.
func TestFinalizeIndex_OwnersOfFileDanglingResource(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/homelab/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres\n")
	testutil.WriteFile(t, dir, "apps/base/postgres/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cluster.yaml\n")

	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps/homelab"}
	s.AddObject(ks)

	idx := rendererFixture(t, loaderFixture(s, nil), dir, nil)

	if got := ownersForTest(idx, "apps/base/postgres/cluster.yaml"); !slices.Contains(got, ks.Named()) {
		t.Errorf("OwnersOfFile(dangling cluster.yaml) = %v, want to contain apps", got)
	}
}

// TestFinalizeIndex_OwnersOfFileDanglingDirectory: the directory
// form of the dangling case. A resources: entry pointing at a directory
// that no longer exists records only the directory path, but change
// detection reports the deleted files individually — OwnersOfFile must
// resolve them through the recorded parent directory.
func TestFinalizeIndex_OwnersOfFileDanglingDirectory(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/homelab/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres\n")
	// A peer overlay directly references a deleted file beneath the deleted
	// directory: its exact-key owner must not mask the directory owner.
	testutil.WriteFile(t, dir, "apps/other/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../base/postgres/cluster.yaml\n")

	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps/homelab"}
	other := &manifest.Kustomization{Name: "other", Namespace: "flux-system", Path: "./apps/other"}
	s.AddObject(ks)
	s.AddObject(other)

	idx := rendererFixture(t, loaderFixture(s, nil), dir, nil)

	for _, file := range []string{
		"apps/base/postgres/kustomization.yaml",
		"apps/base/postgres/nested/cluster.yaml",
	} {
		if got := ownersForTest(idx, file); !slices.Contains(got, ks.Named()) {
			t.Errorf("OwnersOfFile(%s under deleted dir) = %v, want to contain apps", file, got)
		}
	}
	// Owned by both: other by its exact entry, apps through the deleted dir.
	if got := ownersForTest(idx, "apps/base/postgres/cluster.yaml"); !slices.Contains(got, ks.Named()) || !slices.Contains(got, other.Named()) {
		t.Errorf("OwnersOfFile(deleted cluster.yaml) = %v, want to contain both apps and other", got)
	}
	if got := ownersForTest(idx, "apps/base/redis/cm.yaml"); got != nil {
		t.Errorf("OwnersOfFile(unrelated file) = %v, want nil", got)
	}
}

// EmissionParentByFile records a base ks.yaml reached cross-tree through EXACTLY
// one parent's render subtree (apps/base/app-a/ks.yaml pulled in via
// apps/test/app-a -> ../../base/app-a, applied by cluster-apps) — the #777 gate
// signal. A base shared by two distinct parents is ambiguous and omitted.
func TestFinalizeIndex_EmissionParentByFile(t *testing.T) {
	dir := t.TempDir()

	// app-a: pulled by exactly one parent (cluster-apps). ${VAR} path so the
	// walk records only cluster-apps (no literal self-include).
	testutil.WriteFile(t, dir, "apps/test/kustomization.yaml", "resources:\n  - ./app-a\n  - ./shared\n")
	testutil.WriteFile(t, dir, "apps/test/app-a/kustomization.yaml", "resources:\n  - ../../base/app-a\n")
	testutil.WriteFile(t, dir, "apps/base/app-a/kustomization.yaml", "resources:\n  - ks.yaml\n")
	testutil.WriteFile(t, dir, "apps/base/app-a/ks.yaml",
		"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata:\n  name: app-a\n  namespace: flux-system\nspec:\n  path: ./apps/${CLUSTER_ENV}/app-a\n")
	// shared: pulled by BOTH cluster-apps (./apps/test -> ./shared) and
	// cluster-apps2 (./apps/test2) — two distinct parents, ambiguous.
	testutil.WriteFile(t, dir, "apps/test/shared/kustomization.yaml", "resources:\n  - ../../base/shared\n")
	testutil.WriteFile(t, dir, "apps/test2/kustomization.yaml", "resources:\n  - ../base/shared\n")
	testutil.WriteFile(t, dir, "apps/base/shared/kustomization.yaml", "resources:\n  - ks.yaml\n")
	testutil.WriteFile(t, dir, "apps/base/shared/ks.yaml",
		"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata:\n  name: shared\n  namespace: flux-system\nspec:\n  path: ./apps/${CLUSTER_ENV}/shared\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{Name: "cluster-apps", Namespace: "flux-system",
		Path: "./apps/test"})
	s.AddObject(&manifest.Kustomization{Name: "cluster-apps2", Namespace: "flux-system",
		Path: "./apps/test2"})

	l := loaderFixture(s, nil)
	for _, name := range []string{"app-a", "shared"} {
		id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
		l.Index.Records[id] = manifest.DiscoveryRecord{File: "apps/base/" + name + "/ks.yaml"}
	}
	l.FinalizeIndex(dir, nil)
	idx := l.Index

	clusterApps := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
	if got, ok := emissionParentForTest(idx, "apps/base/app-a/ks.yaml"); !ok || got != clusterApps {
		t.Errorf("EmissionParentByFile(app-a) = (%v, %v), want (cluster-apps, true)", got, ok)
	}
	if got, ok := emissionParentForTest(idx, "apps/base/shared/ks.yaml"); ok {
		t.Errorf("EmissionParentByFile(shared) = (%v, true), want absent (two distinct parents are ambiguous)", got)
	}
}

func secretID(ns, name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindSecret, Namespace: ns, Name: name}
}

// The discovery-time producer scan rides the same self-produce walk: an in-repo
// ExternalSecret / SealedSecret under a KS spec.path is recorded as
// target-Secret → producer, with the SAME effective-namespace resolution the
// ConfigMap path uses — an enclosing `namespace:` wins over the file's own.
func TestFinalizeIndex_RecordsProducers(t *testing.T) {
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
		Path: "./apps",
	})

	producers := &manifest.ProducerIndex{}
	rendererFixture(t, loaderFixture(s, nil), dir, producers)

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
func TestFinalizeIndex_NamePrefixNotFollowed(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nnamePrefix: prod-\nresources:\n  - ./es.yaml\n")
	testutil.WriteFile(t, dir, "apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app-creds\nspec:\n  target:\n    name: app-values\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	producers := &manifest.ProducerIndex{}
	rendererFixture(t, loaderFixture(s, nil), dir, producers)

	if _, ok := producers.Producer(secretID("secure", "app-values")); !ok {
		t.Error("producer not recorded under its raw target name")
	}
	// The materialized Secret would be prod-app-values; the scan does NOT
	// record that — pinning the documented namePrefix coverage gap.
	if _, ok := producers.Producer(secretID("secure", "prod-app-values")); ok {
		t.Error("scan unexpectedly followed namePrefix; the caveat test is stale")
	}
}

// An ExternalSecret that statically declares its output keys (target.template.data
// here) materializes a stand-in placeholder Secret in the store, so a downstream
// substituteFrom / valuesFrom resolves those keys to ..PLACEHOLDER_<key>.. rather
// than empty — flate's SOPS shape, applied to a secret whose keys are knowable
// but whose values are not. The scan never clobbers a real Secret already
// occupying the target id; a dataFrom-only producer declares nothing, so none is
// synthesized and it stays genuinely unreadable.
func TestFinalizeIndex_SynthesizesPlaceholderSecret(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nresources:\n  - ./declared.yaml\n  - ./datafrom.yaml\n  - ./preexisting.yaml\n")
	// Declares output keys via target.template.data → synthesized.
	testutil.WriteFile(t, dir, "apps/declared.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app\nspec:\n  target:\n    name: app-secret\n    template:\n      data:\n        HOST: \"{{ .h }}\"\n        TOKEN: \"{{ .t }}\"\n")
	// dataFrom-only → declares nothing → NOT synthesized.
	testutil.WriteFile(t, dir, "apps/datafrom.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: dyn\nspec:\n  target:\n    name: dyn-secret\n  dataFrom:\n    - extract:\n        key: vault/dyn\n")
	// Declares a key, but a real Secret already occupies the target id → never clobbered.
	testutil.WriteFile(t, dir, "apps/preexisting.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: pre\nspec:\n  target:\n    name: pre-secret\n    template:\n      data:\n        K: \"{{ .k }}\"\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})
	existing := &manifest.Secret{Name: "pre-secret", Namespace: "secure", StringData: map[string]any{"K": "real"}}
	s.AddObject(existing)

	rendererFixture(t, loaderFixture(s, nil), dir, &manifest.ProducerIndex{})

	got, _ := s.GetObject(secretID("secure", "app-secret")).(*manifest.Secret)
	if got == nil {
		t.Fatal("app-secret was not synthesized from its declared keys")
	}
	for _, k := range []string{"HOST", "TOKEN"} {
		if want := fmt.Sprintf(manifest.ValuePlaceholderTemplate, k); got.StringData[k] != want {
			t.Errorf("app-secret stringData[%s] = %v, want %q", k, got.StringData[k], want)
		}
	}
	if obj := s.GetObject(secretID("secure", "dyn-secret")); obj != nil {
		t.Errorf("dataFrom-only ExternalSecret must NOT be synthesized; got %#v", obj)
	}
	if obj := s.GetObject(secretID("secure", "pre-secret")); obj != manifest.BaseManifest(existing) {
		t.Errorf("a real Secret on the target id must not be clobbered; got %#v", obj)
	}
}

// A SOPS-encrypted Secret in a Kustomization's render subtree is materialized
// into the store under its rendered namespace, values wiped to placeholders — so
// a root Kustomization that substituteFroms a secret its OWN render produces (the
// cluster-secrets-in-a-substitutions-component self-produce pattern) resolves it
// at Prepare instead of seeing it absent and flagging it unreadable. A cleartext
// Secret carries no placeholders and is left alone.
func TestFinalizeIndex_MaterializesSopsSecret(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources:\n  - ./secret.sops.yaml\n  - ./plain.yaml\n")
	// The top-level sops block makes parseSecret wipe stringData to placeholders.
	testutil.WriteFile(t, dir, "apps/secret.sops.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata:\n  name: cluster-secrets\nstringData:\n  SECRET_DOMAIN: ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]\nsops:\n  mac: ENC[AES256_GCM,data:a,type:str]\n  version: 3.9.0\n")
	// Cleartext Secret → no placeholders → not materialized.
	testutil.WriteFile(t, dir, "apps/plain.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata:\n  name: plain-secret\nstringData:\n  TOKEN: hunter2\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	rendererFixture(t, loaderFixture(s, nil), dir, &manifest.ProducerIndex{})

	got, _ := s.GetObject(secretID("flux-system", "cluster-secrets")).(*manifest.Secret)
	if got == nil {
		t.Fatal("SOPS cluster-secrets was not materialized into the store")
	}
	if want := fmt.Sprintf(manifest.ValuePlaceholderTemplate, "SECRET_DOMAIN"); got.StringData["SECRET_DOMAIN"] != want {
		t.Errorf("materialized SECRET_DOMAIN = %v, want %q", got.StringData["SECRET_DOMAIN"], want)
	}
	if obj := s.GetObject(secretID("flux-system", "plain-secret")); obj != nil {
		t.Errorf("a cleartext Secret must not be materialized; got %#v", obj)
	}
}

// The placeholder Secret is a wipe-mode stand-in (a stand-in for a value flate
// can't read), so --no-wipe-secrets suppresses synthesis entirely — the secret
// is left genuinely absent, exactly as it was before this feature.
func TestFinalizeIndex_NoSynthesisWithoutWipe(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nresources:\n  - ./es.yaml\n")
	testutil.WriteFile(t, dir, "apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app\nspec:\n  target:\n    name: app-secret\n    template:\n      data:\n        TOKEN: \"{{ .t }}\"\n")

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	l := loaderFixture(s, nil)
	l.Options.WipeSecrets = false
	rendererFixture(t, l, dir, &manifest.ProducerIndex{})

	if obj := s.GetObject(secretID("secure", "app-secret")); obj != nil {
		t.Errorf("wipeSecrets=false must suppress synthesis; got %#v", obj)
	}
}

func TestFinalizeIndex_ProducerOwners(t *testing.T) {
	for _, namespace := range []string{"", "authored"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			root := t.TempDir()
			s := store.New()
			var want []manifest.NamedResource
			for _, name := range []string{"b", "a"} {
				ks := &manifest.Kustomization{Name: name, Namespace: "flux-system", Path: "./" + name}
				s.AddObject(ks)
				want = append(want, ks.Named())
				testutil.WriteFile(t, root, name+"/kustomization.yaml", "namespace: effective\nresources: [../shared, missing.yaml]\n")
			}
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [producer.yaml]\n")
			testutil.WriteFile(t, root, "shared/producer.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer, namespace: '"+namespace+"'}\nspec:\n  target: {name: values}\n  dataFrom: [{extract: {key: values}}]\n")
			producers := &manifest.ProducerIndex{}
			idx := rendererFixture(t, loaderFixture(s, nil), root, producers)
			slices.SortFunc(want, manifest.NamedResource.Compare)
			for _, ns := range []string{namespace, "effective"} {
				id := manifest.NamedResource{Kind: "ExternalSecret", Namespace: ns, Name: "producer"}
				got := declarationOwnersForTest(idx, id)
				slices.SortFunc(got, manifest.NamedResource.Compare)
				if !slices.Equal(got, want) {
					t.Fatalf("declaration %s owners = %v, want %v", id, got, want)
				}
				got[0] = manifest.NamedResource{}
				fresh := declarationOwnersForTest(idx, id)
				slices.SortFunc(fresh, manifest.NamedResource.Compare)
				if !slices.Equal(fresh, want) {
					t.Fatal("caller mutated index")
				}
			}
			if len(declarationOwnersForTest(idx, manifest.NamedResource{Kind: "ExternalSecret", Namespace: "foreign", Name: "producer"})) != 0 {
				t.Fatal("foreign namespace inherited declaration owners")
			}
			if got := renderers(idx, manifest.NamedResource{Kind: "ConfigMap", Namespace: "effective", Name: "values"}); len(got) != 0 {
				t.Fatalf("producer ownership changed self-substitution admission: %v", got)
			}
		})
	}
}

func TestFinalizeIndex_ProducerOwnersMultipleTargets(t *testing.T) {
	for _, namespace := range []string{"", "apps"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			root := t.TempDir()
			s := store.New()
			ks := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "./values"}
			s.AddObject(ks)
			testutil.WriteFile(t, root, "values/kustomization.yaml", "namespace: apps\nresources: [bucket.yaml]\n")
			testutil.WriteFile(t, root, "values/bucket.yaml", "apiVersion: objectbucket.io/v1alpha1\nkind: ObjectBucketClaim\nmetadata: {name: bucket, namespace: '"+namespace+"'}\nspec: {generateBucketName: bucket, storageClassName: bucket}\n")
			producers := &manifest.ProducerIndex{}
			idx := rendererFixture(t, loaderFixture(s, nil), root, producers)
			for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
				target := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "bucket"}
				if _, ok := producers.Producer(target); !ok {
					t.Fatalf("missing %s target", kind)
				}
			}
			for _, ns := range []string{namespace, "apps"} {
				producer := manifest.NamedResource{Kind: "ObjectBucketClaim", Namespace: ns, Name: "bucket"}
				if got := declarationOwnersForTest(idx, producer); !slices.Equal(got, []manifest.NamedResource{ks.Named()}) {
					t.Fatalf("repeated targets duplicate declaration owner: %v", got)
				}
			}
		})
	}
}

func TestSelfProduceBuilder_SecretPrecedence(t *testing.T) {
	for _, order := range []string{"placeholder first", "SOPS first", "two SOPS"} {
		t.Run(order, func(t *testing.T) {
			id := secretID("apps", "input")
			b := &selfProduceBuilder{store: store.New(), wipeSecrets: true, secrets: map[manifest.NamedResource]*manifest.Secret{}}
			first := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"first": "..PLACEHOLDER_first.."}}
			second := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"second": "..PLACEHOLDER_second.."}}
			want := first
			switch order {
			case "placeholder first":
				b.synthesizePlaceholderSecret(id, []string{"first"})
				want = second
				b.materializeSecret(second, "apps", "apps")
			case "SOPS first":
				b.materializeSecret(first, "apps", "apps")
				b.synthesizePlaceholderSecret(id, []string{"second"})
			case "two SOPS":
				want = second
				b.materializeSecret(first, "apps", "apps")
				b.materializeSecret(second, "apps", "apps")
			}
			assert.Diff(t, b.secrets[id], want)
		})
	}
}

func TestFinalizeIndex_MixedKustomizationDataFile(t *testing.T) {
	for _, mode := range []string{"mixed", "mixed_producer", "duplicate_record", "duplicate_store_object"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec: {path: ./apps}
`)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources: [bundle.yaml]\n")
			bundle := `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec: {path: ./child}
`
			if mode == "mixed_producer" {
				bundle += "---\napiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer}\nspec: {target: {name: values}}\n"
			} else {
				bundle += `---
apiVersion: v1
kind: ConfigMap
metadata: {name: values, namespace: apps}
data: {value: fixture}
`
			}
			testutil.WriteFile(t, root, "apps/bundle.yaml", bundle)
			if strings.HasPrefix(mode, "duplicate") {
				testutil.WriteFile(t, root, "flux/shadow.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {value: shadow}\n")
			}
			l := loaderFixture(store.New(), nil)
			l.Options.DiscoveryOnly = mode != "duplicate_store_object"
			l.PreferExisting = true
			l.SourceRoot = root
			for _, dir := range []string{"flux", "apps"} {
				if _, err := l.Load(t.Context(), filepath.Join(root, dir)); err != nil {
					t.Fatal(err)
				}
			}
			producers := &manifest.ProducerIndex{}
			l.FinalizeIndex(root, producers)
			owner := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "owner"}
			child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "child"}
			values := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}
			if record, _ := l.Index.Lookup(child); record.Parent != owner {
				t.Fatalf("child parent = %s, want %s", record.Parent, owner)
			}
			if mode == "mixed_producer" {
				if _, ok := producers.Producer(secretID("apps", "values")); !ok {
					t.Fatal("mixed-file raw producer is missing")
				}
			} else if record, _ := l.Index.Lookup(values); !slices.Contains(record.Renderers, owner) {
				t.Fatalf("mixed-file data renderers = %v, want %s", record.Renderers, owner)
			}
		})
	}
}

func TestLoader_DotDotPaths(t *testing.T) {
	for _, tc := range []struct {
		path     string
		accepted bool
	}{
		{path: "..foo", accepted: true},
		{path: "..foo/bar", accepted: true},
		{path: "..foo/../../outside"},
		{path: "a/../../x"},
		{path: "./../x"},
		{path: ".."},
		{path: "../x"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "repo")
			dir := filepath.Join(root, tc.path)
			testutil.WriteFile(t, dir, "kustomization.yaml", "configMapGenerator:\n- name: settings\n  literals: [key=value]\n")
			documents := documentReader{cache: map[string]document{}}
			documents.read(dir)

			l := &Loader{documents: documents}
			var wantRoots []string
			if tc.accepted {
				wantRoots = []string{tc.path}
			}
			if got := l.standaloneRoots(root, nil); !slices.Equal(got, wantRoots) {
				t.Errorf("standaloneRoots = %v, want %v", got, wantRoots)
			}

			w := graphWalker{documents: documents, visited: map[graphContext]struct{}{}}
			var files []string
			if !w.walk(graphVisit{root: root, dir: tc.path}, func(v graphVisit) bool {
				files = append(files, v.file)
				return true
			}) {
				t.Fatal("walk stopped before completing")
			}
			var wantFiles []string
			if tc.accepted {
				wantFiles = []string{filepath.Join(dir, "kustomization.yaml")}
			}
			if !slices.Equal(files, wantFiles) {
				t.Errorf("walk files = %v, want %v", files, wantFiles)
			}

			wantRef := ""
			if tc.accepted {
				wantRef = tc.path
			}
			if got, ok := resolveRef(".", tc.path); ok != tc.accepted || got != wantRef {
				t.Errorf("resolveRef = (%q, %v), want (%q, %v)", got, ok, wantRef, tc.accepted)
			}
		})
	}
}
