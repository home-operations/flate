package orchestrator

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

// Prediction must precede the leaf's first render so children receive the
// overlay's target namespace even without a structural parent.
func TestOrchestrator_TransformerTargetNamespace(t *testing.T) {
	dir := t.TempDir()

	// Shared builtin NamespaceTransformer (kubernetes/transformers/).
	testutil.WriteFile(t, dir, "transformers/kustomization.yaml", "resources:\n  - ./transformer.yaml\n")
	testutil.WriteFile(t, dir, "transformers/transformer.yaml", `apiVersion: builtin
kind: NamespaceTransformer
metadata:
  name: apply-ns
  namespace: .invalid
fieldSpecs:
  - path: metadata/name
    kind: Namespace
    create: true
  - path: spec/targetNamespace
    group: kustomize.toolkit.fluxcd.io
    kind: Kustomization
    create: true
`)

	// Namespace overlay: pulls in the transformer subtree whose
	// `namespace:` directive feeds the NamespaceTransformer.
	testutil.WriteFile(t, dir, "apps/myapp/kustomization.yaml", `resources:
  - ./namespace.yaml
  - ./myapp/ks.yaml
transformers:
  - ./transformers
`)
	testutil.WriteFile(t, dir, "apps/myapp/namespace.yaml", `apiVersion: v1
kind: Namespace
metadata:
  name: .invalid
`)
	testutil.WriteFile(t, dir, "apps/myapp/transformers/kustomization.yaml", `namespace: myns
resources:
  - ../../../transformers
`)

	// Leaf Flux KS — no targetNamespace in the file; it is supplied by
	// the overlay's transformer. No other KS covers its path, so flate
	// has no structural parent to gate its first render on.
	testutil.WriteFile(t, dir, "apps/myapp/myapp/ks.yaml", testutil.KustomizationYAML("myapp", "flux-system", "  path: ./apps/myapp/myapp/app\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))
	testutil.WriteFile(t, dir, "apps/myapp/myapp/app/kustomization.yaml", "resources:\n  - ./helmrelease.yaml\n")
	// Namespace-less, suspended HR — suspend keeps the controller from
	// reaching for a chart, so the test asserts the rendered namespace
	// without needing a live Helm source.
	testutil.WriteFile(t, dir, "apps/myapp/myapp/app/helmrelease.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: myapp
spec:
  suspend: true
  chartRef:
    kind: OCIRepository
    name: myapp
`)

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(t.Context())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	canonical := testutil.KustomizationID("myns", "myapp")
	if o.store.GetObject(canonical) == nil {
		t.Error("transformer-projected Kustomization is missing")
	}
	stale := canonical
	stale.Namespace = "flux-system"
	if o.store.GetObject(stale) != nil {
		t.Error("authored Kustomization identity survived the namespace transformer")
	}

	namespaced := testutil.HelmReleaseID("myns", "myapp")
	if o.store.GetObject(namespaced) == nil {
		t.Errorf("expected HelmRelease at myns/myapp; HR objects in store: %v",
			hrIDs(o))
	}

	// The empty-namespace phantom must never reach the store.
	phantom := testutil.HelmReleaseID("", "myapp")
	if obj := o.store.GetObject(phantom); obj != nil {
		t.Errorf("empty-namespace HelmRelease phantom present in store: %v", phantom)
	}

	for id := range res.Failed {
		if id.Kind == manifest.KindHelmRelease {
			t.Errorf("unexpected HelmRelease failure: %s = %v", id, res.Failed[id])
		}
	}

	testutil.WriteFile(t, dir, "apps/shared/kustomization.yaml", "namespace: other\nresources: [../myapp/myapp/ks.yaml]\npatches:\n- target: {kind: Kustomization}\n  patch: |-\n    - op: add\n      path: /spec/targetNamespace\n      value: other\n")
	testutil.WriteFile(t, dir, "cluster/ks.yaml", testutil.KustomizationYAML("cluster-a", "flux-system", "  {path: ./apps/myapp}\n")+"---\n"+testutil.KustomizationYAML("cluster-b", "flux-system", "  {path: ./apps/shared}\n"))
	path := dir + "/cluster"
	o, err = New(t.Context(), Config{Path: path, RepoRoot: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	res, err = o.Render(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Failed) != 0 {
		t.Fatalf("shared-context failures: %v", res.Failed)
	}
	for _, ns := range []string{"myns", "other"} {
		for _, info := range reconcilableKinds {
			kind := info.Kind
			id := manifest.NamedResource{Group: info.Group, Kind: kind, Namespace: ns, Name: "myapp"}
			if o.store.GetObject(id) == nil {
				t.Errorf("shared-context render omitted %s", id)
			}
		}
	}

	t.Run("changed_only_second_source", func(t *testing.T) {
		testutil.WriteFile(t, dir, "apps/myapp/myapp/ks.yaml", testutil.KustomizationYAML("myapp", "flux-system", "  path: ./apps/myapp/myapp/app\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n"))
		for _, ns := range []string{"myns", "other"} {
			testutil.WriteFile(t, dir, "cluster/source-"+ns+".yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: `+ns+`}
spec:
  url: https://fixture.invalid/cluster
`)
		}
		const dirty = "cluster/source-other.yaml"
		o, err := New(t.Context(), Config{
			Path: path, RepoRoot: dir, WipeSecrets: true, Concurrency: 4,
			SelfURLs:        []string{"https://fixture.invalid/cluster"},
			ExternalChanges: change.NewSet([]string{dirty}),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := o.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Bootstrap: %v", err)
		}
		for _, ns := range []string{"myns", "other"} {
			source := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: ns, Name: "flux-system"}
			record, ok := o.index.Lookup(source)
			if !ok || record.File != "cluster/source-"+ns+".yaml" {
				t.Fatalf("source fixture %s: indexed=%t record=%+v", source, ok, record)
			}
			owners, ancestors := o.index.OwnersOfFile(record.File)
			t.Logf("source %s: File=%s owners=%v ancestors=%v", source, record.File, owners, ancestors)
			if len(owners) != 0 || len(ancestors) != 0 {
				t.Fatalf("source fixture %s has unrelated ownership selection", source)
			}
			id := testutil.KustomizationID(ns, "myapp")
			record, ok = o.index.Lookup(id)
			t.Logf("consumer %s: indexed=%t File=%s Authored=%s Refs=%v", id, ok, record.File, record.Authored, record.Refs)
			authored := testutil.KustomizationID("flux-system", "myapp")
			if !ok || record.File != "apps/myapp/myapp/ks.yaml" || record.Authored != authored || !slices.Equal(record.Refs, []manifest.NamedResource{source}) {
				t.Errorf("projected consumer %s: indexed=%t record=%+v", id, ok, record)
			}
		}
		other := testutil.KustomizationID("other", "myapp")
		t.Logf("dirty=%s ShouldReconcile(%s)=%t keep=%v", dirty, other, o.filter.ShouldReconcile(other), o.filter.KeepNames())
		if !o.filter.Enabled() || !o.filter.ShouldReconcile(other) {
			t.Errorf("second-source change did not select %s", other)
		}
		res, err := o.Render(t.Context())
		if err != nil || len(res.Failed) != 0 {
			t.Fatalf("changed-only Render: %v; failures=%v", err, res.Failed)
		}
		docs := outputByParent(res.Manifests)[other]
		if len(docs) != 1 || manifest.DocKind(docs[0]) != manifest.KindHelmRelease {
			t.Fatalf("rendered children of %s = %v, want one HelmRelease", other, docs)
		}
		if name, _ := manifest.DocMetadata(docs[0]); name != "myapp" {
			t.Errorf("rendered child name = %q, want myapp", name)
		}
	})
}

func hrIDs(o *Orchestrator) []string {
	var out []string
	for _, obj := range o.store.ListObjects(manifest.HelmReleaseDomain, manifest.KindHelmRelease) {
		out = append(out, obj.Named().String())
	}
	return out
}
