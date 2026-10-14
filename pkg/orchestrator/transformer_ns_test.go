package orchestrator

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// TestOrchestrator_TransformerTargetNamespace reproduces issue #528: a
// namespace-less HelmRelease under a leaf Flux Kustomization whose
// namespace is supplied by a builtin NamespaceTransformer (flatops
// pattern). The leaf KS has no file-loaded structural parent, so its
// first reconcile fires before any render could inject targetNamespace.
//
// Pre-fix: the leaf renders with targetNamespace="" and emits an
// empty-namespace HelmRelease that lingers in the store. Post-fix:
// StampTransformerTargetNamespaces resolves the NamespaceTransformer's
// namespace at load time and stamps it onto the leaf KS, so the render
// produces exactly one correctly-namespaced HelmRelease.
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
	testutil.WriteFile(t, dir, "apps/myapp/myapp/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: myapp
  namespace: flux-system
spec:
  path: ./apps/myapp/myapp/app
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
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

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	namespaced := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "myns", Name: "myapp"}
	if o.store.GetObject(namespaced) == nil {
		t.Errorf("expected HelmRelease at myns/myapp; HR objects in store: %v",
			hrIDs(o))
	}

	// The empty-namespace phantom must never reach the store.
	phantom := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "", Name: "myapp"}
	if obj := o.store.GetObject(phantom); obj != nil {
		t.Errorf("empty-namespace HelmRelease phantom present in store: %v", phantom)
	}

	for id := range res.Failed {
		if id.Kind == manifest.KindHelmRelease {
			t.Errorf("unexpected HelmRelease failure: %s = %v", id, res.Failed[id])
		}
	}
}

func hrIDs(o *Orchestrator) []string {
	var out []string
	for _, obj := range o.store.ListObjects(manifest.KindHelmRelease) {
		out = append(out, obj.Named().String())
	}
	return out
}
