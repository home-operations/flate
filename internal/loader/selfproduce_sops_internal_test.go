package loader

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFinalizeIndex_DeclaredOutputsStayUnpublished(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nresources:\n  - ./declared.yaml\n  - ./datafrom.yaml\n  - ./preexisting.yaml\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/declared.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app\nspec:\n  target:\n    name: app-secret\n    template:\n      data:\n        HOST: \"{{ .h }}\"\n        TOKEN: \"{{ .t }}\"\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/datafrom.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: dyn\nspec:\n  target:\n    name: dyn-secret\n  dataFrom:\n    - extract:\n        key: vault/dyn\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/preexisting.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: pre\nspec:\n  target:\n    name: pre-secret\n    template:\n      data:\n        K: \"{{ .k }}\"\n",
	)

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})
	existing := &manifest.Secret{Name: "pre-secret", Namespace: "secure", StringData: map[string]any{"K": "real"}}
	s.AddObject(existing)

	testutil.WriteFile(
		t,
		dir,
		"implicit/deep/declared.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: deferred}\nspec: {target: {name: deferred-secret, template: {data: {KEY: placeholder}}}}\n",
	)
	deferred := &manifest.Kustomization{
		Name:            "deferred",
		Namespace:       "flux-system",
		Path:            "./implicit",
		TargetNamespace: "secure",
	}
	s.AddObject(deferred)
	idx := rendererFixture(t, loaderFixture(s, nil), dir)
	assert.Diff(
		t,
		idx.Readers["implicit/deep/declared.yaml"],
		[]manifest.FileReader{{Reader: deferred.Named(), Source: manifest.BootstrapSourceID}},
	)
	assert.FatalIf(t, s.GetObject(secretID("secure", "deferred-secret")) != nil ||
		len(
			idx.Records[secretID("secure", "deferred-secret")].Productions,
		) != 0, "%s", "implicit producer was registered or synthesized before rendering")
	assert.FatalIf(t, s.GetObject(secretID("secure", "app-secret")) != nil ||
		len(
			s.Productions(secretID("secure", "app-secret"), idx),
		) == 0, "%s", "declared keys must retain production evidence without canonical publication")

	if obj := s.GetObject(secretID("secure", "dyn-secret")); obj != nil {
		t.Errorf("dataFrom-only ExternalSecret must NOT be synthesized; got %#v", obj)
	}
	if obj := s.GetObject(secretID("secure", "pre-secret")); obj != manifest.BaseManifest(existing) {
		t.Errorf("a real Secret on the target id must not be clobbered; got %#v", obj)
	}
}

func TestFinalizeIndex_DeclaredOutputRemainsAbsent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: secure\nresources:\n  - ./es.yaml\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/es.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata:\n  name: app\nspec:\n  target:\n    name: app-secret\n    template:\n      data:\n        TOKEN: \"{{ .t }}\"\n",
	)

	s := store.New()
	s.AddObject(&manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	})

	l := loaderFixture(s, nil)
	rendererFixture(t, l, dir)

	if obj := s.GetObject(secretID("secure", "app-secret")); obj != nil {
		t.Errorf("declared output must remain absent; got %#v", obj)
	}
}
