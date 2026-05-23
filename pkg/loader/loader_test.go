package loader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoader_Load(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
`)
	testutil.WriteFile(t, dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cm
  namespace: ns
data:
  k: v
`)
	testutil.WriteFile(t, dir, "README.md", "ignored")

	s := store.New()
	n, err := New(s).Load(t.Context(), dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 objects, got %d", n)
	}
	if got := len(s.ListObjects(manifest.KindKustomization)); got != 1 {
		t.Errorf("expected 1 Kustomization, got %d", got)
	}
}

func TestLoader_SkipsTemplatesDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "chart", "templates"), 0o750); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, dir, "chart/templates/cm.yaml", `{{ if .Values.x }}foo: bar{{ end }}`)
	testutil.WriteFile(t, dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
`)
	n, err := New(store.New()).Load(t.Context(), dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 (templates skipped), got %d", n)
	}
}

// A directory whose kustomization.yaml declares `kind: Component` is
// a template fragment — parents reference it via spec.components and
// kustomize materializes it at parent-render time. flate's standalone
// loader must skip such subtrees, otherwise unresolved template names
// (e.g. `${APP}-db`) land in the store as bogus standalone resources.
func TestLoader_SkipsKustomizeComponentSubtree(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/db/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n")
	testutil.WriteFile(t, dir, "components/db/template.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: "${APP}-db", namespace: ns}
spec:
  path: ./does/not/matter
  sourceRef: {kind: GitRepository, name: flux-system}
  interval: 1h
`)
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: real, namespace: ns}
`)
	s := store.New()
	n, err := New(s).Load(context.Background(), dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 object loaded (only the real ConfigMap); got %d", n)
	}
	if got := s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "${APP}-db"}); got != nil {
		t.Errorf("Component-subtree resource should not be loaded; got %v", got)
	}
}

// isKustomizeComponent must catch the JSON form too — a substring
// check on "kind: Component" against the file's first 256 bytes missed
// `kustomization.json` outright (the JSON form is "kind":"Component"
// without the YAML separator pattern).
func TestLoader_SkipsKustomizeComponent_JSONForm(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/foo/kustomization.json",
		`{"apiVersion":"kustomize.config.k8s.io/v1alpha1","kind":"Component"}`)
	testutil.WriteFile(t, dir, "components/foo/leak.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: "${APP}-leak", namespace: ns}
spec:
  path: ./irrelevant
  sourceRef: {kind: GitRepository, name: flux-system}
  interval: 1h
`)
	s := store.New()
	if _, err := New(s).Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "${APP}-leak"})
	if got != nil {
		t.Errorf("Component subtree leaked a templated KS: %v", got)
	}
}

// TestLoader_SkipsConfigMapGeneratorDataFile mirrors the home-ops
// repo shape that triggered #192: a `kustomization.yaml` declares a
// `configMapGenerator.files` entry pointing at a YAML data file
// whose top level is a sequence (e.g. webhook hook definitions).
// flate's loader walks every .yaml under --path, but this one is
// data — not a manifest — and must not trip the generic decode.
func TestLoader_SkipsConfigMapGeneratorDataFile(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./helmrelease.yaml
configMapGenerator:
  - name: notifier-configmap
    files:
      - hooks.yaml=./resources/hooks.yaml
`)
	testutil.WriteFile(t, dir, "helmrelease.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: notifier, namespace: default}
spec:
  chart:
    spec:
      chart: foo
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`)
	// Top-level YAML sequence — a valid data file but not a manifest.
	// Without the data-file pre-pass this trips
	// "cannot construct !!seq into map[string]interface {}".
	testutil.WriteFile(t, dir, "resources/hooks.yaml", `---
- id: radarr-pushover
  execute-command: /config/radarr-pushover.sh
- id: seerr-pushover
  execute-command: /config/seerr-pushover.sh
`)

	s := store.New()
	n, err := New(s).Load(context.Background(), dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Exactly one object loaded: the HelmRelease.
	if n != 1 {
		t.Errorf("expected 1 object loaded (HelmRelease only); got %d", n)
	}
	if got := s.GetObject(manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "notifier"}); got == nil {
		t.Errorf("HelmRelease should still load")
	}
}

// TestLoader_SkipsSecretGeneratorDataFile is the secretGenerator twin
// — same exclusion logic, different kustomize field.
func TestLoader_SkipsSecretGeneratorDataFile(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
secretGenerator:
  - name: tls-bundle
    files:
      - ca.crt=./data/ca.crt
    envs:
      - ./data/extra.env
`)
	// `.crt` extension — loader wouldn't try to parse it anyway, so
	// this just verifies the exclusion is harmless when extension
	// already excludes the file.
	testutil.WriteFile(t, dir, "data/ca.crt", "-----BEGIN CERTIFICATE-----\n")
	// `.env` is also not in manifestExtensions; same point.
	testutil.WriteFile(t, dir, "data/extra.env", "FOO=bar\n")
	// But: an arbitrary .yaml data file (e.g. a sealed-secret payload
	// chunk) WOULD be parsed without the exclusion.
	testutil.WriteFile(t, dir, "data/raw.yaml", "this: is: not: valid: yaml: ::\n")
	// Override kustomization to also use the .yaml data file so it
	// goes through the secretGenerator.files exclusion.
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
secretGenerator:
  - name: tls-bundle
    files:
      - raw.yaml=./data/raw.yaml
`)

	s := store.New()
	if _, err := New(s).Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

// TestLoader_GeneratorFilesKeyParsing covers the "KEY=PATH" entry
// shape: the loader must strip the optional `KEY=` prefix before
// resolving the path, otherwise the exclusion never matches and the
// data file goes through the generic decode.
func TestLoader_GeneratorFilesKeyParsing(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
configMapGenerator:
  - name: with-keys
    files:
      - keyed=./data/keyed.yaml
      - ./data/unkeyed.yaml
`)
	testutil.WriteFile(t, dir, "data/keyed.yaml", "- top: level\n- seq: here\n")
	testutil.WriteFile(t, dir, "data/unkeyed.yaml", "- another: top\n- level: seq\n")

	s := store.New()
	if _, err := New(s).Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// No objects added — both files excluded. The test passes as long
	// as Load returns nil (i.e. didn't hit a decode error on either).
}

// TestLoader_ResourceWinsOverGenerator pins the conflict-resolution
// rule from the design: if the same path is declared as both
// `resources:` AND `configMapGenerator.files:`, the resource
// interpretation wins. Pathological but legal per the kustomize spec.
func TestLoader_ResourceWinsOverGenerator(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./shared.yaml
configMapGenerator:
  - name: also-used-as-data
    files:
      - ./shared.yaml
`)
	testutil.WriteFile(t, dir, "shared.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: shared, namespace: ns}
`)
	s := store.New()
	if _, err := New(s).Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := s.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "shared"}); got == nil {
		t.Errorf("a path declared in both resources: and configMapGenerator must still load as a resource")
	}
}

// TestLoader_NestedKustomizationDataFile checks that a data file
// declared by a kustomization.yaml deep in the tree is excluded
// during the load of a higher-level --path. Same exclusion logic;
// just verifies the pre-pass actually walks recursively.
func TestLoader_NestedKustomizationDataFile(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/notifier/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
configMapGenerator:
  - name: cm
    files:
      - hooks.yaml=./resources/hooks.yaml
`)
	testutil.WriteFile(t, dir, "apps/notifier/resources/hooks.yaml", `- id: a\n- id: b\n`)
	testutil.WriteFile(t, dir, "apps/notifier/manifest.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: real, namespace: ns}
`)
	s := store.New()
	if _, err := New(s).Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "real"}) == nil {
		t.Errorf("the real manifest under the same kustomization dir must still load")
	}
}

// TestLoader_RespectsCanceledContext asserts the walk bails out on
// context cancellation. Useful when a stuck NFS mount or symlink
// loop would otherwise block Bootstrap indefinitely.
func TestLoader_RespectsCanceledContext(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "a.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := New(store.New()).Load(ctx, dir)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
