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

// TestLoader_FollowsEscapingDirResource pins the Biohazard healthcheck
// fix: a directory listed under `resources:` via a parent-escaping
// path is descended into, so a Flux Kustomization defined there reaches
// the store even though nothing points spec.path at it and a tree walk
// would never reach it (it's only a kustomize resources: include).
func TestLoader_FollowsEscapingDirResource(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../gate/
`)
	testutil.WriteFile(t, root, "gate/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ks.yaml
`)
	testutil.WriteFile(t, root, "gate/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: crds
  namespace: flux-system
spec:
  path: ./blank
`)

	s := store.New()
	if _, err := New(s).Load(t.Context(), filepath.Join(root, "entry")); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "crds"}) == nil {
		t.Fatal("escaping dir resource not followed: Kustomization flux-system/crds missing from store")
	}
}

// TestLoader_EscapingFileResourceStillRejected guards the asymmetry at
// the heart of the fix: directory resources may escape (descend-only),
// but a parent-escaping FILE resource gets opened, so resolveDataPath's
// TOCTOU/path-traversal rejection must still apply.
func TestLoader_EscapingFileResourceStillRejected(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../escape.yaml
`)
	testutil.WriteFile(t, root, "escape.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: escaped
  namespace: flux-system
spec:
  path: ./x
`)

	s := store.New()
	if _, err := New(s).Load(t.Context(), filepath.Join(root, "entry")); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "escaped"}) != nil {
		t.Fatal("escaping FILE resource was loaded; the opened-path escape guard regressed")
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

// TestLoader_ForeignComponentKindIsNotSkipped pins the apiVersion
// gate on the Component detection: a CR with `kind: Component` but a
// non-kustomize apiVersion (e.g. someone's operator's custom CR) MUST
// NOT be treated as a kustomize Component fragment — the subtree
// would silently be skipped and any Flux resources inside vanish from
// the load. Use a kustomization.yaml as the carrier so descend's
// readKustomization picks it up.
func TestLoader_ForeignComponentKindIsNotSkipped(t *testing.T) {
	dir := t.TempDir()
	// Foreign Component CR shaped exactly like the kustomize one
	// except for apiVersion. With the kind-only check, descend would
	// short-circuit and the sibling KS would never load.
	testutil.WriteFile(t, dir, "addons/kustomization.yaml", `apiVersion: example.com/v1alpha1
kind: Component
resources:
  - ./ks.yaml
`)
	testutil.WriteFile(t, dir, "addons/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: addons, namespace: flux-system}
spec:
  path: ./addons
  sourceRef: {kind: GitRepository, name: flux-system}
  interval: 1h
`)
	s := store.New()
	if _, err := New(s).Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "addons"})
	if got == nil {
		t.Error("foreign 'kind: Component' (non-kustomize apiVersion) silently skipped sibling KS — Component gate fired without apiVersion check")
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
//
// manifest.yaml must be declared in resources for the loader to
// pick it up — the orphan-skip rule (issue #342) skips YAMLs in a
// kustomization-governed directory that aren't referenced. Without
// the resources entry, manifest.yaml would be a toggle-stub orphan
// and correctly excluded.
func TestLoader_NestedKustomizationDataFile(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/notifier/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./manifest.yaml
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

// TestLoader_OrphanNestedSubtreeSkipped pins the buroa/k8s-gitops
// case: a commented-out wrapper KS in `default/kustomization.yaml`
// (resources: [./valheim/ks.yaml, # ./atuin/ks.yaml]) should leave
// BOTH the wrapper AND the deep HelmRelease under
// `default/atuin/app/helmrelease.yaml` invisible. The wrapper lives
// in a directory (`atuin/`) that has no kustomization.yaml of its
// own; the HR lives in `atuin/app/` which DOES have one but is only
// reachable via the orphan wrapper's spec.path. PR #346 fixed only
// the same-dir orphan case; this test reproduces the deeper shape.
func TestLoader_OrphanNestedSubtreeSkipped(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "default/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./valheim/ks.yaml
  # - ./atuin/ks.yaml   # commented toggle stub — should be invisible
`)
	for _, app := range []string{"valheim", "atuin"} {
		testutil.WriteFile(t, dir, "default/"+app+"/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: `+app+`
spec:
  path: ./default/`+app+`/app
  sourceRef: { kind: GitRepository, name: flux-system, namespace: flux-system }
`)
		testutil.WriteFile(t, dir, "default/"+app+"/app/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./helmrelease.yaml
`)
		testutil.WriteFile(t, dir, "default/"+app+"/app/helmrelease.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: `+app+`
spec:
  chart:
    spec:
      chart: app-template
      sourceRef: { kind: HelmRepository, name: bjw-s, namespace: flux-system }
      version: "5.0.0"
`)
	}
	s := store.New()
	// Entry the loader at `default/` so its kustomization.yaml is the
	// graph root. The orphan-tree skip only applies under graph-walk;
	// see Loader.Load doc-comment.
	if _, err := New(s).Load(context.Background(), filepath.Join(dir, "default")); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Valheim (claimed) MUST be present.
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Name: "valheim"}) == nil {
		t.Error("claimed Kustomization 'valheim' missing")
	}
	// Atuin (commented out) MUST be absent.
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Name: "atuin"}) != nil {
		t.Error("orphan Kustomization 'atuin' should be skipped (#346 deep-orphan)")
	}
	// Atuin's deep HR MUST also be absent — its only kustomize-graph
	// connection is via the orphan wrapper.
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "atuin"}) != nil {
		t.Error("HelmRelease only reachable via orphan wrapper should be skipped")
	}
	// Valheim's HR should NOT load via the initial graph walk either
	// — it lives in `default/valheim/app/` which is only reachable
	// via the wrapper's spec.path, NOT via `default/kustomization.yaml`'s
	// resource graph. The orchestrator's discovery later calls
	// loadAt(valheim/app/) which becomes a fresh entry-point.
	// (This test stops before that step, so the HR is absent here —
	// which is the correct flux-local behavior.)
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "valheim"}) != nil {
		t.Error("HelmRelease in unreachable subtree leaked into initial walk")
	}
}

// TestLoader_OrphanYAMLSkipped pins the issue-#342 fix: a YAML
// file in a directory governed by a kustomization.yaml is loaded
// only when it appears in `resources:`. The unreferenced file (a
// "toggle stub" — common pattern where maintainers comment out
// resources entries to disable a wrapper without deleting the file)
// must NOT reach the store. Before the fix, the orphan was
// discovered and reconciled against the wrong overlay state,
// producing spurious "dependency not found" failures for any chart
// source the parent's namespace transform was supposed to inject.
func TestLoader_OrphanYAMLSkipped(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/games/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: games
resources:
  - ./valheim.yaml
  # - ./vrising.yaml   # disabled toggle stub
`)
	testutil.WriteFile(t, dir, "apps/games/valheim.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: valheim
spec:
  interval: 1h
  path: ./apps/base/games/valheim
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/games/vrising.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: vrising
spec:
  interval: 1h
  path: ./apps/base/games/vrising
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	s := store.New()
	if _, err := New(s).Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// valheim is referenced — must load. Kustomization namespace
	// is left empty by the parser (the YAML has no
	// metadata.namespace); the parent kustomization.yaml's
	// `namespace:` overlay would fill it at render time.
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Name: "valheim"}) == nil {
		t.Error("referenced Kustomization 'valheim' missing from store")
	}
	// vrising is NOT referenced — must be skipped as orphan.
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Name: "vrising"}) != nil {
		t.Error("orphan Kustomization 'vrising' should not be loaded (issue #342)")
	}
}

// TestLoader_DiscoveryOnlyRecordsComponentDataResources pins the
// Phase 1B contract: under DiscoveryOnly, kustomize Component
// subtrees still skip publishing standalone Flux CRs (the templated
// ones don't render outside their parent KS) but the loader walks
// the Component's `resources:` for ConfigMap/Secret data files and
// records them in Existence + SourceFiles. This is what lets the
// change filter's producer index resolve a downstream KS's
// substituteFrom dep to the unchanged renderer KS that owns the
// Component (issue #418).
func TestLoader_DiscoveryOnlyRecordsComponentDataResources(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/settings/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources:
  - ./cluster-settings.yaml
  - ./templated-ks.yaml
`)
	testutil.WriteFile(t, dir, "components/settings/cluster-settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  FOO: bar
`)
	testutil.WriteFile(t, dir, "components/settings/templated-ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: "${APP}-leak", namespace: ns}
spec:
  path: ./irrelevant
  sourceRef: {kind: GitRepository, name: flux-system}
  interval: 1h
`)
	s := store.New()
	l := New(s)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	l.SourceFiles = map[manifest.NamedResource]string{}
	l.SourceRoot = dir
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	if _, ok := l.Existence.Get(cmID); !ok {
		t.Errorf("Component-housed CM must be recorded in Existence under DiscoveryOnly")
	}
	if _, ok := l.SourceFiles[cmID]; !ok {
		t.Errorf("Component-housed CM must be recorded in SourceFiles under DiscoveryOnly")
	}
	if s.GetObject(cmID) != nil {
		t.Errorf("Component-housed CM must NOT be in Store under DiscoveryOnly (existence-only)")
	}
	// The templated KS would be filtered upstream by parseFile's
	// envsubst guard even if walkComponentData tried to publish it,
	// but this assertion pins the "Component CRs stay hidden" contract
	// at the boundary the caller cares about: the Store.
	leakID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "${APP}-leak"}
	if s.GetObject(leakID) != nil {
		t.Errorf("templated Flux CR inside a Component must NOT leak into the Store")
	}
}

// TestLoader_DiscoveryOnlyRecordsSourceRefs pins the reverse-edge
// plumbing: under DiscoveryOnly a HelmRelease never reaches the Store,
// yet its chart source reference must still be captured in SourceRefs
// so the change filter can re-render the HR when its (centralized)
// OCIRepository changes. The ref is read from the parse-time chart
// projection, not the Store.
func TestLoader_DiscoveryOnlyRecordsSourceRefs(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/envoy/helm-release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: envoy-gateway
  namespace: envoy-gateway
spec:
  chartRef:
    kind: OCIRepository
    name: envoy-gateway
    namespace: flux-system
`)
	s := store.New()
	l := New(s)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	l.SourceFiles = map[manifest.NamedResource]string{}
	l.SourceRefs = map[manifest.NamedResource][]manifest.NamedResource{}
	l.SourceRoot = dir
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	hrID := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "envoy-gateway", Name: "envoy-gateway"}
	if s.GetObject(hrID) != nil {
		t.Fatalf("HR must NOT be in Store under DiscoveryOnly (existence-only)")
	}
	refs, ok := l.SourceRefs[hrID]
	if !ok {
		t.Fatalf("HR chart source ref must be captured in SourceRefs; got %v", l.SourceRefs)
	}
	want := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "envoy-gateway"}
	if len(refs) != 1 || refs[0] != want {
		t.Errorf("SourceRefs[HR] = %v; want [%v]", refs, want)
	}
}

// TestLoader_DiscoveryOnlyRecordsNestedComponentData pins the
// Component-of-Component recursion in walkComponentData: an outer
// Component that references an inner Component via `components:`
// must surface the inner Component's CM/Secret data files into the
// producer index. The reference PR (tjwallace/flate#1) walked only
// `resources:` and missed this graph shape — this test is the fence.
func TestLoader_DiscoveryOnlyRecordsNestedComponentData(t *testing.T) {
	dir := t.TempDir()
	// Outer Component points at inner via `components:`. The path is
	// relative to the outer Component's directory; resolveComponentPath
	// allows the `..` escape that resolveDataPath forbids.
	testutil.WriteFile(t, dir, "components/outer/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
components:
  - ../inner
`)
	testutil.WriteFile(t, dir, "components/inner/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources:
  - ./shared.yaml
`)
	testutil.WriteFile(t, dir, "components/inner/shared.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: shared-cm
  namespace: flux-system
data:
  KEY: value
`)
	s := store.New()
	l := New(s)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	l.SourceFiles = map[manifest.NamedResource]string{}
	l.SourceRoot = dir
	// Drive the loader at the outer Component directly — same shape
	// as TestLoader_SkipsKustomizeComponentSubtree's entry pattern.
	if _, err := l.Load(context.Background(), filepath.Join(dir, "components/outer")); err != nil {
		t.Fatalf("Load: %v", err)
	}

	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "shared-cm"}
	if _, ok := l.Existence.Get(id); !ok {
		t.Errorf("inner Component CM reached via Component-of-Component must be recorded in Existence")
	}
	if _, ok := l.SourceFiles[id]; !ok {
		t.Errorf("inner Component CM must be recorded in SourceFiles")
	}
}

// TestLoader_DiscoveryOnlyComponentCycleTerminates pins the
// w.visited cycle protection in walkComponentData: a pair of
// Components that reference each other via `components:` must not
// infinite-loop. The same primitive descend uses; verify here so a
// future change can't drop it.
func TestLoader_DiscoveryOnlyComponentCycleTerminates(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/a/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
components:
  - ../b
`)
	testutil.WriteFile(t, dir, "components/b/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
components:
  - ../a
`)
	s := store.New()
	l := New(s)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	// No timeout context: if cycle protection fails, the test hangs
	// and the test framework's overall timeout surfaces the bug.
	if _, err := l.Load(context.Background(), filepath.Join(dir, "components/a")); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

// TestLoader_NonDiscoveryOnlyStillSkipsComponents is the pre-existing
// behavior fence: when DiscoveryOnly is OFF, Component subtrees stay
// fully invisible — no CM/Secret data files surface in Existence or
// SourceFiles. Without this fence the new walkComponentData gate
// could be loosened by mistake and silently leak Component-housed
// CMs into non-DiscoveryOnly Store-loading consumers.
func TestLoader_NonDiscoveryOnlyStillSkipsComponents(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/settings/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources:
  - ./cluster-settings.yaml
`)
	testutil.WriteFile(t, dir, "components/settings/cluster-settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  FOO: bar
`)
	s := store.New()
	l := New(s)
	// DiscoveryOnly intentionally OFF.
	l.Existence = NewExistenceIndex()
	l.SourceFiles = map[manifest.NamedResource]string{}
	l.SourceRoot = dir
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	if _, ok := l.Existence.Get(cmID); ok {
		t.Errorf("non-DiscoveryOnly walk must not record Component-housed CMs in Existence")
	}
	if _, ok := l.SourceFiles[cmID]; ok {
		t.Errorf("non-DiscoveryOnly walk must not record Component-housed CMs in SourceFiles")
	}
	if s.GetObject(cmID) != nil {
		t.Errorf("non-DiscoveryOnly walk must not publish Component-housed CMs to the Store")
	}
}

// TestLoader_DiscoveryOnlyPicksUpBootstrapSiblingKS pins the
// bootstrap-sibling scan: a Flux Kustomization CR authored in a YAML
// file next to a kustomization.yaml but NOT listed in its `resources:`
// is a real Flux pattern (the KS is kubectl-applied directly). Without
// the scan, that KS is invisible and the change filter has no parent
// to attribute file changes under spec.path to. Only Flux Kustomizations
// are picked up — a sibling HelmRelease must stay invisible, since
// extending the scan to every CR would silently relax the
// "kustomize package = resources only" contract for unrelated kinds.
func TestLoader_DiscoveryOnlyPicksUpBootstrapSiblingKS(t *testing.T) {
	dir := t.TempDir()
	// kustomization.yaml exists but does NOT list bootstrap.yaml.
	testutil.WriteFile(t, dir, "repositories/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./source.yaml
`)
	testutil.WriteFile(t, dir, "repositories/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: chart
  namespace: flux-system
spec:
  url: oci://example.com/chart
  ref:
    tag: 1.0.0
`)
	// The bootstrap-style Flux KS, sibling to kustomization.yaml but
	// outside its resources graph.
	testutil.WriteFile(t, dir, "repositories/flux-entry.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: flux-entry-repos
  namespace: flux-system
spec:
  path: ./repositories
  sourceRef:
    kind: GitRepository
    name: cluster
  interval: 10m
`)
	// A sibling HelmRelease should stay invisible — the scan is
	// scoped to Flux Kustomization CRs only.
	testutil.WriteFile(t, dir, "repositories/stray-hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: stray
  namespace: flux-system
spec:
  chartRef:
    kind: OCIRepository
    name: chart
    namespace: flux-system
`)

	s := store.New()
	l := New(s)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	l.SourceFiles = map[manifest.NamedResource]string{}
	l.SourceRoot = dir
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "flux-entry-repos"}
	if s.GetObject(ksID) == nil {
		t.Errorf("bootstrap-sibling Flux KS must reach the Store under DiscoveryOnly")
	}
	if got, want := l.SourceFiles[ksID], "repositories/flux-entry.yaml"; got != want {
		t.Errorf("SourceFiles[KS] = %q; want %q", got, want)
	}

	hrID := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "stray"}
	if s.GetObject(hrID) != nil {
		t.Errorf("sibling HR must NOT be picked up by the bootstrap scan (scope is Flux KS only)")
	}
	if _, ok := l.Existence.Get(hrID); ok {
		t.Errorf("sibling HR must NOT be recorded in Existence either — it's not in the kustomize graph")
	}
}

// TestLoader_BootstrapSiblingScanDedupesWithResources guards the
// "in BOTH places" edge case: a Flux KS file listed in resources AND
// sitting beside the kustomization.yaml must load exactly once. The
// dedup key is the resolved absolute path on each side.
func TestLoader_BootstrapSiblingScanDedupesWithResources(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./entry.yaml
`)
	testutil.WriteFile(t, dir, "entry.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: entry
  namespace: flux-system
spec:
  path: ./
  sourceRef:
    kind: GitRepository
    name: cluster
  interval: 10m
`)
	s := store.New()
	l := New(s)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	l.SourceFiles = map[manifest.NamedResource]string{}
	l.SourceRoot = dir
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := len(s.ListObjects(manifest.KindKustomization)); got != 1 {
		t.Errorf("expected exactly 1 Kustomization (no double-load), got %d", got)
	}
}

// TestLoader_NonDiscoveryOnlySkipsBootstrapSibling fences the gating
// rule: without DiscoveryOnly, sibling Flux KS files stay invisible —
// non-DiscoveryOnly callers (SDK consumers) opted into the strict
// "kustomize package = resources only" contract and a silent change
// in that contract would break them.
func TestLoader_NonDiscoveryOnlySkipsBootstrapSibling(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
`)
	testutil.WriteFile(t, dir, "flux-entry.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: bootstrap
  namespace: flux-system
spec:
  path: ./
  sourceRef:
    kind: GitRepository
    name: cluster
  interval: 10m
`)
	s := store.New()
	l := New(s) // DiscoveryOnly intentionally OFF.
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := len(s.ListObjects(manifest.KindKustomization)); got != 0 {
		t.Errorf("non-DiscoveryOnly must NOT pick up bootstrap-sibling KS, got %d in Store", got)
	}
}

// TestLoader_BootstrapSiblingHonorsKrmignore guards the ignore
// integration: a Flux KS sibling that matches a .krmignore pattern
// must NOT be loaded. Otherwise the bootstrap scan would silently
// override the user's explicit "don't look here" directive — the rest
// of the loader honors .krmignore for ad-hoc and resource paths.
func TestLoader_BootstrapSiblingHonorsKrmignore(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, ".krmignore", "flux-entry.yaml\n")
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
`)
	testutil.WriteFile(t, dir, "flux-entry.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: ignored
  namespace: flux-system
spec:
  path: ./
  sourceRef:
    kind: GitRepository
    name: cluster
  interval: 10m
`)
	s := store.New()
	l := New(s)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	l.SourceRoot = dir
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := len(s.ListObjects(manifest.KindKustomization)); got != 0 {
		t.Errorf("ignored bootstrap-sibling KS must NOT load, got %d in Store", got)
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
