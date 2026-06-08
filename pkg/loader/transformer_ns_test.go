package loader

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// namespaceTransformer is the shared builtin NamespaceTransformer
// flatops uses (kubernetes/transformers/transformer.yaml): it writes
// spec/targetNamespace onto every Kustomization.
const namespaceTransformer = `apiVersion: builtin
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
`

// writeFlatopsOverlay lays down the flatops namespace-overlay shape: an
// overlay kustomization.yaml that pulls in a transformers/ subtree whose
// `namespace:` directive feeds the shared NamespaceTransformer.
func writeFlatopsOverlay(t *testing.T, root, ns string) {
	t.Helper()
	writeFile(t, root, "apps/"+ns+"/kustomization.yaml", `resources:
  - ./namespace.yaml
  - ./`+ns+`/ks.yaml
transformers:
  - ./transformers
`)
	writeFile(t, root, "apps/"+ns+"/transformers/kustomization.yaml", `namespace: `+ns+`
resources:
  - ../../../transformers
`)
	writeFile(t, root, "transformers/kustomization.yaml", "resources:\n  - ./transformer.yaml\n")
	writeFile(t, root, "transformers/transformer.yaml", namespaceTransformer)
}

func leafKS(name, path string) *manifest.Kustomization {
	return &manifest.Kustomization{
		Name:              name,
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: path},
		Contents: map[string]any{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind":       "Kustomization",
			"metadata":   map[string]any{"name": name},
			"spec":       map[string]any{"path": path},
		},
	}
}

func TestStampTransformerTargetNamespaces_NamespaceTransformer(t *testing.T) {
	root := t.TempDir()
	writeFlatopsOverlay(t, root, "gpu-operator")

	s := store.New()
	ks := leafKS("gpu-operator", "./apps/gpu-operator/gpu-operator/app")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/gpu-operator/gpu-operator/ks.yaml",
	}

	StampTransformerTargetNamespaces(s, sourceFiles, root)

	got, ok := store.Get[*manifest.Kustomization](s, ks.Named())
	if !ok {
		t.Fatal("KS missing after stamp")
	}
	if got.TargetNamespace != "gpu-operator" {
		t.Errorf("TargetNamespace=%q want gpu-operator", got.TargetNamespace)
	}
	// RenderFlux feeds Contents to kustomize, so the raw doc must carry
	// targetNamespace too — not just the typed field.
	spec, _ := got.Contents["spec"].(map[string]any)
	if spec["targetNamespace"] != "gpu-operator" {
		t.Errorf("Contents spec.targetNamespace=%v want gpu-operator", spec["targetNamespace"])
	}
}

func TestStampTransformerTargetNamespaces_PlainNamespaceUntouched(t *testing.T) {
	root := t.TempDir()
	// A plain `namespace:` directive with NO NamespaceTransformer — the
	// existing metadata.namespace inheritance owns this; we must not
	// stamp targetNamespace.
	writeFile(t, root, "apps/plain/kustomization.yaml", `namespace: media
resources:
  - ./app/ks.yaml
`)

	s := store.New()
	ks := leafKS("plain", "./apps/plain/app/workload")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/plain/app/ks.yaml",
	}

	StampTransformerTargetNamespaces(s, sourceFiles, root)

	got, _ := store.Get[*manifest.Kustomization](s, ks.Named())
	if got.TargetNamespace != "" {
		t.Errorf("TargetNamespace=%q want empty (no NamespaceTransformer)", got.TargetNamespace)
	}
}

func TestStampTransformerTargetNamespaces_TransformersWithoutNamespaceTransformerIgnored(t *testing.T) {
	root := t.TempDir()
	// Overlay references a transformers subtree carrying a `namespace:`
	// directive, but the transformer is some OTHER builtin (here a
	// LabelTransformer) — not a NamespaceTransformer targeting
	// Kustomization spec/targetNamespace. Must not stamp.
	writeFile(t, root, "apps/other/kustomization.yaml", `resources:
  - ./other/ks.yaml
transformers:
  - ./transformers
`)
	writeFile(t, root, "apps/other/transformers/kustomization.yaml", `namespace: other
resources:
  - ./labels.yaml
`)
	writeFile(t, root, "apps/other/transformers/labels.yaml", `apiVersion: builtin
kind: LabelTransformer
metadata:
  name: add-labels
labels:
  team: platform
fieldSpecs:
  - path: metadata/labels
    create: true
`)

	s := store.New()
	ks := leafKS("other", "./apps/other/other/app")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/other/other/ks.yaml",
	}

	StampTransformerTargetNamespaces(s, sourceFiles, root)

	got, _ := store.Get[*manifest.Kustomization](s, ks.Named())
	if got.TargetNamespace != "" {
		t.Errorf("TargetNamespace=%q want empty (no NamespaceTransformer)", got.TargetNamespace)
	}
}

func TestStampTransformerTargetNamespaces_ExplicitTargetNamespacePreserved(t *testing.T) {
	root := t.TempDir()
	writeFlatopsOverlay(t, root, "gpu-operator")

	s := store.New()
	ks := leafKS("gpu-operator", "./apps/gpu-operator/gpu-operator/app")
	ks.TargetNamespace = "explicit" // already set in the source YAML
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/gpu-operator/gpu-operator/ks.yaml",
	}

	StampTransformerTargetNamespaces(s, sourceFiles, root)

	got, _ := store.Get[*manifest.Kustomization](s, ks.Named())
	if got.TargetNamespace != "explicit" {
		t.Errorf("TargetNamespace=%q want explicit (must not override)", got.TargetNamespace)
	}
}

func TestStampTransformerTargetNamespaces_DeepestOverlayWins(t *testing.T) {
	root := t.TempDir()
	// Outer overlay injects "outer"; an inner overlay nested under it
	// injects "inner". The KS lives under the inner overlay, so inner
	// (the deepest enclosing overlay) wins.
	writeFile(t, root, "apps/outer/kustomization.yaml", `resources:
  - ./inner
transformers:
  - ./transformers
`)
	writeFile(t, root, "apps/outer/transformers/kustomization.yaml", `namespace: outer
resources:
  - ../../../transformers
`)
	writeFile(t, root, "apps/outer/inner/kustomization.yaml", `resources:
  - ./app/ks.yaml
transformers:
  - ./transformers
`)
	writeFile(t, root, "apps/outer/inner/transformers/kustomization.yaml", `namespace: inner
resources:
  - ../../../../transformers
`)
	writeFile(t, root, "transformers/kustomization.yaml", "resources:\n  - ./transformer.yaml\n")
	writeFile(t, root, "transformers/transformer.yaml", namespaceTransformer)

	s := store.New()
	ks := leafKS("app", "./apps/outer/inner/app/workload")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/outer/inner/app/ks.yaml",
	}

	StampTransformerTargetNamespaces(s, sourceFiles, root)

	got, _ := store.Get[*manifest.Kustomization](s, ks.Named())
	if got.TargetNamespace != "inner" {
		t.Errorf("TargetNamespace=%q want inner (deepest overlay)", got.TargetNamespace)
	}
}

// nsReplacementRule is the home-operations replacement that copies a
// Namespace's name into every Flux Kustomization's spec.targetNamespace
// (kubernetes/components/replacements/ks.yaml). The file is a YAML list.
const nsReplacementRule = `- source:
    kind: Namespace
    fieldPath: metadata.name
  targets:
    - select:
        kind: Kustomization
        group: kustomize.toolkit.fluxcd.io
      fieldPaths:
        - spec.targetNamespace
      options:
        create: true
`

// stampReplacementsKS is the shared harness for the replacements-pattern
// tests: store a leaf KS under overlayDir/app, run the stamp, return the
// resolved TargetNamespace.
func stampReplacementsKS(t *testing.T, root, overlayDir, overlayKustomization string) *manifest.Kustomization {
	t.Helper()
	writeFile(t, root, overlayDir+"/kustomization.yaml", overlayKustomization)
	s := store.New()
	ks := leafKS("app", "./"+overlayDir+"/app/workload")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): overlayDir + "/app/ks.yaml",
	}
	StampTransformerTargetNamespaces(s, sourceFiles, root)
	got, _ := store.Get[*manifest.Kustomization](s, ks.Named())
	return got
}

// home-operations injects targetNamespace via `namespace:` + a
// `replacements:` file ref (not a NamespaceTransformer). The leaf KS must
// pick up the overlay's namespace as its targetNamespace.
func TestStampTransformerTargetNamespaces_ReplacementsFileRef(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "components/replacements/ks.yaml", nsReplacementRule)
	got := stampReplacementsKS(t, root, "apps/storage", `namespace: storage
replacements:
  - path: ../../components/replacements/ks.yaml
resources:
  - ./app/ks.yaml
`)
	if got.TargetNamespace != "storage" {
		t.Errorf("TargetNamespace=%q want storage", got.TargetNamespace)
	}
	// RenderFlux feeds Contents to kustomize, so the raw doc must carry it.
	spec, _ := got.Contents["spec"].(map[string]any)
	if spec["targetNamespace"] != "storage" {
		t.Errorf("Contents spec.targetNamespace=%v want storage", spec["targetNamespace"])
	}
}

// The replacement rule inlined directly under `replacements:` (no path ref).
func TestStampTransformerTargetNamespaces_ReplacementsInline(t *testing.T) {
	root := t.TempDir()
	got := stampReplacementsKS(t, root, "apps/storage", `namespace: storage
replacements:
  - source:
      kind: Namespace
      fieldPath: metadata.name
    targets:
      - select:
          kind: Kustomization
          group: kustomize.toolkit.fluxcd.io
        fieldPaths:
          - spec.targetNamespace
resources:
  - ./app/ks.yaml
`)
	if got.TargetNamespace != "storage" {
		t.Errorf("TargetNamespace=%q want storage", got.TargetNamespace)
	}
}

// A replacement that writes a DIFFERENT field (spec.path) must not be
// mistaken for a targetNamespace injection — no stamp.
func TestStampTransformerTargetNamespaces_ReplacementsNotTargetingTargetNamespace(t *testing.T) {
	root := t.TempDir()
	got := stampReplacementsKS(t, root, "apps/storage", `namespace: storage
replacements:
  - source:
      kind: Namespace
      fieldPath: metadata.name
    targets:
      - select:
          kind: Kustomization
          group: kustomize.toolkit.fluxcd.io
        fieldPaths:
          - spec.path
resources:
  - ./app/ks.yaml
`)
	if got.TargetNamespace != "" {
		t.Errorf("TargetNamespace=%q want empty (replacement not targeting targetNamespace)", got.TargetNamespace)
	}
}

// The targetNamespace value is derived from the overlay's `namespace:`
// directive (the Namespace the replacement reads is renamed to it). With no
// directive present, the value is unknown — skip rather than guess.
func TestStampTransformerTargetNamespaces_ReplacementsNoNamespaceDirective(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "components/replacements/ks.yaml", nsReplacementRule)
	got := stampReplacementsKS(t, root, "apps/storage", `replacements:
  - path: ../../components/replacements/ks.yaml
resources:
  - ./app/ks.yaml
`)
	if got.TargetNamespace != "" {
		t.Errorf("TargetNamespace=%q want empty (no namespace directive)", got.TargetNamespace)
	}
}

// Wrong source kind (ConfigMap, not Namespace) must not match.
func TestStampTransformerTargetNamespaces_ReplacementsWrongSourceKind(t *testing.T) {
	root := t.TempDir()
	got := stampReplacementsKS(t, root, "apps/storage", `namespace: storage
replacements:
  - source:
      kind: ConfigMap
      name: settings
      fieldPath: data.namespace
    targets:
      - select:
          kind: Kustomization
          group: kustomize.toolkit.fluxcd.io
        fieldPaths:
          - spec.targetNamespace
resources:
  - ./app/ks.yaml
`)
	if got.TargetNamespace != "" {
		t.Errorf("TargetNamespace=%q want empty (source not a Namespace)", got.TargetNamespace)
	}
}

// The replacement source may spell the path as `fieldPaths: [metadata.name]`
// (plural) rather than `fieldPath:` — accept both.
func TestStampTransformerTargetNamespaces_ReplacementsSourceFieldPathsPlural(t *testing.T) {
	root := t.TempDir()
	got := stampReplacementsKS(t, root, "apps/storage", `namespace: storage
replacements:
  - source:
      kind: Namespace
      fieldPaths:
        - metadata.name
    targets:
      - select:
          kind: Kustomization
          group: kustomize.toolkit.fluxcd.io
        fieldPaths:
          - spec.targetNamespace
resources:
  - ./app/ks.yaml
`)
	if got.TargetNamespace != "storage" {
		t.Errorf("TargetNamespace=%q want storage (plural source fieldPaths)", got.TargetNamespace)
	}
}

// Deepest enclosing overlay wins for the replacements pattern too.
func TestStampTransformerTargetNamespaces_ReplacementsDeepestOverlayWins(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "components/replacements/ks.yaml", nsReplacementRule)
	writeFile(t, root, "apps/outer/kustomization.yaml", `namespace: outer
replacements:
  - path: ../../components/replacements/ks.yaml
resources:
  - ./inner
`)
	writeFile(t, root, "apps/outer/inner/kustomization.yaml", `namespace: inner
replacements:
  - path: ../../../components/replacements/ks.yaml
resources:
  - ./app/ks.yaml
`)
	s := store.New()
	ks := leafKS("app", "./apps/outer/inner/app/workload")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/outer/inner/app/ks.yaml",
	}
	StampTransformerTargetNamespaces(s, sourceFiles, root)
	got, _ := store.Get[*manifest.Kustomization](s, ks.Named())
	if got.TargetNamespace != "inner" {
		t.Errorf("TargetNamespace=%q want inner (deepest overlay)", got.TargetNamespace)
	}
}
