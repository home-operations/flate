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
