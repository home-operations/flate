package scope_test

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_BootstrapSiblingNamespaceProjection(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [sibling.yaml]\ntransformers: [transformers]\n")
	testutil.WriteFile(
		t,
		root,
		"app/transformers/kustomization.yaml",
		"namespace: apps\nresources: [../../transformers]\n",
	)
	testutil.WriteFile(t, root, "transformers/kustomization.yaml", "resources: [transformer.yaml]\n")
	testutil.WriteFile(t, root, "transformers/transformer.yaml", `apiVersion: builtin
kind: NamespaceTransformer
metadata: {name: apply-ns, namespace: .invalid}
fieldSpecs:
  - path: metadata/name
    kind: Namespace
    create: true
  - path: spec/targetNamespace
    group: kustomize.toolkit.fluxcd.io
    kind: Kustomization
    create: true
`)
	testutil.WriteFile(
		t,
		root,
		"app/sibling.yaml",
		testutil.KustomizationYAML("sibling", "flux-system", "  path: ./leaf\n"),
	)
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\n")

	args := []string{
		"build",
		"all",
		"--path",
		filepath.Join(root, "app"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	}
	first, _ := harness.RequireCLIOK(t, args...)
	second, _ := harness.RequireCLIOK(t, args...)
	assert.FatalIf(t, first != second, "%v", "projected bootstrap output changed between runs")
	docs, err := testutil.SplitDocs([]byte(first))
	assert.NoError(t, err)
	found := false
	for _, doc := range docs {
		name, namespace := manifest.DocMetadata(doc)
		if manifest.DocKind(doc) == "ConfigMap" && name == "leaf" {
			assert.FatalIf(t, namespace != "apps", "leaf namespace = %q, want apps:\n%s", namespace, first)
			found = true
		}
	}
	assert.FatalIf(t, !found, "projected bootstrap root did not render its leaf:\n%s", first)
}
