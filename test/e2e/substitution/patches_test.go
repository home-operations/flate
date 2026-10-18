// Package substitution tests data reading, substitution and inherited patches.
package substitution

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_ParentPatchesPropagateToChildren(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := harness.CopyTree(t, src)

	out := harness.RunCLI(t, "test", "ks", "--path", root)

	leafLine := harness.MustExtractLine(t, out, "flux-system/leaf")
	if !strings.Contains(leafLine, "✓") {
		t.Errorf("leaf should pass once parent patches inject substituteFrom; got: %s", leafLine)
	}
	clusterLine := harness.MustExtractLine(t, out, "flux-system/cluster-apps")
	if !strings.Contains(clusterLine, "✓") {
		t.Errorf("cluster-apps should pass; SOPS values are wiped to placeholder, not fail-loud; got: %s", clusterLine)
	}
	if strings.Contains(out, `is undefined and has no default`) {
		t.Errorf("no postBuild variable should be reported undefined:\n%s", out)
	}
}

func TestE2E_SOPSValueWipedToPlaceholder(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := harness.CopyTree(t, src)

	out := harness.RunCLIStdout(t, "build", "ks", "leaf", "-n", "flux-system",
		"--path", root, "-o", "yaml")

	if !strings.Contains(out, "secret: ..PLACEHOLDER_MY_SECRET..") {
		t.Errorf("expected SOPS-derived MY_SECRET to substitute to its placeholder:\n%s", out)
	}
	if !strings.Contains(out, "value: from-cluster-settings") {
		t.Errorf("cleartext cluster-settings value should substitute normally:\n%s", out)
	}
}

func TestE2E_SubstituteDisabledAnnotation(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := harness.CopyTree(t, src)

	out := harness.RunCLI(t, "test", "ks", "--path", root)

	leafLine := harness.MustExtractLine(t, out, "flux-system/leaf")
	if !strings.Contains(leafLine, "✓") {
		t.Errorf("leaf should pass — the script ConfigMap opts out of substitution; got: %s", leafLine)
	}
	if strings.Contains(out, "missing closing brace") {
		t.Errorf("substitute-disabled annotation should prevent the parse error:\n%s", out)
	}
}

func TestE2E_BootstrapGeneratorEnvAndFileSubstitution(t *testing.T) {
	root := t.TempDir()
	for _, file := range []struct{ path, body string }{
		{"kustomization.yaml", "namespace: flux-system\nresources: [bootstrap]\n"},
		{"bootstrap/kustomization.yaml", "resources: [ks.yaml]\ngeneratorOptions: {disableNameSuffixHash: true}\nconfigMapGenerator:\n- name: bootstrap-values\n  envs: [../settings.env]\n  files: [FROM_FILE=../payload.txt]\n  literals: [FROM_LITERAL=literal-value]\n"},
		{"settings.env", "FROM_ENV=env-value\n"},
		{"payload.txt", "file-value"},
		{"bootstrap/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild:
    substituteFrom: [{kind: ConfigMap, name: bootstrap-values}]
`},
		{"apps/kustomization.yaml", "resources: [result.yaml]\n"},
		{"apps/result.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: result}
data:
  env: "${FROM_ENV}"
  file: "${FROM_FILE}"
  literal: "${FROM_LITERAL}"
`},
	} {
		testutil.WriteFile(t, root, file.path, file.body)
	}
	var previous string
	for _, concurrency := range []string{"2", "8"} {
		out := harness.RunCLIStdout(t, "build", "all", "--path", root, "--concurrency", concurrency, "--no-progress")
		for _, want := range []string{"env-value", "file-value", "literal-value"} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing substituted %q in %s", want, out)
			}
		}
		if previous != "" && out != previous {
			t.Fatal("generator substitution output differs across concurrency levels")
		}
		previous = out
	}
}
