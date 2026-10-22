// Package substitution tests data reading, substitution and inherited patches.
package substitution

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_ParentPatchesPropagateToChildren(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

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
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

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
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

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
		{"bootstrap/ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: bootstrap-values}]\n")},
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

func TestE2E_SubstitutionPrecedence(t *testing.T) {
	root, external := t.TempDir(), t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./app\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n"))
	testutil.WriteFile(t, root, "flux/settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: repository}\n")
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\ndata: {value: '${VALUE}'}\n")
	testutil.WriteFile(t, external, "settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: private-value}\n")
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			for range 2 {
				stdout, stderr := harness.RequireCLIOK(t, verb, "all", "--path", filepath.Join(root, "flux"), "--substitute-from", filepath.Join(external, "settings.yaml"), "--concurrency=2", "--log-level=info")
				if strings.Count(stderr, "[SubstitutionPrecedence]") != 1 || !strings.Contains(stderr, "ConfigMap apps/settings") || strings.Contains(stdout, "repository substitution source takes precedence") || strings.Contains(stdout+stderr, "private-value") || strings.Contains(stderr, "level=WARN") {
					t.Fatalf("precedence destination: stdout=%s stderr=%s", stdout, stderr)
				}
				if verb == "build" && !strings.Contains(stdout, "value: repository") || verb == "test" && !strings.Contains(stdout, "passed") {
					t.Fatalf("precedence product: %s", stdout)
				}
			}
		})
	}
}
