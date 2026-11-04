// Package substitution tests data reading, substitution and inherited patches.
package substitution_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ParentPatchesPropagateToChildren(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

	out := harness.RunCLI(t, "test", "ks", "--path", root)

	leafLine := harness.MustExtractLine(t, out, "flux-system/leaf")
	assert.ErrorIf(
		t,
		!strings.Contains(leafLine, "✓"),
		"leaf should pass once parent patches inject substituteFrom; got: %s",
		leafLine,
	)
	clusterLine := harness.MustExtractLine(t, out, "flux-system/cluster-apps")
	assert.ErrorIf(
		t,
		!strings.Contains(clusterLine, "✓"),
		"cluster-apps should pass; SOPS values are wiped to placeholder, not fail-loud; got: %s",
		clusterLine,
	)
	assert.ErrorIf(
		t,
		strings.Contains(out, `is undefined and has no default`),
		"no postBuild variable should be reported undefined:\n%s",
		out,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SOPSValueWipedToPlaceholder(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

	out := harness.RunCLIStdout(t, "build", "ks", "leaf", "-n", "flux-system",
		"--path", root, "--concurrency", "2", "-o", "yaml")
	again := harness.RunCLIStdout(t, "build", "ks", "leaf", "-n", "flux-system",
		"--path", root, "--concurrency", "2", "-o", "yaml")
	assert.FatalIf(t, again != out, "%v", "SOPS substitution output changed between runs")

	assert.ErrorIf(
		t,
		!strings.Contains(out, "secret: ..PLACEHOLDER_MY_SECRET.."),
		"expected SOPS-derived MY_SECRET to substitute to its placeholder:\n%s",
		out,
	)
	assert.ErrorIf(
		t,
		!strings.Contains(out, "value: from-cluster-settings"),
		"cleartext cluster-settings value should substitute normally:\n%s",
		out,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SubstituteDisabledAnnotation(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

	out := harness.RunCLI(t, "test", "ks", "--path", root)

	leafLine := harness.MustExtractLine(t, out, "flux-system/leaf")
	assert.ErrorIf(
		t,
		!strings.Contains(leafLine, "✓"),
		"leaf should pass — the script ConfigMap opts out of substitution; got: %s",
		leafLine,
	)
	assert.ErrorIf(
		t,
		strings.Contains(out, "missing closing brace"),
		"substitute-disabled annotation should prevent the parse error:\n%s",
		out,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
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
			assert.FatalIf(t, !strings.Contains(out, want), "missing substituted %q in %s", want, out)
		}
		assert.FatalIf(
			t,
			previous != "" && out != previous,
			"%v",
			"generator substitution output differs across concurrency levels",
		)
		previous = out
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SubstitutionPrecedence(t *testing.T) {
	root, external := t.TempDir(), t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./app\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: repository}\n",
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\ndata: {value: '${VALUE}'}\n",
	)
	testutil.WriteFile(
		t,
		external,
		"settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: private-value}\n",
	)
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			for range 2 {
				requireCLIOKResult1 := harness.RequireCLIOK(
					t,
					verb,
					"all",
					"--path",
					filepath.Join(root, "flux"),
					"--supply",
					filepath.Join(external, "settings.yaml"),
					"--concurrency=2",
					"--log-level=info",
				)
				stdout, stderr := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
				assert.FatalIf(t, strings.Count(stderr, "[SuppliedPrecedence]") != 1 ||
					!strings.Contains(stderr, "ConfigMap/apps/settings: VALUE") ||
					strings.Contains(stdout, "repository data takes precedence") ||
					strings.Contains(stdout+stderr, "private-value") ||
					strings.Contains(
						stderr,
						"level=WARN",
					), "precedence destination: stdout=%s stderr=%s", stdout, stderr)
				assert.FatalIf(t, verb == "build" && !strings.Contains(stdout, "value: repository") ||
					verb == "test" && !strings.Contains(stdout, "passed"), "precedence product: %s", stdout)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_UnappliedPackagePatches(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "unapplied", true: "applied"}[applied], func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			for _, file := range []struct{ path, body string }{
				{"cluster/app/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: app-cm
data:
  a: ${VAR_A}
  b: ${VAR_B}
`},
				{"cluster/app/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cm.yaml
`},
				{"cluster/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: app
  namespace: flux-system
spec:
  interval: 5m
  path: ./cluster/app
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`},
				{"cluster/vars/base.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  VAR_A: base-value
`},
				{"cluster/vars/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - base.yaml
patches:
  - path: patch.yaml
`},
				{"cluster/vars/patch.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  VAR_B: patch-value
`},
			} {
				testutil.WriteFile(t, root, file.path, file.body)
			}
			if applied {
				testutil.WriteFile(
					t,
					root,
					"cluster/vars-ks.yaml",
					testutil.KustomizationYAML(
						"vars",
						"flux-system",
						"  path: ./cluster/vars\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
					),
				)
			}
			wantValues := []string{"a: base-value", "b: null"}
			if applied {
				wantValues[1] = "b: patch-value"
			}
			var previous string
			for range 2 {
				out := harness.RunCLIStdout(
					t,
					"build",
					"ks",
					"app",
					"--path",
					filepath.Join(root, "cluster"),
					"--concurrency",
					"2",
					"--no-progress",
				)
				for _, want := range wantValues {
					assert.FatalIf(t, !strings.Contains(out, want), "missing %q in app output:\n%s", want, out)
				}
				assert.FatalIf(t, previous != "" && out != previous, "%v", "equivalent renders differ")
				previous = out
			}
		})
	}
}
