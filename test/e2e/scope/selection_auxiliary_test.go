package scope_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

type auxiliaryInputCase struct {
	name, kind, unrelatedKind, value string
	supply                           bool
}

func (tc auxiliaryInputCase) assertOutput(t *testing.T, verb, out, warnings string) {
	t.Helper()
	for _, excluded := range []string{"other/external", "unrelated", "unrelated-supply"} {
		if strings.Contains(out+warnings, excluded) {
			t.Fatalf("unrelated input escaped selection: %s\n%s", out, warnings)
		}
	}
	omitted := 1
	if tc.supply {
		omitted = 0
	}
	advisory := "[OmittedValues] HelmRelease apps/selected: required valuesFrom " + tc.kind +
		"/apps/external omitted from offline render"
	assert.Equal(t, strings.Count(warnings, advisory), omitted)
	if verb == "build" {
		docs, err := testutil.SplitDocs([]byte(out))
		assert.NoError(t, err)
		assert.Diff(t, docs, []map[string]any{
			{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      "selected-rendered",
					"namespace": "apps",
					"labels": map[string]any{
						"helm.toolkit.fluxcd.io/name":      "selected",
						"helm.toolkit.fluxcd.io/namespace": "apps",
					},
				},
				"data": map[string]any{"value": tc.value},
			},
		})
	} else {
		roster, verdict, _ := strings.Cut(strings.TrimSpace(out), "\n\n")
		counts, _, _ := strings.Cut(strings.TrimSpace(verdict), "   ")
		assert.Equal(t, roster, "✓  HelmRelease  apps/selected")
		assert.Equal(t, counts, "✓ 1 passed")
	}
}

func runAuxiliaryInputScope(t *testing.T, tc auxiliaryInputCase, args []string) {
	t.Helper()
	var priorOutput, priorWarnings string
	for run := range 2 {
		requireCLIOKResult := harness.RequireCLIOK(t, args...)
		out, warnings := requireCLIOKResult.Stdout, requireCLIOKResult.Stderr
		tc.assertOutput(t, args[0], out, warnings)
		if run != 0 {
			if args[0] == "build" {
				assert.Equal(t, out, priorOutput)
			}
			assert.Equal(t, warnings, priorWarnings)
		}
		priorOutput, priorWarnings = out, warnings
	}
}

func auxiliaryInputFlags(t *testing.T, tc auxiliaryInputCase) []string {
	t.Helper()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [selected.yaml, unrelated.yaml]\n")
	for _, release := range []struct{ name, namespace, kind string }{
		{"selected", "apps", tc.kind},
		{"unrelated", "other", tc.unrelatedKind},
	} {
		testutil.WriteFile(
			t,
			root,
			"flux/"+release.name+".yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: "+release.name+
				", namespace: "+release.namespace+"}\nspec:\n  releaseName: "+release.name+
				"\n  chart:\n    spec:\n      chart: ./charts/app\n      sourceRef: {kind:"+
				" GitRepository, name: flux-system, namespace: flux-system}\n  valuesFrom:"+
				" [{kind: "+release.kind+", name: external}]\n",
		)
	}
	testutil.WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "charts/app/values.yaml", "greeting: fallback\n")
	testutil.WriteFile(
		t,
		root,
		"charts/app/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-rendered\n"+
			"  namespace: {{ .Release.Namespace }}\ndata: {value: {{ .Values.greeting }}}\n",
	)
	flags := []string{
		"--path",
		filepath.Join(root, "flux"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	}
	runCLIBuffersResult := harness.RunCLIBuffers(append([]string{"build", "hr", "selected"}, flags...)...)
	diagnostic, code := runCLIBuffersResult.Stderr, runCLIBuffersResult.Code
	if code != 1 || !strings.Contains(diagnostic, "apps/external") {
		t.Fatalf("missing selected input did not fail: exit=%d stderr=%s", code, diagnostic)
	}
	if tc.supply {
		input := t.TempDir()
		testutil.WriteFile(
			t,
			input,
			"values.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: external, namespace: apps}\n"+
				"data: {values.yaml: 'greeting: supplied'}\n---\napiVersion: v1\nkind: ConfigMap\n"+
				"metadata: {name: external, namespace: other}\ndata: {values.yaml: 'greeting: unrelated-supply'}\n",
		)
		flags = append(flags, "--supply", filepath.Join(input, "values.yaml"))
	} else {
		flags = append(flags, "--allow-missing-kinds", tc.kind)
	}
	return flags
}
