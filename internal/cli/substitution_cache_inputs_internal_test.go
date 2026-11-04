package cli

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SubstitutionSecretMissingChartValue(t *testing.T) {
	root := substitutionFixture(t, "", "literal")
	testutil.WriteFile(
		t,
		root,
		"flux.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  {path: ./app, sourceRef: {kind: GitRepository, name: flux-system}, postBuild: {substituteFrom: [{kind: Secret, name: cluster-secrets}]}}\n",
		),
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml, hr.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/hr.yaml",
		"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec: {chart: {spec: {chart: charts/mychart, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}, values: {greeting: '${SECRET_X}'}}\n",
	)
	testutil.WriteFile(t, root, "charts/mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo-cm}\ndata: {greeting: {{ required \"greeting needed\" .Values.greeting | quote }}}\n",
	)
	input := filepath.Join(t.TempDir(), "sec.yaml")
	testutil.WriteFileAt(
		t,
		input,
		"apiVersion: v1\nkind: Secret\nmetadata: {name: cluster-secrets, namespace: flux-system}\nstringData: {OTHER: y}\n",
	)
	for _, supplied := range []bool{false, true} {
		t.Run(fmt.Sprintf("supplied=%t", supplied), func(t *testing.T) {
			args := []string{"test", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2"}
			if supplied {
				args = append(args, "--supply", input)
			}
			cli := runCLI(t, args...)
			out, diagnostic, code := cli.stdout, cli.stderr, cli.code
			assert.FatalIf(t, code != 1 || supplied && !strings.Contains(out+diagnostic, "greeting needed") ||
				!supplied &&
					!strings.Contains(out+diagnostic, "cluster-secrets"), "exit %d: %s %s", code, out, diagnostic)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_StrictSubstitutionCachedRender(t *testing.T) {
	root := substitutionFixture(t, "  postBuild: {substituteStrategy: Always}\n", "${VALUE}")
	cache := t.TempDir()
	args := []string{"build", "all", "--path", root, "--cache-dir", cache, "--concurrency", "2"}
	cli := runCLI(t, args...)
	first, diagnostic, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "%v", diagnostic)
	cli2 := runCLI(t, append(slices.Clone(args), "--strict-substitutions")...)
	diagnostic, code = cli2.stderr, cli2.code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(diagnostic, "variable not set (strict mode)"),
		"cached raw output bypassed strictness: %s",
		diagnostic,
	)
	cli3 := runCLI(t, args...)
	last, diagnostic, code := cli3.stdout, cli3.stderr, cli3.code
	assert.FatalIf(t, code != 0 || first != last, "strict run changed cached default output: %s", diagnostic)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SubstitutionCachedInputChanges(t *testing.T) {
	for _, tc := range []struct{ name, kind, input string }{
		{"ConfigMap", "ConfigMap", externalCM},
		{"Secret", "Secret", externalSecret},
	} {
		t.Run(tc.name, func(t *testing.T) {
			postBuild := "  postBuild: {}\n"
			if tc.kind != "" {
				postBuild = substitutionReference(tc.kind, false)
			}
			root := substitutionFixture(t, postBuild, "${VALUE}")
			cache, inputs := t.TempDir(), t.TempDir()
			var outputs []string
			for i, value := range []string{"first", "second", "second", "first"} {
				args := make([]string, 0, 11)
				args = append(args,
					"build",
					"all",
					"--path",
					root,
					"--cache-dir",
					cache,
					"--concurrency",
					"2",
					"--strict-substitutions",
				)
				testutil.WriteFile(t, inputs, "values.yaml", strings.Replace(tc.input, "supplied", value, 1))
				args = append(args, "--supply", filepath.Join(inputs, "values.yaml"))
				cli := runCLI(t, args...)
				out, diagnostic, code := cli.stdout, cli.stderr, cli.code
				assert.FatalIf(t, code != 0, "%v", diagnostic)
				{
					got := substitutionOutput(t, out)
					assert.FatalIf(t, got != value, "run=%d: cached value %q, want %q", i, got, value)
				}
				if outputs == nil {
					outputs = make([]string, 0, 4)
				}
				outputs = append(outputs, out)
			}
			assert.FatalIf(
				t,
				outputs[1] != outputs[2] || outputs[0] != outputs[3],
				"%v",
				"identical inputs changed warm-cache output bytes",
			)
		})
	}
}
