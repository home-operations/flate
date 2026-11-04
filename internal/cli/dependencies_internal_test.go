package cli

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GeneratedValuesDirectDependentAndKS(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteGeneratedValuesCluster(t, dir)
	cache := t.TempDir()
	for _, workers := range []string{"1", "2", "4"} {
		for _, tc := range []struct{ kind, name string }{{"hr", "a"}, {"hr", "b"}, {"ks", "apps"}} {
			t.Run(
				workers+"/"+tc.kind+"/"+tc.name,
				func(t *testing.T) {
					runGeneratedValuesDirectDependentAndKSCase(t, &tc, &dir, struct {
						cache   *string
						workers *string
					}{cache: &cache, workers: &workers})
				},
			)
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiffExcludedOrderingAndSelectedSourceFailure(t *testing.T) {
	for _, kind := range []string{"hr", "ks"} {
		for _, broken := range []bool{false, true} {
			name := "healthy"
			if broken {
				name = "selected source failure"
			}
			t.Run(
				fmt.Sprintf("%s/%s", kind, name),
				func(t *testing.T) { runDiffExcludedOrderingAndSelectedSourceFailureCase(t, &broken, &kind) },
			)
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Diff_UnchangedInputProducerOutput(t *testing.T) {
	before, after := t.TempDir(), t.TempDir()
	for _, root := range []string{before, after} {
		testutil.GitInit(t, root)
		testutil.WriteFile(
			t,
			root,
			"flux/inputs.yaml",
			testutil.KustomizationYAML("inputs", "apps", "  path: ./inputs\n  targetNamespace: apps\n"),
		)
		testutil.WriteFile(
			t,
			root,
			"flux/app.yaml",
			testutil.KustomizationYAML(
				"app",
				"apps",
				"  path: ./app\n  targetNamespace: apps\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: vars}]}\n",
			),
		)
		testutil.WriteFile(t, root, "inputs/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"inputs/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: apps}\ndata: {VALUE: stable}\n",
		)
		testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: '${VALUE}'}\n",
		)
	}
	testutil.WriteFile(
		t,
		after,
		"flux/app.yaml",
		testutil.KustomizationYAML(
			"app",
			"apps",
			"  path: ./app\n  targetNamespace: apps\n  postBuild: {substitute: {VALUE: stable}}\n",
		),
	)
	var full []string
	for _, root := range []string{before, after} {
		cli := runCLI(
			t,
			"build",
			"ks",
			"app",
			"--path",
			filepath.Join(root, "flux"),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		output, diagnostic, code := cli.stdout, cli.stderr, cli.code
		assert.FatalIf(t, code != 0, "full render failed: %s", diagnostic)
		if full == nil {
			full = make([]string, 0, 2)
		}
		full = append(full, output)
	}
	if full[0] != full[1] {
		t.Fatalf("full outputs differ: before=%s after=%s", full[0], full[1])
	}
	cli2 := runCLI(
		t,
		"diff",
		"ks",
		"app",
		"--path",
		filepath.Join(after, "flux"),
		"--path-orig",
		filepath.Join(before, "flux"),
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"-o",
		"diff",
	)
	output, diagnostic, code := cli2.stdout, cli2.stderr, cli2.code
	assert.FatalIf(
		t,
		code != 0 || output != "",
		"unchanged input producer appeared in diff: code=%d stdout=%s stderr=%s",
		code,
		output,
		diagnostic,
	)
}

func runGeneratedValuesDirectDependentAndKSCase(t *testing.T, tc *struct {
	kind string
	name string
}, dir *string, capturedInputs struct {
	cache   *string
	workers *string
},
) {
	t.Helper()

	cli := runCLI(
		t,
		"build",
		tc.kind,
		tc.name,
		"--path",
		(*dir),
		"--cache-dir",
		(*capturedInputs.cache),
		"--concurrency",
		(*capturedInputs.workers),
		"-o",
		"json",
	)
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "build exited %d: %s", code, stderr)
	var docs []map[string]any
	assert.NoError(t, json.Unmarshal([]byte(stdout), &docs))
	found := false
	for _, doc := range docs {
		meta, _ := doc["metadata"].(map[string]any)
		if tc.kind == "hr" && doc["kind"] == "ConfigMap" && meta["name"] == tc.name+"-rendered" {
			data := doc["data"].(map[string]any)
			assert.FatalIf(
				t,
				data["greeting"] != "hello" || data["replicas"] != "2",
				"effective values differ: %v",
				data,
			)
			found = true
		}
		if tc.kind == "ks" && doc["kind"] == "HelmRelease" && meta["name"] == "a" {
			spec := doc["spec"].(map[string]any)
			refs := spec["valuesFrom"].([]any)
			name := refs[0].(map[string]any)["name"].(string)
			assert.FatalIf(
				t,
				!strings.HasPrefix(name, "generated-values-"),
				"KS retained raw reference: %s",
				name,
			)
			found = true
		}
	}
	assert.FatalIf(t, !found, "selected output missing: %s", stdout)
	cli2 := runCLI(
		t,
		"test",
		tc.kind,
		tc.name,
		"--path",
		(*dir),
		"--cache-dir",
		(*capturedInputs.cache),
		"--concurrency",
		(*capturedInputs.workers),
	)
	stdout, stderr, code = cli2.stdout, cli2.stderr, cli2.code
	assert.FatalIf(
		t,
		code != 0 || !strings.Contains(stdout, "1 passed"),
		"test exited %d: stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
}

func runDiffExcludedOrderingAndSelectedSourceFailureCase(t *testing.T, broken *bool, kind *string) {
	t.Helper()

	baseDir, headDir := t.TempDir(), t.TempDir()
	testutil.WriteFilteredOrderingCluster(t, baseDir, "v1", (*broken))
	testutil.WriteFilteredOrderingCluster(t, headDir, "v2", (*broken))
	foreignKind := "Kustomization"
	if (*kind) == "hr" {
		foreignKind = "HelmRelease"
	}
	for _, dir := range []string{baseDir, headDir} {
		testutil.WriteFile(
			t,
			dir,
			"foreign.yaml",
			"apiVersion: example.test/v1\nkind: "+foreignKind+"\nmetadata: {name: selected, namespace: flux-system}\nspec: {value: foreign-marker}\n",
		)
	}
	cli := runCLI(t, "diff", (*kind), "selected", "--path", headDir, "--path-orig", baseDir,
		"--cache-dir", t.TempDir(), "--concurrency", "4", "-o", "diff")
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	if *broken {
		assert.FatalIf(
			t,
			code == 0 || !strings.Contains(stderr, "selected-source"),
			"selected source failure hidden: code=%d stdout=%s stderr=%s",
			code,
			stdout,
			stderr,
		)
		return
	}
	assert.FatalIf(t, code != 0, "healthy diff exited %d: %s", code, stderr)
	indent := "  "
	if (*kind) == "ks" {
		indent = "    "
	}
	wants := []string{"-" + indent + "greeting: v1", "+" + indent + "greeting: v2"}
	if (*kind) == "hr" {
		wants = append(wants, "selected-rendered")
	}
	for _, want := range wants {
		assert.ErrorIf(t, !strings.Contains(stdout, want), "missing selected diff %q: %s", want, stdout)
	}
	for _, excluded := range []string{"monitor-a", "monitor-b", "excluded-source", "excluded-values", "foreign-marker", "example.test"} {
		assert.ErrorIf(
			t,
			((*kind) == "hr" && strings.Contains(stdout, excluded)) || strings.Contains(stderr, excluded),
			"excluded chain contaminated diff: %s\n%s",
			stdout,
			stderr,
		)
	}
}
