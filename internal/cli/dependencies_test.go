package cli

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestRun_GeneratedValuesDirectDependentAndKS(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteGeneratedValuesCluster(t, dir)
	cache := t.TempDir()
	for _, workers := range []string{"1", "2", "4"} {
		for _, tc := range []struct{ kind, name string }{{"hr", "a"}, {"hr", "b"}, {"ks", "apps"}} {
			t.Run(workers+"/"+tc.kind+"/"+tc.name, func(t *testing.T) {
				stdout, stderr, code := runCLI(t, "build", tc.kind, tc.name, "--path", dir, "--cache-dir", cache, "--concurrency", workers, "-o", "json")
				if code != 0 {
					t.Fatalf("build exited %d: %s", code, stderr)
				}
				var docs []map[string]any
				if err := json.Unmarshal([]byte(stdout), &docs); err != nil {
					t.Fatal(err)
				}
				found := false
				for _, doc := range docs {
					meta, _ := doc["metadata"].(map[string]any)
					if tc.kind == "hr" && doc["kind"] == "ConfigMap" && meta["name"] == tc.name+"-rendered" {
						data := doc["data"].(map[string]any)
						if data["greeting"] != "hello" || data["replicas"] != "2" {
							t.Fatalf("effective values differ: %v", data)
						}
						found = true
					}
					if tc.kind == "ks" && doc["kind"] == "HelmRelease" && meta["name"] == "a" {
						spec := doc["spec"].(map[string]any)
						refs := spec["valuesFrom"].([]any)
						name := refs[0].(map[string]any)["name"].(string)
						if !strings.HasPrefix(name, "generated-values-") {
							t.Fatalf("KS retained raw reference: %s", name)
						}
						found = true
					}
				}
				if !found {
					t.Fatalf("selected output missing: %s", stdout)
				}
				stdout, stderr, code = runCLI(t, "test", tc.kind, tc.name, "--path", dir, "--cache-dir", cache, "--concurrency", workers)
				if code != 0 || !strings.Contains(stdout, "1 passed") {
					t.Fatalf("test exited %d: stdout=%s stderr=%s", code, stdout, stderr)
				}
			})
		}
	}
}

func TestRun_DiffExcludedOrderingAndSelectedSourceFailure(t *testing.T) {
	for _, kind := range []string{"hr", "ks"} {
		for _, broken := range []bool{false, true} {
			name := "healthy"
			if broken {
				name = "selected source failure"
			}
			t.Run(fmt.Sprintf("%s/%s", kind, name), func(t *testing.T) {
				baseDir, headDir := t.TempDir(), t.TempDir()
				testutil.WriteFilteredOrderingCluster(t, baseDir, "v1", broken)
				testutil.WriteFilteredOrderingCluster(t, headDir, "v2", broken)
				foreignKind := "Kustomization"
				if kind == "hr" {
					foreignKind = "HelmRelease"
				}
				for _, dir := range []string{baseDir, headDir} {
					testutil.WriteFile(t, dir, "foreign.yaml", "apiVersion: example.test/v1\nkind: "+foreignKind+"\nmetadata: {name: selected, namespace: flux-system}\nspec: {value: foreign-marker}\n")
				}
				stdout, stderr, code := runCLI(t, "diff", kind, "selected", "--path", headDir, "--path-orig", baseDir,
					"--cache-dir", t.TempDir(), "--concurrency", "4", "-o", "diff")
				if broken {
					if code == 0 || !strings.Contains(stderr, "selected-source") {
						t.Fatalf("selected source failure hidden: code=%d stdout=%s stderr=%s", code, stdout, stderr)
					}
					return
				}
				if code != 0 {
					t.Fatalf("healthy diff exited %d: %s", code, stderr)
				}
				indent := "  "
				if kind == "ks" {
					indent = "    "
				}
				wants := []string{"-" + indent + "greeting: v1", "+" + indent + "greeting: v2"}
				if kind == "hr" {
					wants = append(wants, "selected-rendered")
				}
				for _, want := range wants {
					if !strings.Contains(stdout, want) {
						t.Errorf("missing selected diff %q: %s", want, stdout)
					}
				}
				for _, excluded := range []string{"monitor-a", "monitor-b", "excluded-source", "excluded-values", "foreign-marker", "example.test"} {
					if (kind == "hr" && strings.Contains(stdout, excluded)) || strings.Contains(stderr, excluded) {
						t.Errorf("excluded chain contaminated diff: %s\n%s", stdout, stderr)
					}
				}
			})
		}
	}
}

func TestRun_Diff_UnchangedInputProducerOutput(t *testing.T) {
	before, after := t.TempDir(), t.TempDir()
	for _, root := range []string{before, after} {
		testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "flux/inputs.yaml", testutil.KustomizationYAML("inputs", "apps", "  path: ./inputs\n  targetNamespace: apps\n"))
		testutil.WriteFile(t, root, "flux/app.yaml", testutil.KustomizationYAML("app", "apps", "  path: ./app\n  targetNamespace: apps\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: vars}]}\n"))
		testutil.WriteFile(t, root, "inputs/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "inputs/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: apps}\ndata: {VALUE: stable}\n")
		testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: '${VALUE}'}\n")
	}
	testutil.WriteFile(t, after, "flux/app.yaml", testutil.KustomizationYAML("app", "apps", "  path: ./app\n  targetNamespace: apps\n  postBuild: {substitute: {VALUE: stable}}\n"))
	var full []string
	for _, root := range []string{before, after} {
		output, diagnostic, code := runCLI(t, "build", "ks", "app", "--path", filepath.Join(root, "flux"), "--cache-dir", t.TempDir(), "--concurrency", "2")
		if code != 0 {
			t.Fatalf("full render failed: %s", diagnostic)
		}
		full = append(full, output)
	}
	if full[0] != full[1] {
		t.Fatalf("full outputs differ: before=%s after=%s", full[0], full[1])
	}
	output, diagnostic, code := runCLI(t, "diff", "ks", "app", "--path", filepath.Join(after, "flux"), "--path-orig", filepath.Join(before, "flux"), "--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff")
	if code != 0 || output != "" {
		t.Fatalf("unchanged input producer appeared in diff: code=%d stdout=%s stderr=%s", code, output, diagnostic)
	}
}
