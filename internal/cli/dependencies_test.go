package cli

import (
	"encoding/json"
	"fmt"
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
				for _, excluded := range []string{"monitor-a", "monitor-b", "excluded-source", "excluded-values"} {
					if (kind == "hr" && strings.Contains(stdout, excluded)) || strings.Contains(stderr, excluded) {
						t.Errorf("excluded chain contaminated diff: %s\n%s", stdout, stderr)
					}
				}
			})
		}
	}
}
