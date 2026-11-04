package scope_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_EntryPackageJSON(t *testing.T) {
	for _, verb := range []string{"build", "diff"} {
		t.Run(verb, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml]\n")
			document := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {value: before}\n"
			testutil.WriteFile(t, root, "cm.yaml", document)
			testutil.GitCommitAll(
				t,
				repo,
				"init",
				&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
			)
			testutil.WriteFile(t, root, "cm.yaml", strings.ReplaceAll(document, "before", "after"))
			t.Chdir(root)
			args := []string{
				verb,
				"--path",
				".",
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
				"-o",
				"json",
			}
			if verb == "diff" {
				args = append(args, "--base", "HEAD")
			}
			stdout, stderr := harness.RequireCLIOK(t, args...)
			for _, forbidden := range []string{`\u0000`, "\x00", "entry"} {
				assert.ErrorIf(
					t,
					strings.Contains(stdout+stderr, forbidden),
					"output contains %q:\n%s\n%s",
					forbidden,
					stdout,
					stderr,
				)
			}
			want := map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "settings"},
				"data":     map[string]any{"value": "after"},
			}
			if verb == "build" {
				var documents []map[string]any
				assert.NoError(t, json.Unmarshal([]byte(stdout), &documents))
				assert.Diff(t, documents, []map[string]any{want})
				return
			}
			var result struct {
				Changes []struct {
					APIVersion string `json:"APIVersion"`
					Kind       string `json:"Kind"`
					Namespace  string `json:"Namespace"`
					Name       string `json:"Name"`
					Status     string `json:"Status"`
					Parent     struct {
						Group     string `json:"Group"`
						Kind      string `json:"Kind"`
						Namespace string `json:"Namespace"`
						Name      string `json:"Name"`
						Path      string `json:"Path"`
					} `json:"Parent"`
					Old map[string]any `json:"Old"`
					New map[string]any `json:"New"`
				} `json:"Changes"`
				Suppressed []any `json:"Suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(stdout), &result))
			assert.FatalIf(
				t,
				len(result.Changes) != 1 || len(result.Suppressed) != 0,
				"unexpected ConfigMap diff:\n%s",
				stdout,
			)
			change := result.Changes[0]
			assert.Equal(t, change.APIVersion, "v1")
			assert.Equal(t, change.Kind, "ConfigMap")
			assert.Equal(t, change.Namespace, "")
			assert.Equal(t, change.Name, "settings")
			assert.Equal(t, change.Status, "changed")
			assert.Diff(t, change.Parent, struct {
				Group     string `json:"Group"`
				Kind      string `json:"Kind"`
				Namespace string `json:"Namespace"`
				Name      string `json:"Name"`
				Path      string `json:"Path"`
			}{Path: "."})
			assert.Diff(t, change.New, want)
			want["data"] = map[string]any{"value": "before"}
			assert.Diff(t, change.Old, want)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_EntryPackageFailure(t *testing.T) {
	roots := [2]string{t.TempDir(), t.TempDir()}
	for _, root := range roots {
		testutil.GitInit(t, root)
		testutil.WriteFile(
			t,
			root,
			"entry/kustomization.yaml",
			"resources: [child.yaml]\nconfigMapGenerator: [{name: settings, envs: [.cluster-vars.env]}]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/child.yaml",
			testutil.KustomizationYAML("child", "apps", "  path: ./workload\n"),
		)
		testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: []\n")
	}
	for _, tc := range []struct{ verb, format string }{{verb: "test"}, {verb: "diff"}, {verb: "diff", format: "json"}} {
		t.Run(tc.verb+"/"+tc.format, func(t *testing.T) {
			args := []string{
				tc.verb,
				"all",
				"--path",
				filepath.Join(roots[1], "entry"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			}
			if tc.verb == "diff" {
				args = append(args, "--path-orig", filepath.Join(roots[0], "entry"))
			}
			if tc.format != "" {
				args = append(args, "-o", tc.format)
			}
			out, stderr, code := harness.RunCLIBuffers(args...)
			assert.FatalIf(t, code != 1 || !strings.Contains(out+stderr, ".cluster-vars.env") ||
				!strings.Contains(out+stderr, "entry") ||
				strings.Contains(stderr, "discovery package") ||
				strings.Contains(out+stderr, "Kustomization entry") ||
				strings.Contains(
					out+stderr,
					"Kustomization.kustomize.toolkit.fluxcd.io entry",
				), "package failure lost producer authority: exit=%d stdout=%s stderr=%s", code, out, stderr)
			assert.FatalIf(
				t,
				tc.verb == "test" && !strings.Contains(out, "1 failed"),
				"package failure lost its status table: %s",
				out,
			)
			if tc.format == "json" {
				var report struct {
					Suppressed []struct {
						Parent struct {
							Name string `json:"Name"`
							Path string `json:"Path"`
						} `json:"Parent"`
					} `json:"Suppressed"`
				}
				{
					err := json.Unmarshal(
						[]byte(out),
						&report,
					)
					assert.FatalIf(
						t,
						err != nil || len(report.Suppressed) != 1 || report.Suppressed[0].Parent.Name != "" ||
							report.Suppressed[0].Parent.Path != "entry",
						"JSON exposed the internal producer: %s; err=%v",
						out,
						err,
					)
				}
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DiscoveryPackageEntries(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "entry/Taskfile.yaml", "version: '3'\ntasks: {}\n")
	for _, name := range []string{"one", "two"} {
		testutil.WriteFile(
			t,
			root,
			"entry/"+name+"/kustomization.yaml",
			"resources: [role.yaml]\nnamePrefix: "+name+"-\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/"+name+"/role.yaml",
			"apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata: {name: retained}\nrules: []\n",
		)
	}
	for _, name := range []string{"one", "two"} {
		t.Run(name, func(t *testing.T) {
			args := []string{
				"build",
				"all",
				"--path",
				filepath.Join(root, "entry", name),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			}
			out, stderr := harness.RequireCLIOK(t, args...)
			assert.FatalIf(
				t,
				strings.Count(out, "kind: ClusterRole") != 1 || !strings.Contains(out, "name: "+name+"-retained") ||
					strings.Contains(stderr, "Taskfile"),
				"package entry lost its output: %s\n%s",
				out,
				stderr,
			)
			again, _ := harness.RequireCLIOK(t, args...)
			assert.Equal(t, again, out)
		})
	}
}
