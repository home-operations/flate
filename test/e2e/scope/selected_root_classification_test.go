package scope_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SelectedKustomizeClassification(t *testing.T) {
	for _, tc := range []struct {
		name, file, body string
		fail             bool
	}{
		{name: "yaml", file: "kustomization.yaml", body: "resources: [cm.yaml]\nnameSuffix: -dev\n"},
		{name: "yml", file: "kustomization.yml", body: "resources: [cm.yaml]\nnameSuffix: -dev\n"},
		{name: "capitalized", file: "Kustomization", body: "resources: [cm.yaml]\nnameSuffix: -dev\n"},
		{name: "plain"},
		{name: "malformed", file: "kustomization.yaml", body: "resources: [\n", fail: true},
		{name: "multiple", file: "kustomization.yaml", body: "resources: [cm.yaml]\n", fail: true},
		{name: "component", file: "kustomization.yaml", body: "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [missing.yaml]\n", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\ndata: {value: original}\n",
			)
			if tc.file != "" {
				testutil.WriteFile(t, root, tc.file, tc.body)
			}
			if tc.name == "multiple" {
				testutil.WriteFile(t, root, "kustomization.yml", tc.body)
			}
			if tc.name == "plain" {
				testutil.WriteFile(
					t,
					root,
					"entry.yaml",
					testutil.KustomizationYAML("plain", "apps", "  path: ./unavailable\n  suspend: true\n"),
				)
				requireCLIOKResult1 := harness.RequireCLIOK(
					t,
					"get",
					"ks",
					"--path",
					root,
					"-o",
					"json",
					"--concurrency",
					"2",
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
				)
				out, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
				var owners []struct {
					Name string `json:"Name"`
				}
				{
					err := json.Unmarshal(
						[]byte(out),
						&owners,
					)
					assert.FatalIf(t, err != nil || len(owners) != 1 ||
						owners[0].Name != "plain", "plain folder roots = %s, %v", out, err)
				}
				return
			}
			runCLIBuffersResult2 := harness.RunCLIBuffers("build", "all", "--path", root,
				"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			out, stderr, code := runCLIBuffersResult2.Stdout, runCLIBuffersResult2.Stderr, runCLIBuffersResult2.Code
			if tc.fail {
				assert.FatalIf(
					t,
					code != 1 || strings.Contains(out, "value: original") || stderr == "",
					"upstream failure exposed raw fallback: code=%d stdout=%s stderr=%s",
					code,
					out,
					stderr,
				)
				return
			}
			want := "input-dev"
			if tc.name == "plain" {
				want = "input"
			}
			assert.FatalIf(
				t,
				code != 0 || !strings.Contains(out, "name: "+want+"\n"),
				"entry classification lost its resource: code=%d stdout=%s stderr=%s",
				code,
				out,
				stderr,
			)
		})
	}
}
