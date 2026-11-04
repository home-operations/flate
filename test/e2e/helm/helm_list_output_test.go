package helm_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_InvalidHelmList(t *testing.T) {
	dir := simpleFixture(t)
	testutil.WriteFile(
		t,
		dir,
		"charts/mychart/templates/configmap.yaml",
		"apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, metadata: {name: invalid}}]\n",
	)
	for _, tc := range []struct {
		name string
		skip bool
	}{
		{name: "included"},
		{name: "excluded", skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{
				"build",
				"hr",
				"--path",
				filepath.Join(dir, "cluster"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			}
			if tc.skip {
				args = append(args, "--skip-kinds=ConfigMapList")
			}
			out, stderr, code := harness.RunCLIBuffers(args...)
			diagnostic := harness.MustExtractLine(t, stderr, "Object 'Kind' is missing")
			assert.FatalIf(t, code != 1 || strings.Count(stderr, "Object 'Kind' is missing") != 1 ||
				strings.Count(
					diagnostic,
					"apps/demo",
				) != 1, "invalid list exit=%d stdout=%s stderr=%s", code, out, stderr)
			for _, text := range []string{"name: invalid", "kind: ConfigMapList"} {
				{
					got := strings.Contains(out, text)
					assert.ErrorIf(t, got != !tc.skip, "output contains %q=%v, want %v:\n%s", text, got, !tc.skip, out)
				}
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_HelmListMemberDiff(t *testing.T) {
	before, after := simpleFixture(t), t.TempDir()
	body := `apiVersion: v1
kind: ConfigMapList
items:
- metadata: {name: stable}
  data: {value: fixed}
`
	testutil.WriteFile(t, before, "charts/mychart/templates/configmap.yaml", body)
	testutil.CopyTree(t, before, after)
	testutil.WriteFile(
		t,
		after,
		"charts/mychart/templates/configmap.yaml",
		body+"- metadata: {name: added}\n  data: {value: extra}\n",
	)
	for _, tc := range []struct{ current, original, status string }{{after, before, "added"}, {before, after, "removed"}} {
		t.Run(tc.status, func(t *testing.T) {
			out, stderr := harness.RequireCLIOK(
				t,
				"diff",
				"all",
				"-o",
				"json",
				"--path",
				filepath.Join(tc.current, "cluster"),
				"--path-orig",
				filepath.Join(tc.original, "cluster"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			)
			var result struct {
				Changes []struct {
					Name   string `json:"Name"`
					Status string `json:"Status"`
					Parent struct {
						Kind      string `json:"Kind"`
						Name      string `json:"Name"`
						Namespace string `json:"Namespace"`
					} `json:"Parent"`
				} `json:"Changes"`
				Suppressed []any `json:"Suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(out), &result))
			assert.FatalIf(
				t,
				len(result.Changes) != 1 || len(result.Suppressed) != 0,
				"list diff = %s\n%s",
				out,
				stderr,
			)
			change := result.Changes[0]
			assert.FatalIf(
				t,
				change.Name != "added" || change.Status != tc.status || change.Parent.Kind != "HelmRelease" ||
					change.Parent.Name != "demo" ||
					change.Parent.Namespace != "apps",
				"list member lost pairing/parent: %s",
				out,
			)
		})
	}
}
