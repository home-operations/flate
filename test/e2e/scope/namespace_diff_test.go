package scope_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_NamespaceDiff(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
		want                map[string]string
	}{
		{name: "changed", before: "config", after: "config", want: map[string]string{"config": "changed"}},
		{name: "added", after: "config", want: map[string]string{"config": "added"}},
		{name: "removed", before: "config", want: map[string]string{"config": "removed"}},
		{name: "renamed", before: "before", after: "after", want: map[string]string{"before": "removed", "after": "added"}},
		{name: "empty", want: map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roots := []string{t.TempDir(), t.TempDir()}
			for side, root := range roots {
				testutil.GitInit(t, root)
				testutil.WriteFile(
					t,
					root,
					"flux/peer.yaml",
					testutil.KustomizationYAML("peer", "monitoring", "  path: ./peer\n  targetNamespace: monitoring\n"),
				)
				testutil.WriteFile(t, root, "peer/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					"peer/cm.yaml",
					fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: peer}\ndata: {value: '%d'}\n", side),
				)
				name := []string{tc.before, tc.after}[side]
				if name != "" {
					testutil.WriteFile(
						t,
						root,
						"flux/selected.yaml",
						testutil.KustomizationYAML("selected", "apps", "  path: ./payload\n  targetNamespace: apps\n"),
					)
					testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: [cm.yaml]\n")
					testutil.WriteFile(
						t,
						root,
						"payload/cm.yaml",
						fmt.Sprintf(
							"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s}\ndata: {value: '%d'}\n",
							name,
							side,
						),
					)
				}
			}
			for _, selection := range [][]string{{"all"}, {"ks"}, {"ks", "selected"}} {
				if tc.before == "" && tc.after == "" && len(selection) > 1 {
					continue
				}
				args := append([]string{"diff"}, selection...)
				args = append(
					args,
					"--namespace",
					"apps",
					"--path",
					filepath.Join(roots[1], "flux"),
					"--path-orig",
					filepath.Join(roots[0], "flux"),
					"--concurrency",
					"2",
					"--no-progress",
					"--cache-dir",
					t.TempDir(),
					"-o",
					"json",
				)
				first, _ := harness.RequireCLIOK(t, args...)
				second, _ := harness.RequireCLIOK(t, args...)
				assert.Equal(t, first, second)
				var result struct {
					Changes []struct {
						Name   string `json:"Name"`
						Status string `json:"Status"`
					} `json:"Changes"`
				}
				assert.NoError(t, json.Unmarshal([]byte(first), &result))
				got := map[string]string{}
				for _, change := range result.Changes {
					got[change.Name] = change.Status
				}
				assert.Diff(t, got, tc.want)
			}
		})
	}
}
