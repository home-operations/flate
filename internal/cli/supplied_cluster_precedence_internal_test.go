package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ClusterGroups_SuppliedPrecedence(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old-alpha", "omega": "old-omega"})
	head := writeClusterGroupsFixture(t, map[string]string{"alpha": "new-alpha", "omega": "new-omega"})
	for _, root := range []string{base, head} {
		for _, cluster := range []string{"alpha", "omega"} {
			testutil.WriteFile(
				t,
				root,
				cluster+"/flux.yaml",
				testutil.KustomizationYAML(
					"shared",
					"flux-system",
					"  path: ./"+cluster+"/app\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: settings}]\n",
				),
			)
			value := "old-" + cluster
			if root == head {
				value = "new-" + cluster
			}
			testutil.WriteFile(
				t,
				root,
				cluster+"/settings.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {CLUSTER: "+value+"}\n",
			)
			testutil.WriteFile(t, root, cluster+"/kustomization.yaml", "resources: [flux.yaml, settings.yaml]\n")
		}
	}
	input := substitutionInput(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {CLUSTER: supplied}\n",
	)
	cluster2 := runClusterInputs(
		t,
		"diff",
		"all",
		"--path",
		head,
		"--path-orig",
		base,
		"--supply",
		input,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"-o",
		"diff",
	)
	stdout, stderr, code := cluster2.stdout, cluster2.stderr, cluster2.code
	assert.FatalIf(t, code != 0 || strings.Count(stderr, "[SuppliedPrecedence]") != 2 ||
		!strings.Contains(stderr, "[SuppliedPrecedence] alpha:") ||
		!strings.Contains(stderr, "[SuppliedPrecedence] omega:") ||
		strings.Contains(stderr, "Cluster:") ||
		strings.Contains(
			stdout,
			"supplied",
		), "cluster supply reduction: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	for _, cluster := range []string{"alpha", "omega"} {
		assert.Equal(t, strings.Count(stderr, "[SuppliedPrecedence] "+cluster+":"), 1)
	}
	for _, value := range []string{"old-alpha", "new-alpha", "old-omega", "new-omega"} {
		assert.FatalIf(t, !strings.Contains(stdout, value), "cluster precedence lost %q: %s", value, stdout)
	}
}
