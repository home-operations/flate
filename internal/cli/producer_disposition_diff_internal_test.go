package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Diff_ProducerDispositionAndPath(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		for _, mode := range []string{"both-failed", "both-suspended", "path-only", "path-content"} {
			t.Run(workers+"/"+mode, func(t *testing.T) { runDiffProducerDispositionAndPathCase(t, &mode, &workers) })
		}
	}
}

func runDiffProducerDispositionAndPathCase(t *testing.T, mode, workers *string) {
	t.Helper()

	orig, current := writeFixture(t), writeFixture(t)
	for _, root := range []string{orig, current} {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux/cluster.yaml]\n")
	}
	switch *mode {
	case "both-failed":
		testutil.WriteFile(
			t,
			orig,
			"apps/kustomization.yaml",
			"resources: [cm.yaml, missing-before.yaml]\n",
		)
		testutil.WriteFile(
			t,
			current,
			"apps/kustomization.yaml",
			"resources: [cm.yaml, missing-after.yaml]\n",
		)
	case "both-suspended":
		appendScopeSpec(
			t,
			orig,
			"flux/cluster.yaml",
			"  suspend: true\n  postBuild: {substitute: {CONTROL: before}}\n",
		)
		appendScopeSpec(
			t,
			current,
			"flux/cluster.yaml",
			"  suspend: true\n  postBuild: {substitute: {CONTROL: after}}\n",
		)
	default:
		assert.NoError(t, os.Rename(filepath.Join(current, "apps"), filepath.Join(current, "moved")))
		body, err := os.ReadFile(filepath.Join(current, "flux", "cluster.yaml"))
		assert.NoError(t, err)
		testutil.WriteFile(
			t,
			current,
			"flux/cluster.yaml",
			strings.Replace(string(body), "./apps", "./moved", 1),
		)
		if (*mode) == "path-content" {
			testutil.WriteFile(
				t,
				current,
				"moved/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: changed}\n",
			)
		}
	}
	cli := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig,
		"--output", "json", "--cache-dir", t.TempDir(), "--concurrency", (*workers))
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	wantCode := 0
	if (*mode) == "both-failed" {
		wantCode = 1
	}
	assert.FatalIf(t, code != wantCode, "exit=%d want=%d stderr=%s", code, wantCode, stderr)
	var out struct {
		Changes    []diff.Change      `json:"Changes"`
		Suppressed []diff.Suppression `json:"Suppressed"`
	}
	{
		err := json.Unmarshal([]byte(stdout), &out)
		assert.FatalIf(t, err != nil, "JSON %q: %v", stdout, err)
	}
	switch *mode {
	case "both-failed":
		assert.FatalIf(
			t,
			len(out.Changes) != 0 || len(out.Suppressed) != 1 || out.Suppressed[0].Side != "both" ||
				out.Suppressed[0].Count != 0,
			"unavailable producer disclosure: %s",
			stdout,
		)
	case "both-suspended":
		assert.FatalIf(t, len(out.Changes) != 1 || out.Changes[0].Kind != manifest.KindKustomization ||
			out.Changes[0].Name != "apps" ||
			out.Changes[0].Status != diff.StatusChanged ||
			len(out.Suppressed) != 1 ||
			out.Suppressed[0].Side != "both" ||
			out.Suppressed[0].Count != 0, "suspended workload or applied declaration disclosure: %s", stdout)
	case "path-only":
		assert.FatalIf(t, len(out.Changes) != 1 || out.Changes[0].Kind != manifest.KindKustomization ||
			out.Changes[0].Name != "apps" ||
			out.Changes[0].Status != diff.StatusChanged ||
			len(
				out.Suppressed,
			) != 0, "path-only move changed workload output or lost its applied declaration: %s", stdout)
	case "path-content":
		assert.FatalIf(t, len(out.Changes) != 2 || out.Changes[0].Kind != manifest.KindKustomization ||
			out.Changes[1].Status != diff.StatusChanged ||
			out.Changes[1].Parent.Path != "moved", "content move did not pair with current path: %s", stdout)
	}
}
