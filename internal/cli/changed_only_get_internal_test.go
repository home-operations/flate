package cli

import (
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetAll_ChangedOnly(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "same", "omega": "same"})
	head := writeClusterGroupsFixture(t, map[string]string{"alpha": "same", "omega": "same"})
	for _, root := range []string{base, head} {
		writeGetRelease(t, root, "alpha")
		writeGetRelease(t, root, "omega")
		testutil.WriteFile(
			t,
			root,
			"alpha/app/private.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: private, namespace: apps}\ndata: {value: original}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"alpha/app/kustomization.yaml",
			"resources: [pod.yaml, release.yaml, private.yaml]\n",
		)
	}
	testutil.WriteFile(
		t,
		head,
		"alpha/app/private.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: private, namespace: apps}\ndata: {value: changed}\n",
	)
	testutil.WriteFile(
		t,
		base,
		"alpha/removed.yaml",
		testutil.KustomizationYAML("removed", "flux-system", "  path: ./alpha/removed\n"),
	)
	testutil.WriteFile(t, base, "alpha/kustomization.yaml", "resources: [flux.yaml, removed.yaml]\n")
	testutil.WriteFile(t, base, "alpha/removed/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, base, "alpha/removed/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed}\n")
	flags := []string{"--path", head, "--path-orig", base, "--cache-dir", t.TempDir(), "--concurrency", "2"}
	var selected [][]output.DocumentGroup
	runGetAllChangedOnlyLoop(t, struct {
		flags    *[]string
		selected *[][]output.DocumentGroup
	}{flags: &flags, selected: &selected})
	for _, command := range []string{"get all", "get"} {
		t.Run(command, func(t *testing.T) { runGetAllChangedOnlyBody(t, &command, &flags, &selected) })
	}
}

func runGetAllChangedOnlyBody(t *testing.T, command *string, flags *[]string, selected *[][]output.DocumentGroup) {
	t.Helper()

	cluster := runClusterInputs(t, append(append(strings.Fields((*command)), "-o", "json"), (*flags)...)...)
	out, stderr, code := cluster.stdout, cluster.stderr, cluster.code
	assert.FatalIf(t, code != 0, "exit=%d stdout=%s stderr=%s", code, out, stderr)
	groups := decodeGetDocuments(t, out, "json", []string{"alpha", "omega"})
	for i, group := range groups {
		want := append(slices.Clone((*selected)[0][i].Documents), (*selected)[1][i].Documents...)
		assert.Diff(t, group.Documents, want)
		var identities []string
		for _, doc := range group.Documents {
			identities = append(identities, doc["kind"].(string)+"/"+doc["name"].(string))
		}
		if i == 0 {
			assert.Diff(t, identities, []string{"Kustomization/shared", "HelmRelease/shared"})
			assert.Equal(t, group.Documents[1]["version"], any("0.1.0"))
		} else {
			assert.Diff(t, identities, []string{"Kustomization/shared"})
		}
	}
	cluster2 := runClusterInputs(t, append(strings.Fields((*command)), (*flags)...)...)
	out, stderr, code = cluster2.stdout, cluster2.stderr, cluster2.code
	assert.FatalIf(t, code != 0 || strings.Count(out, "CLUSTER") != 2 || !strings.Contains(out, "omega") ||
		strings.Contains(out, "removed") ||
		!strings.Contains(out, "alpha") ||
		!strings.Contains(
			out,
			"HelmRelease/shared",
		), "changed table: exit=%d stdout=%s stderr=%s", code, out, stderr)
	for _, format := range []string{"json", "yaml"} {
		args := append(append(strings.Fields((*command)), "-o", format, "-n", "apps"), (*flags)...)
		cluster3 := runClusterInputs(t, args...)
		out, stderr, code := cluster3.stdout, cluster3.stderr, cluster3.code
		assert.FatalIf(t, code != 0, "namespace selection: exit=%d stdout=%s stderr=%s", code, out, stderr)
		groups := decodeGetDocuments(t, out, format, []string{"alpha", "omega"})
		assert.Equal(t, len(groups[0].Documents), 1)
		assert.Equal(t, groups[0].Documents[0]["kind"], any("HelmRelease"))
		assert.Equal(t, len(groups[1].Documents), 0)
		assert.FatalIf(
			t,
			format == "json" && groups[1].Documents == nil,
			"%v",
			"empty retained cluster documents must be []",
		)
	}
}

func runGetAllChangedOnlyLoop(t *testing.T, loopInputs struct {
	flags    *[]string
	selected *[][]output.DocumentGroup
},
) {
	t.Helper()

	for _, kind := range []string{"ks", "hr"} {
		cluster := runClusterInputs(t, append([]string{"get", kind, "-o", "json"}, (*loopInputs.flags)...)...)
		out, stderr, code := cluster.stdout, cluster.stderr, cluster.code
		assert.FatalIf(t, code != 0, "get %s: exit=%d stdout=%s stderr=%s", kind, code, out, stderr)
		if (*loopInputs.selected) == nil {
			(*loopInputs.selected) = make([][]output.DocumentGroup, 0, 2)
		}
		(*loopInputs.selected) = append(
			(*loopInputs.selected),
			decodeGetDocuments(t, out, "json", []string{"alpha", "omega"}),
		)
	}
}
