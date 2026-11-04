package cli

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
)

func runClusterGroupsDiffAbsenceAndFailureScenario(t *testing.T, scenario *string, base *string) {
	t.Helper()

	values := map[string]string{"omega": "new"}
	if (*scenario) == "failed" {
		values["alpha"] = "new"
	}
	current := writeClusterGroupsFixture(t, values)
	if (*scenario) == "failed" {
		appendScopeSpec(t, current, "alpha/flux.yaml", "  dependsOn: [{name: missing}]\n")
	}
	stdout, stderr, code := runClusterInputs(
		t,
		"diff",
		"all",
		"--path",
		current,
		"--path-orig",
		(*base),
		"-o",
		"json",
		"--concurrency",
		"2",
	)
	var groups []struct {
		Cluster    string             `json:"cluster"`
		Removed    bool               `json:"removed"`
		Changes    []map[string]any   `json:"changes"`
		Suppressed []diff.Suppression `json:"suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
	hasPod := func(change map[string]any) bool { return change["kind"] == "Pod" }
	assert.FatalIf(t, len(groups) != 2 || groups[0].Cluster != "alpha" || groups[1].Cluster != "omega" ||
		!slices.ContainsFunc(
			groups[1].Changes,
			hasPod,
		), "keyed diff lost healthy group: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	entry, diagnostics, status := runClusterInputs(
		t,
		"diff",
		"all",
		"--path",
		filepath.Join(current, "omega"),
		"--path-orig",
		filepath.Join((*base), "omega"),
		"-o",
		"json",
		"--concurrency",
		"2",
	)
	assert.FatalIf(
		t,
		status != 0,
		"healthy entry comparison: code=%d stdout=%s stderr=%s",
		status,
		entry,
		diagnostics,
	)
	var control struct {
		Changes []map[string]any `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal([]byte(entry), &control))
	assert.Diff(t, groups[1].Changes, control.Changes)
	if (*scenario) == "removed" {
		assert.FatalIf(t, code != 0 || !groups[0].Removed || !slices.ContainsFunc(groups[0].Changes, hasPod) ||
			len(
				groups[0].Suppressed,
			) != 0, "absent side suppressed removal: code=%d stdout=%s stderr=%s", code, stdout, stderr)
		images, diagnostics, status := runClusterInputs(
			t,
			"diff",
			"images",
			"--path",
			current,
			"--path-orig",
			(*base),
			"--concurrency",
			"2",
		)
		assert.FatalIf(
			t,
			status != 0 || images != "omega: example.test/app:new\n",
			"empty removed image group disappeared: code=%d stdout=%s stderr=%s",
			status,
			images,
			diagnostics,
		)
	} else {
		assert.FatalIf(t, code != 1 || groups[0].Removed || slices.ContainsFunc(groups[0].Changes, hasPod) ||
			len(
				groups[0].Suppressed,
			) == 0, "failed side looked absent: code=%d stdout=%s stderr=%s", code, stdout, stderr)
		for noteIndex := range groups[0].Suppressed {
			note := groups[0].Suppressed[noteIndex]
			assert.Equal(t, note.Side, diff.SideCurrent)
		}
		for _, row := range []string{
			"  ✗  current snapshot: Kustomization  alpha: flux-system/missing  not found\n",
			"         current snapshot: required by alpha: flux-system/shared\n",
		} {
			assert.Equal(t, strings.Count(stderr, row), 1)
		}
		for _, format := range []string{"json", "yaml"} {
			t.Run("images/"+format, func(t *testing.T) {
				images, diagnostics, status := runClusterInputs(
					t,
					"diff",
					"images",
					"--path",
					current,
					"--path-orig",
					(*base),
					"-o",
					format,
					"--concurrency",
					"2",
				)
				assert.Equal(t, status, 1)
				var imageGroups []output.ImageGroup
				assert.NoError(t, yaml.Unmarshal([]byte(images), &imageGroups))
				assert.FatalIf(t, len(imageGroups) != 2 || imageGroups[0].Cluster != "alpha" ||
					imageGroups[1].Cluster != "omega" ||
					len(imageGroups[0].Images) != 0 ||
					imageGroups[0].Removed, "failed image side looked absent: %s", images)
				assert.Diff(t, imageGroups[1].Images, []string{"example.test/app:new"})
				for _, row := range []string{
					"  ✗  current snapshot: Kustomization  alpha: flux-system/missing  not found\n",
					"         current snapshot: required by alpha: flux-system/shared\n",
				} {
					assert.Equal(t, strings.Count(diagnostics, row), 1)
				}
			})
		}
	}
}

func runClusterGroupsDiffAbsenceAndFailureName(t *testing.T, tc *struct {
	name     string
	path     string
	resource string
},
) {
	t.Helper()

	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old", "omega": "old"})
	current := writeClusterGroupsFixture(t, map[string]string{"alpha": "old", "omega": "old"})
	const input = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s, namespace: apps}\ndata: {value: %s}\n"
	for _, root := range []string{base, current} {
		testutil.WriteFile(
			t,
			root,
			"components/shared/kustomization.yaml",
			"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [input.yaml]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"components/shared/input.yaml",
			fmt.Sprintf(input, "shared-input", "original"),
		)
		for _, entry := range []string{"alpha", "omega"} {
			testutil.WriteFile(
				t,
				root,
				entry+"/app/kustomization.yaml",
				"resources: [pod.yaml, private.yaml]\ncomponents: [../../components/shared]\n",
			)
			testutil.WriteFile(
				t,
				root,
				entry+"/app/private.yaml",
				fmt.Sprintf(input, "private-input", "original"),
			)
		}
	}
	testutil.WriteFile(t, current, tc.path, fmt.Sprintf(input, tc.resource, "changed"))
	stdout, stderr, code := runClusterInputs(
		t,
		"diff",
		"all",
		"--path",
		current,
		"--path-orig",
		base,
		"-o",
		"json",
		"--concurrency",
		"2",
	)
	assert.FatalIf(t, code != 0, "group comparison: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	var groups []struct {
		Cluster    string             `json:"cluster"`
		Changes    []diff.Change      `json:"changes"`
		Suppressed []diff.Suppression `json:"suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
	assert.FatalIf(
		t,
		len(groups) != 2 || groups[0].Cluster != "alpha" || groups[1].Cluster != "omega",
		"shared-input fixture lost cluster roster: %s",
		stdout,
	)
	for i, entry := range []string{"alpha", "omega"} {
		out, diagnostics, status := runClusterInputs(
			t,
			"diff",
			"all",
			"--path",
			filepath.Join(current, entry),
			"--path-orig",
			filepath.Join(base, entry),
			"-o",
			"json",
			"--concurrency",
			"2",
		)
		assert.FatalIf(
			t,
			status != 0,
			"entry %s comparison: code=%d stdout=%s stderr=%s",
			entry,
			status,
			out,
			diagnostics,
		)
		var control struct {
			Changes []diff.Change `json:"changes"`
		}
		assert.NoError(t, json.Unmarshal([]byte(out), &control))
		assert.Diff(t, groups[i].Changes, control.Changes)
		assert.Equal(t, len(groups[i].Suppressed), 0)
		if tc.name == "private-file" && entry == "omega" {
			assert.Equal(t, len(groups[i].Changes), 0)
			continue
		}
		assert.FatalIf(t, len(groups[i].Changes) != 1, "cluster %s lost input change: %s", entry, stdout)
		change := groups[i].Changes[0]
		assert.Equal(t, change.Kind, "ConfigMap")
		assert.Equal(t, change.Name, tc.resource)
		assert.Equal(t, change.Status, diff.StatusChanged)
		assert.Diff(t, change.Old["data"], map[string]any{"value": "original"})
		assert.Diff(t, change.New["data"], map[string]any{"value": "changed"})
	}
}
