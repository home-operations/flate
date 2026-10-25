package cli

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
)

func writeClusterGroupsFixture(t *testing.T, values map[string]string) string {
	t.Helper()
	root := t.TempDir()
	testutil.GitInit(t, root)
	for dir, value := range values {
		testutil.WriteFile(t, root, dir+"/kustomization.yaml", "resources: [flux.yaml]\n")
		testutil.WriteFile(t, root, dir+"/flux.yaml", testutil.KustomizationYAML("shared", "flux-system",
			"  path: ./"+dir+"/app\n  postBuild: {substitute: {CLUSTER: "+value+"}}\n"))
		testutil.WriteFile(t, root, dir+"/app/kustomization.yaml", "resources: [pod.yaml]\n")
		testutil.WriteFile(t, root, dir+"/app/pod.yaml", `apiVersion: v1
kind: Pod
metadata: {name: shared, namespace: apps}
spec:
  containers:
    - name: app
      image: example.test/app:${CLUSTER}
`)
	}
	return root
}

func TestRun_ClusterGroups_PayloadAndSelection(t *testing.T) {
	root := writeClusterGroupsFixture(t, map[string]string{"omega": "second", "alpha": "first"})
	args := []string{"build", "all", "--path", root, "-o", "json", "--concurrency", "2"}
	stdout, stderr, code := runCLI(t, args...)
	if code != 0 {
		t.Fatalf("group build: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var groups []output.DocumentGroup
	assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
	if len(groups) != 2 {
		t.Fatalf("got groups: %s", stdout)
	}
	for i, dir := range []string{"alpha", "omega"} {
		assert.Equal(t, groups[i].Cluster, dir)
		entry, diagnostics, status := runCLI(t, "build", "all", "--path", filepath.Join(root, dir), "-o", "json", "--concurrency", "2")
		if status != code {
			t.Fatalf("entry %s status=%d stderr=%s", dir, status, diagnostics)
		}
		var documents []map[string]any
		assert.NoError(t, json.Unmarshal([]byte(entry), &documents))
		assert.Diff(t, groups[i].Documents, documents)
	}
	again, diagnostics, status := runCLI(t, args...)
	assert.Equal(t, status, code)
	assert.Equal(t, again, stdout)
	assert.Equal(t, diagnostics, stderr)

	stdout, stderr, code = runCLI(t, "get", "ks", "shared", "--path", root, "-o", "name", "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "alpha:shared\nomega:shared\n")
	assert.Equal(t, stderr, "")
	stdout, stderr, code = runCLI(t, "get", "images", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "alpha: example.test/app:first\nomega: example.test/app:second\n")
	assert.Equal(t, stderr, "")
	stdout, stderr, code = runCLI(t, "test", "ks", "shared", "--path", root, "--concurrency", "2")
	if code != 0 || !strings.Contains(stdout, "Cluster: alpha") || !strings.Contains(stdout, "Cluster: omega") || !strings.Contains(stdout, "Invocation totals") || strings.Contains(stdout, "failed") {
		t.Fatalf("group tests: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}

	testutil.WriteFile(t, root, "alpha/extra.yaml", testutil.KustomizationYAML("only-alpha", "flux-system", "  path: ./alpha/app\n"))
	testutil.WriteFile(t, root, "alpha/kustomization.yaml", "resources: [flux.yaml, extra.yaml]\n")
	stdout, stderr, code = runCLI(t, "get", "ks", "only-alpha", "--path", root, "-o", "name", "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "only-alpha\n")
	if strings.Contains(stderr, "not found") {
		t.Fatalf("another cluster's absence rejected a named match: %s", stderr)
	}
	_, stderr, code = runCLI(t, "get", "ks", "absent", "--path", root, "--concurrency", "2")
	if code != 1 || !strings.Contains(stderr, "absent") {
		t.Fatalf("global named absence: code=%d stderr=%s", code, stderr)
	}
}

func TestRun_ClusterGroups_PartialFailure(t *testing.T) {
	root := writeClusterGroupsFixture(t, map[string]string{"alpha": "first", "omega": "second"})
	appendScopeSpec(t, root, "alpha/flux.yaml", "  dependsOn: [{name: missing}]\n")
	stdout, stderr, code := runCLI(t, "build", "all", "--path", root, "-o", "json", "--concurrency", "2")
	if code != 1 || !strings.Contains(stderr, "Cluster: alpha") || !strings.Contains(stderr, "missing") {
		t.Fatalf("unattributed failure: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var groups []output.DocumentGroup
	assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
	hasPod := func(doc map[string]any) bool { return doc["kind"] == "Pod" }
	if len(groups) != 2 || groups[0].Cluster != "alpha" || slices.ContainsFunc(groups[0].Documents, hasPod) || groups[1].Cluster != "omega" || !slices.ContainsFunc(groups[1].Documents, hasPod) {
		t.Fatalf("failed cluster erased healthy output: %s", stdout)
	}
}

func TestRun_ClusterGroups_DiffAbsenceAndFailure(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old", "omega": "old"})
	for _, scenario := range []string{"removed", "failed"} {
		t.Run(scenario, func(t *testing.T) {
			values := map[string]string{"omega": "new"}
			if scenario == "failed" {
				values["alpha"] = "new"
			}
			current := writeClusterGroupsFixture(t, values)
			if scenario == "failed" {
				appendScopeSpec(t, current, "alpha/flux.yaml", "  dependsOn: [{name: missing}]\n")
			}
			stdout, stderr, code := runCLI(t, "diff", "all", "--path", current, "--path-orig", base, "-o", "json", "--concurrency", "2")
			var groups []struct {
				Cluster    string            `json:"cluster"`
				Removed    bool              `json:"removed"`
				Changes    []map[string]any  `json:"changes"`
				Suppressed []json.RawMessage `json:"suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
			hasPod := func(change map[string]any) bool { return change["kind"] == "Pod" }
			if len(groups) != 2 || groups[0].Cluster != "alpha" || groups[1].Cluster != "omega" || !slices.ContainsFunc(groups[1].Changes, hasPod) {
				t.Fatalf("keyed diff lost healthy group: code=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			entry, diagnostics, status := runCLI(t, "diff", "all", "--path", filepath.Join(current, "omega"), "--path-orig", filepath.Join(base, "omega"), "-o", "json", "--concurrency", "2")
			if status != 0 {
				t.Fatalf("healthy entry comparison: code=%d stdout=%s stderr=%s", status, entry, diagnostics)
			}
			var control struct {
				Changes []map[string]any `json:"changes"`
			}
			assert.NoError(t, json.Unmarshal([]byte(entry), &control))
			assert.Diff(t, groups[1].Changes, control.Changes)
			if scenario == "removed" {
				if code != 0 || !groups[0].Removed || !slices.ContainsFunc(groups[0].Changes, hasPod) || len(groups[0].Suppressed) != 0 {
					t.Fatalf("absent side suppressed removal: code=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				images, diagnostics, status := runCLI(t, "diff", "images", "--path", current, "--path-orig", base, "--concurrency", "2")
				if status != 0 || images != "Cluster: alpha (removed)\nomega: example.test/app:new\n" {
					t.Fatalf("empty removed image group disappeared: code=%d stdout=%s stderr=%s", status, images, diagnostics)
				}
			} else if code != 1 || groups[0].Removed || slices.ContainsFunc(groups[0].Changes, hasPod) || len(groups[0].Suppressed) == 0 || !strings.Contains(stderr, "cluster alpha") || !strings.Contains(stderr, "current") {
				t.Fatalf("failed side looked absent: code=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
	for _, tc := range []struct {
		name, path, resource string
	}{
		{"shared-component", "components/shared/input.yaml", "shared-input"},
		{"private-file", "alpha/app/private.yaml", "private-input"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old", "omega": "old"})
			current := writeClusterGroupsFixture(t, map[string]string{"alpha": "old", "omega": "old"})
			const input = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s, namespace: apps}\ndata: {value: %s}\n"
			for _, root := range []string{base, current} {
				testutil.WriteFile(t, root, "components/shared/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [input.yaml]\n")
				testutil.WriteFile(t, root, "components/shared/input.yaml", fmt.Sprintf(input, "shared-input", "original"))
				for _, entry := range []string{"alpha", "omega"} {
					testutil.WriteFile(t, root, entry+"/app/kustomization.yaml", "resources: [pod.yaml, private.yaml]\ncomponents: [../../components/shared]\n")
					testutil.WriteFile(t, root, entry+"/app/private.yaml", fmt.Sprintf(input, "private-input", "original"))
				}
			}
			testutil.WriteFile(t, current, tc.path, fmt.Sprintf(input, tc.resource, "changed"))
			stdout, stderr, code := runCLI(t, "diff", "all", "--path", current, "--path-orig", base, "-o", "json", "--concurrency", "2")
			if code != 0 {
				t.Fatalf("group comparison: code=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			var groups []struct {
				Cluster    string             `json:"cluster"`
				Changes    []diff.Change      `json:"changes"`
				Suppressed []diff.Suppression `json:"suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
			if len(groups) != 2 || groups[0].Cluster != "alpha" || groups[1].Cluster != "omega" {
				t.Fatalf("shared-input fixture lost cluster roster: %s", stdout)
			}
			for i, entry := range []string{"alpha", "omega"} {
				out, diagnostics, status := runCLI(t, "diff", "all", "--path", filepath.Join(current, entry), "--path-orig", filepath.Join(base, entry), "-o", "json", "--concurrency", "2")
				if status != 0 {
					t.Fatalf("entry %s comparison: code=%d stdout=%s stderr=%s", entry, status, out, diagnostics)
				}
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
				if len(groups[i].Changes) != 1 {
					t.Fatalf("cluster %s lost input change: %s", entry, stdout)
				}
				change := groups[i].Changes[0]
				assert.Equal(t, change.Kind, "ConfigMap")
				assert.Equal(t, change.Name, tc.resource)
				assert.Equal(t, change.Status, diff.StatusChanged)
				assert.Diff(t, change.Old["data"], map[string]any{"value": "original"})
				assert.Diff(t, change.New["data"], map[string]any{"value": "changed"})
			}
		})
	}
}
