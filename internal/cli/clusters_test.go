package cli

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diffprep"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/homeport/dyff/pkg/dyff"
	"sigs.k8s.io/yaml"
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
	assert.Equal(t, stderr, "\n  alpha: ✓ 0 failed\n\n  omega: ✓ 0 failed\n")
	stdout, _, code = runCLI(t, "get", "ks", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	if strings.Contains(stdout, "CLUSTER") || !strings.Contains(stdout, "alpha: shared") || !strings.Contains(stdout, "omega: shared") {
		t.Fatalf("table names must carry the shared qualifier: %s", stdout)
	}
	stdout, stderr, code = runCLI(t, "get", "images", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "alpha: example.test/app:first\nomega: example.test/app:second\n")
	assert.Equal(t, stderr, "\n  alpha: ✓ 0 failed\n\n  omega: ✓ 0 failed\n")
	stdout, stderr, code = runCLI(t, "test", "ks", "shared", "--path", root, "--concurrency", "2")
	if code != 0 || !strings.Contains(stdout, "alpha: flux-system/shared") || !strings.Contains(stdout, "omega: flux-system/shared") || strings.Contains(stdout, "Cluster:") || !strings.Contains(stdout, "Invocation totals") || strings.Contains(stdout, "failed") {
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
	stdout, stderr, code = runCLI(t, "test", "ks", "only-alpha", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	if strings.Contains(stdout, "alpha:") || !strings.Contains(stdout, "Kustomization  flux-system/only-alpha") {
		t.Fatalf("a single retained named cluster must stay unqualified: %s %s", stdout, stderr)
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
	if code != 1 || !strings.Contains(stderr, "alpha: flux-system/missing") || strings.Contains(stderr, "Cluster:") || !strings.Contains(stderr, "missing") {
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
				Cluster    string             `json:"cluster"`
				Removed    bool               `json:"removed"`
				Changes    []map[string]any   `json:"changes"`
				Suppressed []diff.Suppression `json:"suppressed"`
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
				if status != 0 || images != "omega: example.test/app:new\n" {
					t.Fatalf("empty removed image group disappeared: code=%d stdout=%s stderr=%s", status, images, diagnostics)
				}
			} else {
				if code != 1 || groups[0].Removed || slices.ContainsFunc(groups[0].Changes, hasPod) || len(groups[0].Suppressed) == 0 {
					t.Fatalf("failed side looked absent: code=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				for _, note := range groups[0].Suppressed {
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
						images, diagnostics, status := runCLI(t, "diff", "images", "--path", current, "--path-orig", base, "-o", format, "--concurrency", "2")
						assert.Equal(t, status, 1)
						var imageGroups []output.ImageGroup
						assert.NoError(t, yaml.Unmarshal([]byte(images), &imageGroups))
						if len(imageGroups) != 2 || imageGroups[0].Cluster != "alpha" || imageGroups[1].Cluster != "omega" || len(imageGroups[0].Images) != 0 || imageGroups[0].Removed {
							t.Fatalf("failed image side looked absent: %s", images)
						}
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

func TestRun_ClusterReport_NativeAggregateInputs(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old", "omega": "old"})
	current := writeClusterGroupsFixture(t, map[string]string{"alpha": "new", "omega": "old"})
	for _, format := range []string{"human", "diff", "brief", "github", "gitlab", "gitea", "html"} {
		t.Run(format, func(t *testing.T) {
			args := []string{"diff", "all", "--path", current, "--path-orig", base, "-o", format, "--concurrency", "2"}
			out, stderr, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("diff failed: %s", stderr)
			}
			if strings.Contains(out, "Cluster:") || strings.Contains(out, "omega:") || strings.Contains(out, "iframe") {
				t.Fatalf("unchanged cluster or retired framing in diff: %s", out)
			}
			switch format {
			case "brief":
				assert.Equal(t, out, "two changes detected between from, two documents and to, two documents\n\n")
			case "diff":
				if !strings.Contains(out, "--- alpha: Pod apps/shared") {
					t.Fatalf("missing unified label: %s", out)
				}
			case "html":
				if strings.Count(strings.ToLower(out), "<!doctype html>") != 1 || !strings.Contains(out, "Pod alpha: apps/shared") || !strings.Contains(out, "Kustomization alpha: flux-system/shared") {
					t.Fatalf("HTML must share a single named document: %s", out)
				}
			default:
				if !strings.Contains(out, "alpha: v1/Pod/apps/shared") {
					t.Fatalf("missing native label: %s", out)
				}
			}
			again, _, status := runCLI(t, args...)
			assert.Equal(t, status, code)
			assert.Equal(t, again, out)
		})
	}
	testutil.WriteFile(t, current, "omega/flux.yaml", testutil.KustomizationYAML("shared", "flux-system",
		"  path: ./omega/app\n  postBuild: {substitute: {CLUSTER: new}}\n"))
	html, diagnostics, status := runCLI(t, "diff", "all", "--path", current, "--path-orig", base, "-o", "html", "--concurrency", "2")
	assert.Equal(t, status, 0)
	for _, token := range []string{`<section class="resource" id="r0">`, `<section class="resource" id="r1">`, "Pod alpha: apps/shared", "Pod omega: apps/shared"} {
		if strings.Count(html, token) != 1 {
			t.Fatalf("missing or duplicate HTML anchor/label %q: %s %s", token, html, diagnostics)
		}
	}
	if strings.Contains(html, "iframe") {
		t.Fatalf("retired HTML wrapper: %s", html)
	}
	for _, format := range []string{"human", "diff", "brief", "github", "gitlab", "gitea", "html"} {
		t.Run("unchanged/"+format, func(t *testing.T) {
			out, stderr, code := runCLI(t, "diff", "all", "--path", base, "--path-orig", base, "-o", format, "--concurrency", "2")
			assert.Equal(t, code, 0)
			assert.Equal(t, out, "")
			if strings.Contains(stderr, "Cluster:") {
				t.Fatalf("empty body has a cluster heading: %s", stderr)
			}
		})
	}
	for _, tc := range []struct {
		name          string
		orig, current bool
	}{
		{name: "zero_original", current: true},
		{name: "one_original_changed", orig: true, current: true},
		{name: "one_original_removed", orig: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old", "omega": "old"})
			current := writeClusterGroupsFixture(t, map[string]string{"alpha": "new", "omega": "old"})
			for _, side := range []struct {
				path    string
				present bool
			}{{base, tc.orig}, {current, tc.current}} {
				testutil.WriteFile(t, side.path, "omega/app/kustomization.yaml", "resources: []\n")
				if !side.present {
					testutil.WriteFile(t, side.path, "alpha/app/kustomization.yaml", "resources: []\n")
				}
			}
			for _, format := range []string{"human", "brief", "github", "gitlab", "gitea"} {
				t.Run(format, func(t *testing.T) {
					out, stderr, code := runCLI(t, "diff", "ks", "shared", "--path", current, "--path-orig", base, "-o", format, "--skip-kinds", "Kustomization", "--concurrency", "2")
					if code != 0 {
						t.Fatalf("diff failed: %s", stderr)
					}
					want := "one change detected between from and to\n\n"
					if format != "brief" {
						path, change, prefix := "@@ (root level) @@", "! + one document added:", "+ "
						value := "new"
						if tc.orig {
							change, prefix, value = "! - one document removed:", "- ", "old"
						}
						switch format {
						case "human":
							path, change, prefix = "(root level)", strings.TrimPrefix(change, "! "), "  "
						case "gitlab":
							path, change = "= (root level)", "#"+strings.TrimPrefix(change, "!")
						}
						body := "apiVersion: v1\nkind: Pod\nmetadata:\n  name: shared\n  namespace: apps\n  labels:\n" +
							"    kustomize.toolkit.fluxcd.io/name: shared\n    kustomize.toolkit.fluxcd.io/namespace: flux-system\n" +
							"spec:\n  containers:\n  - name: app\n    image: \"example.test/app:" + value + "\""
						want = "\n" + path + "\n" + change + "\n" + prefix + strings.ReplaceAll(body, "\n", "\n"+prefix) + "\n\n"
						if tc.orig && tc.current {
							want = "\n@@ spec.containers.app.image @@\n! ± value change\n- example.test/app:old\n+ example.test/app:new\n\n"
							switch format {
							case "human":
								want = "\nspec.containers.app.image\n  ± value change\n    - example.test/app:old\n    + example.test/app:new\n\n"
							case "gitlab":
								want = "\n= spec.containers.app.image\n# ± value change\n- example.test/app:old\n+ example.test/app:new\n\n"
							}
						}
					}
					assert.Equal(t, out, want)
					for _, label := range []string{"alpha:", "omega:", "v1/Pod/apps/shared", "Cluster:"} {
						if strings.Contains(out, label) {
							t.Fatalf("native omission gained label %q: %s", label, out)
						}
					}
				})
			}
		})
	}
}

func TestClusterReport_NativeAggregateIndexes(t *testing.T) {
	t.Parallel()
	doc := func(name, value string) diffprep.Doc {
		return diffprep.Doc{Manifest: map[string]any{"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name}, "data": map[string]any{"value": value}}}
	}
	groups := make([]dyff.Report, 2)
	var err error
	groups[0], err = diffprep.Prepare([]diffprep.Doc{doc("same", "old"), doc("removed", "old")}, []diffprep.Doc{doc("same", "new")}, diffprep.Options{}).Native()
	assert.NoError(t, err)
	groups[1], err = diffprep.Prepare([]diffprep.Doc{doc("same", "old")}, []diffprep.Doc{doc("same", "new"), doc("added", "new")}, diffprep.Options{}).Native()
	assert.NoError(t, err)
	aggregate := aggregateNative(groups)
	assert.Equal(t, len(aggregate.From.Documents), 3)
	assert.Equal(t, len(aggregate.To.Documents), 3)
	assert.Equal(t, len(aggregate.Diffs), 4)
	assert.Diff(t, aggregate.From.Names, []string{"v1/ConfigMap/same", "v1/ConfigMap/removed", "v1/ConfigMap/same"})
	assert.Diff(t, aggregate.To.Names, []string{"v1/ConfigMap/same", "v1/ConfigMap/same", "v1/ConfigMap/added"})
	for i, want := range []struct {
		to    bool
		index int
	}{{false, 0}, {false, 1}, {false, 2}, {true, 2}} {
		path := aggregate.Diffs[i].Path
		root := &aggregate.From
		if want.to {
			root = &aggregate.To
		}
		if path.Root != root || path.DocumentIdx != want.index {
			t.Fatalf("diff %d has wrong side or offset: %+v", i, path)
		}
	}
	assert.Equal(t, groups[1].Diffs[0].Path.DocumentIdx, 0)
	if groups[1].Diffs[0].Path.Root == &aggregate.From {
		t.Fatal("aggregation mutated its input paths")
	}
}
