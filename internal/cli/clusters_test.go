package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diffprep"
	"github.com/home-operations/flate/internal/imageutil"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/homeport/dyff/pkg/dyff"
	"github.com/spf13/pflag"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

func runClusterInputs(t *testing.T, argv ...string) (string, string, int) {
	t.Helper()
	verb, kind, name := argv[0], "all", ""
	args := argv[1:]
	if len(args) != 0 && !strings.HasPrefix(args[0], "-") {
		kind, args = args[0], args[1:]
	}
	if len(args) != 0 && !strings.HasPrefix(args[0], "-") {
		name, args = args[0], args[1:]
	}
	c := commonFlags{}
	flags := pflag.NewFlagSet("independent cluster inputs", pflag.ContinueOnError)
	bindCommon(flags, &c)
	format := output.FormatTable
	switch verb {
	case "build":
		format = output.FormatYAML
	case "diff":
		format = output.DiffFormats[0]
		if kind == "images" {
			format = output.FormatName
		}
	}
	flags.StringVarP(&c.output, "output", "o", string(format), "")
	assert.NoError(t, flags.Parse(args))
	assert.NoError(t, c.loadInputs(t.Context()))
	var trees []orchestrator.Tree
	for _, cluster := range []string{"alpha", "omega"} {
		for _, side := range []struct {
			root string
			id   manifest.TreeSide
		}{{c.pathOrig, manifest.TreeBase}, {c.path, manifest.TreeHead}} {
			if side.root == "" {
				continue
			}
			path := filepath.Join(side.root, cluster)
			if _, err := os.Stat(filepath.Join(path, "kustomization.yaml")); err == nil {
				trees = append(trees, orchestrator.Tree{ID: manifest.TreeID{Cluster: manifest.ClusterID(cluster), Side: side.id}, Path: path, RepoRoot: side.root})
			}
		}
	}
	if len(trees) == 0 {
		return runCLI(t, argv...)
	}
	if c.cacheDir == "" {
		c.cacheDir = t.TempDir()
	}
	cfg := buildOrchCfg(c)
	cfg.HeadOnly = verb != "diff" && c.pathOrig != ""
	runs, err := orchestrator.RenderTrees(t.Context(), trees, cfg)
	if len(runs) == 0 {
		t.Fatalf("independent trees unavailable: %v", err)
	}
	kinds := renderKinds
	switch kind {
	case "ks":
		kinds = []string{manifest.KindKustomization}
	case "hr":
		kinds = []string{manifest.KindHelmRelease}
	}
	var stdout, stderr bytes.Buffer
	req := selectedRequest{flags: c, roots: c.roots(name, kinds...), pair: verb == "diff", test: verb == "test", stdout: &stdout, stderr: &stderr}
	inv := &selectedInvocation{}
	for first := 0; first < len(runs); {
		last := first + 1
		for last < len(runs) && runs[last].ID.Cluster == runs[first].ID.Cluster {
			last++
		}
		groupRuns := runs[first:last]
		group := selectedGroup{cluster: string(runs[first].ID.Cluster), complete: true}
		var groupErr error
		for _, run := range groupRuns {
			groupErr = errors.Join(groupErr, run.Err)
			group.absent[run.ID.Side] = run.Absent
		}
		if !req.pair {
			groupRuns = groupRuns[len(groupRuns)-1:]
		}
		group.sides, group.err, group.remainder = orchestrator.Select(groupRuns, groupErr, req.roots, nil)
		inv.groups = append(inv.groups, group)
		first = last
	}
	if name != "" {
		inv.groups = slices.DeleteFunc(inv.groups, func(group selectedGroup) bool { return group.sides[0].Counts[kinds[0]] == 0 })
		if len(inv.groups) == 0 {
			return "", noNamedError(kinds[0], name, "independent trees").Error(), 1
		}
	}
	encoder := output.Encoder{Writer: &stdout, Format: output.Format(c.output)}
	payload := selectedPayload{}
	switch verb {
	case "get":
		sections := []listSection{collectSelected(inv, ksView()), collectSelected(inv, hrView())}
		switch kind {
		case "ks":
			sections = sections[:1]
		case "hr":
			sections = sections[1:]
		}
		payload = resourcePayload(inv, sections, encoder, kind == "all")
	case "build":
		groups := make([]output.DocumentGroup, len(inv.groups))
		for i, group := range inv.groups {
			groups[i] = output.DocumentGroup{Cluster: group.cluster, Documents: collectRendered(group.sides[0], &buildFlags{})}
		}
		payload.write = func() error { return encoder.DocumentGroups(groups) }
	case "test":
		for _, group := range inv.groups {
			payload.rosters = append(payload.rosters, testrunner.Run(group.sides[0]))
		}
	case "diff":
		payload = clusterDiffPayload(t, inv, encoder)
	}
	if kind == "images" {
		groups := make([]output.ImageGroup, len(inv.groups))
		payload = selectedPayload{warnings: make([][]manifest.Warning, len(inv.groups))}
		table := output.Table{Columns: []output.Column{{Header: "IMAGE", Key: "image"}}}
		for i, group := range inv.groups {
			images := slices.Sorted(maps.Keys(imageutil.Collect(group.sides[0].Manifests)))
			if verb == "diff" {
				var notes []diff.Suppression
				images, notes, err = diff.CompareImages(clusterSnapshot(group.sides[0]), clusterSnapshot(group.sides[1]), nil, false)
				payload.err = errors.Join(payload.err, err)
				payload.warnings[i] = appendSuppressionWarnings(nil, notes)
			}
			groups[i] = output.ImageGroup{Cluster: group.cluster, Images: images, Added: group.absent[manifest.TreeBase], Removed: group.absent[manifest.TreeHead]}
			for _, image := range images {
				table.Rows = append(table.Rows, map[string]string{"cluster": group.cluster, "image": image})
			}
		}
		tables, _ := prepareGetTables(inv, []output.Table{table})
		payload.prepare = func(names []output.Names) {
			for i := range groups {
				for _, image := range groups[i].Images {
					groups[i].Names = append(groups[i].Names, names[i].Prefix+image)
				}
			}
		}
		payload.write = func() error {
			if encoder.Format == output.FormatTable {
				return encoder.Tables(tables)
			}
			return encoder.ImageGroups(groups)
		}
	}
	if err := finishSelected(req, inv, payload); err != nil {
		return stdout.String(), stderr.String(), 1
	}
	return stdout.String(), stderr.String(), 0
}

func clusterSnapshot(side *selection.Selection) diff.Snapshot {
	return diff.Snapshot{Manifests: side.Manifests, Unavailable: side.Unavailable, ProducerKeys: side.ProducerKeys}
}

func clusterDiffPayload(t *testing.T, inv *selectedInvocation, encoder output.Encoder) selectedPayload {
	t.Helper()
	groups := make([]output.DiffGroup, len(inv.groups))
	native := make([]dyff.Report, len(inv.groups))
	lines := make([][]output.LineChange, len(inv.groups))
	payload := selectedPayload{warnings: make([][]manifest.Warning, len(inv.groups)), subjects: make([][]manifest.NamedResource, len(inv.groups))}
	for i, group := range inv.groups {
		before, after := clusterSnapshot(group.sides[0]), clusterSnapshot(group.sides[1])
		if encoder.Format == output.FormatJSON {
			body, warnings, notes, err := diff.Compare(before, after, nil, diff.Options{Format: diff.FormatJSON, StripAttrs: diff.DefaultStripAttrs, StripFields: diff.DefaultStripFields})
			assert.NoError(t, err)
			groups[i] = output.DiffGroup{Cluster: group.cluster, Body: body, Added: group.absent[manifest.TreeBase], Removed: group.absent[manifest.TreeHead]}
			payload.warnings[i] = appendSuppressionWarnings(warnings, notes)
			continue
		}
		left, right, notes := diffprep.Suppress(diffprep.Snapshot{Manifests: before.Manifests, Unavailable: before.Unavailable, ProducerKeys: before.ProducerKeys}, diffprep.Snapshot{Manifests: after.Manifests, Unavailable: after.Unavailable, ProducerKeys: after.ProducerKeys})
		prepared := diffprep.Prepare(diffprep.Documents(left), diffprep.Documents(right), diffprep.Options{StripAttrs: diff.DefaultStripAttrs, StripFields: diff.DefaultStripFields})
		payload.warnings[i] = prepared.Warnings
		for _, note := range notes {
			payload.warnings[i] = append(payload.warnings[i], manifest.Warning{Resource: note.Parent.NamedResource, Category: manifest.WarnDiffSuppressed, Message: note.String()})
		}
		var err error
		if encoder.Format == output.FormatDiff || encoder.Format == output.FormatHTML {
			lines[i], err = output.PrepareLines(prepared)
			payload.subjects[i] = output.LineSubjects(lines[i], encoder.Format)
		} else {
			native[i], err = prepared.Native()
		}
		assert.NoError(t, err)
	}
	aggregate := aggregateNative(native)
	payload.prepare = func(names []output.Names) {
		from, to := 0, 0
		for i := range inv.groups {
			output.NameLines(lines[i], names[i])
			for j, name := range native[i].From.Names {
				aggregate.From.Names[from+j] = names[i].Prefix + name
			}
			for j, name := range native[i].To.Names {
				aggregate.To.Names[to+j] = names[i].Prefix + name
			}
			from, to = from+len(native[i].From.Names), to+len(native[i].To.Names)
		}
	}
	payload.write = func() error {
		switch encoder.Format {
		case output.FormatJSON:
			return encoder.DiffGroups(groups)
		case output.FormatDiff, output.FormatHTML:
			return encoder.Lines(slices.Concat(lines...))
		default:
			return encoder.Native(*aggregate)
		}
	}
	return payload
}

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
	collision, diagnostic, status := runCLI(t, args...)
	if status != 1 || collision != "[]\n" || !strings.Contains(diagnostic, "duplicate resource(s)") || !strings.Contains(diagnostic, "select an individual overlay with -p") {
		t.Fatalf("literal duplicate root was not rejected: code=%d stdout=%s stderr=%s", status, collision, diagnostic)
	}
	stdout, stderr, code := runClusterInputs(t, args...)
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
		entry, diagnostics, status := runClusterInputs(t, "build", "all", "--path", filepath.Join(root, dir), "-o", "json", "--concurrency", "2")
		if status != code {
			t.Fatalf("entry %s status=%d stderr=%s", dir, status, diagnostics)
		}
		var documents []map[string]any
		assert.NoError(t, json.Unmarshal([]byte(entry), &documents))
		assert.Diff(t, groups[i].Documents, documents)
	}
	again, diagnostics, status := runClusterInputs(t, args...)
	assert.Equal(t, status, code)
	assert.Equal(t, again, stdout)
	assert.Equal(t, diagnostics, stderr)

	stdout, stderr, code = runClusterInputs(t, "get", "ks", "shared", "--path", root, "-o", "name", "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "alpha:shared\nomega:shared\n")
	assert.Equal(t, stderr, "\n  alpha: ✓ 0 failed\n\n  omega: ✓ 0 failed\n")
	stdout, _, code = runClusterInputs(t, "get", "ks", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "CLUSTER    NAMESPACE      NAME      PATH\nalpha      flux-system    shared    ./alpha/app\nomega      flux-system    shared    ./omega/app\n")
	stdout, stderr, code = runClusterInputs(t, "get", "images", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "CLUSTER    IMAGE\nalpha      example.test/app:first\nomega      example.test/app:second\n")
	assert.Equal(t, stderr, "\n  alpha: ✓ 0 failed\n\n  omega: ✓ 0 failed\n")
	stdout, stderr, code = runClusterInputs(t, "test", "ks", "shared", "--path", root, "--concurrency", "2")
	if code != 0 || !strings.Contains(stdout, "alpha: flux-system/shared") || !strings.Contains(stdout, "omega: flux-system/shared") || strings.Contains(stdout, "Cluster:") || !strings.Contains(stdout, "Invocation totals") || strings.Contains(stdout, "failed") {
		t.Fatalf("group tests: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}

	testutil.WriteFile(t, root, "alpha/extra.yaml", testutil.KustomizationYAML("only-alpha", "flux-system", "  path: ./alpha/app\n"))
	testutil.WriteFile(t, root, "alpha/kustomization.yaml", "resources: [flux.yaml, extra.yaml]\n")
	stdout, stderr, code = runClusterInputs(t, "get", "ks", "only-alpha", "--path", root, "-o", "name", "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "only-alpha\n")
	if strings.Contains(stderr, "not found") {
		t.Fatalf("another cluster's absence rejected a named match: %s", stderr)
	}
	stdout, _, code = runClusterInputs(t, "get", "ks", "only-alpha", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "NAMESPACE      NAME          PATH\nflux-system    only-alpha    ./alpha/app\n")
	stdout, stderr, code = runClusterInputs(t, "test", "ks", "only-alpha", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	if strings.Contains(stdout, "alpha:") || !strings.Contains(stdout, "Kustomization  flux-system/only-alpha") {
		t.Fatalf("a single retained named cluster must stay unqualified: %s %s", stdout, stderr)
	}
	_, stderr, code = runClusterInputs(t, "get", "ks", "absent", "--path", root, "--concurrency", "2")
	if code != 1 || !strings.Contains(stderr, "absent") {
		t.Fatalf("global named absence: code=%d stderr=%s", code, stderr)
	}
	writeGetRelease(t, root, "alpha")
	stdout, stderr, code = runClusterInputs(t, "get", "all", "--path", root, "--concurrency", "2")
	if code != 0 || strings.Count(stdout, "CLUSTER") != 2 || !strings.Contains(stdout, "HelmRelease/shared") || !strings.Contains(stdout, "omega") {
		t.Fatalf("a kind present in one cluster lost the invocation roster: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	for section := range strings.SplitSeq(strings.TrimSuffix(stdout, "\n"), "\n\n") {
		if !strings.HasPrefix(section, "CLUSTER    ") {
			t.Fatalf("mixed section lost CLUSTER: %s", section)
		}
	}

	for _, format := range []string{"table", "name", "json", "yaml"} {
		for _, command := range []string{"get all", "get"} {
			out, diagnostics, status := runClusterInputs(t, append(strings.Fields(command), "--path", root, "--concurrency", "2", "-n", "absent", "-o", format)...)
			if status != 0 {
				t.Fatalf("empty roster: exit=%d stdout=%s stderr=%s", status, out, diagnostics)
			}
			if format == "json" || format == "yaml" {
				groups := decodeGetDocuments(t, out, format, []string{"alpha", "omega"})
				for _, group := range groups {
					assert.Equal(t, len(group.Documents), 0)
					if format == "json" && group.Documents == nil {
						t.Fatal("empty documents must be []")
					}
				}
			} else {
				assert.Equal(t, out, "")
			}
		}
	}

}

func TestRun_ClusterGroups_PartialFailure(t *testing.T) {
	root := writeClusterGroupsFixture(t, map[string]string{"alpha": "first", "omega": "second"})
	appendScopeSpec(t, root, "alpha/flux.yaml", "  dependsOn: [{name: missing}]\n")
	stdout, stderr, code := runClusterInputs(t, "build", "all", "--path", root, "-o", "json", "--concurrency", "2")
	if code != 1 || !strings.Contains(stderr, "alpha: flux-system/missing") || strings.Contains(stderr, "Cluster:") || !strings.Contains(stderr, "missing") {
		t.Fatalf("unattributed failure: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var groups []output.DocumentGroup
	assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
	hasPod := func(doc map[string]any) bool { return doc["kind"] == "Pod" }
	if len(groups) != 2 || groups[0].Cluster != "alpha" || slices.ContainsFunc(groups[0].Documents, hasPod) || groups[1].Cluster != "omega" || !slices.ContainsFunc(groups[1].Documents, hasPod) {
		t.Fatalf("failed cluster erased healthy output: %s", stdout)
	}
	writeGetRelease(t, root, "omega")
	for _, command := range []string{"get all", "get", "get images"} {
		for _, format := range []string{"table", "name", "json", "yaml"} {
			t.Run(command+"/"+format, func(t *testing.T) {
				args := append(strings.Fields(command), "--path", root, "-o", format, "--concurrency", "2", "--cache-dir", t.TempDir())
				out, diagnostics, status := runClusterInputs(t, args...)
				if status != 1 || !strings.Contains(diagnostics, "alpha: flux-system/missing") || strings.Contains(diagnostics, "Cluster:") {
					t.Fatalf("lost qualified failure: exit=%d stdout=%s stderr=%s", status, out, diagnostics)
				}
				if command == "get images" {
					if format == "table" {
						assert.Equal(t, out, "CLUSTER    IMAGE\nomega      example.test/app:second\n")
					}
					if format == "name" {
						assert.Equal(t, out, "omega: example.test/app:second\n")
					}
					if format == "json" || format == "yaml" {
						var images []output.ImageGroup
						assert.NoError(t, yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(out), 4096).Decode(&images))
						assert.Equal(t, len(images), 2)
						assert.Equal(t, images[0].Cluster, "alpha")
						assert.Equal(t, len(images[0].Images), 0)
						assert.Equal(t, images[1].Cluster, "omega")
						assert.Diff(t, images[1].Images, []string{"example.test/app:second"})
					}
					return
				}
				switch format {
				case "table":
					if strings.Count(out, "CLUSTER") != 2 || !strings.Contains(out, "omega") || !strings.Contains(out, "Kustomization/shared") || !strings.Contains(out, "HelmRelease/shared") {
						t.Fatalf("partial table lost healthy resources: %s", out)
					}
				case "name":
					assert.Equal(t, out, "alpha:Kustomization/shared\nomega:Kustomization/shared\nomega:HelmRelease/shared\n")
				default:
					groups := decodeGetDocuments(t, out, format, []string{"alpha", "omega"})
					assert.Equal(t, len(groups[0].Documents), 1)
					assert.Equal(t, groups[0].Documents[0]["kind"], any("Kustomization"))
					assert.Equal(t, len(groups[1].Documents), 2)
					assert.Equal(t, groups[1].Documents[0]["kind"], any("Kustomization"))
					assert.Equal(t, groups[1].Documents[1]["kind"], any("HelmRelease"))
					assert.Equal(t, groups[1].Documents[1]["name"], any("shared"))
				}
			})
		}
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
			stdout, stderr, code := runClusterInputs(t, "diff", "all", "--path", current, "--path-orig", base, "-o", "json", "--concurrency", "2")
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
			entry, diagnostics, status := runClusterInputs(t, "diff", "all", "--path", filepath.Join(current, "omega"), "--path-orig", filepath.Join(base, "omega"), "-o", "json", "--concurrency", "2")
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
				images, diagnostics, status := runClusterInputs(t, "diff", "images", "--path", current, "--path-orig", base, "--concurrency", "2")
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
						images, diagnostics, status := runClusterInputs(t, "diff", "images", "--path", current, "--path-orig", base, "-o", format, "--concurrency", "2")
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
			stdout, stderr, code := runClusterInputs(t, "diff", "all", "--path", current, "--path-orig", base, "-o", "json", "--concurrency", "2")
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
				out, diagnostics, status := runClusterInputs(t, "diff", "all", "--path", filepath.Join(current, entry), "--path-orig", filepath.Join(base, entry), "-o", "json", "--concurrency", "2")
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
			out, stderr, code := runClusterInputs(t, args...)
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
			again, _, status := runClusterInputs(t, args...)
			assert.Equal(t, status, code)
			assert.Equal(t, again, out)
		})
	}
	testutil.WriteFile(t, current, "omega/flux.yaml", testutil.KustomizationYAML("shared", "flux-system",
		"  path: ./omega/app\n  postBuild: {substitute: {CLUSTER: new}}\n"))
	html, diagnostics, status := runClusterInputs(t, "diff", "all", "--path", current, "--path-orig", base, "-o", "html", "--concurrency", "2")
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
			out, stderr, code := runClusterInputs(t, "diff", "all", "--path", base, "--path-orig", base, "-o", format, "--concurrency", "2")
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
					out, stderr, code := runClusterInputs(t, "diff", "ks", "shared", "--path", current, "--path-orig", base, "-o", format, "--skip-kinds", "Kustomization", "--concurrency", "2")
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

func writeGetRelease(t *testing.T, root, dir string) {
	t.Helper()
	testutil.WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: installed, namespace: workloads}\ndata: {value: rendered}\n")
	testutil.WriteFile(t, root, dir+"/app/kustomization.yaml", "resources: [pod.yaml, release.yaml]\n")
	testutil.WriteFile(t, root, dir+"/app/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: shared, namespace: apps}
spec:
  targetNamespace: workloads
  releaseName: installed
  chart:
    spec:
      chart: ./charts/app
      version: 0.1.0
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
}

func TestRun_Get_Formats(t *testing.T) {
	root := writeClusterGroupsFixture(t, map[string]string{"alpha": "first", "omega": "second"})
	for _, dir := range []string{"alpha", "omega"} {
		appendScopeSpec(t, root, dir+"/flux.yaml", "  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")
		writeGetRelease(t, root, dir)
		testutil.WriteFile(t, root, dir+"/app/images.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: images, namespace: apps}\ndata: {shared: 'example.test/shared:v1', unusual: 'count:up0'}\n")
		testutil.WriteFile(t, root, dir+"/app/kustomization.yaml", "resources: [pod.yaml, release.yaml, images.yaml]\n")
	}
	for _, scope := range []struct {
		name, path string
		clusters   []string
	}{
		{"single", filepath.Join(root, "alpha"), []string{"alpha"}},
		{"multiple", root, []string{"alpha", "omega"}},
	} {
		t.Run(scope.name, func(t *testing.T) {
			for _, format := range []string{"", "table", "yaml", "json", "name"} {
				t.Run("format="+format, func(t *testing.T) {
					var combined string
					for _, command := range []string{"get all", "get", "get ks", "get hr", "get images"} {
						t.Run(command, func(t *testing.T) {
							args := append(strings.Fields(command), "--path", scope.path, "--cache-dir", t.TempDir(), "--concurrency", "2")
							if format != "" {
								args = append(args, "-o", format)
							}
							out, diagnostics, status := runClusterInputs(t, args...)
							if status != 0 {
								t.Fatalf("exit=%d stdout=%s stderr=%s", status, out, diagnostics)
							}
							again, stderr, code := runClusterInputs(t, args...)
							assert.Equal(t, code, status)
							assert.Equal(t, again, out)
							assert.Equal(t, stderr, diagnostics)
							if command == "get all" {
								combined = out
							}
							if command == "get" {
								assert.Equal(t, out, combined)
							}
							if command == "get images" {
								want := []string{"example.test/app:first", "example.test/shared:v1"}
								if format == "json" || format == "yaml" {
									var images []string
									if len(scope.clusters) == 1 {
										assert.NoError(t, yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(out), 4096).Decode(&images))
										assert.Diff(t, images, want)
									} else {
										var groups []output.ImageGroup
										assert.NoError(t, yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(out), 4096).Decode(&groups))
										assert.Equal(t, len(groups), 2)
										for i, cluster := range scope.clusters {
											assert.Equal(t, groups[i].Cluster, cluster)
											if i == 1 {
												want[0] = "example.test/app:second"
											}
											assert.Diff(t, groups[i].Images, want)
										}
									}
								} else {
									expected := ""
									if format != "name" {
										if len(scope.clusters) == 2 {
											expected = "CLUSTER    "
										}
										expected += "IMAGE\n"
									}
									for i, cluster := range scope.clusters {
										if i == 1 {
											want[0] = "example.test/app:second"
										}
										for _, image := range want {
											prefix := ""
											if len(scope.clusters) == 2 {
												prefix = fmt.Sprintf("%-11s", cluster)
												if format == "name" {
													prefix = cluster + ": "
												}
											}
											expected += prefix + image + "\n"
										}
									}
									assert.Equal(t, out, expected)
								}
								return
							}
							kinds := []string{"Kustomization", "HelmRelease"}
							if command == "get ks" {
								kinds = kinds[:1]
							}
							if command == "get hr" {
								kinds = kinds[1:]
							}
							all := command == "get" || command == "get all"
							if format == "yaml" || format == "json" {
								groups := decodeGetDocuments(t, out, format, scope.clusters)
								for i, group := range groups {
									assert.Equal(t, len(group.Documents), len(kinds))
									for j, kind := range kinds {
										doc := group.Documents[j]
										assert.Equal(t, doc["kind"], any(kind))
										assert.Equal(t, doc["name"], any("shared"))
										assert.Diff(t, doc["sourceRef"], map[string]any{"kind": "GitRepository", "name": "flux-system", "namespace": "flux-system"})
										if kind == "Kustomization" {
											assert.Equal(t, doc["path"], any("./"+scope.clusters[i]+"/app"))
											assert.Equal(t, doc["prune"], any(false))
											assert.Equal(t, doc["wait"], any(false))
										} else {
											assert.Equal(t, doc["chart"], any("flux-system-flux-system/./charts/app"))
											assert.Equal(t, doc["version"], any("0.1.0"))
											assert.Equal(t, doc["releaseName"], any("installed"))
											assert.Equal(t, doc["targetNamespace"], any("workloads"))
										}
									}
								}
							} else {
								var expected string
								for j, kind := range kinds {
									if format != "name" {
										if j > 0 {
											expected += "\n"
										}
										if len(scope.clusters) == 2 {
											expected += "CLUSTER    "
										}
										if kind == "Kustomization" {
											if all {
												expected += "NAMESPACE      NAME                    PATH\n"
											} else {
												expected += "NAMESPACE      NAME      PATH\n"
											}
										} else {
											if all {
												expected += "NAMESPACE    NAME                  "
											} else {
												expected += "NAMESPACE    NAME      "
											}
											expected += "CHART                                   VERSION    SOURCE\n"
										}
									}
									for _, cluster := range scope.clusters {
										name := "shared"
										if all {
											name = kind + "/" + name
										}
										if format == "name" {
											if len(scope.clusters) == 2 {
												name = cluster + ":" + name
											}
											expected += name + "\n"
											continue
										}
										if len(scope.clusters) == 2 {
											expected += fmt.Sprintf("%-11s", cluster)
										}
										if kind == "Kustomization" {
											expected += fmt.Sprintf("flux-system    %-*s./%s/app\n", map[bool]int{false: 10, true: 24}[all], name, cluster)
										} else {
											expected += fmt.Sprintf("apps         %-*sflux-system-flux-system/./charts/app    0.1.0      flux-system\n", map[bool]int{false: 10, true: 22}[all], name)
										}
									}
								}
								assert.Equal(t, out, expected)
							}
						})
					}
				})
			}
		})
	}
}

func decodeGetDocuments(t *testing.T, out, format string, clusters []string) []output.DocumentGroup {
	t.Helper()
	if len(clusters) > 1 && format == "json" {
		var groups []output.DocumentGroup
		assert.NoError(t, json.Unmarshal([]byte(out), &groups))
		assert.Equal(t, len(groups), len(clusters))
		for i, cluster := range clusters {
			assert.Equal(t, groups[i].Cluster, cluster)
		}
		return groups
	}
	groups := make([]output.DocumentGroup, len(clusters))
	for i, cluster := range clusters {
		groups[i] = output.DocumentGroup{Cluster: cluster, Documents: []map[string]any{}}
	}
	if format == "json" {
		assert.NoError(t, json.Unmarshal([]byte(out), &groups[0].Documents))
		return groups
	}
	streams := []string{out}
	if len(clusters) > 1 {
		streams = nil
		for i, cluster := range clusters {
			marker := "# Cluster: " + cluster + "\n"
			if !strings.HasPrefix(out, marker) {
				t.Fatalf("missing cluster marker %q: %s", marker, out)
			}
			out = strings.TrimPrefix(out, marker)
			stream, rest, found := strings.Cut(out, "# Cluster: ")
			streams = append(streams, stream)
			if found {
				out = "# Cluster: " + rest
			} else {
				out = ""
			}
			if i == len(clusters)-1 && out != "" {
				t.Fatalf("extra groups: %s", out)
			}
		}
	}
	for i, stream := range streams {
		decoder := yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(stream), 4096)
		for {
			var doc map[string]any
			err := decoder.Decode(&doc)
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(t, err)
			if doc != nil {
				groups[i].Documents = append(groups[i].Documents, doc)
			}
		}
	}
	return groups
}

func TestPrepareGetTables_SharedColumns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		groups int
	}{
		{name: "single", groups: 1},
		{name: "multiple", groups: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			columns := []output.Column{{Header: "NAME", Key: "name"}}
			rows := []map[string]string{{"cluster": "alpha", "name": "shared"}}
			original := []output.Table{{Columns: columns, Rows: rows}}
			prepared, multi := prepareGetTables(&selectedInvocation{groups: make([]selectedGroup, tc.groups)}, original)
			assert.Equal(t, multi, tc.groups == 2)
			want := columns
			if multi {
				want = []output.Column{{Header: "CLUSTER", Key: "cluster"}, {Header: "NAME", Key: "name"}}
			}
			assert.Diff(t, prepared[0].Columns, want)
			assert.Diff(t, original, []output.Table{{
				Columns: []output.Column{{Header: "NAME", Key: "name"}},
				Rows:    []map[string]string{{"cluster": "alpha", "name": "shared"}},
			}})
		})
	}
}

func TestRun_GetAll_EmptyKinds(t *testing.T) {
	for _, kinds := range []string{"ks", "hr", "empty"} {
		t.Run(kinds, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [objects.yaml]\n")
			body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: empty}\n"
			want := ""
			switch kinds {
			case "ks":
				body = testutil.KustomizationYAML("only", "flux-system", "  suspend: true\n  path: ./absent\n")
				want = "Kustomization/only\n"
			case "hr":
				body = "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: only, namespace: apps}\nspec: {suspend: true, chart: {spec: {chart: demo, version: 1.0.0, sourceRef: {kind: HelmRepository, name: charts}}}}\n"
				want = "HelmRelease/only\n"
			}
			testutil.WriteFile(t, root, "objects.yaml", body)
			for _, format := range []string{"", "table", "name", "json", "yaml"} {
				t.Run("format="+format, func(t *testing.T) {
					var all string
					for _, command := range []string{"get all", "get"} {
						args := append(strings.Fields(command), "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2")
						if format != "" {
							args = append(args, "-o", format)
						}
						out, stderr, code := runClusterInputs(t, args...)
						if code != 0 {
							t.Fatalf("exit=%d stdout=%s stderr=%s", code, out, stderr)
						}
						if command == "get all" {
							all = out
						} else {
							assert.Equal(t, out, all)
						}
						switch format {
						case "name":
							assert.Equal(t, out, want)
						case "json", "yaml":
							groups := decodeGetDocuments(t, out, format, []string{"."})
							if kinds == "empty" {
								assert.Equal(t, len(groups[0].Documents), 0)
								if format == "json" {
									assert.Equal(t, out, "[]\n")
								} else {
									assert.Equal(t, out, "")
								}
							} else {
								assert.Equal(t, len(groups[0].Documents), 1)
								assert.Equal(t, groups[0].Documents[0]["kind"].(string)+"/"+groups[0].Documents[0]["name"].(string)+"\n", want)
							}
						default:
							if kinds == "empty" {
								assert.Equal(t, out, "")
							} else if strings.Contains(out, "\n\n") || !strings.Contains(out, strings.TrimSpace(want)) {
								t.Fatalf("empty kind left a section: %q", out)
							}
						}
					}
				})
			}
		})
	}
}

func TestRun_GetAll_ChangedOnly(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "same", "omega": "same"})
	head := writeClusterGroupsFixture(t, map[string]string{"alpha": "same", "omega": "same"})
	for _, root := range []string{base, head} {
		writeGetRelease(t, root, "alpha")
		writeGetRelease(t, root, "omega")
		testutil.WriteFile(t, root, "alpha/app/private.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: private, namespace: apps}\ndata: {value: original}\n")
		testutil.WriteFile(t, root, "alpha/app/kustomization.yaml", "resources: [pod.yaml, release.yaml, private.yaml]\n")
	}
	testutil.WriteFile(t, head, "alpha/app/private.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: private, namespace: apps}\ndata: {value: changed}\n")
	testutil.WriteFile(t, base, "alpha/removed.yaml", testutil.KustomizationYAML("removed", "flux-system", "  path: ./alpha/removed\n"))
	testutil.WriteFile(t, base, "alpha/kustomization.yaml", "resources: [flux.yaml, removed.yaml]\n")
	testutil.WriteFile(t, base, "alpha/removed/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, base, "alpha/removed/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed}\n")
	flags := []string{"--path", head, "--path-orig", base, "--cache-dir", t.TempDir(), "--concurrency", "2"}
	var selected [][]output.DocumentGroup
	for _, kind := range []string{"ks", "hr"} {
		out, stderr, code := runClusterInputs(t, append([]string{"get", kind, "-o", "json"}, flags...)...)
		if code != 0 {
			t.Fatalf("get %s: exit=%d stdout=%s stderr=%s", kind, code, out, stderr)
		}
		selected = append(selected, decodeGetDocuments(t, out, "json", []string{"alpha", "omega"}))
	}
	for _, command := range []string{"get all", "get"} {
		t.Run(command, func(t *testing.T) {
			out, stderr, code := runClusterInputs(t, append(append(strings.Fields(command), "-o", "json"), flags...)...)
			if code != 0 {
				t.Fatalf("exit=%d stdout=%s stderr=%s", code, out, stderr)
			}
			groups := decodeGetDocuments(t, out, "json", []string{"alpha", "omega"})
			for i, group := range groups {
				want := append(slices.Clone(selected[0][i].Documents), selected[1][i].Documents...)
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
			out, stderr, code = runClusterInputs(t, append(strings.Fields(command), flags...)...)
			if code != 0 || strings.Count(out, "CLUSTER") != 2 || !strings.Contains(out, "omega") || strings.Contains(out, "removed") || !strings.Contains(out, "alpha") || !strings.Contains(out, "HelmRelease/shared") {
				t.Fatalf("changed table: exit=%d stdout=%s stderr=%s", code, out, stderr)
			}
			for _, format := range []string{"json", "yaml"} {
				args := append(append(strings.Fields(command), "-o", format, "-n", "apps"), flags...)
				out, stderr, code := runClusterInputs(t, args...)
				if code != 0 {
					t.Fatalf("namespace selection: exit=%d stdout=%s stderr=%s", code, out, stderr)
				}
				groups := decodeGetDocuments(t, out, format, []string{"alpha", "omega"})
				assert.Equal(t, len(groups[0].Documents), 1)
				assert.Equal(t, groups[0].Documents[0]["kind"], any("HelmRelease"))
				assert.Equal(t, len(groups[1].Documents), 0)
				if format == "json" && groups[1].Documents == nil {
					t.Fatal("empty retained cluster documents must be []")
				}
			}
		})
	}
}

func TestRun_ClusterGroups_SuppliedPrecedence(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old-alpha", "omega": "old-omega"})
	head := writeClusterGroupsFixture(t, map[string]string{"alpha": "new-alpha", "omega": "new-omega"})
	for _, root := range []string{base, head} {
		for _, cluster := range []string{"alpha", "omega"} {
			testutil.WriteFile(t, root, cluster+"/flux.yaml", testutil.KustomizationYAML("shared", "flux-system", "  path: ./"+cluster+"/app\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: settings}]\n"))
			value := "old-" + cluster
			if root == head {
				value = "new-" + cluster
			}
			testutil.WriteFile(t, root, cluster+"/settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {CLUSTER: "+value+"}\n")
			testutil.WriteFile(t, root, cluster+"/kustomization.yaml", "resources: [flux.yaml, settings.yaml]\n")
		}
	}
	input := substitutionInput(t, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {CLUSTER: supplied}\n")
	stdout, stderr, code := runClusterInputs(t, "diff", "all", "--path", head, "--path-orig", base, "--supply", input, "--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff")
	if code != 0 || strings.Count(stderr, "[SuppliedPrecedence]") != 2 || !strings.Contains(stderr, "[SuppliedPrecedence] alpha:") || !strings.Contains(stderr, "[SuppliedPrecedence] omega:") || strings.Contains(stderr, "Cluster:") || strings.Contains(stdout, "supplied") {
		t.Fatalf("cluster supply reduction: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	for _, cluster := range []string{"alpha", "omega"} {
		assert.Equal(t, strings.Count(stderr, "[SuppliedPrecedence] "+cluster+":"), 1)
	}
	for _, value := range []string{"old-alpha", "new-alpha", "old-omega", "new-omega"} {
		if !strings.Contains(stdout, value) {
			t.Fatalf("cluster precedence lost %q: %s", value, stdout)
		}
	}
}
