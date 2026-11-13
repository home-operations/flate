package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/imageutil"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testrunner"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/selection"
)

func runClusterInputs(t *testing.T, argv ...string) cliResult {
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
	runClusterInputsLoop2(struct {
		verb   *string
		format *output.Format
		kind   *string
	}{verb: &verb, format: &format, kind: &kind})
	flags.StringVarP(&c.output, "output", "o", string(format), "")
	assert.NoError(t, flags.Parse(args))
	assert.NoError(t, c.loadInputs(t.Context(), nil))
	var trees []orchestrator.Tree
	runClusterInputsLoop(struct {
		c     *commonFlags
		trees *[]orchestrator.Tree
	}{c: &c, trees: &trees})
	if len(trees) == 0 {
		return runCLI(t, argv...)
	}
	if c.cacheDir == "" {
		c.cacheDir = t.TempDir()
	}
	cfg := buildOrchCfg(&c)
	cfg.HeadOnly = verb != "diff" && c.pathOrig != ""
	runs, err := renderTreesForTest(t.Context(), t, trees, cfg)
	assert.FatalIf(t, len(runs) == 0, "independent trees unavailable: %v", err)
	kinds := renderKinds
	switch kind {
	case "ks":
		kinds = []string{manifest.KindKustomization}
	case "hr":
		kinds = []string{manifest.KindHelmRelease}
	}
	var stdout, stderr bytes.Buffer
	req := selectedRequest{
		flags:  c,
		roots:  c.roots(name, kinds...),
		pair:   verb == "diff",
		test:   verb == "test",
		stdout: &stdout,
		stderr: &stderr,
	}
	inv := &selectedInvocation{}
	clusterSelectGroups(&req, inv, runs)
	if name != "" {
		inv.groups = slices.DeleteFunc(
			inv.groups,
			func(group selectedGroup) bool { return group.sides[0].Counts[kinds[0]] == 0 },
		)
		if len(inv.groups) == 0 {
			return cliResult{stderr: noNamedError(kinds[0], name, "independent trees").Error(), code: 1}
		}
	}
	encoder := output.Encoder{Writer: &stdout, Format: output.Format(c.output)}
	command := clusterInputCommand{verb: verb, kind: kind}
	payload := command.payload(t, inv, encoder)

	if err := finishSelected(&req, inv, &payload); err != nil {
		return cliResult{stdout: stdout.String(), stderr: stderr.String(), code: 1}
	}
	return cliResult{stdout: stdout.String(), stderr: stderr.String(), code: 0}
}

func clusterSnapshot(side *selection.Result) diff.Snapshot {
	return diff.Snapshot{
		Subjects:     side.Subjects,
		Manifests:    side.Manifests,
		Unavailable:  side.Unavailable,
		ProducerKeys: side.ProducerKeys,
	}
}

func clusterDiffPayload(t *testing.T, inv *selectedInvocation, encoder output.Encoder) selectedPayload {
	t.Helper()
	groups := make([]diff.Group, len(inv.groups))
	for i, group := range inv.groups {
		groups[i] = diff.Group{
			Cluster: group.cluster,
			Orig:    clusterSnapshot(group.sides[0]),
			Current: clusterSnapshot(group.sides[1]),
			Added:   group.absent[manifest.TreeBase],
			Removed: group.absent[manifest.TreeHead],
		}
	}
	comparison, err := diff.Compare(
		groups,
		diff.Options{
			Format:      diff.Format(encoder.Format),
			StripAttrs:  diff.DefaultStripAttrs,
			StripFields: diff.DefaultStripFields,
		},
	)
	assert.NoError(t, err)
	payload := selectedPayload{
		warnings: make([][]manifest.Warning, len(groups)),
		subjects: make([][]manifest.NamedResource, len(groups)),
	}
	for i, result := range comparison.Groups {
		payload.warnings[i] = diff.MergeWarnings(result.Warnings, result.Suppressed)
		payload.subjects[i] = result.Subjects
	}
	var names []diff.Names
	payload.prepare = func(resolved []diff.Names) { names = resolved }
	payload.write = func() error { return comparison.Write(encoder.Writer, names) }
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

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ClusterGroups_PayloadAndSelection(t *testing.T) {
	root := writeClusterGroupsFixture(t, map[string]string{"omega": "second", "alpha": "first"})
	args := []string{"build", "all", "--path", root, "-o", "json", "--concurrency", "2"}
	cli := runCLI(t, args...)
	collision, diagnostic, status := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, status != 0 || !strings.Contains(collision, "example.test/app:") ||
		strings.Contains(
			diagnostic,
			"duplicate resource(s)",
		), "discovery root did not render clusters: code=%d stdout=%s stderr=%s", status, collision, diagnostic)
	cluster := runClusterInputs(t, args...)
	stdout, stderr, code := cluster.stdout, cluster.stderr, cluster.code
	assert.FatalIf(t, code != 0, "group build: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	var groups []output.DocumentGroup
	assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
	assert.FatalIf(t, len(groups) != 2, "got groups: %s", stdout)
	for i, dir := range []string{"alpha", "omega"} {
		assert.Equal(t, groups[i].Cluster, dir)
		cluster2 := runClusterInputs(
			t,
			"build",
			"all",
			"--path",
			filepath.Join(root, dir),
			"-o",
			"json",
			"--concurrency",
			"2",
		)
		entry, diagnostics, statusInner := cluster2.stdout, cluster2.stderr, cluster2.code
		assert.FatalIf(t, statusInner != code, "entry %s status=%d stderr=%s", dir, statusInner, diagnostics)
		var documents []map[string]any
		assert.NoError(t, json.Unmarshal([]byte(entry), &documents))
		assert.Diff(t, groups[i].Documents, documents)
	}
	cluster3 := runClusterInputs(t, args...)
	again, diagnostics, status := cluster3.stdout, cluster3.stderr, cluster3.code
	assert.Equal(t, status, code)
	assert.Equal(t, again, stdout)
	assert.Equal(t, diagnostics, stderr)

	cluster4 := runClusterInputs(
		t,
		"get",
		"ks",
		"shared",
		"--path",
		root,
		"-o",
		"name",
		"--concurrency",
		"2",
	)
	stdout, stderr, code = cluster4.stdout, cluster4.stderr, cluster4.code
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "alpha:shared\nomega:shared\n")
	assert.Equal(t, stderr, "\n  alpha: ✓ 0 failed\n\n  omega: ✓ 0 failed\n")
	cluster5 := runClusterInputs(t, "get", "ks", "--path", root, "--concurrency", "2")
	stdout, code = cluster5.stdout, cluster5.code
	assert.Equal(t, code, 0)
	assert.Equal(
		t,
		stdout,
		"CLUSTER    NAMESPACE      NAME      PATH\nalpha      flux-system    shared    ./alpha/app\nomega      flux-system    shared    ./omega/app\n",
	)
	cluster6 := runClusterInputs(t, "get", "images", "--path", root, "--concurrency", "2")
	stdout, stderr, code = cluster6.stdout, cluster6.stderr, cluster6.code
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "CLUSTER    IMAGE\nalpha      example.test/app:first\nomega      example.test/app:second\n")
	assert.Equal(t, stderr, "\n  alpha: ✓ 0 failed\n\n  omega: ✓ 0 failed\n")
	cluster7 := runClusterInputs(t, "test", "ks", "shared", "--path", root, "--concurrency", "2")
	stdout, stderr, code = cluster7.stdout, cluster7.stderr, cluster7.code
	assert.FatalIf(t, code != 0 || !strings.Contains(stdout, "alpha: flux-system/shared") ||
		!strings.Contains(stdout, "omega: flux-system/shared") ||
		strings.Contains(stdout, "Cluster:") ||
		!strings.Contains(stdout, "Invocation totals") ||
		strings.Contains(stdout, "failed"), "group tests: code=%d stdout=%s stderr=%s", code, stdout, stderr)

	testutil.WriteFile(
		t,
		root,
		"alpha/extra.yaml",
		testutil.KustomizationYAML("only-alpha", "flux-system", "  path: ./alpha/app\n"),
	)
	testutil.WriteFile(t, root, "alpha/kustomization.yaml", "resources: [flux.yaml, extra.yaml]\n")
	cluster8 := runClusterInputs(
		t,
		"get",
		"ks",
		"only-alpha",
		"--path",
		root,
		"-o",
		"name",
		"--concurrency",
		"2",
	)
	stdout, stderr, code = cluster8.stdout, cluster8.stderr, cluster8.code
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "only-alpha\n")
	assert.FatalIf(
		t,
		strings.Contains(stderr, "not found"),
		"another cluster's absence rejected a named match: %s",
		stderr,
	)
	cluster9 := runClusterInputs(t, "get", "ks", "only-alpha", "--path", root, "--concurrency", "2")
	stdout, code = cluster9.stdout, cluster9.code
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "NAMESPACE      NAME          PATH\nflux-system    only-alpha    ./alpha/app\n")
	cluster10 := runClusterInputs(t, "test", "ks", "only-alpha", "--path", root, "--concurrency", "2")
	stdout, stderr, code = cluster10.stdout, cluster10.stderr, cluster10.code
	assert.Equal(t, code, 0)
	assert.FatalIf(
		t,
		strings.Contains(stdout, "alpha:") || !strings.Contains(stdout, "Kustomization  flux-system/only-alpha"),
		"a single retained named cluster must stay unqualified: %s %s",
		stdout,
		stderr,
	)
	cluster11 := runClusterInputs(t, "get", "ks", "absent", "--path", root, "--concurrency", "2")
	stderr, code = cluster11.stderr, cluster11.code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "absent"),
		"global named absence: code=%d stderr=%s",
		code,
		stderr,
	)
	writeGetRelease(t, root, "alpha")
	cluster12 := runClusterInputs(t, "get", "all", "--path", root, "--concurrency", "2")
	stdout, stderr, code = cluster12.stdout, cluster12.stderr, cluster12.code
	assert.FatalIf(
		t,
		code != 0 || strings.Count(stdout, "CLUSTER") != 2 || !strings.Contains(stdout, "HelmRelease/shared") ||
			!strings.Contains(
				stdout,
				"omega",
			),
		"a kind present in one cluster lost the invocation roster: exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
	for section := range strings.SplitSeq(strings.TrimSuffix(stdout, "\n"), "\n\n") {
		assert.FatalIf(t, !strings.HasPrefix(section, "CLUSTER    "), "mixed section lost CLUSTER: %s", section)
	}

	for _, format := range []string{"table", "name", "json", "yaml"} {
		for _, command := range []string{"get all", "get"} {
			cluster13 := runClusterInputs(
				t,
				append(strings.Fields(command), "--path", root, "--concurrency", "2", "-n", "absent", "-o", format)...)
			out, diagnostics, status := cluster13.stdout, cluster13.stderr, cluster13.code
			assert.FatalIf(t, status != 0, "empty roster: exit=%d stdout=%s stderr=%s", status, out, diagnostics)
			if format == "json" || format == "yaml" {
				groups := decodeGetDocuments(t, out, format, []string{"alpha", "omega"})
				for _, group := range groups {
					assert.Equal(t, len(group.Documents), 0)
					assert.FatalIf(t, format == "json" && group.Documents == nil, "%v", "empty documents must be []")
				}
			} else {
				assert.Equal(t, out, "")
			}
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ClusterGroups_PartialFailure(t *testing.T) {
	root := writeClusterGroupsFixture(t, map[string]string{"alpha": "first", "omega": "second"})
	appendScopeSpec(t, root, "alpha/flux.yaml", "  dependsOn: [{name: missing}]\n")
	cluster := runClusterInputs(t, "build", "all", "--path", root, "-o", "json", "--concurrency", "2")
	stdout, stderr, code := cluster.stdout, cluster.stderr, cluster.code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "alpha: flux-system/missing") || strings.Contains(stderr, "Cluster:") ||
			!strings.Contains(stderr, "missing"),
		"unattributed failure: code=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
	var groups []output.DocumentGroup
	assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
	hasPod := func(doc map[string]any) bool { return doc["kind"] == "Pod" }
	assert.FatalIf(
		t,
		len(groups) != 2 || groups[0].Cluster != "alpha" || slices.ContainsFunc(groups[0].Documents, hasPod) ||
			groups[1].Cluster != "omega" ||
			!slices.ContainsFunc(groups[1].Documents, hasPod),
		"failed cluster erased healthy output: %s",
		stdout,
	)
	writeGetRelease(t, root, "omega")
	for _, command := range []string{"get all", "get", "get images"} {
		for _, format := range []string{"table", "name", "json", "yaml"} {
			t.Run(
				command+"/"+format,
				func(t *testing.T) { runClusterGroupsPartialFailureCase(t, &command, &root, &format) },
			)
		}
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ClusterGroups_DiffAbsenceAndFailure(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old", "omega": "old"})
	for _, scenario := range []string{"removed", "failed"} {
		t.Run(scenario, func(t *testing.T) { runClusterGroupsDiffAbsenceAndFailureScenario(t, &scenario, &base) })
	}
	for _, tc := range []struct {
		name, path, resource string
	}{
		{"shared-component", "components/shared/input.yaml", "shared-input"},
		{"private-file", "alpha/app/private.yaml", "private-input"},
	} {
		t.Run(tc.name, func(t *testing.T) { runClusterGroupsDiffAbsenceAndFailureName(t, &tc) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ClusterReport_NativeAggregateInputs(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old", "omega": "old"})
	current := writeClusterGroupsFixture(t, map[string]string{"alpha": "new", "omega": "old"})
	for _, format := range []string{"human", "diff", "brief", "github", "gitlab", "gitea", "html"} {
		t.Run(format, func(t *testing.T) { runClusterReportNativeAggregateInputsFormat(t, &current, &base, &format) })
	}
	testutil.WriteFile(t, current, "omega/flux.yaml", testutil.KustomizationYAML("shared", "flux-system",
		"  path: ./omega/app\n  postBuild: {substitute: {CLUSTER: new}}\n"))
	cluster := runClusterInputs(
		t,
		"diff",
		"all",
		"--path",
		current,
		"--path-orig",
		base,
		"-o",
		"html",
		"--concurrency",
		"2",
	)
	html, diagnostics, status := cluster.stdout, cluster.stderr, cluster.code
	assert.Equal(t, status, 0)
	for _, token := range []string{`<section class="resource" id="r0">`, `<section class="resource" id="r1">`, "Pod alpha: apps/shared", "Pod omega: apps/shared"} {
		assert.FatalIf(
			t,
			strings.Count(html, token) != 1,
			"missing or duplicate HTML anchor/label %q: %s %s",
			token,
			html,
			diagnostics,
		)
	}
	assert.FatalIf(t, strings.Contains(html, "iframe"), "retired HTML wrapper: %s", html)
	for _, format := range []string{"human", "diff", "brief", "github", "gitlab", "gitea", "html"} {
		t.Run(
			"unchanged/"+format,
			func(t *testing.T) { runClusterReportNativeAggregateInputsCase(t, &base, &format) },
		)
	}
	for _, tc := range []struct {
		name          string
		orig, current bool
	}{
		{name: "zero_original", current: true},
		{name: "one_original_changed", orig: true, current: true},
		{name: "one_original_removed", orig: true},
	} {
		t.Run(tc.name, func(t *testing.T) { runClusterReportNativeAggregateInputsName(t, &tc) })
	}
}

func runClusterInputsLoop(loopInputs struct {
	c     *commonFlags
	trees *[]orchestrator.Tree
},
) {
	for _, cluster := range []string{"alpha", "omega"} {
		for _, side := range []struct {
			root string
			id   manifest.TreeSide
		}{{loopInputs.c.pathOrig, manifest.TreeBase}, {loopInputs.c.path, manifest.TreeHead}} {
			if side.root == "" {
				continue
			}
			path := filepath.Join(side.root, cluster)
			if _, err := os.Stat(filepath.Join(path, "kustomization.yaml")); err == nil {
				(*loopInputs.trees) = append(
					(*loopInputs.trees),
					orchestrator.Tree{
						ID:       manifest.TreeID{Cluster: manifest.ClusterID(cluster), Side: side.id},
						Path:     path,
						RepoRoot: side.root,
					},
				)
			}
		}
	}
}

func clusterSelectGroups(req *selectedRequest, inv *selectedInvocation, runs []orchestrator.Rendered) {
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
		projectedResults0, projectionErrors0 := orchestrator.Select(groupRuns, groupErr, req.roots, nil)
		group.sides, group.err, group.remainder = projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
		inv.groups = append(inv.groups, group)
		first = last
	}
}

type clusterInputCommand struct{ verb, kind string }

func (c *clusterInputCommand) payload(
	t *testing.T,
	inv *selectedInvocation,
	encoder output.Encoder,
) selectedPayload {
	t.Helper()
	if c.kind == "images" {
		return c.imagePayload(inv, encoder)
	}
	payload := selectedPayload{}
	switch c.verb {
	case "get":
		sections := []listSection{collectSelected(inv, ksView()), collectSelected(inv, hrView())}
		switch c.kind {
		case "ks":
			sections = sections[:1]
		case "hr":
			sections = sections[1:]
		}
		payload = resourcePayload(inv, sections, encoder, c.kind == "all")
	case "build":
		groups := make([]output.DocumentGroup, len(inv.groups))
		for i, group := range inv.groups {
			groups[i] = output.DocumentGroup{
				Cluster:   group.cluster,
				Documents: collectRendered(group.sides[0], &buildFlags{}),
			}
		}
		payload.write = func() error { return encoder.DocumentGroups(groups) }
	case "test":
		for _, group := range inv.groups {
			payload.rosters = append(payload.rosters, testrunner.Run(group.sides[0]))
		}
	case "diff":
		payload = clusterDiffPayload(t, inv, encoder)
	}
	return payload
}

func (c *clusterInputCommand) imagePayload(inv *selectedInvocation, encoder output.Encoder) selectedPayload {
	payload := selectedPayload{}
	var err error

	if c.kind == "images" {
		groups := make([]output.ImageGroup, len(inv.groups))
		payload = selectedPayload{warnings: make([][]manifest.Warning, len(inv.groups))}
		table := output.Table{Columns: []output.Column{{Header: "IMAGE", Key: "image"}}}
		for i, group := range inv.groups {
			images := slices.Sorted(maps.Keys(imageutil.Collect(group.sides[0].Manifests)))
			if c.verb == "diff" {
				var notes []diff.Suppression
				images, notes, err = diff.CompareImages(
					clusterSnapshot(group.sides[0]),
					clusterSnapshot(group.sides[1]),
					nil,
					false,
				)
				payload.err = errors.Join(payload.err, err)
				payload.warnings[i] = diff.MergeWarnings(nil, notes)
			}
			groups[i] = output.ImageGroup{
				Cluster: group.cluster,
				Images:  images,
				Added:   group.absent[manifest.TreeBase],
				Removed: group.absent[manifest.TreeHead],
			}
			for _, image := range images {
				table.Rows = append(table.Rows, map[string]string{"cluster": group.cluster, "image": image})
			}
		}
		tables, _ := prepareGetTables(inv, []output.Table{table})
		payload.prepare = func(names []diff.Names) {
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
	return payload
}

func runClusterInputsLoop2(loopInputs struct {
	verb   *string
	format *output.Format
	kind   *string
},
) {
	switch *loopInputs.verb {
	case "build":
		(*loopInputs.format) = output.FormatYAML
	case "diff":
		(*loopInputs.format) = output.Format(diff.Formats()[0])
		if (*loopInputs.kind) == "images" {
			(*loopInputs.format) = output.FormatName
		}
	}
}
