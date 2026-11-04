package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	io "io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/homeport/dyff/pkg/dyff"
	"github.com/spf13/pflag"

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
	runs, err := orchestrator.RenderTrees(t.Context(), trees, cfg)
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
			return "", noNamedError(kinds[0], name, "independent trees").Error(), 1
		}
	}
	encoder := output.Encoder{Writer: &stdout, Format: output.Format(c.output)}
	command := clusterInputCommand{verb: verb, kind: kind}
	payload := command.payload(t, inv, encoder)

	if err := finishSelected(&req, inv, &payload); err != nil {
		return stdout.String(), stderr.String(), 1
	}
	return stdout.String(), stderr.String(), 0
}

func clusterSnapshot(side *selection.Selection) diff.Snapshot {
	return diff.Snapshot{
		Subjects:     side.Subjects,
		Manifests:    side.Manifests,
		Unavailable:  side.Unavailable,
		ProducerKeys: side.ProducerKeys,
	}
}

func clusterDiffPayload(t *testing.T, inv *selectedInvocation, encoder output.Encoder) selectedPayload {
	t.Helper()
	groups := make([]output.DiffGroup, len(inv.groups))
	native := make([]dyff.Report, len(inv.groups))
	lines := make([][]output.LineChange, len(inv.groups))
	payload := selectedPayload{
		warnings: make([][]manifest.Warning, len(inv.groups)),
		subjects: make([][]manifest.NamedResource, len(inv.groups)),
	}
	clusterDiffPayloadLoop(t, struct {
		inv     **selectedInvocation
		encoder *struct {
			Writer io.Writer
			Format output.Format
			Color  bool
		}
		groups  *[]output.DiffGroup
		payload *selectedPayload
		lines   *[][]output.LineChange
		native  *[]dyff.Report
	}{inv: &inv, encoder: (*struct {
		Writer io.Writer
		Format output.Format
		Color  bool
	})(&encoder), groups: &groups, payload: &payload, lines: &lines, native: &native})
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
		if encoder.Format == output.FormatJSON {
			return encoder.DiffGroups(groups)
		}
		if encoder.Format == output.FormatDiff || encoder.Format == output.FormatHTML {
			return encoder.Lines(slices.Concat(lines...))
		}
		return encoder.Native(aggregate)
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

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ClusterGroups_PayloadAndSelection(t *testing.T) {
	root := writeClusterGroupsFixture(t, map[string]string{"omega": "second", "alpha": "first"})
	args := []string{"build", "all", "--path", root, "-o", "json", "--concurrency", "2"}
	collision, diagnostic, status := runCLI(t, args...)
	assert.FatalIf(t, status != 0 || !strings.Contains(collision, "example.test/app:") ||
		strings.Contains(
			diagnostic,
			"duplicate resource(s)",
		), "discovery root did not render clusters: code=%d stdout=%s stderr=%s", status, collision, diagnostic)
	stdout, stderr, code := runClusterInputs(t, args...)
	assert.FatalIf(t, code != 0, "group build: code=%d stdout=%s stderr=%s", code, stdout, stderr)
	var groups []output.DocumentGroup
	assert.NoError(t, json.Unmarshal([]byte(stdout), &groups))
	assert.FatalIf(t, len(groups) != 2, "got groups: %s", stdout)
	for i, dir := range []string{"alpha", "omega"} {
		assert.Equal(t, groups[i].Cluster, dir)
		entry, diagnostics, statusInner := runClusterInputs(
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
		assert.FatalIf(t, statusInner != code, "entry %s status=%d stderr=%s", dir, statusInner, diagnostics)
		var documents []map[string]any
		assert.NoError(t, json.Unmarshal([]byte(entry), &documents))
		assert.Diff(t, groups[i].Documents, documents)
	}
	again, diagnostics, status := runClusterInputs(t, args...)
	assert.Equal(t, status, code)
	assert.Equal(t, again, stdout)
	assert.Equal(t, diagnostics, stderr)

	stdout, stderr, code = runClusterInputs(
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
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "alpha:shared\nomega:shared\n")
	assert.Equal(t, stderr, "\n  alpha: ✓ 0 failed\n\n  omega: ✓ 0 failed\n")
	stdout, _, code = runClusterInputs(t, "get", "ks", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(
		t,
		stdout,
		"CLUSTER    NAMESPACE      NAME      PATH\nalpha      flux-system    shared    ./alpha/app\nomega      flux-system    shared    ./omega/app\n",
	)
	stdout, stderr, code = runClusterInputs(t, "get", "images", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "CLUSTER    IMAGE\nalpha      example.test/app:first\nomega      example.test/app:second\n")
	assert.Equal(t, stderr, "\n  alpha: ✓ 0 failed\n\n  omega: ✓ 0 failed\n")
	stdout, stderr, code = runClusterInputs(t, "test", "ks", "shared", "--path", root, "--concurrency", "2")
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
	stdout, stderr, code = runClusterInputs(
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
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "only-alpha\n")
	assert.FatalIf(
		t,
		strings.Contains(stderr, "not found"),
		"another cluster's absence rejected a named match: %s",
		stderr,
	)
	stdout, _, code = runClusterInputs(t, "get", "ks", "only-alpha", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.Equal(t, stdout, "NAMESPACE      NAME          PATH\nflux-system    only-alpha    ./alpha/app\n")
	stdout, stderr, code = runClusterInputs(t, "test", "ks", "only-alpha", "--path", root, "--concurrency", "2")
	assert.Equal(t, code, 0)
	assert.FatalIf(
		t,
		strings.Contains(stdout, "alpha:") || !strings.Contains(stdout, "Kustomization  flux-system/only-alpha"),
		"a single retained named cluster must stay unqualified: %s %s",
		stdout,
		stderr,
	)
	_, stderr, code = runClusterInputs(t, "get", "ks", "absent", "--path", root, "--concurrency", "2")
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "absent"),
		"global named absence: code=%d stderr=%s",
		code,
		stderr,
	)
	writeGetRelease(t, root, "alpha")
	stdout, stderr, code = runClusterInputs(t, "get", "all", "--path", root, "--concurrency", "2")
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
			out, diagnostics, status := runClusterInputs(
				t,
				append(strings.Fields(command), "--path", root, "--concurrency", "2", "-n", "absent", "-o", format)...)
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
	stdout, stderr, code := runClusterInputs(t, "build", "all", "--path", root, "-o", "json", "--concurrency", "2")
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
	html, diagnostics, status := runClusterInputs(
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

func clusterDiffPayloadLoop(t *testing.T, loopInputs struct {
	inv     **selectedInvocation
	encoder *struct {
		Writer io.Writer
		Format output.Format
		Color  bool
	}
	groups  *[]output.DiffGroup
	payload *selectedPayload
	lines   *[][]output.LineChange
	native  *[]dyff.Report
},
) {
	t.Helper()

	for i, group := range (*loopInputs.inv).groups {
		before, after := clusterSnapshot(group.sides[0]), clusterSnapshot(group.sides[1])
		if loopInputs.encoder.Format == output.FormatJSON {
			body, warnings, notes, err := diff.Compare(
				before,
				after,
				nil,
				diff.Options{
					Format:      diff.FormatJSON,
					StripAttrs:  diff.DefaultStripAttrs,
					StripFields: diff.DefaultStripFields,
				},
			)
			assert.NoError(t, err)
			(*loopInputs.groups)[i] = output.DiffGroup{
				Cluster: group.cluster,
				Body:    body,
				Added:   group.absent[manifest.TreeBase],
				Removed: group.absent[manifest.TreeHead],
			}
			loopInputs.payload.warnings[i] = appendSuppressionWarnings(warnings, notes)
			continue
		}
		left, right, notes := diffprep.Suppress(
			diffprep.Snapshot{
				Subjects:     before.Subjects,
				Manifests:    before.Manifests,
				Unavailable:  before.Unavailable,
				ProducerKeys: before.ProducerKeys,
			},
			diffprep.Snapshot{
				Subjects:     after.Subjects,
				Manifests:    after.Manifests,
				Unavailable:  after.Unavailable,
				ProducerKeys: after.ProducerKeys,
			},
		)
		prepared := diffprep.Prepare(
			diffprep.Documents(left),
			diffprep.Documents(right),
			diffprep.Options{
				Subjects:    [2]map[manifest.NamedResource]manifest.DiagnosticIdentity{before.Subjects, after.Subjects},
				StripAttrs:  diff.DefaultStripAttrs,
				StripFields: diff.DefaultStripFields,
			},
		)
		loopInputs.payload.warnings[i] = prepared.Warnings
		for noteIndex := range notes {
			note := notes[noteIndex]
			loopInputs.payload.warnings[i] = append(
				loopInputs.payload.warnings[i],
				manifest.Warning{
					Resource: note.Parent.NamedResource,
					Subject:  note.Subject,
					Category: manifest.WarnDiffSuppressed,
					Message:  note.String(),
				},
			)
		}
		var err error
		if loopInputs.encoder.Format == output.FormatDiff || loopInputs.encoder.Format == output.FormatHTML {
			(*loopInputs.lines)[i], err = output.PrepareLines(prepared)
			loopInputs.payload.subjects[i] = output.LineSubjects((*loopInputs.lines)[i], loopInputs.encoder.Format)
		} else {
			(*loopInputs.native)[i], err = prepared.Native()
		}
		assert.NoError(t, err)
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
		group.sides, group.err, group.remainder = orchestrator.Select(groupRuns, groupErr, req.roots, nil)
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
	if c.kind == "images" {
		return c.imagePayload(inv, encoder)
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
				payload.warnings[i] = appendSuppressionWarnings(nil, notes)
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
		(*loopInputs.format) = output.DiffFormats()[0]
		if (*loopInputs.kind) == "images" {
			(*loopInputs.format) = output.FormatName
		}
	}
}
