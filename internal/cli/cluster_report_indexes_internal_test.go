package cli

import (
	"strings"
	"testing"

	"github.com/homeport/dyff/pkg/dyff"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diffprep"
	"github.com/home-operations/flate/internal/testutil"
)

func TestClusterReport_NativeAggregateIndexes(t *testing.T) {
	t.Parallel()
	doc := func(name, value string) diffprep.Doc {
		return diffprep.Doc{Manifest: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name}, "data": map[string]any{"value": value},
		}}
	}
	groups := make([]dyff.Report, 2)
	var err error
	groups[0], err = diffprep.Prepare([]diffprep.Doc{doc("same", "old"), doc("removed", "old")}, []diffprep.Doc{doc("same", "new")}, diffprep.Options{}).
		Native()
	assert.NoError(t, err)
	groups[1], err = diffprep.Prepare([]diffprep.Doc{doc("same", "old")}, []diffprep.Doc{doc("same", "new"), doc("added", "new")}, diffprep.Options{}).
		Native()
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
		assert.FatalIf(
			t,
			path.Root != root || path.DocumentIdx != want.index,
			"diff %d has wrong side or offset: %+v",
			i,
			path,
		)
	}
	assert.Equal(t, groups[1].Diffs[0].Path.DocumentIdx, 0)
	assert.FatalIf(t, groups[1].Diffs[0].Path.Root == &aggregate.From, "%v", "aggregation mutated its input paths")
}

func runClusterReportNativeAggregateInputsFormat(t *testing.T, current *string, base *string, format *string) {
	t.Helper()

	args := []string{"diff", "all", "--path", (*current), "--path-orig", (*base), "-o", (*format), "--concurrency", "2"}
	out, stderr, code := runClusterInputs(t, args...)
	assert.FatalIf(t, code != 0, "diff failed: %s", stderr)
	assert.FatalIf(
		t,
		strings.Contains(out, "Cluster:") || strings.Contains(out, "omega:") || strings.Contains(out, "iframe"),
		"unchanged cluster or retired framing in diff: %s",
		out,
	)
	switch *format {
	case "brief":
		assert.Equal(t, out, "two changes detected between from, two documents and to, two documents\n\n")
	case "diff":
		assert.FatalIf(
			t,
			!strings.Contains(out, "--- alpha: Pod apps/shared"),
			"missing unified label: %s",
			out,
		)
	case "html":
		assert.FatalIf(t, strings.Count(strings.ToLower(out), "<!doctype html>") != 1 ||
			!strings.Contains(out, "Pod alpha: apps/shared") ||
			!strings.Contains(
				out,
				"Kustomization alpha: flux-system/shared",
			), "HTML must share a single named document: %s", out)
	default:
		assert.FatalIf(t, !strings.Contains(out, "alpha: v1/Pod/apps/shared"), "missing native label: %s", out)
	}
	again, _, status := runClusterInputs(t, args...)
	assert.Equal(t, status, code)
	assert.Equal(t, again, out)
}

func runClusterReportNativeAggregateInputsCase(t *testing.T, base *string, format *string) {
	t.Helper()

	out, stderr, code := runClusterInputs(
		t,
		"diff",
		"all",
		"--path",
		(*base),
		"--path-orig",
		(*base),
		"-o",
		(*format),
		"--concurrency",
		"2",
	)
	assert.Equal(t, code, 0)
	assert.Equal(t, out, "")
	assert.FatalIf(t, strings.Contains(stderr, "Cluster:"), "empty body has a cluster heading: %s", stderr)
}

func runClusterReportNativeAggregateInputsName(t *testing.T, tc *struct {
	name    string
	orig    bool
	current bool
},
) {
	t.Helper()

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
			runClusterReportNativeAggregateInputsNameFormat(t, &current, &base, struct {
				format *string
				tc     **struct {
					name    string
					orig    bool
					current bool
				}
			}{format: &format, tc: &tc})
		})
	}
}

func runClusterReportNativeAggregateInputsNameFormat(
	t *testing.T,
	current *string,
	base *string,
	capturedInputs struct {
		format *string
		tc     **struct {
			name    string
			orig    bool
			current bool
		}
	},
) {
	t.Helper()

	out, stderr, code := runClusterInputs(
		t,
		"diff",
		"ks",
		"shared",
		"--path",
		(*current),
		"--path-orig",
		(*base),
		"-o",
		(*capturedInputs.format),
		"--skip-kinds",
		"Kustomization",
		"--concurrency",
		"2",
	)
	assert.FatalIf(t, code != 0, "diff failed: %s", stderr)
	want := "one change detected between from and to\n\n"
	if (*capturedInputs.format) != "brief" {
		path, change, prefix := "@@ (root level) @@", "! + one document added:", "+ "
		value := "new"
		if (*capturedInputs.tc).orig {
			change, prefix, value = "! - one document removed:", "- ", "old"
		}
		switch *capturedInputs.format {
		case "human":
			path, change, prefix = "(root level)", strings.TrimPrefix(change, "! "), "  "
		case "gitlab":
			path, change = "= (root level)", "#"+strings.TrimPrefix(change, "!")
		}
		body := "apiVersion: v1\nkind: Pod\nmetadata:\n  name: shared\n  namespace: apps\n  labels:\n" +
			"    kustomize.toolkit.fluxcd.io/name: shared\n    kustomize.toolkit.fluxcd.io/namespace: flux-system\n" +
			"spec:\n  containers:\n  - name: app\n    image: \"example.test/app:" + value + "\""
		want = "\n" + path + "\n" + change + "\n" + prefix + strings.ReplaceAll(
			body,
			"\n",
			"\n"+prefix,
		) + "\n\n"
		if (*capturedInputs.tc).orig && (*capturedInputs.tc).current {
			want = "\n@@ spec.containers.app.image @@\n! ± value change\n- example.test/app:old\n+ example.test/app:new\n\n"
			switch *capturedInputs.format {
			case "human":
				want = "\nspec.containers.app.image\n  ± value change\n    - example.test/app:old\n    + example.test/app:new\n\n"
			case "gitlab":
				want = "\n= spec.containers.app.image\n# ± value change\n- example.test/app:old\n+ example.test/app:new\n\n"
			}
		}
	}
	assert.Equal(t, out, want)
	for _, label := range []string{"alpha:", "omega:", "v1/Pod/apps/shared", "Cluster:"} {
		assert.FatalIf(
			t,
			strings.Contains(out, label),
			"native omission gained label %q: %s",
			label,
			out,
		)
	}
}
