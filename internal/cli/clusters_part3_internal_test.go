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

func runGetFormatsName(t *testing.T, scope *struct {
	name     string
	path     string
	clusters []string
},
) {
	t.Helper()

	for _, format := range []string{"", "table", "yaml", "json", "name"} {
		t.Run("format="+format, func(t *testing.T) { runGetFormatsNameCase(t, &scope, &format) })
	}
}

func runGetAllEmptyKindsKinds(t *testing.T, kinds *string) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [objects.yaml]\n")
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: empty}\n"
	want := ""
	switch *kinds {
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
			runGetAllEmptyKindsKindsCase(t, &root, &format, struct {
				want  *string
				kinds **string
			}{want: &want, kinds: &kinds})
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

func runGetFormatsNameCase(t *testing.T, scope **struct {
	name     string
	path     string
	clusters []string
}, format *string,
) {
	t.Helper()

	var combined string
	for _, command := range []string{"get all", "get", "get ks", "get hr", "get images"} {
		t.Run(command, func(t *testing.T) {
			runGetFormatsNameCaseCommand(t, &command, &scope, struct {
				format   **string
				combined *string
			}{format: &format, combined: &combined})
		})
	}
}

func runGetAllEmptyKindsKindsCase(t *testing.T, root *string, format *string, capturedInputs struct {
	want  *string
	kinds **string
},
) {
	t.Helper()

	var all string
	runGetAllEmptyKindsKindsCaseLoop(t, struct {
		root           **string
		format         **string
		all            *string
		capturedInputs *struct {
			want  *string
			kinds **string
		}
	}{root: &root, format: &format, all: &all, capturedInputs: &capturedInputs})
}

func runGetAllEmptyKindsKindsCaseLoop(t *testing.T, loopInputs struct {
	root           **string
	format         **string
	all            *string
	capturedInputs *struct {
		want  *string
		kinds **string
	}
},
) {
	t.Helper()

	for _, command := range []string{"get all", "get"} {
		args := append(
			strings.Fields(command),
			"--path",
			(*(*loopInputs.root)),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		if (*(*loopInputs.format)) != "" {
			args = append(args, "-o", (*(*loopInputs.format)))
		}
		out, stderr, code := runClusterInputs(t, args...)
		assert.FatalIf(t, code != 0, "exit=%d stdout=%s stderr=%s", code, out, stderr)
		if command == "get all" {
			(*loopInputs.all) = out
		} else {
			assert.Equal(t, out, (*loopInputs.all))
		}
		switch *(*loopInputs.format) {
		case "name":
			assert.Equal(t, out, (*loopInputs.capturedInputs.want))
		case "json", "yaml":
			groups := decodeGetDocuments(t, out, (*(*loopInputs.format)), []string{"."})
			runGetAllEmptyKindsKindsCaseLoopLoop(t, struct {
				loopInputs *struct {
					root           **string
					format         **string
					all            *string
					capturedInputs *struct {
						want  *string
						kinds **string
					}
				}
				groups *[]output.DocumentGroup
				out    *string
			}{loopInputs: &loopInputs, groups: &groups, out: &out})
		default:
			switch {
			case (*(*loopInputs.capturedInputs.kinds)) == "empty":

				assert.Equal(t, out, "")

			case strings.Contains(out, "\n\n") || !strings.Contains(out, strings.TrimSpace((*loopInputs.capturedInputs.want))):

				t.Fatalf("empty kind left a section: %q", out)
			}
		}
	}
}

func runGetAllEmptyKindsKindsCaseLoopLoop(t *testing.T, loopInputs struct {
	loopInputs *struct {
		root           **string
		format         **string
		all            *string
		capturedInputs *struct {
			want  *string
			kinds **string
		}
	}
	groups *[]output.DocumentGroup
	out    *string
},
) {
	t.Helper()

	if (*(*loopInputs.loopInputs.capturedInputs.kinds)) == "empty" {
		assert.Equal(t, len((*loopInputs.groups)[0].Documents), 0)
		if (*(*loopInputs.loopInputs.format)) == "json" {
			assert.Equal(t, (*loopInputs.out), "[]\n")
		} else {
			assert.Equal(t, (*loopInputs.out), "")
		}
	} else {
		assert.Equal(t, len((*loopInputs.groups)[0].Documents), 1)
		assert.Equal(
			t,
			(*loopInputs.groups)[0].Documents[0]["kind"].(string)+"/"+(*loopInputs.groups)[0].Documents[0]["name"].(string)+"\n",
			(*loopInputs.loopInputs.capturedInputs.want),
		)
	}
}
