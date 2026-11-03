package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestScopedFailures_CanonicalReplacementWithStaleArtifact(t *testing.T) {
	t.Parallel()
	root := writeNamedScopeFixture(t)
	flags := commonFlags{path: root, cacheDir: t.TempDir(), concurrency: 2}
	runs, runErr := orchestrator.RenderTrees(t.Context(), inputTrees(&flags), buildOrchCfg(&flags))
	assert.FatalIf(t, len(runs) != 1, "missing run: %v", runErr)
	assert.NoError(t, runErr)
	run := runs[0]
	o, res, err := run.Orchestrator, run.Result, run.Err
	assert.FatalIf(t, err != nil || res == nil, "fresh fixture failed: %v", err)
	id := testutil.HelmReleaseID("apps", "selected")
	hr, _ := o.Store().Get[*manifest.HelmRelease](id)
	artifact := o.Store().GetArtifact(id, store.ArtifactContent)
	assert.FatalIf(t, artifact == nil, "%v", "fresh render has no artifact")
	clone := hr.Clone()
	clone.ValuesFrom = []helmv2.ValuesReference{{Kind: "Secret", Name: "current"}}
	o.Store().AddObject(clone)
	current := manifest.NamedResource{Kind: "Secret", Namespace: "apps", Name: "current"}
	old := manifest.NamedResource{Kind: "Secret", Namespace: "apps", Name: "old"}
	var currentReads store.ReadSet
	currentReads.Init(o.Store(), id, nil)
	currentReads.GetObject(id)
	currentReads.RequireData(current)
	currentReads.Accept()
	t.Cleanup(currentReads.Retire)
	res = &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		id:      {Status: store.StatusFailed, Message: "selected failed after rendering"},
		current: {Status: store.StatusFailed, Message: "current input failure"},
		old:     {Status: store.StatusFailed, Message: "old input failure"},
	}}
	selection, selectedErr, _ := orchestrator.Select(
		[]orchestrator.Rendered{{Orchestrator: o, Result: res}},
		&orchestrator.FailuresError{
			Message: "raw reconcile failures",
		},
		[]manifest.NamedResource{{Group: id.Group, Kind: id.Kind, Name: id.Name}},
		nil,
	)
	assert.FatalIf(t, len(selection) != 1 || selectedErr == nil, "selection failed: %v", selectedErr)
	{
		_, ok := selection[0].Failed[id]
		assert.FatalIf(t, !ok, "%v", "stale artifact weakened selected failure")
	}
	if len(selection[0].Failed) != 2 || selection[0].Failed[current].String() != "current input failure" ||
		len(
			selection[0].Unrelated,
		) != 1 || selection[0].Unrelated[0].Resource != old || o.Store().GetArtifact(id, store.ArtifactContent) != nil {
		t.Fatalf("projection did not follow current canonical refs: %+v", selection[0])
	}
	_, _, accepted := o.Store().GetEmission(id)
	defer accepted.Finish()
	assert.Equal(t, len(accepted.Manifests), 0)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_UnnamedScope_ErrorBytes(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	root := writeNamedScopeFixture(t)
	want := "reconcile completed with 1 failure(s):\n  Kustomization.kustomize.toolkit.fluxcd.io/flux-system/outside: not found\n  (+1 blocked by failed/missing dependencies)"
	for _, verb := range []string{"get", "test"} {
		t.Run(verb, func(t *testing.T) {
			kind := "ks"
			if verb == "test" {
				kind = "all"
			}
			args := []string{
				verb,
				kind,
				"--path",
				filepath.Join(root, "cluster"),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
			}
			_, stderr, code := runCLI(t, args...)
			assert.Equal(t, code, 1)
			expected := "\n  ✗  Kustomization  flux-system/outside  not found\n         required by flux-system/unrelated\n" + persistenceNotice + "\n  ✗ 1 failed · 1 blocked\n"
			if verb == "test" {
				expected = persistenceNotice
			}
			assert.Equal(t, stderr, expected)
			cmd := New("test")
			cmd.SetContext(t.Context())
			cmd.SetArgs(args)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			err := cmd.Execute()
			assert.FatalIf(t, err == nil, "%v", "unnamed failure returned nil")
			expected = want
			if verb == "test" {
				expected = "test failures detected\n" + want
			}
			assert.Equal(t, err.Error(), expected)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_UnrelatedNamespaceWarnings(t *testing.T) {
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			body, err := os.ReadFile(filepath.Join(root, "cluster/unrelated.yaml"))
			assert.NoError(t, err)
			testutil.WriteFile(
				t,
				root,
				"cluster/unrelated.yaml",
				strings.Replace(string(body), "namespace: flux-system", "namespace: other", 1),
			)
			out, stderr, code := runCLI(
				t,
				verb,
				"hr",
				"selected",
				"--path",
				filepath.Join(root, "cluster"),
				"-n",
				"apps",
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
			)
			assert.Equal(t, code, 0)
			assert.FatalIf(
				t,
				strings.Contains(out+stderr, "unrelated") || strings.Contains(out+stderr, "outside"),
				"excluded namespace diagnostics leaked: %s%s",
				out,
				stderr,
			)
		})
	}
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_NamedScope_TestWarningDestinations(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	for _, fatal := range []bool{false, true} {
		t.Run(strconv.FormatBool(fatal), func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			wantCode := 0
			if fatal {
				appendScopeSpec(t, root, "cluster/hr.yaml", "  dependsOn: [{name: fatal-dependency}]\n")
				wantCode = 1
			}
			out, stderr, code := runCLI(t, "test", "hr", "selected", "--path", filepath.Join(root, "cluster"),
				"--cache-dir", t.TempDir(), "--concurrency", "2")
			assert.Equal(t, code, wantCode)
			assert.Equal(
				t,
				strings.Contains(out, "warnings") || strings.Contains(out, "unrelated") ||
					strings.Contains(out, "outside"),
				false,
			)
			assert.Equal(t, strings.Count(stderr, "warnings (1)"), 1)
			assert.Equal(t, strings.Count(stderr, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/unrelated"), 0)
			assert.Equal(t, strings.Contains(stderr, "outside"), false)
			if fatal {
				assert.Equal(t, strings.Count(stderr, "apps/fatal-dependency"), 1)
				assert.Equal(t, strings.Contains(stderr, "unrelated"), false)
				assert.Equal(t, strings.Contains(stderr, "warnings"), true)
				assert.Equal(t, strings.Count(stderr, "flate error:"), 0)
			} else {
				assert.Equal(t, strings.Contains(stderr, persistenceMessage), true)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_DiffBothSnapshots(t *testing.T) {
	root, orig := writeNamedScopeFixture(t), writeNamedScopeFixture(t)
	testutil.WriteFile(
		t,
		root,
		"charts/app/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: changed}\n",
	)
	appendScopeSpec(t, root, "cluster/hr.yaml", "  values: {greeting: changed}\n")
	args := []string{
		"diff",
		"hr",
		"selected",
		"--path",
		filepath.Join(root, "cluster"),
		"--path-orig",
		filepath.Join(orig, "cluster"),
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	}
	want, _, code := runCLI(t, args...)
	assert.Equal(t, code, 0)
	assert.FatalIf(t, want == "", "%v", "selected chart change produced no diff")
	appendScopeSpec(t, root, "cluster/unrelated.yaml", "  postBuild: {substitute: {GREETING: changed}}\n")
	got, stderr, code := runCLI(t, args...)
	assert.Equal(t, code, 0)
	assert.Equal(t, got, want)
	assert.FatalIf(t, strings.Contains(stderr, "outside") || strings.Contains(stderr, "unrelated") ||
		!strings.Contains(
			stderr,
			"[StaleValues] current snapshot: HelmRelease apps/selected",
		), "selected warning scope differs: %s", stderr)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_DiffFatalWriteError(t *testing.T) {
	root, orig := writeNamedScopeFixture(t), writeNamedScopeFixture(t)
	appendScopeSpec(t, root, "cluster/hr.yaml", "  dependsOn: [{name: missing}]\n")
	want := errors.New("fatal diagnostic write failed")
	cmd := New("test")
	cmd.SetContext(t.Context())
	cmd.SetArgs(
		[]string{
			"diff",
			"hr",
			"selected",
			"--path",
			filepath.Join(root, "cluster"),
			"--path-orig",
			filepath.Join(orig, "cluster"),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		},
	)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(failingWriter{err: want})
	{
		got := cmd.Execute()
		assert.FatalIf(t, !errors.Is(got, want), "fatal diff diagnostic write failure discarded: %v", got)
	}
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_NamedScope_DiffOwningKustomizationFails(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	for _, tc := range []struct{ name, failingSide, layout string }{
		{"current/plain/edited", "current", "plain"},
		{"orig/plain/edited", "orig", "plain"},
		{"current/escape", "current", "escape"},
		{"current/moved", "current", "moved"},
		{"orig/moved", "orig", "moved"},
	} {
		t.Run(tc.name, func(t *testing.T) { runNamedScopeDiffOwningKustomizationFailsName(t, &tc) })
	}
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_NamedScope_DiffDeletedReleaseSiblingFails(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	root, orig := t.TempDir(), t.TempDir()
	release := func(name, chart string) string {
		return fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %s, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: %s
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`, name, chart)
	}
	for _, dir := range []string{root, orig} {
		testutil.GitInit(t, dir)
		testutil.WriteFile(t, dir, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
		testutil.WriteFile(
			t,
			dir,
			"charts/app/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}-rendered', namespace: apps}\ndata: {greeting: hello}\n",
		)
		testutil.WriteFile(
			t,
			dir,
			"flux/apps.yaml",
			testutil.KustomizationYAML(
				"apps",
				"flux-system",
				"  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [selected.yaml, sibling.yaml]\n")
		testutil.WriteFile(t, dir, "apps/selected.yaml", release("selected", "./charts/app"))
		testutil.WriteFile(t, dir, "apps/sibling.yaml", release("sibling", "./charts/app"))
	}
	assert.NoError(t, os.Remove(filepath.Join(root, "apps/selected.yaml")))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [sibling.yaml]\n")
	testutil.WriteFile(t, root, "apps/sibling.yaml", release("sibling", "./charts/missing"))
	flags := []string{
		"--path",
		filepath.Join(root, "flux"),
		"--path-orig",
		filepath.Join(orig, "flux"),
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	}
	for _, tc := range []struct {
		name string
		code int
	}{
		{"selected", 0},
		{"sibling", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, stderr, code := runCLI(t, append([]string{"diff", "hr", tc.name}, flags...)...)
			assert.FatalIf(
				t,
				code != tc.code || !strings.Contains(stderr, "sibling") || !strings.Contains(stderr, "charts/missing"),
				"diff hr %s: exit %d, want %d; stderr: %s",
				tc.name,
				code,
				tc.code,
				stderr,
			)
			switch {
			case tc.code == 0:

				assert.FatalIf(
					t,
					!strings.Contains(stderr, "warnings (2)") || strings.Contains(stderr, "reconcile completed") ||
						!strings.Contains(out, "selected-rendered") ||
						strings.Contains(
							out,
							"sibling",
						),
					"deleted release diff includes unrelated failure: stdout: %s; stderr: %s",
					out,
					stderr,
				)

			case !strings.Contains(stderr, "✗ 1 failed") || !strings.Contains(stderr, "HelmRelease  apps/sibling"):

				t.Fatalf("selected sibling failure hidden: %s", stderr)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_DiffCounterpartSelection(t *testing.T) {
	for _, tc := range []struct{ name, releaseName, namespace string }{
		{"name", "other-release", "apps"},
		{"namespace", "selected", "team"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, orig := writeNamedScopeFixture(t), writeNamedScopeFixture(t)
			for _, dir := range []string{root, orig} {
				testutil.WriteFile(
					t,
					dir,
					"cluster/bad.yaml",
					testutil.KustomizationYAML(
						"bad",
						tc.namespace,
						"  interval: 10m\n  path: ./bad\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
					),
				)
				testutil.WriteFile(t, dir, "bad/kustomization.yaml", "resources: [../shared/hr.yaml]\n")
				testutil.WriteFile(t, dir, "shared/hr.yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %s, namespace: %s}
spec:
  interval: 10m
  chart:
    spec:
      chart: ./charts/app
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`, tc.releaseName, tc.namespace))
			}
			appendScopeSpec(t, root, "cluster/hr.yaml", "  values: {greeting: changed}\n")
			appendScopeSpec(
				t,
				root,
				"cluster/bad.yaml",
				"  postBuild: {substituteFrom: [{kind: ConfigMap, name: nope}]}\n",
			)
			flags := []string{
				"--path",
				filepath.Join(root, "cluster"),
				"--path-orig",
				filepath.Join(orig, "cluster"),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
			}
			if tc.name == "namespace" {
				flags = append(flags, "-n", "apps")
			}
			out, stderr, code := runCLI(t, append([]string{"diff", "hr", "selected"}, flags...)...)
			assert.FatalIf(
				t,
				code != 0 || strings.Contains(stderr, "reconcile completed"),
				"unselected counterpart owner became fatal: %d %s",
				code,
				stderr,
			)
			assert.FatalIf(t, strings.Contains(out, "other-release") ||
				(tc.name == "namespace" && strings.Contains(out+stderr, "team")), "unselected counterpart leaked into diff: %s%s", out, stderr)
		})
	}
}

func TestReportFailures_NamedReportingPolicy(t *testing.T) {
	t.Parallel()
	for _, named := range []bool{false, true} {
		t.Run(strconv.FormatBool(named), func(t *testing.T) {
			t.Parallel()
			o, err := completedOrchestrator(t)
			assert.NoError(t, err)
			id := testutil.KustomizationID("apps", "failed")
			o.Store().AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			res := &orchestrator.Result{
				Failed: map[manifest.NamedResource]store.StatusInfo{
					id: {Status: store.StatusFailed, Message: "render failed"},
				},
			}
			query := manifest.NamedResource{Group: id.Group, Kind: id.Kind}
			if named {
				query.Name = id.Name
			}
			selection, selectedErr, remainder := orchestrator.Select(
				[]orchestrator.Rendered{{Orchestrator: o, Result: res}},
				&orchestrator.FailuresError{Message: "raw reconcile failures"},
				[]manifest.NamedResource{query},
				nil,
			)
			assert.FatalIf(t, len(selection) != 1 || selectedErr == nil, "selection failed: %v", selectedErr)
			var out bytes.Buffer
			extra := fmt.Errorf("reconcile: %w", context.Canceled)
			got := finishSelected(
				&selectedRequest{roots: []manifest.NamedResource{query}, stderr: &out},
				&selectedInvocation{
					groups: []selectedGroup{
						{
							cluster:   ".",
							sides:     selection,
							err:       errors.Join(selectedErr, extra),
							remainder: errors.Join(remainder, extra),
						},
					},
				},
				&selectedPayload{},
			)
			assert.FatalIf(t, !errors.Is(got, context.Canceled), "lost non-resource error: %v", got)
			assert.ErrorIf(
				t,
				!strings.Contains(out.String(), "flate error: reconcile: context canceled"),
				"non-resource error missing from report: %s",
				&out,
			)
			writeErr := errors.New("report write failed")
			got = finishSelected(
				&selectedRequest{roots: []manifest.NamedResource{query}, stderr: failingWriter{err: writeErr}},
				&selectedInvocation{
					groups: []selectedGroup{{cluster: ".", sides: selection, err: selectedErr, remainder: remainder}},
				},
				&selectedPayload{},
			)
			assert.Equal(t, errors.Is(got, writeErr), true)
		})
	}
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_GetNamed_UnrelatedFailureAndLabels(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	for _, tc := range []struct {
		kind, name, label string
		visible           bool
	}{
		{kind: "ks", name: "matching", label: "area=selected", visible: true},
		{kind: "ks", name: "mismatching", label: "area=other"},
		{kind: "ks", name: "missing", label: "absent="},
		{kind: "ks", name: "empty", label: "empty=", visible: true},
		{kind: "hr", name: "matching", label: "area=selected", visible: true},
	} {
		t.Run(tc.kind+"/"+tc.name, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			file := "cluster/ks.yaml"
			if tc.kind == "hr" {
				file = "cluster/hr.yaml"
			}
			body, err := os.ReadFile(filepath.Join(root, file))
			assert.NoError(t, err)
			body = []byte(
				strings.Replace(string(body), "namespace: ", "labels: {area: selected, empty: ''}, namespace: ", 1),
			)
			testutil.WriteFile(t, root, file, string(body))
			args := []string{
				"get",
				tc.kind,
				"selected",
				"--path",
				filepath.Join(root, "cluster"),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
				"-o",
				"name",
				"-l",
				tc.label,
			}
			stdout, stderr, code := runCLI(t, args...)
			assert.FatalIf(
				t,
				code != 0 || (stdout != "") != tc.visible || !strings.Contains(stderr, persistenceMessage) ||
					strings.Contains(stderr, "unrelated") ||
					strings.Contains(
						stderr,
						"outside",
					),
				"healthy named get: exit=%d stdout=%q stderr=%s",
				code,
				stdout,
				stderr,
			)
			appendScopeSpec(t, root, file, "  dependsOn: [{name: missing, namespace: other}]\n")
			_, stderr, code = runCLI(t, args...)
			assert.FatalIf(
				t,
				code != 1 || !strings.Contains(stderr, "missing"),
				"label hid named failure: exit=%d stderr=%s",
				code,
				stderr,
			)
		})
	}
}
