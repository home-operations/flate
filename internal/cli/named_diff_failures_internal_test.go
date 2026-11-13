package cli

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

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
	cli := runCLI(t, args...)
	want, code := cli.stdout, cli.code
	assert.Equal(t, code, 0)
	assert.FatalIf(t, want == "", "%v", "selected chart change produced no diff")
	appendScopeSpec(t, root, "cluster/unrelated.yaml", "  postBuild: {substitute: {GREETING: changed}}\n")
	cli2 := runCLI(t, args...)
	got, stderr, code := cli2.stdout, cli2.stderr, cli2.code
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
	cmd := New("test", RunConfig{})
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
	assert.NoError(t, os.Remove(filepath.Join(root, "apps", "selected.yaml")))
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
			cli := runCLI(t, append([]string{"diff", "hr", tc.name}, flags...)...)
			out, stderr, code := cli.stdout, cli.stderr, cli.code
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
			cli := runCLI(t, append([]string{"diff", "hr", "selected"}, flags...)...)
			out, stderr, code := cli.stdout, cli.stderr, cli.code
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

func runNamedScopeDiffOwningKustomizationFailsName(t *testing.T, tc *struct {
	name        string
	failingSide string
	layout      string
},
) {
	t.Helper()

	root, orig := t.TempDir(), t.TempDir()
	releasePath := "apps/hr.yaml"
	resources := "resources: [hr.yaml]\n"
	if tc.layout == "escape" {
		releasePath = "shared/hr.yaml"
		resources = "resources: [../shared/hr.yaml]\n"
	}
	for _, dir := range []string{root, orig} {
		testutil.GitInit(t, dir)
		testutil.WriteFile(t, dir, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
		testutil.WriteFile(
			t,
			dir,
			"charts/app/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: hello}\n",
		)
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", resources)
		testutil.WriteFile(t, dir, releasePath, `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: selected, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: ./charts/app
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
		for _, name := range []string{"apps", "other"} {
			testutil.WriteFile(
				t,
				dir,
				"flux/"+name+".yaml",
				testutil.KustomizationYAML(
					name,
					"flux-system",
					"  interval: 10m\n  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
				),
			)
		}
		testutil.WriteFile(t, dir, "other/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			dir,
			"other/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: other, namespace: apps}\n",
		)
	}
	if tc.layout == "plain" {
		appendScopeSpec(t, root, releasePath, "  values: {greeting: changed}\n")
	}
	if tc.layout == "moved" {
		assert.NoError(t, os.Rename(
			filepath.Join(root, releasePath),
			filepath.Join(root, "other", "hr.yaml"),
		))
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: apps, namespace: apps}\n",
		)
		testutil.WriteFile(t, root, "other/kustomization.yaml", "resources: [cm.yaml, hr.yaml]\n")
	}
	failing := root
	if tc.failingSide == "orig" {
		failing = orig
	}
	appendScopeSpec(
		t,
		failing,
		"flux/apps.yaml",
		"  postBuild: {substituteFrom: [{kind: ConfigMap, name: nope}]}\n",
	)
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
	cli := runCLI(t, append([]string{"diff", "hr", "selected"}, flags...)...)
	stderr, code := cli.stderr, cli.code
	switch {
	case tc.layout == "moved" && tc.failingSide == "current":

		assert.FatalIf(t, code != 0 || strings.Contains(stderr, "flux-system/nope") ||
			strings.Contains(
				stderr,
				"reconcile completed",
			), "former owning Kustomization became fatal: %d %s", code, stderr)
		cli2 := runCLI(
			t,
			"build",
			"hr",
			"selected",
			"--path",
			filepath.Join(root, "flux"),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		stderr, code = cli2.stderr, cli2.code
		assert.FatalIf(t, code != 0, "former owning Kustomization became fatal in build: %d %s", code, stderr)

	case code != 1 || !strings.Contains(stderr, "✗ 1 failed") || !strings.Contains(stderr, "ConfigMap  flux-system/nope  not found"):

		t.Fatalf("owning Kustomization failure hidden: %d %s", code, stderr)
	}
	cli3 := runCLI(t, append([]string{"diff", "ks", "other"}, flags...)...)
	stderr, code = cli3.stderr, cli3.code
	assert.FatalIf(t, code != 0, "unrelated owning Kustomization became fatal: %d %s", code, stderr)
	if tc.layout == "plain" {
		baseline := orig
		if failing == orig {
			baseline = root
		}
		cli4 := runCLI(
			t,
			"build",
			"hr",
			"selected",
			"--path",
			filepath.Join(failing, "flux"),
			"--path-orig",
			filepath.Join(baseline, "flux"),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		stderr, code = cli4.stderr, cli4.code
		assert.FatalIf(
			t,
			code != 1 || !strings.Contains(stderr, "1 failed") || !strings.Contains(stderr, "flux-system/nope"),
			"changed-only owning Kustomization failure hidden: %d %s",
			code,
			stderr,
		)
	}
}
