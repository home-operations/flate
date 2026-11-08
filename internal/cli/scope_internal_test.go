package cli

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

func writeNamedScopeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"kustomization.yaml",
		"resources: [cluster/ks.yaml, cluster/hr.yaml, cluster/unrelated.yaml, outside.yaml]\n",
	)
	testutil.WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/app/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: hello}\n",
	)
	testutil.WriteFile(t, root, "cluster/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: selected, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: ./charts/app
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	for _, tc := range []struct{ name, file, extra string }{
		{"selected", "cluster/ks.yaml", ""},
		{"unrelated", "cluster/unrelated.yaml", "  dependsOn: [{name: outside}]\n"},
		{"outside", "outside.yaml", ""},
	} {
		testutil.WriteFile(t, root, tc.file, fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: %s, namespace: flux-system}
spec:
  interval: 10m
  path: ./%s
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
%s`, tc.name, tc.name, tc.extra))
		testutil.WriteFile(t, root, tc.name+"/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			tc.name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+tc.name+", namespace: apps}\ndata: {greeting: hello}\n",
		)
	}
	return root
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_UnrelatedFailure(t *testing.T) {
	for _, kind := range []string{"hr", "ks"} {
		t.Run(kind, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			cache := t.TempDir()
			var want string
			for _, path := range []string{root, filepath.Join(root, "cluster")} {
				args := []string{"build", kind, "selected", "--path", path, "--cache-dir", cache, "--concurrency", "2"}
				cli := runCLI(t, args...)
				out, errOut, code := cli.stdout, cli.stderr, cli.code
				assert.FatalIf(t, code != 0, "named build exited %d: %s", code, errOut)
				want = cmp.Or(want, out)
				assert.FatalIf(t, out == "" || out != want, "selected YAML differs: want %s, got %s", want, out)
				assert.FatalIf(
					t,
					strings.Contains(errOut, "outside") || strings.Contains(errOut, "unrelated"),
					"excluded producer diagnostics leaked: %s",
					errOut,
				)
			}
			cli2 := runCLI(
				t,
				"build",
				"all",
				"--path",
				filepath.Join(root, "cluster"),
				"--cache-dir",
				cache,
				"--concurrency",
				"2",
			)
			stderr, code := cli2.stderr, cli2.code
			assert.FatalIf(
				t,
				code != 1 || !strings.Contains(stderr, "outside") || !strings.Contains(stderr, "unrelated"),
				"full render diagnostics missing: exit=%d %s",
				code,
				stderr,
			)
		})
	}
}

func appendScopeSpec(t *testing.T, root, file, spec string) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, file))
	assert.NoError(t, err)
	testutil.WriteFile(t, root, file, string(body)+spec)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_CommandsAndNamespace(t *testing.T) {
	for _, tc := range []struct{ kind, verb string }{
		{"ks", "get"},
		{"ks", "build"},
		{"ks", "test"},
		{"ks", "diff"},
		{"hr", "build"},
		{"hr", "diff"},
	} {
		t.Run(tc.kind+"/"+tc.verb, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			orig := writeNamedScopeFixture(t)
			cache := t.TempDir()
			args := []string{
				tc.verb,
				tc.kind,
				"selected",
				"--path",
				filepath.Join(root, "cluster"),
				"--cache-dir",
				cache,
				"--concurrency",
				"2",
			}
			if tc.verb == "diff" {
				testutil.WriteFile(
					t,
					root,
					"charts/app/templates/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: changed}\n",
				)
				appendScopeSpec(t, root, "cluster/ks.yaml", "  postBuild: {substitute: {GREETING: changed}}\n")
				args = append(args, "--path-orig", filepath.Join(orig, "cluster"), "-o", "diff")
			}
			cli := runCLI(t, args...)
			stderr, code := cli.stderr, cli.code
			assert.FatalIf(t, code != 0, "healthy named command exited %d: %s", code, stderr)
			file, ns := "cluster/hr.yaml", "apps"
			if tc.kind == "ks" {
				file, ns = "cluster/ks.yaml", "flux-system"
			}
			appendScopeSpec(t, root, file, "  dependsOn: [{name: missing, namespace: other}]\n")
			cli2 := runCLI(t, append(args, "-n", ns)...)
			stderr, code = cli2.stderr, cli2.code
			assert.FatalIf(
				t,
				code != 1 || !strings.Contains(stderr, "missing"),
				"selected cross-namespace failure hidden: %d %s",
				code,
				stderr,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_UnnamedAndNoMatch(t *testing.T) {
	root := writeNamedScopeFixture(t)
	for _, args := range [][]string{
		{"build", "all"},
		{"build", "ks"},
		{"build", "hr", "absent"},
		{"build", "ks", "absent"},
		{"get", "ks", "absent"},
		{"test", "ks", "absent"},
		{"test", "hr", "absent"},
		{"diff", "ks", "absent"},
		{"diff", "hr", "absent"},
	} {
		t.Run(strings.Join(args, "/"), func(t *testing.T) {
			if args[0] == "diff" {
				args = append(args, "--path-orig", filepath.Join(writeNamedScopeFixture(t), "cluster"))
			}
			cli := runCLI(
				t,
				append(
					args,
					"--path",
					filepath.Join(root, "cluster"),
					"--cache-dir",
					t.TempDir(),
					"--concurrency",
					"2",
				)...)
			stderr, code := cli.stderr, cli.code
			assert.FatalIf(t, code != 1 || stderr == "", "expected failure: %d %s", code, stderr)
			assert.FatalIf(
				t,
				len(args) > 2 && !strings.Contains(stderr, "absent"),
				"missing name diagnostic: %s",
				stderr,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_FailedDataOwner(t *testing.T) {
	var selectedYAML string
	for _, tc := range []struct {
		name, kind, extra               string
		optional, broken, absent, chain bool
	}{
		{name: "required", kind: "ConfigMap", broken: true},
		{name: "required-chain", kind: "ConfigMap", broken: true, chain: true},
		{name: "healthy", kind: "ConfigMap"},
		{name: "optional", kind: "ConfigMap", optional: true, broken: true},
		{name: "tolerated", kind: "Secret", extra: "--allow-missing-kinds=Secret,ConfigMap", absent: true, broken: true},
		{name: "producer-backed", kind: "Secret", absent: true, broken: true},
		{name: "absent-healthy", kind: "Secret", absent: true},
	} {
		t.Run(tc.name, func(t *testing.T) { runNamedScopeFailedDataOwnerName(t, &tc, &selectedYAML) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_SubstituteOwners(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		secret, self, broken, absent, required bool
	}{
		{name: "secret-producer", secret: true, broken: true},
		{name: "optional-configmap", broken: true},
		{name: "required-configmap", broken: true, required: true},
		{name: "self-produced", self: true, broken: true},
		{name: "healthy-self-produced", self: true},
		{name: "absent-no-owner", absent: true},
	} {
		t.Run(tc.name, func(t *testing.T) { runNamedScopeSubstituteOwnersName(t, &tc) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_AllMatchingNamespaces(t *testing.T) {
	root := writeNamedScopeFixture(t)
	body, err := os.ReadFile(filepath.Join(root, "cluster", "hr.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(
		t,
		root,
		"cluster/other.yaml",
		strings.Replace(string(body), "namespace: apps", "namespace: other", 1)+"  dependsOn: [{name: missing}]\n",
	)
	for _, verb := range []string{"build", "test"} {
		const argumentCapacity = 11
		args := make([]string, 0, argumentCapacity)
		args = append(args,
			verb,
			"hr",
			"selected",
			"--path",
			filepath.Join(root, "cluster"),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		cli := runCLI(t, append(args, "-n", "apps")...)
		stderr, code := cli.stderr, cli.code
		assert.FatalIf(t, code != 0, "healthy namespace failed: %d %s", code, stderr)
		cli2 := runCLI(t, args...)
		stderr, code = cli2.stderr, cli2.code
		assert.FatalIf(
			t,
			code != 1 || !strings.Contains(stderr, "missing"),
			"matching unhealthy namespace hidden: %d %s",
			code,
			stderr,
		)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_ParentChain(t *testing.T) {
	root := writeNamedScopeFixture(t)
	testutil.WriteFile(
		t,
		root,
		"kustomization.yaml",
		"resources: [cluster/ks.yaml, cluster/unrelated.yaml, cluster/parent.yaml, outside.yaml]\n",
	)
	body, err := os.ReadFile(filepath.Join(root, "cluster", "hr.yaml"))
	assert.NoError(t, err)
	assert.NoError(t, os.Remove(filepath.Join(root, "cluster", "hr.yaml")))
	testutil.WriteFile(t, root, "child/hr.yaml", string(body))
	testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [hr.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"parent/child.yaml",
		testutil.KustomizationYAML(
			"child",
			"apps",
			"  path: ./child\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [child.yaml, missing.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"cluster/parent.yaml",
		testutil.KustomizationYAML(
			"parent",
			"flux-system",
			"  path: ./parent\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	cli := runCLI(
		t,
		"build",
		"ks",
		"child",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"-n",
		"apps",
	)
	stderr, code := cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "parent") || !strings.Contains(stderr, "missing.yaml"),
		"cross-namespace grandparent failure hidden: %d %s",
		code,
		stderr,
	)
}

func TestScopedFailures_WarningsPreserveFatalErrors(t *testing.T) {
	t.Parallel()
	o, err := completedOrchestrator(t)
	assert.NoError(t, err)
	selected := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
	o.Store().AddObject(selected)
	other := testutil.KustomizationID("apps", "other")
	res := &orchestrator.Result{
		Manifests: []manifest.DocumentBatch{
			{
				Parent:    selected,
				Documents: []map[string]any{{"kind": "ConfigMap", "metadata": map[string]any{"name": "partial"}}},
			},
		},
		Failed: map[manifest.NamedResource]store.StatusInfo{
			other: {Status: store.StatusFailed, Message: "unrelated failure"},
		},
	}
	cleanupErr := errors.New("backing close failed")
	for _, fatal := range []error{context.Canceled, errors.New("unattributed panic"), errors.New("output write failed"), cleanupErr, errors.Join(context.Canceled, cleanupErr)} {
		t.Run(fatal.Error(), func(t *testing.T) {
			t.Parallel()
			runErr := errors.Join(&selection.FailuresError{Message: "raw reconcile failures"}, fatal)
			projectedResults0, projectionErrors0 := orchestrator.Select(
				[]orchestrator.Rendered{{Orchestrator: o, Result: res}},
				runErr,
				[]manifest.NamedResource{testutil.KustomizationID("", selected.Name)},
				nil,
			)
			selectedResults, selectedErr, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
			assert.FatalIf(
				t,
				len(selectedResults) != 1 || len(outputByParent(selectedResults[0].Manifests)[selected.Named()]) != 1,
				"selection failed: %v",
				selectedErr,
			)
			var out bytes.Buffer
			got := finishSelected(
				&selectedRequest{roots: []manifest.NamedResource{{Name: "selected"}}, stderr: &out},
				&selectedInvocation{
					groups: []selectedGroup{
						{cluster: ".", sides: selectedResults, err: selectedErr, remainder: remainder},
					},
				},
				&selectedPayload{},
			)
			assert.FatalIf(t, !errors.Is(got, fatal) || !strings.Contains(out.String(), fatal.Error()) ||
				!strings.Contains(
					out.String(),
					"unrelated failure",
				), "warning-only footer hid fatal cause: %v %s", got, out.String())
			assert.FatalIf(t, errors.Is(fatal, cleanupErr) &&
				(!errors.Is(got, cleanupErr) || strings.Count(out.String(), cleanupErr.Error()) != 1), "cleanup failure lost or duplicated: %v %s", got, out.String())
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_WarningWriteErrors(t *testing.T) {
	for _, verb := range []string{"build", "diff"} {
		t.Run(verb, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			appendScopeSpec(t, root, "cluster/hr.yaml", "  values: {unused: selected}\n")
			args := []string{
				verb,
				"hr",
				"selected",
				"--path",
				filepath.Join(root, "cluster"),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
			}
			if verb == "diff" {
				orig := writeNamedScopeFixture(t)
				testutil.WriteFile(
					t,
					root,
					"charts/app/templates/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: changed}\n",
				)
				appendScopeSpec(t, root, "cluster/ks.yaml", "  postBuild: {substitute: {GREETING: changed}}\n")

				args = append(args, "--path-orig", filepath.Join(orig, "cluster"))
			}
			want := errors.New("warning write failed")
			cmd := New("test")
			cmd.SetContext(t.Context())
			cmd.SetArgs(args)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(failingWriter{err: want})
			{
				got := cmd.Execute()
				assert.FatalIf(t, !errors.Is(got, want), "warning write failure discarded: %v", got)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_Descendants(t *testing.T) {
	for _, shape := range []string{"direct", "nested", "resourceset"} {
		t.Run(shape, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			orig := writeNamedScopeFixture(t)
			child := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: owned-child, namespace: other}
spec:
  interval: 10m
  dependsOn: [{name: missing-child-dependency}]
  chart:
    spec:
      chart: ./charts/app
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`
			switch shape {
			case "nested":
				testutil.WriteFile(t, root, "nested/kustomization.yaml", "resources: [child.yaml]\n")
				testutil.WriteFile(t, root, "nested/child.yaml", child)
				child = testutil.KustomizationYAML(
					"nested-child",
					"other",
					"  interval: 10m\n  path: ./nested\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
				)
			case "resourceset":
				child = "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: child-set, namespace: other}\nspec:\n  resources:\n  - " + strings.ReplaceAll(
					strings.TrimSuffix(child, "\n"),
					"\n",
					"\n    ",
				) + "\n"
			}
			testutil.WriteFile(t, root, "selected/kustomization.yaml", "resources: [cm.yaml, child.yaml]\n")
			testutil.WriteFile(t, root, "selected/child.yaml", child)
			for _, verb := range []string{"build", "test", "diff"} {
				args := []string{
					verb,
					"ks",
					"selected",
					"--path",
					root,
					"-n",
					"flux-system",
					"--cache-dir",
					t.TempDir(),
					"--concurrency",
					"2",
				}
				if verb == "diff" {
					args = append(args, "--path-orig", orig)
				}
				cli := runCLI(t, args...)
				stderr, code := cli.stderr, cli.code
				assert.FatalIf(
					t,
					code != 1 || !strings.Contains(stderr, "missing-child-dependency"),
					"%s hid owned descendant failure: code=%d %s",
					verb,
					code,
					stderr,
				)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_PrerequisiteSiblings(t *testing.T) {
	root := writeNamedScopeFixture(t)
	testutil.WriteFile(
		t,
		root,
		"kustomization.yaml",
		"resources: [cluster/hr.yaml, cluster/unrelated.yaml, cluster/parent.yaml, outside.yaml]\n",
	)
	body, err := os.ReadFile(filepath.Join(root, "cluster", "ks.yaml"))
	assert.NoError(t, err)
	assert.NoError(t, os.Remove(filepath.Join(root, "cluster", "ks.yaml")))
	testutil.WriteFile(
		t,
		root,
		"siblings/selected.yaml",
		string(body)+"  postBuild: {substituteFrom: [{kind: ConfigMap, name: values}]}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"siblings/values.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: flux-system}\ndata: {GREETING: hello}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"siblings/kustomization.yaml",
		"resources: [selected.yaml, sibling.yaml, values.yaml]\n",
	)
	testutil.WriteFile(t, root, "siblings/sibling.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: sibling, namespace: apps}
spec:
  interval: 10m
  dependsOn: [{name: missing-sibling-dependency}]
  chart:
    spec:
      chart: ./charts/app
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(
		t,
		root,
		"cluster/parent.yaml",
		testutil.KustomizationYAML(
			"parent",
			"flux-system",
			"  interval: 10m\n  path: ./siblings\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	for _, verb := range []string{"build", "test"} {
		cli := runCLI(
			t,
			verb,
			"ks",
			"selected",
			"--path",
			root,
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		stderr, code := cli.stderr, cli.code
		assert.FatalIf(
			t,
			code != 0 || strings.Contains(stderr, "missing-sibling-dependency"),
			"%s included prerequisite parent's sibling: code=%d %s",
			verb,
			code,
			stderr,
		)
	}
	cli2 := runCLI(t, "build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2")
	stderr, code := cli2.stderr, cli2.code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "missing-sibling-dependency"),
		"full render hid sibling failure: code=%d %s",
		code,
		stderr,
	)
}

func TestReportFailures_FatalCycleWithWarnings(t *testing.T) {
	t.Parallel()
	o, err := completedOrchestrator(t)
	assert.NoError(t, err)
	id := func(name string) manifest.NamedResource {
		return testutil.KustomizationID("apps", name)
	}
	a, b, other := id("a"), id("b"), id("other")
	for _, pair := range [][2]manifest.NamedResource{{a, b}, {b, a}} {
		o.Store().AddObject(&manifest.Kustomization{Name: pair[0].Name, Namespace: pair[0].Namespace})
		reads := new(store.ReadSet)
		reads.Init(o.Store(), pair[0], nil)
		reads.GetObject(pair[0])
		reads.RequireData(pair[1])
		reads.Accept(store.AssessmentReconciled)
		t.Cleanup(reads.Retire)
	}
	res := &orchestrator.Result{
		Failed: map[manifest.NamedResource]store.StatusInfo{
			a:     {Status: store.StatusFailed, Message: "cycle a"},
			b:     {Status: store.StatusFailed, Message: "cycle b"},
			other: {Status: store.StatusFailed, Message: "unrelated failure"},
		},
		Blocked: map[manifest.NamedResource][]manifest.NamedResource{a: {b}, b: {a}},
	}
	projectedResults1, projectionErrors1 := orchestrator.Select(
		[]orchestrator.Rendered{{Orchestrator: o, Result: res}},
		&selection.FailuresError{
			Message: "raw reconcile failures",
		},
		[]manifest.NamedResource{{Group: a.Group, Kind: a.Kind, Name: a.Name}},
		nil,
	)
	selectedResults, selectedErr, remainder := projectedResults1, projectionErrors1.Selected, projectionErrors1.Remaining
	assert.FatalIf(t, len(selectedResults) != 1, "selection failed: %v", selectedErr)
	var out bytes.Buffer
	{
		got := finishSelected(
			&selectedRequest{roots: []manifest.NamedResource{{Name: "selected"}}, stderr: &out},
			&selectedInvocation{
				groups: []selectedGroup{{cluster: ".", sides: selectedResults, err: selectedErr, remainder: remainder}},
			},
			&selectedPayload{},
		)
		assert.FatalIf(t, got == nil, "%v", "fatal cycle became successful")
	}
	for _, want := range []string{"blocked by " + b.String(), "blocked by " + a.String(), "unrelated failure", "2 failed"} {
		assert.FatalIf(t, !strings.Contains(out.String(), want), "fatal cycle hidden by warnings: %s", out.String())
	}
}
