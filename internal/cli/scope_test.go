package cli

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func writeNamedScopeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: hello}\n")
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
		testutil.WriteFile(t, root, tc.name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+tc.name+", namespace: apps}\ndata: {greeting: hello}\n")
	}
	return root
}

func TestRun_NamedScope_UnrelatedFailure(t *testing.T) {
	for _, kind := range []string{"hr", "ks"} {
		t.Run(kind, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			cache := t.TempDir()
			var want string
			for _, path := range []string{root, filepath.Join(root, "cluster")} {
				args := []string{"build", kind, "selected", "--path", path, "--cache-dir", cache, "--concurrency", "2"}
				out, errOut, code := runCLI(t, args...)
				if code != 0 {
					t.Fatalf("named build exited %d: %s", code, errOut)
				}
				want = cmp.Or(want, out)
				if out == "" || out != want {
					t.Fatalf("selected YAML differs: want %s, got %s", want, out)
				}
				if path != root && (!strings.Contains(errOut, "warnings") || !strings.Contains(errOut, "outside") || !strings.Contains(errOut, "unrelated")) {
					t.Fatalf("unrelated diagnostics missing: %s", errOut)
				}
			}
		})
	}
}

func appendScopeSpec(t *testing.T, root, file, spec string) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, file))
	if err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, file, string(body)+spec)
}

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
			args := []string{tc.verb, tc.kind, "selected", "--path", filepath.Join(root, "cluster"), "--cache-dir", cache, "--concurrency", "2"}
			if tc.verb == "diff" {
				testutil.WriteFile(t, root, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: changed}\n")
				appendScopeSpec(t, root, "cluster/ks.yaml", "  postBuild: {substitute: {GREETING: changed}}\n")
				args = append(args, "--path-orig", filepath.Join(orig, "cluster"), "-o", "diff")
			}
			_, stderr, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("healthy named command exited %d: %s", code, stderr)
			}
			file, ns := "cluster/hr.yaml", "apps"
			if tc.kind == "ks" {
				file, ns = "cluster/ks.yaml", "flux-system"
			}
			appendScopeSpec(t, root, file, "  dependsOn: [{name: missing, namespace: other}]\n")
			_, stderr, code = runCLI(t, append(args, "-n", ns)...)
			if code != 1 || !strings.Contains(stderr, "missing") {
				t.Fatalf("selected cross-namespace failure hidden: %d %s", code, stderr)
			}
		})
	}
}

func TestRun_NamedScope_UnnamedAndNoMatch(t *testing.T) {
	root := writeNamedScopeFixture(t)
	for _, args := range [][]string{
		{"build", "all"}, {"build", "ks"}, {"build", "hr", "absent"},
		{"build", "ks", "absent"}, {"get", "ks", "absent"},
		{"test", "ks", "absent"}, {"test", "hr", "absent"},
		{"diff", "ks", "absent"}, {"diff", "hr", "absent"},
	} {
		t.Run(strings.Join(args, "/"), func(t *testing.T) {
			if args[0] == "diff" {
				args = append(args, "--path-orig", filepath.Join(writeNamedScopeFixture(t), "cluster"))
			}
			_, stderr, code := runCLI(t, append(args, "--path", filepath.Join(root, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2")...)
			if code != 1 || stderr == "" {
				t.Fatalf("expected failure: %d %s", code, stderr)
			}
			if len(args) > 2 && !strings.Contains(stderr, "absent") {
				t.Fatalf("missing name diagnostic: %s", stderr)
			}
		})
	}
}

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
		{name: "tolerated", kind: "Secret", extra: "--allow-missing-secrets", absent: true, broken: true},
		{name: "producer-backed", kind: "Secret", absent: true, broken: true},
		{name: "absent-healthy", kind: "Secret", absent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			appendScopeSpec(t, root, "cluster/hr.yaml", fmt.Sprintf("  valuesFrom: [{kind: %s, name: values, optional: %t}]\n", tc.kind, tc.optional))
			testutil.WriteFile(t, root, "cluster/producer.yaml", testutil.KustomizationYAML("producer", "flux-system", "  interval: 10m\n  path: ./values\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
			if tc.chain {
				appendScopeSpec(t, root, "cluster/producer.yaml", "  dependsOn: [{name: producer-chain}]\n")
				testutil.WriteFile(t, root, "cluster/producer-chain.yaml", testutil.KustomizationYAML("producer-chain", "flux-system", "  path: ./values\n  dependsOn: [{name: missing-producer-root, namespace: other}]\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
			}
			inputFile := "values/input.yaml"
			resources := "resources: [input.yaml"
			if tc.broken {
				resources += ", missing.yaml"
			}
			testutil.WriteFile(t, root, "values/kustomization.yaml", resources+"]\n")
			input := "apiVersion: v1\nkind: " + tc.kind + "\nmetadata: {name: values, namespace: apps}\ndata:\n  values.yaml: 'greeting: hello'\n"
			if tc.absent {
				input = "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: values-producer, namespace: apps}\nspec:\n  target: {name: values}\n  dataFrom: [{extract: {key: values}}]\n"
			}
			testutil.WriteFile(t, root, inputFile, input)
			cfg := orchestrator.Config{Path: filepath.Join(root, "cluster"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2, AllowMissingSecrets: tc.extra != ""}
			run := runOrchestratorCfg(t.Context(), cfg)
			res, runErr := run.Result, run.Err
			if run.Orchestrator == nil || res == nil {
				t.Fatal("full-mode fixture did not complete")
			}
			hr := testutil.HelmReleaseID("apps", "selected")
			producer := testutil.KustomizationID("flux-system", "producer")
			wantFailure := tc.broken && !tc.optional
			requiredFailure := tc.broken && !tc.optional && !tc.absent
			_, readerFailed := res.Failed[hr]
			if (len(outputByParent(res.Manifests)[hr]) == 0) != requiredFailure || readerFailed != requiredFailure || (len(res.Blocked[hr]) != 0) != requiredFailure {
				t.Fatalf("consumer did not render independently: failed=%v blocked=%v", res.Failed, res.Blocked)
			}
			if _, failed := res.Failed[producer]; failed != tc.broken {
				t.Fatalf("producer failure=%t, want %t: %v", failed, tc.broken, res.Failed)
			}
			args := []string{"build", "hr", "selected", "--path", filepath.Join(root, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2"}
			if tc.extra != "" {
				args = append(args, tc.extra)
			}
			stdout, stderr, code := runCLI(t, args...)
			selection, selectedErr, _ := orchestrator.Select([]orchestrator.Rendered{run}, runErr,
				[]manifest.NamedResource{{Group: hr.Group, Kind: hr.Kind, Name: hr.Name}}, nil)
			if len(selection) != 1 {
				t.Fatalf("selection failed: %v", selectedErr)
			}
			if _, failed := selection[0].Failed[producer]; failed != wantFailure || (selectedErr != nil) != wantFailure {
				t.Fatalf("required owner failure projection violated: failed=%v unrelated=%v error=%v", selection[0].Failed, selection[0].Unrelated, selectedErr)
			}
			for _, warning := range selection[0].Unrelated {
				if wantFailure && warning.Resource == producer {
					t.Fatalf("required owner became unrelated: %+v", warning)
				}
			}

			if (code == 1) != wantFailure || strings.Contains(stdout, "selected-rendered") == requiredFailure {
				t.Fatalf("authored owner policy violated: %d %s\n%s", code, stdout, stderr)
			}
			if !requiredFailure {
				selectedYAML = cmp.Or(selectedYAML, stdout)
				if stdout != selectedYAML {
					t.Fatalf("producer health changed selected YAML: want %s, got %s", selectedYAML, stdout)
				}
			}
			if requiredFailure && !strings.Contains(stderr, "values") {
				t.Fatalf("required input failure missing: %s", stderr)
			}
			if tc.chain && (!strings.Contains(stderr, "producer-chain") || !strings.Contains(stderr, "missing-producer-root")) {
				t.Fatalf("producer's transitive root missing: %s", stderr)
			}
			if tc.broken && !strings.Contains(stderr, "producer") {
				t.Fatalf("producer diagnostic missing: %s", stderr)
			}
		})
	}
}

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
		t.Run(tc.name, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			kind := "ConfigMap"
			if tc.secret {
				kind = "Secret"
			}
			appendScopeSpec(t, root, "cluster/ks.yaml", fmt.Sprintf("  postBuild:\n    substituteFrom: [{kind: %s, name: scope-input, optional: %t}]\n", kind, !tc.self && !tc.required))
			if !tc.absent {
				testutil.WriteFile(t, root, "cluster/producer.yaml", testutil.KustomizationYAML("producer", "flux-system", "  path: ./values\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
				resources := "resources: [input.yaml"
				if tc.broken {
					resources += ", missing.yaml"
				}
				testutil.WriteFile(t, root, "values/kustomization.yaml", resources+"]\n")
				input := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: scope-input, namespace: flux-system}\ndata: {SCOPE: ready}\n"
				if tc.secret {
					input = "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: scope-input, namespace: flux-system}\nspec:\n  dataFrom: [{extract: {key: scope-input}}]\n"
				}
				testutil.WriteFile(t, root, "values/input.yaml", input)
				if tc.self {
					testutil.WriteFile(t, root, "selected/kustomization.yaml", "resources: [cm.yaml, input.yaml]\n")
					testutil.WriteFile(t, root, "selected/input.yaml", input)
				}
			}
			cfg := buildOrchCfg(commonFlags{path: filepath.Join(root, "cluster"), cacheDir: t.TempDir(), concurrency: 2})
			run := runOrchestratorCfg(t.Context(), cfg)
			res := run.Result
			id := testutil.KustomizationID("flux-system", "selected")
			requiredFailure := tc.broken && tc.required
			if res == nil || (len(outputByParent(res.Manifests)[id]) == 0) != requiredFailure || (len(res.Blocked[id]) != 0) != requiredFailure {
				t.Fatalf("consumer did not render independently: %+v", res)
			}
			out, stderr, code := runCLI(t, "build", "ks", "selected", "--path", cfg.Path, "--cache-dir", t.TempDir(), "--concurrency", "2")
			if (code == 1) != requiredFailure || strings.Contains(out, "name: selected") == requiredFailure {
				t.Fatalf("substitution owner policy: code=%d output=%s diagnostics=%s", code, out, stderr)
			}
			if tc.broken && !strings.Contains(stderr, "producer") {
				t.Fatalf("producer failure missing: %s", stderr)
			}
		})
	}
}

func TestRun_NamedScope_AllMatchingNamespaces(t *testing.T) {
	root := writeNamedScopeFixture(t)
	body, err := os.ReadFile(filepath.Join(root, "cluster/hr.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "cluster/other.yaml", strings.Replace(string(body), "namespace: apps", "namespace: other", 1)+"  dependsOn: [{name: missing}]\n")
	for _, verb := range []string{"build", "test"} {
		args := []string{verb, "hr", "selected", "--path", filepath.Join(root, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2"}
		_, stderr, code := runCLI(t, append(args, "-n", "apps")...)
		if code != 0 {
			t.Fatalf("healthy namespace failed: %d %s", code, stderr)
		}
		_, stderr, code = runCLI(t, args...)
		if code != 1 || !strings.Contains(stderr, "missing") {
			t.Fatalf("matching unhealthy namespace hidden: %d %s", code, stderr)
		}
	}
}

func TestRun_NamedScope_ParentChain(t *testing.T) {
	root := writeNamedScopeFixture(t)
	body, err := os.ReadFile(filepath.Join(root, "cluster/hr.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "cluster/hr.yaml")); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "child/hr.yaml", string(body))
	testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [hr.yaml]\n")
	testutil.WriteFile(t, root, "parent/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./child\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [child.yaml, missing.yaml]\n")
	testutil.WriteFile(t, root, "cluster/parent.yaml", testutil.KustomizationYAML("parent", "flux-system", "  path: ./parent\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	_, stderr, code := runCLI(t, "build", "ks", "child", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "-n", "apps")
	if code != 1 || !strings.Contains(stderr, "parent") || !strings.Contains(stderr, "missing.yaml") {
		t.Fatalf("cross-namespace grandparent failure hidden: %d %s", code, stderr)
	}
}

func TestScopedFailures_WarningsPreserveFatalErrors(t *testing.T) {
	o, err := orchestrator.New(t.Context(), orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	selected := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
	o.Store().AddObject(selected)
	other := testutil.KustomizationID("apps", "other")
	res := &orchestrator.Result{Manifests: []manifest.DocumentBatch{{Parent: selected, Documents: []map[string]any{{"kind": "ConfigMap", "metadata": map[string]any{"name": "partial"}}}}}, Failed: map[manifest.NamedResource]store.StatusInfo{other: {Status: store.StatusFailed, Message: "unrelated failure"}}}
	t.Cleanup(func() {
		if err := o.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	cleanupErr := errors.New("backing close failed")
	for _, fatal := range []error{context.Canceled, errors.New("unattributed panic"), errors.New("output write failed"), cleanupErr, errors.Join(context.Canceled, cleanupErr)} {
		t.Run(fatal.Error(), func(t *testing.T) {
			runErr := errors.Join(&orchestrator.FailuresError{Message: "raw reconcile failures"}, fatal)
			selection, selectedErr, remainder := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res}}, runErr,
				[]manifest.NamedResource{testutil.KustomizationID("", selected.Name)}, nil)
			if len(selection) != 1 || len(outputByParent(selection[0].Manifests)[selected.Named()]) != 1 {
				t.Fatalf("selection failed: %v", selectedErr)
			}
			var out bytes.Buffer
			got := finishSelected(selectedRequest{roots: []manifest.NamedResource{{Name: "selected"}}, stderr: &out}, &selectedInvocation{sides: selection, err: selectedErr, remainder: remainder}, selectedPayload{})
			if !errors.Is(got, fatal) || !strings.Contains(out.String(), fatal.Error()) || !strings.Contains(out.String(), "unrelated failure") {
				t.Fatalf("warning-only footer hid fatal cause: %v %s", got, out.String())
			}
			if errors.Is(fatal, cleanupErr) && (!errors.Is(got, cleanupErr) || strings.Count(out.String(), cleanupErr.Error()) != 1) {
				t.Fatalf("cleanup failure lost or duplicated: %v %s", got, out.String())
			}

		})
	}
}

func TestRun_NamedScope_WarningWriteErrors(t *testing.T) {
	for _, verb := range []string{"build", "diff"} {
		t.Run(verb, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			args := []string{verb, "hr", "selected", "--path", filepath.Join(root, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2"}
			if verb == "diff" {
				orig := writeNamedScopeFixture(t)
				testutil.WriteFile(t, root, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: changed}\n")
				appendScopeSpec(t, root, "cluster/ks.yaml", "  postBuild: {substitute: {GREETING: changed}}\n")
				body, err := os.ReadFile(filepath.Join(root, "cluster/unrelated.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				testutil.WriteFile(t, root, "cluster/unrelated.yaml", strings.Replace(string(body), "path: ./unrelated", "path: ./missing-dir", 1))
				args = append(args, "--path-orig", filepath.Join(orig, "cluster"))
			}
			want := errors.New("warning write failed")
			cmd := New("test")
			cmd.SetContext(t.Context())
			cmd.SetArgs(args)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(failingWriter{err: want})
			if got := cmd.Execute(); !errors.Is(got, want) {
				t.Fatalf("warning write failure discarded: %v", got)
			}
		})
	}
}

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
				child = testutil.KustomizationYAML("nested-child", "other", "  interval: 10m\n  path: ./nested\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")
			case "resourceset":
				child = "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: child-set, namespace: other}\nspec:\n  resources:\n  - " + strings.ReplaceAll(strings.TrimSuffix(child, "\n"), "\n", "\n    ") + "\n"
			}
			testutil.WriteFile(t, root, "selected/kustomization.yaml", "resources: [cm.yaml, child.yaml]\n")
			testutil.WriteFile(t, root, "selected/child.yaml", child)
			for _, verb := range []string{"build", "test", "diff"} {
				args := []string{verb, "ks", "selected", "--path", root, "-n", "flux-system", "--cache-dir", t.TempDir(), "--concurrency", "2"}
				if verb == "diff" {
					args = append(args, "--path-orig", orig)
				}
				_, stderr, code := runCLI(t, args...)
				if code != 1 || !strings.Contains(stderr, "missing-child-dependency") {
					t.Fatalf("%s hid owned descendant failure: code=%d %s", verb, code, stderr)
				}
			}

		})
	}
}

func TestRun_NamedScope_PrerequisiteSiblings(t *testing.T) {
	root := writeNamedScopeFixture(t)
	body, err := os.ReadFile(filepath.Join(root, "cluster/ks.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "cluster/ks.yaml")); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "siblings/selected.yaml", string(body)+"  postBuild: {substituteFrom: [{kind: ConfigMap, name: values}]}\n")
	testutil.WriteFile(t, root, "siblings/values.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: flux-system}\ndata: {GREETING: hello}\n")
	testutil.WriteFile(t, root, "siblings/kustomization.yaml", "resources: [selected.yaml, sibling.yaml, values.yaml]\n")
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
	testutil.WriteFile(t, root, "cluster/parent.yaml", testutil.KustomizationYAML("parent", "flux-system", "  interval: 10m\n  path: ./siblings\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	for _, verb := range []string{"build", "test"} {
		_, stderr, code := runCLI(t, verb, "ks", "selected", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2")
		if code != 0 || !strings.Contains(stderr, "sibling") && verb == "build" {
			t.Fatalf("%s included prerequisite parent's sibling: code=%d %s", verb, code, stderr)
		}
	}
}

func TestReportFailures_FatalCycleWithWarnings(t *testing.T) {
	o, err := orchestrator.New(t.Context(), orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := o.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
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
		reads.Accept()
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
	selection, selectedErr, remainder := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res}},
		&orchestrator.FailuresError{Message: "raw reconcile failures"}, []manifest.NamedResource{{Group: a.Group, Kind: a.Kind, Name: a.Name}}, nil)
	if len(selection) != 1 {
		t.Fatalf("selection failed: %v", selectedErr)
	}
	var out bytes.Buffer
	if got := finishSelected(selectedRequest{roots: []manifest.NamedResource{{Name: "selected"}}, stderr: &out}, &selectedInvocation{sides: selection, err: selectedErr, remainder: remainder}, selectedPayload{}); got == nil {
		t.Fatal("fatal cycle became successful")
	}
	for _, want := range []string{"blocked by " + b.String(), "blocked by " + a.String(), "unrelated failure", "2 failed"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("fatal cycle hidden by warnings: %s", out.String())
		}
	}
}

func TestScopedFailures_CanonicalReplacementWithStaleArtifact(t *testing.T) {
	root := writeNamedScopeFixture(t)
	run := runOrchestrator(t.Context(), commonFlags{path: root, cacheDir: t.TempDir(), concurrency: 2})
	o, res, err := run.Orchestrator, run.Result, run.Err
	if err != nil || res == nil {
		t.Fatalf("fresh fixture failed: %v", err)
	}
	id := testutil.HelmReleaseID("apps", "selected")
	hr, _ := o.Store().Get[*manifest.HelmRelease](id)
	artifact := o.Store().GetArtifact(id)
	if artifact == nil {
		t.Fatal("fresh render has no artifact")
	}
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
	selection, selectedErr, _ := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res}},
		&orchestrator.FailuresError{Message: "raw reconcile failures"}, []manifest.NamedResource{{Group: id.Group, Kind: id.Kind, Name: id.Name}}, nil)
	if len(selection) != 1 || selectedErr == nil {
		t.Fatalf("selection failed: %v", selectedErr)
	}
	if _, ok := selection[0].Failed[id]; !ok {
		t.Fatal("stale artifact weakened selected failure")
	}
	if len(selection[0].Failed) != 2 || selection[0].Failed[current].String() != "current input failure" ||
		len(selection[0].Unrelated) != 1 || selection[0].Unrelated[0].Resource != old || o.Store().GetArtifact(id) != nil {
		t.Fatalf("projection did not follow current canonical refs: %+v", selection[0])
	}
	_, _, accepted := o.Store().GetEmission(id)
	defer accepted.Finish()
	assert.Equal(t, len(accepted.Manifests), 0)
}

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
			args := []string{verb, kind, "--path", filepath.Join(root, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2"}
			_, stderr, code := runCLI(t, args...)
			assert.Equal(t, code, 1)
			expected := "flate error: " + want + "\n"
			if verb == "test" {
				expected = ""
			}
			assert.Equal(t, stderr, persistenceNotice+expected)
			cmd := New("test")
			cmd.SetContext(t.Context())
			cmd.SetArgs(args)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			err := cmd.Execute()
			if err == nil {
				t.Fatal("unnamed failure returned nil")
			}
			expected = want
			if verb == "test" {
				expected = "test failures detected\n" + want
			}
			assert.Equal(t, err.Error(), expected)
		})
	}
}

func TestRun_NamedScope_UnrelatedNamespaceWarnings(t *testing.T) {
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			body, err := os.ReadFile(filepath.Join(root, "cluster/unrelated.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "cluster/unrelated.yaml", strings.Replace(string(body), "namespace: flux-system", "namespace: other", 1))
			out, stderr, code := runCLI(t, verb, "hr", "selected", "--path", filepath.Join(root, "cluster"), "-n", "apps", "--cache-dir", t.TempDir(), "--concurrency", "2")
			assert.Equal(t, code, 0)
			if strings.Contains(out+stderr, "unrelated") || strings.Contains(out+stderr, "outside") {
				t.Fatalf("excluded namespace diagnostics leaked: %s%s", out, stderr)
			}
		})
	}
}

func TestRun_NamedScope_TestWarningDestinations(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	for _, fatal := range []bool{false, true} {
		t.Run(fmt.Sprint(fatal), func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			wantCode := 0
			if fatal {
				appendScopeSpec(t, root, "cluster/hr.yaml", "  dependsOn: [{name: fatal-dependency}]\n")
				wantCode = 1
			}
			out, stderr, code := runCLI(t, "test", "hr", "selected", "--path", filepath.Join(root, "cluster"),
				"--cache-dir", t.TempDir(), "--concurrency", "2")
			assert.Equal(t, code, wantCode)
			assert.Equal(t, strings.Contains(out, "warnings") || strings.Contains(out, "unrelated") || strings.Contains(out, "outside"), false)
			assert.Equal(t, strings.Count(strings.TrimPrefix(stderr, persistenceNotice), "warnings (1)"), 1)
			assert.Equal(t, strings.Count(stderr, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/unrelated"), 1)
			assert.Equal(t, strings.Contains(stderr, "outside"), true)
			if fatal {
				assert.Equal(t, strings.Count(stderr, "apps/fatal-dependency"), 1)
				assert.Equal(t, strings.Contains(stderr, "unrelated"), true)
				assert.Equal(t, strings.Contains(stderr, "warnings"), true)
				assert.Equal(t, strings.Count(stderr, "flate error:"), 0)
			} else {
				assert.Equal(t, strings.HasPrefix(stderr, persistenceNotice), true)
			}
		})
	}
}

func TestRun_NamedScope_DiffBothSnapshots(t *testing.T) {
	root, orig := writeNamedScopeFixture(t), writeNamedScopeFixture(t)
	testutil.WriteFile(t, root, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: changed}\n")
	appendScopeSpec(t, root, "cluster/hr.yaml", "  values: {greeting: changed}\n")
	args := []string{"diff", "hr", "selected", "--path", filepath.Join(root, "cluster"), "--path-orig", filepath.Join(orig, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2"}
	want, _, code := runCLI(t, args...)
	assert.Equal(t, code, 0)
	if want == "" {
		t.Fatal("selected chart change produced no diff")
	}
	appendScopeSpec(t, root, "cluster/unrelated.yaml", "  postBuild: {substitute: {GREETING: changed}}\n")
	got, stderr, code := runCLI(t, args...)
	assert.Equal(t, code, 0)
	assert.Equal(t, got, want)
	for _, label := range []string{"orig snapshot:", "current snapshot:"} {
		_, block, ok := strings.Cut(stderr, label)
		block, _, _ = strings.Cut(block, "snapshot:")
		if !ok || !strings.Contains(block, "warnings") {
			t.Fatalf("missing %s warning block: %s", label, stderr)
		}
	}
}

func TestRun_NamedScope_DiffFatalWriteError(t *testing.T) {
	root, orig := writeNamedScopeFixture(t), writeNamedScopeFixture(t)
	appendScopeSpec(t, root, "cluster/hr.yaml", "  dependsOn: [{name: missing}]\n")
	want := errors.New("fatal diagnostic write failed")
	cmd := New("test")
	cmd.SetContext(t.Context())
	cmd.SetArgs([]string{"diff", "hr", "selected", "--path", filepath.Join(root, "cluster"), "--path-orig", filepath.Join(orig, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(failingWriter{err: want})
	if got := cmd.Execute(); !errors.Is(got, want) {
		t.Fatalf("fatal diff diagnostic write failure discarded: %v", got)
	}
}

func TestRun_NamedScope_DiffOwningKustomizationFails(t *testing.T) {
	for _, tc := range []struct{ name, failingSide, layout string }{
		{"current/plain/edited", "current", "plain"},
		{"orig/plain/edited", "orig", "plain"},
		{"current/escape", "current", "escape"},
		{"current/moved", "current", "moved"},
		{"orig/moved", "orig", "moved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				testutil.WriteFile(t, dir, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: hello}\n")
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
					testutil.WriteFile(t, dir, "flux/"+name+".yaml", testutil.KustomizationYAML(name, "flux-system", "  interval: 10m\n  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
				}
				testutil.WriteFile(t, dir, "other/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, dir, "other/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: other, namespace: apps}\n")
			}
			if tc.layout == "plain" {
				appendScopeSpec(t, root, releasePath, "  values: {greeting: changed}\n")
			}
			if tc.layout == "moved" {
				if err := os.Rename(filepath.Join(root, releasePath), filepath.Join(root, "other/hr.yaml")); err != nil {
					t.Fatal(err)
				}
				testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: apps, namespace: apps}\n")
				testutil.WriteFile(t, root, "other/kustomization.yaml", "resources: [cm.yaml, hr.yaml]\n")
			}
			failing := root
			if tc.failingSide == "orig" {
				failing = orig
			}
			appendScopeSpec(t, failing, "flux/apps.yaml", "  postBuild: {substituteFrom: [{kind: ConfigMap, name: nope}]}\n")
			flags := []string{"--path", filepath.Join(root, "flux"), "--path-orig", filepath.Join(orig, "flux"), "--cache-dir", t.TempDir(), "--concurrency", "2"}
			_, stderr, code := runCLI(t, append([]string{"diff", "hr", "selected"}, flags...)...)
			_, block, ok := strings.Cut(stderr, tc.failingSide+" snapshot:")
			if tc.layout == "moved" && tc.failingSide == "current" {
				if code != 0 || !ok || !strings.Contains(block, "warnings (1)") || !strings.Contains(block, "ConfigMap flux-system/nope: not found") || strings.Contains(stderr, "reconcile completed") {
					t.Fatalf("former owning Kustomization became fatal: %d %s", code, stderr)
				}
				_, stderr, code = runCLI(t, "build", "hr", "selected", "--path", filepath.Join(root, "flux"), "--cache-dir", t.TempDir(), "--concurrency", "2")
				if code != 0 {
					t.Fatalf("former owning Kustomization became fatal in build: %d %s", code, stderr)
				}
			} else if code != 1 || !ok || !strings.Contains(block, "reconcile completed with 1 failure(s):") || !strings.Contains(block, "ConfigMap/flux-system/nope: not found") {
				t.Fatalf("owning Kustomization failure hidden: %d %s", code, stderr)
			}
			_, stderr, code = runCLI(t, append([]string{"diff", "ks", "other"}, flags...)...)
			if code != 0 {
				t.Fatalf("unrelated owning Kustomization became fatal: %d %s", code, stderr)
			}
			if tc.layout == "plain" {
				baseline := orig
				if failing == orig {
					baseline = root
				}
				_, stderr, code = runCLI(t, "build", "hr", "selected", "--path", filepath.Join(failing, "flux"), "--path-orig", filepath.Join(baseline, "flux"), "--cache-dir", t.TempDir(), "--concurrency", "2")
				if code != 1 || !strings.Contains(stderr, "1 failed") || !strings.Contains(stderr, "flux-system/nope") {
					t.Fatalf("changed-only owning Kustomization failure hidden: %d %s", code, stderr)
				}
			}
		})
	}
}

func TestRun_NamedScope_DiffDeletedReleaseSiblingFails(t *testing.T) {
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
		testutil.WriteFile(t, dir, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}-rendered', namespace: apps}\ndata: {greeting: hello}\n")
		testutil.WriteFile(t, dir, "flux/apps.yaml", testutil.KustomizationYAML("apps", "flux-system", "  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [selected.yaml, sibling.yaml]\n")
		testutil.WriteFile(t, dir, "apps/selected.yaml", release("selected", "./charts/app"))
		testutil.WriteFile(t, dir, "apps/sibling.yaml", release("sibling", "./charts/app"))
	}
	if err := os.Remove(filepath.Join(root, "apps/selected.yaml")); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [sibling.yaml]\n")
	testutil.WriteFile(t, root, "apps/sibling.yaml", release("sibling", "./charts/missing"))
	flags := []string{"--path", filepath.Join(root, "flux"), "--path-orig", filepath.Join(orig, "flux"), "--cache-dir", t.TempDir(), "--concurrency", "2"}
	for _, tc := range []struct {
		name string
		code int
	}{
		{"selected", 0},
		{"sibling", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, stderr, code := runCLI(t, append([]string{"diff", "hr", tc.name}, flags...)...)
			if code != tc.code || !strings.Contains(stderr, "sibling") || !strings.Contains(stderr, "charts/missing") {
				t.Fatalf("diff hr %s: exit %d, want %d; stderr: %s", tc.name, code, tc.code, stderr)
			}
			if tc.code == 0 {
				if !strings.Contains(stderr, "warnings (1)") || strings.Contains(stderr, "reconcile completed") || !strings.Contains(out, "selected-rendered") || strings.Contains(out, "sibling") {
					t.Fatalf("deleted release diff includes unrelated failure: stdout: %s; stderr: %s", out, stderr)
				}
			} else if !strings.Contains(stderr, "reconcile completed with 1 failure(s):") {
				t.Fatalf("selected sibling failure hidden: %s", stderr)
			}
		})
	}
}

func TestRun_NamedScope_DiffCounterpartSelection(t *testing.T) {
	for _, tc := range []struct{ name, releaseName, namespace string }{
		{"name", "other-release", "apps"},
		{"namespace", "selected", "team"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, orig := writeNamedScopeFixture(t), writeNamedScopeFixture(t)
			for _, dir := range []string{root, orig} {
				testutil.WriteFile(t, dir, "cluster/bad.yaml", testutil.KustomizationYAML("bad", tc.namespace, "  interval: 10m\n  path: ./bad\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
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
			appendScopeSpec(t, root, "cluster/bad.yaml", "  postBuild: {substituteFrom: [{kind: ConfigMap, name: nope}]}\n")
			flags := []string{"--path", filepath.Join(root, "cluster"), "--path-orig", filepath.Join(orig, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2"}
			if tc.name == "namespace" {
				flags = append(flags, "-n", "apps")
			}
			out, stderr, code := runCLI(t, append([]string{"diff", "hr", "selected"}, flags...)...)
			if code != 0 || strings.Contains(stderr, "reconcile completed") {
				t.Fatalf("unselected counterpart owner became fatal: %d %s", code, stderr)
			}
			if strings.Contains(out, "other-release") || (tc.name == "namespace" && strings.Contains(out+stderr, "team")) {
				t.Fatalf("unselected counterpart leaked into diff: %s%s", out, stderr)
			}
		})
	}
}

func TestReportFailures_NamedReportingPolicy(t *testing.T) {
	for _, named := range []bool{false, true} {
		t.Run(fmt.Sprint(named), func(t *testing.T) {
			o, err := orchestrator.New(t.Context(), orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := o.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			id := testutil.KustomizationID("apps", "failed")
			o.Store().AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{id: {Status: store.StatusFailed, Message: "render failed"}}}
			query := manifest.NamedResource{Group: id.Group, Kind: id.Kind}
			if named {
				query.Name = id.Name
			}
			selection, selectedErr, remainder := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res}},
				&orchestrator.FailuresError{Message: "raw reconcile failures"}, []manifest.NamedResource{query}, nil)
			if len(selection) != 1 || selectedErr == nil {
				t.Fatalf("selection failed: %v", selectedErr)
			}
			var out bytes.Buffer
			extra := fmt.Errorf("reconcile: %w", context.Canceled)
			got := finishSelected(selectedRequest{roots: []manifest.NamedResource{query}, stderr: &out}, &selectedInvocation{sides: selection, err: errors.Join(selectedErr, extra), remainder: errors.Join(remainder, extra)}, selectedPayload{})
			if !errors.Is(got, context.Canceled) {
				t.Fatalf("lost non-resource error: %v", got)
			}
			if !strings.Contains(out.String(), "flate error: reconcile: context canceled") {
				t.Errorf("non-resource error missing from report: %s", &out)
			}
			writeErr := errors.New("report write failed")
			got = finishSelected(selectedRequest{roots: []manifest.NamedResource{query}, stderr: failingWriter{err: writeErr}}, &selectedInvocation{sides: selection, err: selectedErr, remainder: remainder}, selectedPayload{})
			assert.Equal(t, errors.Is(got, writeErr), true)
		})
	}
}

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
			if err != nil {
				t.Fatal(err)
			}
			body = []byte(strings.Replace(string(body), "namespace: ", "labels: {area: selected, empty: ''}, namespace: ", 1))
			testutil.WriteFile(t, root, file, string(body))
			args := []string{"get", tc.kind, "selected", "--path", filepath.Join(root, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "name", "-l", tc.label}
			stdout, stderr, code := runCLI(t, args...)
			if code != 0 || (stdout != "") != tc.visible || !strings.HasPrefix(stderr, persistenceNotice) || strings.Count(stderr, "unrelated") != 1 || !strings.Contains(stderr, "outside") {
				t.Fatalf("healthy named get: exit=%d stdout=%q stderr=%s", code, stdout, stderr)
			}
			appendScopeSpec(t, root, file, "  dependsOn: [{name: missing, namespace: other}]\n")
			_, stderr, code = runCLI(t, args...)
			if code != 1 || !strings.Contains(stderr, "missing") {
				t.Fatalf("label hid named failure: exit=%d stderr=%s", code, stderr)
			}
		})
	}
}

func TestFailureModel(t *testing.T) {
	id := func(name string) manifest.NamedResource {
		return testutil.KustomizationID("flux-system", name)
	}
	failed := func(ids ...manifest.NamedResource) map[manifest.NamedResource]store.StatusInfo {
		m := make(map[manifest.NamedResource]store.StatusInfo, len(ids))
		for _, id := range ids {
			m[id] = store.StatusInfo{Status: store.StatusFailed, Message: id.Name + " boom"}
		}
		return m
	}
	root, mid, leaf := id("root"), id("mid"), id("leaf")
	missing, dependent, dual, peer := id("missing"), id("dependent"), id("dual"), id("peer")
	for _, tc := range []struct {
		name   string
		result selection.Selection
		want   output.Model
		raw    *orchestrator.Result
	}{
		{
			name: "GroupsCascadeUnderRoots",
			result: selection.Selection{
				Failed:  failed(root, mid, leaf, dependent),
				Blocked: map[manifest.NamedResource][]manifest.NamedResource{mid: {root}, leaf: {mid}, dependent: {missing}},
				Causes:  map[manifest.NamedResource][]manifest.NamedResource{root: {leaf, mid}, missing: {dependent}},
			},
			want: output.Model{
				Primary: []output.Primary{{ID: root, Msg: "root boom", Blocks: []manifest.NamedResource{leaf, mid}}},
				Missing: []output.Missing{{ID: missing, RequiredBy: []manifest.NamedResource{dependent}}},
				Blocked: 3,
			},
		},
		{
			name:   "PrimaryOnly",
			result: selection.Selection{Failed: failed(root), Causes: map[manifest.NamedResource][]manifest.NamedResource{root: nil}},
			want:   output.Model{Primary: []output.Primary{{ID: root, Msg: "root boom"}}},
		},
		{name: "Empty"},
		{
			name: "BlockedCountsDistinctResources",
			result: selection.Selection{
				Failed:  failed(root, dual),
				Blocked: map[manifest.NamedResource][]manifest.NamedResource{dual: {root, missing}},
				Causes:  map[manifest.NamedResource][]manifest.NamedResource{root: {dual}, missing: {dual}},
			},
			want: output.Model{
				Primary: []output.Primary{{ID: root, Msg: "root boom", Blocks: []manifest.NamedResource{dual}}},
				Missing: []output.Missing{{ID: missing, RequiredBy: []manifest.NamedResource{dual}}},
				Blocked: 1,
			},
		},
		{
			name: "PreparedCycleRoot",
			raw: &orchestrator.Result{
				Failed: map[manifest.NamedResource]store.StatusInfo{
					root: {Status: store.StatusFailed, Message: "dependency cycle: blocked by " + peer.String()},
					peer: {Status: store.StatusFailed, Message: "dependency cycle: blocked by " + root.String()},
					leaf: {Status: store.StatusFailed, Message: "dependencies failed"},
				},
				Blocked: map[manifest.NamedResource][]manifest.NamedResource{root: {peer}, peer: {root}, leaf: {root}},
			},
			want: output.Model{
				Primary: []output.Primary{
					{ID: peer, Msg: "dependency cycle: blocked by " + root.String()},
					{ID: root, Msg: "dependency cycle: blocked by " + peer.String(), Blocks: []manifest.NamedResource{leaf}},
				},
				Blocked: 1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := &tc.result
			if tc.raw != nil {
				failed := maps.Clone(tc.raw.Failed)
				blocked := maps.Clone(tc.raw.Blocked)
				for id, deps := range blocked {
					blocked[id] = slices.Clone(deps)
				}
				o, err := orchestrator.New(t.Context(), orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir(), Concurrency: 2})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := o.Stop(); err != nil {
						t.Errorf("Stop: %v", err)
					}
				})
				selected, err, _ := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: tc.raw}},
					&orchestrator.FailuresError{Message: "raw failures"}, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
				if len(selected) != 1 || err == nil || !strings.Contains(err.Error(), fmt.Sprintf("(+%d blocked", tc.want.Blocked)) {
					t.Fatalf("selected error and report counts disagree: %v", err)
				}
				result = selected[0]
				assert.Equal(t, maps.Equal(tc.raw.Failed, failed), true)
				assert.Diff(t, tc.raw.Blocked, blocked)
			}
			got := failureModel(result)
			assert.Diff(t, got, tc.want)
			assert.Equal(t, got.Empty(), tc.want.Empty())
		})
	}
}
