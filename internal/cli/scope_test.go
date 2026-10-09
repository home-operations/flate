package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/go-git/go-git/v5"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func writeNamedScopeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if _, err := git.PlainInit(root, false); err != nil {
		t.Fatal(err)
	}
	if got := repoRootOf(filepath.Join(root, "cluster")); got != root {
		t.Fatalf("fixture root = %s, want %s", got, root)
	}
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
	for _, workers := range []string{"2", "4"} {
		for _, kind := range []string{"hr", "ks"} {
			t.Run(workers+"/"+kind, func(t *testing.T) {
				root := writeNamedScopeFixture(t)
				cache := t.TempDir()
				var want string
				for _, path := range []string{root, filepath.Join(root, "cluster"), filepath.Join(root, "cluster")} {
					for _, stream := range []bool{false, true} {
						args := []string{"build", kind, "selected", "--path", path, "--cache-dir", cache, "--concurrency", workers}
						if stream {
							args = append(args, "--stream")
						}
						out, errOut, code := runCLI(t, args...)
						if code != 0 {
							t.Fatalf("named build exited %d: %s", code, errOut)
						}
						if want == "" {
							want = out
						}
						if out == "" || out != want {
							t.Fatalf("selected YAML differs: want %s, got %s", want, out)
						}
						if path != root && (!strings.Contains(errOut, "warnings") || !strings.Contains(errOut, "outside") || !strings.Contains(errOut, "unrelated")) {
							t.Fatalf("unrelated diagnostics missing: %s", errOut)
						}
					}
				}
			})
		}
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
	for _, workers := range []string{"2", "4"} {
		for _, kind := range []string{"hr", "ks"} {
			for _, verb := range []string{"build", "test", "diff"} {
				t.Run(workers+"/"+kind+"/"+verb, func(t *testing.T) {
					root := writeNamedScopeFixture(t)
					orig := writeNamedScopeFixture(t)
					cache := t.TempDir()
					args := []string{verb, kind, "selected", "--path", filepath.Join(root, "cluster"), "--cache-dir", cache, "--concurrency", workers}
					if verb == "diff" {
						// Different chart input makes both snapshots' selected render active.
						testutil.WriteFile(t, root, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: changed}\n")
						appendScopeSpec(t, root, "cluster/ks.yaml", "  postBuild: {substitute: {GREETING: changed}}\n")
						args = append(args, "--path-orig", filepath.Join(orig, "cluster"), "-o", "diff")
					}
					_, stderr, code := runCLI(t, args...)
					if code != 0 {
						t.Fatalf("healthy named command exited %d: %s", code, stderr)
					}
					file, ns := "cluster/hr.yaml", "apps"
					if kind == "ks" {
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
	}
}

func TestRun_NamedScope_UnnamedAndNoMatch(t *testing.T) {
	root := writeNamedScopeFixture(t)
	for _, args := range [][]string{
		{"build", "all"}, {"build", "ks"}, {"build", "hr", "absent"},
		{"build", "ks", "absent", "--stream"}, {"test", "hr", "absent"},
		{"diff", "hr", "absent", "--path-orig", filepath.Join(writeNamedScopeFixture(t), "cluster")},
	} {
		t.Run(strings.Join(args[:2], "/")+fmt.Sprint(len(args)), func(t *testing.T) {
			_, stderr, code := runCLI(t, append(args, "--path", filepath.Join(root, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", "2")...)
			if code != 1 || stderr == "" {
				t.Fatalf("expected failure: %d %s", code, stderr)
			}
		})
	}
}

func TestRun_NamedScope_FailedDataOwner(t *testing.T) {
	for _, workers := range []int{2, 4} {
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
			t.Run(fmt.Sprintf("%d/%s", workers, tc.name), func(t *testing.T) {
				root := writeNamedScopeFixture(t)
				appendScopeSpec(t, root, "cluster/hr.yaml", fmt.Sprintf("  valuesFrom: [{kind: %s, name: values, optional: %t}]\n", tc.kind, tc.optional))
				testutil.WriteFile(t, root, "cluster/producer.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: producer, namespace: flux-system}
spec:
  interval: 10m
  path: ./values
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
				if tc.chain {
					appendScopeSpec(t, root, "cluster/producer.yaml", "  dependsOn: [{name: producer-chain}]\n")
					testutil.WriteFile(t, root, "cluster/producer-chain.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: producer-chain, namespace: flux-system}
spec:
  path: ./values
  dependsOn: [{name: missing-producer-root, namespace: other}]
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
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
				cfg := orchestrator.Config{Path: filepath.Join(root, "cluster"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: workers, AllowMissingSecrets: tc.extra != ""}
				o, res, _ := runOrchestratorCfg(t.Context(), cfg)
				if o == nil || o.Filter().Enabled() {
					t.Fatal("full-mode fixture did not complete")
				}
				hr := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "selected"}
				producer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "producer"}
				if len(res.Manifests[hr]) == 0 || len(res.Blocked[hr]) != 0 {
					t.Fatalf("consumer did not render independently: failed=%v blocked=%v", res.Failed, res.Blocked)
				}
				if _, failed := res.Failed[producer]; failed != tc.broken {
					t.Fatalf("producer failure=%t, want %t: %v", failed, tc.broken, res.Failed)
				}
				args := []string{"build", "hr", "selected", "--path", filepath.Join(root, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", fmt.Sprint(workers)}
				if tc.extra != "" {
					args = append(args, tc.extra)
				}
				stdout, stderr, code := runCLI(t, args...)
				if !slices.Contains(o.RequiredDataDependencies(hr), producer) {
					t.Fatalf("CLI exit=%d (required 1), output=%s, diagnostics=%s; healthy consumer has %d rendered documents, no blockers; failed owner %s is missing from authored lookup %v (producer failed=%t)", code, stdout, stderr, len(res.Manifests[hr]), producer, o.RequiredDataDependencies(hr), tc.broken)
				}

				if (code == 1) != tc.broken || !strings.Contains(stdout, "selected-rendered") {
					t.Fatalf("authored owner policy violated: %d %s\n%s", code, stdout, stderr)
				}
				if selectedYAML == "" {
					selectedYAML = stdout
				} else if stdout != selectedYAML {
					t.Fatalf("producer health changed selected YAML: want %s, got %s", selectedYAML, stdout)
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
}

func TestScopedFailures_SharedRootCyclesAndErrors(t *testing.T) {
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	id := func(name string) manifest.NamedResource {
		return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: name}
	}
	selected, root, other, a, b := id("selected"), id("root"), id("other"), id("a"), id("b")
	res := &orchestrator.Result{
		Failed:  map[manifest.NamedResource]store.StatusInfo{},
		Blocked: map[manifest.NamedResource][]manifest.NamedResource{selected: {root}, other: {root}, a: {b}, b: {a}},
	}
	for _, v := range []manifest.NamedResource{selected, root, other, a, b} {
		res.Failed[v] = store.StatusInfo{Status: store.StatusFailed, Message: v.Name + " failure"}
	}
	scope := scopedFailures(o, res, &commonFlags{}, selected)
	if len(scope.failed) != 2 || len(scope.warnings) != 3 {
		t.Fatalf("projection lost shared/cyclic failures: %+v", scope)
	}
	for _, warning := range scope.warnings {
		if warning.Resource == root || warning.Category != "" {
			t.Fatalf("fatal root reclassified or category introduced: %+v", warning)
		}
	}
	for range 10 {
		got := scopedFailures(o, res, &commonFlags{}, selected)
		if !slices.EqualFunc(got.warnings, scope.warnings, func(a, b manifest.Warning) bool {
			return a.Resource == b.Resource && a.Message == b.Message && slices.Equal(a.Detail, b.Detail)
		}) {
			t.Fatal("warnings are nondeterministic")
		}
	}
	panicErr := errors.New("unattributed panic")
	runErr := scopedRunError(scope, errors.Join(aggregateScopedFailures(res.Failed, res.Blocked), context.Canceled, panicErr))
	var out bytes.Buffer
	got := reportFailures(&out, scope, runErr)
	if !errors.Is(got, context.Canceled) || !errors.Is(got, panicErr) || !strings.Contains(out.String(), panicErr.Error()) || !strings.Contains(out.String(), "root failure") {
		t.Fatalf("fatal diagnostics hidden: %v %s", got, out.String())
	}
	if err := reportFailures(failingWriter{err: errors.New("warning write failed")}, scope, nil); err == nil {
		t.Fatal("warning write failure discarded")
	}
}

func TestRun_NamedScope_SubstituteOwners(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		for _, tc := range []struct {
			name                         string
			secret, self, broken, absent bool
		}{
			{name: "secret-producer", secret: true, broken: true},
			{name: "optional-configmap", broken: true},
			{name: "self-produced", self: true, broken: true},
			{name: "healthy-self-produced", self: true},
			{name: "absent-no-owner", absent: true},
		} {
			t.Run(workers+"/"+tc.name, func(t *testing.T) {
				root := writeNamedScopeFixture(t)
				kind := "ConfigMap"
				if tc.secret {
					kind = "Secret"
				}
				appendScopeSpec(t, root, "cluster/ks.yaml", fmt.Sprintf("  postBuild:\n    substituteFrom: [{kind: %s, name: scope-input, optional: %t}]\n", kind, !tc.self))
				if !tc.absent {
					testutil.WriteFile(t, root, "cluster/producer.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: producer, namespace: flux-system}
spec:
  path: ./values
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
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
				cfg := buildOrchCfg(commonFlags{path: filepath.Join(root, "cluster"), cacheDir: t.TempDir(), concurrency: 2}, helmFlags{})
				if workers == "4" {
					cfg.Concurrency = 4
				}
				_, res, _ := runOrchestratorCfg(t.Context(), cfg)
				id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "selected"}
				if res == nil || len(res.Manifests[id]) == 0 || len(res.Blocked[id]) != 0 {
					t.Fatalf("consumer did not render independently: %+v", res)
				}
				out, stderr, code := runCLI(t, "build", "ks", "selected", "--path", cfg.Path, "--cache-dir", t.TempDir(), "--concurrency", workers)
				if (code == 1) != tc.broken || !strings.Contains(out, "name: selected") {
					t.Fatalf("substitution owner policy: code=%d output=%s diagnostics=%s", code, out, stderr)
				}
				if tc.broken && !strings.Contains(stderr, "producer") {
					t.Fatalf("producer failure missing: %s", stderr)
				}
			})
		}
	}
}

func TestRun_NamedScope_AllMatchingNamespaces(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		t.Run(workers, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			body, err := os.ReadFile(filepath.Join(root, "cluster/hr.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "cluster/other.yaml", strings.Replace(string(body), "namespace: apps", "namespace: other", 1)+"  dependsOn: [{name: missing}]\n")
			for _, verb := range []string{"build", "test"} {
				args := []string{verb, "hr", "selected", "--path", filepath.Join(root, "cluster"), "--cache-dir", t.TempDir(), "--concurrency", workers}
				_, stderr, code := runCLI(t, append(args, "-n", "apps")...)
				if code != 0 {
					t.Fatalf("healthy namespace failed: %d %s", code, stderr)
				}
				_, stderr, code = runCLI(t, args...)
				if code != 1 || !strings.Contains(stderr, "missing") {
					t.Fatalf("matching unhealthy namespace hidden: %d %s", code, stderr)
				}
			}
		})
	}
}

func TestRun_NamedScope_ParentChain(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		t.Run(workers, func(t *testing.T) {
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
			testutil.WriteFile(t, root, "parent/child.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec:
  path: ./child
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [child.yaml, missing.yaml]\n")
			testutil.WriteFile(t, root, "cluster/parent.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: parent, namespace: flux-system}
spec:
  path: ./parent
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			_, stderr, code := runCLI(t, "build", "ks", "child", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "-n", "apps")
			if code != 1 || !strings.Contains(stderr, "parent") || !strings.Contains(stderr, "missing.yaml") {
				t.Fatalf("cross-namespace grandparent failure hidden: %d %s", code, stderr)
			}
		})
	}
}

func TestScopedFailures_WarningsPreserveFatalErrors(t *testing.T) {
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	selected := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
	o.Store().AddObject(selected)
	other := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "other"}
	res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{other: {Status: store.StatusFailed, Message: "unrelated failure"}}}
	scope := scopedFailures(o, res, &commonFlags{}, selected.Named())
	for _, fatal := range []error{context.Canceled, errors.New("unattributed panic"), errors.New("output write failed")} {
		t.Run(fatal.Error(), func(t *testing.T) {
			err := scopedRunError(scope, errors.Join(aggregateScopedFailures(res.Failed, nil), fatal))
			var out bytes.Buffer
			got := reportFailures(&out, scope, err)
			if !errors.Is(got, fatal) || !strings.Contains(out.String(), fatal.Error()) || !strings.Contains(out.String(), "unrelated failure") {
				t.Fatalf("warning-only footer hid fatal cause: %v %s", got, out.String())
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
	for _, workers := range []string{"2", "4"} {
		for _, shape := range []string{"direct", "nested", "resourceset"} {
			t.Run(workers+"/"+shape, func(t *testing.T) {
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
					child = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: nested-child, namespace: other}
spec:
  interval: 10m
  path: ./nested
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`
				case "resourceset":
					child = "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: child-set, namespace: other}\nspec:\n  resources:\n  - " + strings.ReplaceAll(strings.TrimSuffix(child, "\n"), "\n", "\n    ") + "\n"
				}
				testutil.WriteFile(t, root, "selected/kustomization.yaml", "resources: [cm.yaml, child.yaml]\n")
				testutil.WriteFile(t, root, "selected/child.yaml", child)
				for _, verb := range []string{"build", "test", "diff"} {
					args := []string{verb, "ks", "selected", "--path", root, "-n", "flux-system", "--cache-dir", t.TempDir(), "--concurrency", workers}
					if verb == "diff" {
						args = append(args, "--path-orig", orig)
					}
					_, stderr, code := runCLI(t, args...)
					if code != 1 || !strings.Contains(stderr, "missing-child-dependency") {
						t.Fatalf("%s hid owned descendant failure: code=%d %s", verb, code, stderr)
					}
				}
				_, stderr, code := runCLI(t, "build", "ks", "selected", "--path", root, "-n", "flux-system", "--cache-dir", t.TempDir(), "--concurrency", workers, "--stream")
				if code != 1 || !strings.Contains(stderr, "missing-child-dependency") {
					t.Fatalf("stream hid owned descendant failure: code=%d %s", code, stderr)
				}
			})
		}
	}
}

func TestRun_NamedScope_PrerequisiteSiblings(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		t.Run(workers, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			body, err := os.ReadFile(filepath.Join(root, "cluster/ks.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(root, "cluster/ks.yaml")); err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, "siblings/selected.yaml", string(body))
			testutil.WriteFile(t, root, "siblings/kustomization.yaml", "resources: [selected.yaml, sibling.yaml]\n")
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
			testutil.WriteFile(t, root, "cluster/parent.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: parent, namespace: flux-system}
spec:
  interval: 10m
  path: ./siblings
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			for _, verb := range []string{"build", "test"} {
				_, stderr, code := runCLI(t, verb, "ks", "selected", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers)
				if code != 0 || !strings.Contains(stderr, "sibling") && verb == "build" {
					t.Fatalf("%s included prerequisite parent's sibling: code=%d %s", verb, code, stderr)
				}
			}
		})
	}
}

func TestAggregateScopedFailures_MissingAndCycles(t *testing.T) {
	id := func(name string) manifest.NamedResource {
		return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: name}
	}
	selected, missing, a, b := id("selected"), id("missing"), id("a"), id("b")
	failed := map[manifest.NamedResource]store.StatusInfo{
		selected: {Status: store.StatusFailed, Message: "missing prerequisite"},
		a:        {Status: store.StatusFailed, Message: "cycle a"},
		b:        {Status: store.StatusFailed, Message: "cycle b"},
	}
	blocked := map[manifest.NamedResource][]manifest.NamedResource{selected: {missing}, a: {b}, b: {a}}
	got := aggregateScopedFailures(failed, blocked).Error()
	for _, want := range []string{missing.String(), "cycle a", "cycle b"} {
		if !strings.Contains(got, want) {
			t.Fatalf("fatal cause %q hidden: %s", want, got)
		}
	}
}

func TestReportFailures_FatalCycleWithWarnings(t *testing.T) {
	id := func(name string) manifest.NamedResource {
		return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: name}
	}
	a, b := id("a"), id("b")
	scope := failureScope{
		failed: map[manifest.NamedResource]store.StatusInfo{
			a: {Status: store.StatusFailed, Message: "cycle a"},
			b: {Status: store.StatusFailed, Message: "cycle b"},
		},
		blocked:  map[manifest.NamedResource][]manifest.NamedResource{a: {b}, b: {a}},
		warnings: []manifest.Warning{{Resource: id("other"), Message: "unrelated failure"}},
	}
	runErr := aggregateScopedFailures(scope.failed, scope.blocked)
	var out bytes.Buffer
	if got := reportFailures(&out, scope, runErr); got == nil {
		t.Fatal("fatal cycle became successful")
	}
	for _, want := range []string{"cycle a", "cycle b", "unrelated failure", "2 failed"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("fatal cycle hidden by warnings: %s", out.String())
		}
	}
}

func TestRequiredClosure_ResourceSetAuthoredReferences(t *testing.T) {
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	rs := &manifest.ResourceSet{Name: "selected", Namespace: "apps"}
	rs.DependsOn = []fluxopv1.Dependency{{Kind: manifest.KindHelmRelease, Name: "dep", Namespace: "other"}}
	rs.InputsFrom = []fluxopv1.InputProviderReference{{Name: "provider"}, {}}
	o.Store().AddObject(rs)
	dep := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "other", Name: "dep"}
	provider := manifest.NamedResource{Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "provider"}
	res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		dep:      {Status: store.StatusFailed, Message: "dependency failed"},
		provider: {Status: store.StatusFailed, Message: "provider failed"},
	}}
	scope := scopedFailures(o, res, &commonFlags{namespace: "apps"}, rs.Named())
	if len(scope.failed) != 2 || len(scope.warnings) != 0 {
		t.Fatalf("authored ResourceSet prerequisite omitted: %+v", scope)
	}
}

func TestScopedFailures_CanonicalReplacementWithStaleArtifact(t *testing.T) {
	root := writeNamedScopeFixture(t)
	o, res, err := runOrchestrator(t.Context(), commonFlags{path: root, cacheDir: t.TempDir(), concurrency: 2}, helmFlags{})
	if err != nil || res == nil {
		t.Fatalf("fresh fixture failed: %v", err)
	}
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "selected"}
	hr, _ := o.Store().Get[*manifest.HelmRelease](id)
	artifact := o.Store().GetArtifact(id)
	if artifact == nil {
		t.Fatal("fresh render has no artifact")
	}
	clone := hr.Clone()
	clone.ValuesFrom = []helmv2.ValuesReference{{Kind: "Secret", Name: "current", Optional: true}}
	o.Store().AddObject(clone)
	current := manifest.NamedResource{Kind: "Secret", Namespace: "apps", Name: "current"}
	old := manifest.NamedResource{Kind: "Secret", Namespace: "apps", Name: "old"}
	res = &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		id:      {Status: store.StatusFailed, Message: "selected failed after rendering"},
		current: {Status: store.StatusFailed, Message: "current input failure"},
		old:     {Status: store.StatusFailed, Message: "old input failure"},
	}}
	scope := scopedFailures(o, res, &commonFlags{}, id)
	if _, ok := scope.failed[id]; !ok {
		t.Fatal("stale artifact weakened selected failure")
	}
	if len(scope.failed) != 2 || scope.failed[current].Message != "current input failure" ||
		len(scope.warnings) != 1 || scope.warnings[0].Resource != old || o.Store().GetArtifact(id) != artifact {
		t.Fatalf("projection did not follow current canonical refs: %+v", scope)
	}
}
