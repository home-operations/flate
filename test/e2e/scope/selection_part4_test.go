package scope_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/go-git/go-git/v5/config"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_UnreadableManifestScope(t *testing.T) {
	for _, owned := range []bool{true, false} {
		name := "unowned nested census"
		if owned {
			name = "owned with independent sibling"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.GitInit(t, dir)
			testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux]\n")
			testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [drivers.yaml]\n")
			drivers := testutil.KustomizationYAML(
				"sibling",
				"flux-system",
				"  path: ./healthy\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			)
			if owned {
				drivers += "---\n" + testutil.KustomizationYAML(
					"owner",
					"flux-system",
					"  path: ./incidental/nested\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				)
			}
			testutil.WriteFile(t, dir, "flux/drivers.yaml", drivers)
			testutil.WriteFile(t, dir, "healthy/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				dir,
				"healthy/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: independent-output}\n",
			)
			builder := "resources: [unreadable.yaml]\n"
			if !owned {
				builder = "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n" + builder
			}
			testutil.WriteFile(t, dir, "incidental/nested/kustomization.yaml", builder)
			assert.NoError(t, os.Symlink("missing.yaml", filepath.Join(dir, "incidental/nested/unreadable.yaml")))
			args := []string{
				"build",
				"all",
				"--path",
				dir,
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			want := 0
			if owned {
				want = 1
			}
			assert.FatalIf(
				t,
				code != want || !strings.Contains(stdout, "independent-output"),
				"exit=%d want=%d\n%s\n%s",
				code,
				want,
				stdout,
				stderr,
			)
			switch {
			case owned:

				assert.FatalIf(
					t,
					!strings.Contains(stderr, "flux-system/owner") || !strings.Contains(stderr, "unreadable.yaml"),
					"file failure lost its owner: %s",
					stderr,
				)

			case strings.Contains(stderr, "unreadable.yaml"):

				t.Fatalf("incidental failure acquired authority: %s", stderr)
			}
			nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
			assert.Equal(t, nextCode, code)
			assert.Equal(t, nextOut, stdout)
			assert.Equal(t, nextErr, stderr)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_UnownedMalformedEntryAuthority(t *testing.T) {
	for _, unreadable := range []bool{false, true} {
		name := "malformed"
		if unreadable {
			name = "unreadable"
		}
		t.Run(name, func(t *testing.T) { e2EUnownedMalformedEntryAuthorityName(t, &unreadable) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_Diff_MalformedManifestTransitions(t *testing.T) {
	for _, tc := range []struct{ name, side string }{
		{name: "malformed to valid", side: "orig"},
		{name: "valid to malformed", side: "current"},
	} {
		t.Run(tc.name, func(t *testing.T) { e2EDiffMalformedManifestTransitionsName(t, &tc) })
	}
}

func TestE2E_EntryPackageJSON(t *testing.T) {
	for _, verb := range []string{"build", "diff"} {
		t.Run(verb, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml]\n")
			document := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {value: before}\n"
			testutil.WriteFile(t, root, "cm.yaml", document)
			testutil.GitCommitAll(
				t,
				repo,
				"init",
				&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
			)
			testutil.WriteFile(t, root, "cm.yaml", strings.ReplaceAll(document, "before", "after"))
			t.Chdir(root)
			args := []string{
				verb,
				"--path",
				".",
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
				"-o",
				"json",
			}
			if verb == "diff" {
				args = append(args, "--base", "HEAD")
			}
			stdout, stderr := harness.RequireCLIOK(t, args...)
			for _, forbidden := range []string{`\u0000`, "\x00", "entry"} {
				assert.ErrorIf(
					t,
					strings.Contains(stdout+stderr, forbidden),
					"output contains %q:\n%s\n%s",
					forbidden,
					stdout,
					stderr,
				)
			}
			want := map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "settings"},
				"data":     map[string]any{"value": "after"},
			}
			if verb == "build" {
				var documents []map[string]any
				assert.NoError(t, json.Unmarshal([]byte(stdout), &documents))
				assert.Diff(t, documents, []map[string]any{want})
				return
			}
			var result struct {
				Changes []struct {
					APIVersion string `json:"APIVersion"`
					Kind       string `json:"Kind"`
					Namespace  string `json:"Namespace"`
					Name       string `json:"Name"`
					Status     string `json:"Status"`
					Parent     struct {
						Group     string `json:"Group"`
						Kind      string `json:"Kind"`
						Namespace string `json:"Namespace"`
						Name      string `json:"Name"`
						Path      string `json:"Path"`
					} `json:"Parent"`
					Old map[string]any `json:"Old"`
					New map[string]any `json:"New"`
				} `json:"Changes"`
				Suppressed []any `json:"Suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(stdout), &result))
			assert.FatalIf(
				t,
				len(result.Changes) != 1 || len(result.Suppressed) != 0,
				"unexpected ConfigMap diff:\n%s",
				stdout,
			)
			change := result.Changes[0]
			assert.Equal(t, change.APIVersion, "v1")
			assert.Equal(t, change.Kind, "ConfigMap")
			assert.Equal(t, change.Namespace, "")
			assert.Equal(t, change.Name, "settings")
			assert.Equal(t, change.Status, "changed")
			assert.Diff(t, change.Parent, struct {
				Group     string `json:"Group"`
				Kind      string `json:"Kind"`
				Namespace string `json:"Namespace"`
				Name      string `json:"Name"`
				Path      string `json:"Path"`
			}{Path: "."})
			assert.Diff(t, change.New, want)
			want["data"] = map[string]any{"value": "before"}
			assert.Diff(t, change.Old, want)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_EntryPackageFailure(t *testing.T) {
	roots := [2]string{t.TempDir(), t.TempDir()}
	for _, root := range roots {
		testutil.GitInit(t, root)
		testutil.WriteFile(
			t,
			root,
			"entry/kustomization.yaml",
			"resources: [child.yaml]\nconfigMapGenerator: [{name: settings, envs: [.cluster-vars.env]}]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/child.yaml",
			testutil.KustomizationYAML("child", "apps", "  path: ./workload\n"),
		)
		testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: []\n")
	}
	for _, tc := range []struct{ verb, format string }{{verb: "test"}, {verb: "diff"}, {verb: "diff", format: "json"}} {
		t.Run(tc.verb+"/"+tc.format, func(t *testing.T) {
			args := []string{
				tc.verb,
				"all",
				"--path",
				filepath.Join(roots[1], "entry"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			}
			if tc.verb == "diff" {
				args = append(args, "--path-orig", filepath.Join(roots[0], "entry"))
			}
			if tc.format != "" {
				args = append(args, "-o", tc.format)
			}
			out, stderr, code := harness.RunCLIBuffers(args...)
			assert.FatalIf(t, code != 1 || !strings.Contains(out+stderr, ".cluster-vars.env") ||
				!strings.Contains(out+stderr, "entry") ||
				strings.Contains(stderr, "discovery package") ||
				strings.Contains(out+stderr, "Kustomization entry") ||
				strings.Contains(
					out+stderr,
					"Kustomization.kustomize.toolkit.fluxcd.io entry",
				), "package failure lost producer authority: exit=%d stdout=%s stderr=%s", code, out, stderr)
			assert.FatalIf(
				t,
				tc.verb == "test" && !strings.Contains(out, "1 failed"),
				"package failure lost its status table: %s",
				out,
			)
			if tc.format == "json" {
				var report struct {
					Suppressed []struct {
						Parent struct {
							Name string `json:"Name"`
							Path string `json:"Path"`
						} `json:"Parent"`
					} `json:"Suppressed"`
				}
				{
					err := json.Unmarshal(
						[]byte(out),
						&report,
					)
					assert.FatalIf(
						t,
						err != nil || len(report.Suppressed) != 1 || report.Suppressed[0].Parent.Name != "" ||
							report.Suppressed[0].Parent.Path != "entry",
						"JSON exposed the internal producer: %s; err=%v",
						out,
						err,
					)
				}
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_EntryApplicationRendered(t *testing.T) {
	for _, kind := range []string{"FluxInstance", "Kustomization"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			{
				_, err := repo.CreateRemote(
					&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.test/entry/repo"}},
				)
				assert.FatalIf(t, err != nil, "%v", err)
			}
			testutil.WriteFile(
				t,
				root,
				"entry/source.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: https://example.test/entry/repo}\n",
			)
			application := "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: https://example.test/entry/repo, ref: main, path: ./entry}\n"
			if kind == "Kustomization" {
				application = testutil.KustomizationYAML(
					"sync",
					"apps",
					"  path: ./entry\n  sourceRef: {kind: GitRepository, name: self}\n",
				)
			}
			testutil.WriteFile(
				t,
				root,
				"entry/generator.yaml",
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/bootstrap\n      sourceRef: {kind: GitRepository, name: self, namespace: apps}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"charts/bootstrap/Chart.yaml",
				"apiVersion: v2\nname: bootstrap\nversion: 1.0.0\n",
			)
			testutil.WriteFile(
				t,
				root,
				"charts/bootstrap/templates/application.yaml",
				"{{ print "+fmt.Sprintf("%q", application)+" }}",
			)
			testutil.WriteFile(
				t,
				root,
				"entry/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal-layer, namespace: apps}\n",
			)
			testutil.WriteFile(t, root, "entry/unapplied/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"entry/unapplied/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unused, namespace: apps}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"entry/workload.yaml",
				testutil.KustomizationYAML(
					"workload",
					"apps",
					"  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
				),
			)
			testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: [release.yaml]\n")
			testutil.WriteFile(t, root, "elsewhere/kustomization.yaml", "resources: []\n")
			workload := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: workload, namespace: apps}\nspec:\n  chart:\n    spec:\n      chart: charts/workload\n      sourceRef: {kind: GitRepository, name: self}\n  values: {value: before}\n"
			testutil.WriteFile(t, root, "workload/release.yaml", workload)
			testutil.WriteFile(
				t,
				root,
				"charts/workload/Chart.yaml",
				"apiVersion: v2\nname: workload\nversion: 1.0.0\n",
			)
			testutil.WriteFile(
				t,
				root,
				"charts/workload/templates/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\ndata: {value: {{ .Values.value }}}\n",
			)
			testutil.GitCommitAll(
				t,
				repo,
				"fixture",
				&gogit.CommitOptions{
					Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
				},
			)
			cache := t.TempDir()
			common := []string{
				"--path",
				filepath.Join(root, "entry"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				cache,
				"--log-level",
				"debug",
			}
			args := append([]string{"build", "all"}, common...)
			out, stderr := harness.RequireCLIOK(t, args...)
			assert.FatalIf(t, !strings.Contains(out, "name: literal-layer") || strings.Contains(out, "\x00") ||
				strings.Contains(
					stderr,
					"ConflictingProducers",
				), "late application lost the literal layer: %s\n%s", out, stderr)
			generatorEvent := `msg="helm: render" release=HelmRelease.helm.toolkit.fluxcd.io/apps/generator`
			assert.FatalIf(
				t,
				strings.Count(stderr, generatorEvent) != 1,
				"cold qualification did not execute Helm once: %s",
				stderr,
			)
			again, _ := harness.RequireCLIOK(t, args...)
			assert.Equal(t, again, out)
			if strings.Contains(stderr, "[RenderPersistenceDisabled]") {
				t.Skip("warm receipt witness requires a clean VCS-stamped build")
			}
			testutil.WriteFile(t, root, "workload/release.yaml", strings.ReplaceAll(workload, "before", "after"))
			diffArgs := append([]string{"diff", "all", "--base", "HEAD", "-o", "diff"}, common...)
			diff, debug := harness.RequireCLIOK(t, diffArgs...)
			assert.FatalIf(t, !strings.Contains(diff, "before") || !strings.Contains(diff, "after") ||
				strings.Contains(debug, generatorEvent) ||
				strings.Contains(
					debug,
					`msg="kustomize: build" path=entry/unapplied`,
				), "warm workload edit executed bootstrap work or lost the change: %s\n%s", diff, debug)
			repeated, repeatedDebug := harness.RequireCLIOK(t, diffArgs...)
			assert.Equal(t, repeated, diff)
			assert.FatalIf(t, strings.Contains(repeatedDebug, generatorEvent) ||
				strings.Contains(
					repeatedDebug,
					`msg="kustomize: build" path=entry/unapplied`,
				), "repeated diff executed bootstrap work: %s", repeatedDebug)
			testutil.WriteFile(
				t,
				root,
				"charts/bootstrap/templates/application.yaml",
				"{{ print "+fmt.Sprintf("%q", strings.ReplaceAll(application, "./entry", "./elsewhere"))+" }}",
			)
			invalidated, invalidatedDebug := harness.RequireCLIOK(t, args...)
			assert.FatalIf(t, strings.Count(invalidatedDebug, generatorEvent) != 1 ||
				strings.Contains(
					invalidated,
					"name: literal-layer",
				), "changed generator input retained stale entry qualification: %s\n%s", invalidated, invalidatedDebug)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DiscoveryPackageEntries(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "entry/Taskfile.yaml", "version: '3'\ntasks: {}\n")
	for _, name := range []string{"one", "two"} {
		testutil.WriteFile(
			t,
			root,
			"entry/"+name+"/kustomization.yaml",
			"resources: [role.yaml]\nnamePrefix: "+name+"-\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/"+name+"/role.yaml",
			"apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata: {name: retained}\nrules: []\n",
		)
	}
	for _, name := range []string{"one", "two"} {
		t.Run(name, func(t *testing.T) {
			args := []string{
				"build",
				"all",
				"--path",
				filepath.Join(root, "entry", name),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			}
			out, stderr := harness.RequireCLIOK(t, args...)
			assert.FatalIf(
				t,
				strings.Count(out, "kind: ClusterRole") != 1 || !strings.Contains(out, "name: "+name+"-retained") ||
					strings.Contains(stderr, "Taskfile"),
				"package entry lost its output: %s\n%s",
				out,
				stderr,
			)
			again, _ := harness.RequireCLIOK(t, args...)
			assert.Equal(t, again, out)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_Diff_IgnoredRequiredSubstitution(t *testing.T) {
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		testutil.WriteFile(
			t,
			root,
			"flux/consumer.yaml",
			"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec:\n interval: 10m\n path: ./app\n sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n postBuild:\n  substituteFrom: [{kind: ConfigMap, name: vars}]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/vars.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: flux-system}\ndata:\n FLAG: old\n",
		)
		testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumed, namespace: apps}\ndata:\n flag: ${FLAG}\n",
		)
		ignore := ""
		if side == 1 {
			ignore = "vars.yaml\n"
		}
		testutil.WriteFile(t, root, "flux/.krmignore", ignore)
		testutil.GitInit(t, root)
	}
	for side, root := range roots {
		stdout, stderr, code := harness.RunCLIBuffers(
			"build",
			"all",
			"--path",
			filepath.Join(root, "flux"),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
			"--no-progress",
		)
		assert.Equal(t, code, side)
		switch {
		case side == 0:

			assert.FatalIf(
				t,
				!strings.Contains(stdout, "flag: old"),
				"full oracle: exit=%d stdout=%s stderr=%s",
				code,
				stdout,
				stderr,
			)

		case !strings.Contains(stderr, "vars") || !strings.Contains(stderr, "consumer"):

			t.Fatalf("full oracle: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
		}
	}
	for _, named := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("named=%t/reverse=%t", named, reverse),
				func(t *testing.T) { e2EDiffIgnoredRequiredSubstitutionCase(t, &roots, &reverse, &named) },
			)
		}
	}
}
