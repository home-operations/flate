package scope_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

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
			child := testutil.KustomizationYAML(
				"workload",
				"apps",
				"  interval: 1h\n  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
			)
			testutil.WriteFile(t, root, "entry/workload.yaml", child)
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
				"entry/workload.yaml",
				strings.ReplaceAll(child, "interval: 1h", "interval: 2h"),
			)
			assertEntryApplicationIntervalDiff(t, common, generatorEvent)

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

func assertEntryApplicationIntervalDiff(t *testing.T, common []string, generatorEvent string) {
	t.Helper()
	for _, mode := range []string{"warm", "cold"} {
		t.Run(mode+" interval", func(t *testing.T) {
			intervalArgs := append([]string{"diff", "all", "--base", "HEAD", "-o", "json"}, common...)
			if mode == "cold" {
				intervalArgs = append(intervalArgs, "--cache-dir", t.TempDir())
			}
			intervalDiff, debug := harness.RequireCLIOK(t, intervalArgs...)
			wantRenders := 0
			if mode == "cold" {
				wantRenders = 1
			}
			if strings.Count(debug, generatorEvent) != wantRenders {
				t.Fatalf("%s applier renders != %d: %s", mode, wantRenders, debug)
			}
			assertEntryApplicationIntervalChange(t, intervalDiff)
			again, _ := harness.RequireCLIOK(t, intervalArgs...)
			assert.Equal(t, again, intervalDiff)
		})
	}
}

func assertEntryApplicationIntervalChange(t *testing.T, intervalDiff string) {
	t.Helper()

	var report struct {
		Changes []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
			Old  struct {
				Spec map[string]any `json:"spec"`
			} `json:"old"`
			New struct {
				Spec map[string]any `json:"spec"`
			} `json:"new"`
		} `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal([]byte(intervalDiff), &report))
	found := false
	for _, item := range report.Changes {
		if item.Kind == "Kustomization" && item.Name == "workload" {
			assert.Equal(t, item.Old.Spec["interval"], any("1h"))
			assert.Equal(t, item.New.Spec["interval"], any("2h"))
			found = true
		}
	}
	if !found {
		t.Fatalf("root interval edit missing: %s", intervalDiff)
	}
}
