package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/engine/baseline"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

const ociPlexReleaseFile = "kubernetes/apps/default/plex/app/release.yaml"

type ociChangedOnlyCase struct {
	name, file, field, before, after string
	unpatched                        [2]bool
	alsoExplicit                     bool
}

//nolint:paralleltest // Routes share a comparison fixture and the primed cache.
func TestRender_FluxInstanceChangedOnlySupport(t *testing.T) {
	t.Parallel()
	registryURL := fluxInstanceChartRegistry(t)
	const strategy = "strategy: RemediateOnFailure"
	const generator = "kubernetes/apps/flux-system/flux-instance/app/release.yaml"
	roots := []manifest.NamedResource{
		{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization},
		{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease},
	}
	for _, tc := range []ociChangedOnlyCase{
		{name: "app input", file: ociPlexReleaseFile, field: "input", before: "plain", after: "changed"},
		{
			name: "root patch", file: generator, field: "strategy", before: strategy,
			after: "strategy: changed", alsoExplicit: true,
		},
		{
			name: "withdraw patch", field: "strategy", before: strategy, after: "plain",
			unpatched: [2]bool{false, true}, alsoExplicit: true,
		},
		{
			name: "restore patch", field: "strategy", before: "plain", after: strategy,
			unpatched: [2]bool{true, false},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var trees [2]Tree
			var full [2][]map[string]any
			var fullCache [2]string
			plex := testutil.HelmReleaseID("default", "plex")
			sibling := testutil.HelmReleaseID("default", "sibling")
			for i := range trees {
				tree := tc.prepareTree(t, registryURL, i)
				if i != 0 {
					origin := trees[0].Input
					tree.Input.LocalPath, tree.Input.LocalRoot, tree.Input.URL = origin.LocalPath, origin.LocalRoot, origin.URL
					tree.RepoRoot = origin.LocalRoot
				}
				tree.Path = filepath.Join(tree.RepoRoot, "kubernetes", "apps")
				trees[i] = tree
				fullCache[i] = t.TempDir()
				runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{
					Concurrency: 2, CacheDir: fullCache[i],
				})
				assert.NoError(t, err)
				output := outputByParent(runs[0].Result.Manifests)
				full[i] = output[plex]
				assert.FatalIf(t, len(full[i]) != 1, "full build did not render Plex exactly once")
				assert.FatalIf(t, len(output[sibling]) != 1, "full build did not render the sibling exactly once")
				siblingData, _ := output[sibling][0]["data"].(map[string]any)
				assert.Equal(t, siblingData["strategy"], any("sibling"))
			}
			for i, want := range []string{tc.before, tc.after} {
				data, _ := full[i][0]["data"].(map[string]any)
				assert.Equal(t, data[tc.field], any(strings.TrimPrefix(want, "strategy: ")))
				trees[i].ID.Side, trees[i].BaseInput = manifest.TreeSide(i), trees[1-i].Input
			}
			type selectionRoute struct {
				name  string
				roots []manifest.NamedResource
			}
			routes := []selectionRoute{{name: "default roots"}}
			if tc.alsoExplicit {
				routes = append(routes, selectionRoute{name: "explicit roots", roots: roots})
			}
			for _, route := range routes {
				t.Run(route.name, func(t *testing.T) {
					request := selection.Request{Changed: true, Roots: route.roots}
					runs, err := renderTreesForTest(t.Context(), t, trees[:], Config{
						Concurrency: 2, CacheDir: t.TempDir(), RecordWork: true, Selection: request,
					})
					assert.NoError(t, err)
					assert.Equal(t, len(runs), len(trees))
					tc.assertSelection(t, runs, &full)
				})
			}
			if tc.field != "input" {
				return
			}
			request := selection.Request{Changed: true}
			t.Run("primed generator", func(t *testing.T) {
				runs, err := renderTreesForTest(t.Context(), t, trees[:], Config{
					Concurrency: 2, CacheDir: fullCache[0], RecordWork: true, Selection: request,
				})
				assert.NoError(t, err)
				assertOCIChangedOnlyReplay(t, runs, full, 1)
			})
			t.Run("primed Git comparison", func(t *testing.T) {
				tc.assertGitReplay(t, &trees[0], full, request)
			})
		})
	}
}

func (tc *ociChangedOnlyCase) assertGitReplay(
	t *testing.T,
	baseTree *Tree,
	full [2][]map[string]any,
	request selection.Request,
) {
	t.Helper()
	root := t.TempDir()
	testutil.CopyTree(t, baseTree.RepoRoot, root)
	repo, err := git.PlainOpen(root)
	assert.NoError(t, err)
	base := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{
		Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"},
	})
	body, err := os.ReadFile(filepath.Join(root, tc.file))
	assert.NoError(t, err)
	testutil.WriteFile(t, root, tc.file, strings.Replace(string(body), tc.before, tc.after, 1))

	cacheDir := t.TempDir()
	input, err := baseline.AutoResolve(t.Context(), baseline.Config{
		Path: filepath.Join(root, "kubernetes", "apps"), Base: base.String(),
		Cache: cachetest.New(t, cacheDir, cache.New),
	})
	assert.NoError(t, err)
	trees := historyWorkTrees(input, ".", baseTree.SelfURLs)
	cfg := Config{Concurrency: 2, CacheDir: cacheDir, RecordWork: true, Selection: request}
	_, err = renderTreesForTest(t.Context(), t, trees, cfg)
	assert.NoError(t, err)
	runs, err := renderTreesForTest(t.Context(), t, trees, cfg)
	assert.NoError(t, err)
	assertOCIChangedOnlyReplay(t, runs, full, 1)
}

func assertOCIChangedOnlyReplay(
	t *testing.T,
	runs []Rendered,
	full [2][]map[string]any,
	kustomizationPrepares uint64,
) {
	t.Helper()
	assert.Equal(t, len(runs), len(full))
	plex := testutil.HelmReleaseID("default", "plex")
	sibling := testutil.HelmReleaseID("default", "sibling")
	generator := testutil.HelmReleaseID("flux-system", "flux-instance")
	generatorKustomization := testutil.KustomizationID("flux-system", "flux-instance")
	persistent := cache.RenderPersistenceDisabledReason() == ""
	var work manifest.WorkCounts
	for i := range runs {
		result := runs[i].Result
		output := outputByParent(result.Manifests)
		assert.Diff(t, output[plex], full[i])
		assert.Equal(t, len(output[sibling]), 0)
		prepares := make(map[manifest.NamedResource]uint64)
		for i := range result.Work {
			record := &result.Work[i]
			if record.Key.Owner == generator && record.Key.Phase == manifest.WorkCache {
				work.Add(&record.Counts)
			}
			if record.Key.Phase == manifest.WorkPreparation && record.Key.Input == "" {
				prepares[record.Key.Owner] += record.Counts.OwnerPrepares
			}
		}
		if persistent {
			assert.Equal(t, plexApplicationAttempts(result.Work)[generator], uint64(0))
			for _, entry := range []struct {
				id   manifest.NamedResource
				want uint64
			}{{generator, 1}, {generatorKustomization, kustomizationPrepares}} {
				got := prepares[entry.id]
				t.Logf("side %v %v preparations: %d", runs[i].ID.Side, entry.id, got)
				assert.ErrorIf(t, got != entry.want, "side %v %v prepared %d times, want %d",
					runs[i].ID.Side, entry.id, got, entry.want)
			}
		}
	}
	if persistent {
		assert.ErrorIf(t, work.CacheService == 0, "accepted generator output did not replay")
	}
}

func (tc *ociChangedOnlyCase) prepareTree(t *testing.T, registryURL string, side int) Tree {
	t.Helper()
	mode := "covered"
	if tc.unpatched[side] {
		mode = "unpatched"
	}
	tree := ociPlexTree(t, registryURL, mode)
	root := tree.RepoRoot
	addOCIChangedOnlySibling(t, root)
	if tc.file == ociPlexReleaseFile {
		body, err := os.ReadFile(filepath.Join(root, ociPlexReleaseFile))
		assert.NoError(t, err)
		testutil.WriteFile(t, root, ociPlexReleaseFile, string(body)+`  postRenderers:
    - kustomize:
        patches:
          - target: {kind: ConfigMap, name: plex}
            patch: |
              - {op: add, path: /data/input, value: plain}
`)
	}
	if side != 0 && tc.file != "" {
		body, err := os.ReadFile(filepath.Join(root, tc.file))
		assert.NoError(t, err)
		assert.FatalIf(t, !strings.Contains(string(body), tc.before), "fixture edit matched no input")
		testutil.WriteFile(t, root, tc.file, strings.Replace(string(body), tc.before, tc.after, 1))
	}
	tree.Input = testDirectoryInput(t, root)
	return tree
}

func (tc *ociChangedOnlyCase) assertSelection(t *testing.T, runs []Rendered, full *[2][]map[string]any) {
	t.Helper()
	plex := testutil.HelmReleaseID("default", "plex")
	var attempts [2]map[manifest.NamedResource]uint64
	for i := range runs {
		attempts[i] = plexApplicationAttempts(runs[i].Result.Work)
		assert.Diff(t, outputByParent(runs[i].Result.Manifests)[plex], full[i])
	}
	t.Logf("engine attempts: %v", attempts)
	generator := testutil.KustomizationID("flux-system", "flux-instance")
	support := []manifest.NamedResource{
		generator,
		testutil.HelmReleaseID("flux-system", "flux-instance"),
		testutil.KustomizationID("flux-system", "flux-system"),
	}
	for _, id := range support {
		if tc.field == "strategy" {
			for side := range attempts {
				count := attempts[side][id]
				if id == generator {
					// Execution can reuse the construction build when its captured reads still match.
					assert.ErrorIf(
						t, count < 1 || count > 3,
						"side %d generator attempts = %d, want 1 to 3", side, count,
					)
				} else {
					assert.Equal(t, count, uint64(1))
				}
			}
		} else {
			// Either side can supply a shared immutable support render, exactly once per owner.
			assert.Equal(t, attempts[0][id]+attempts[1][id], uint64(1))
		}
		for side := range attempts {
			delete(attempts[side], id)
		}
	}
	want := map[manifest.NamedResource]uint64{
		testutil.KustomizationID("default", "plex"): 1,
		testutil.HelmReleaseID("default", "plex"):   1,
	}
	for _, got := range attempts {
		assert.Diff(t, got, want)
	}
}

func assertOCIPlexAttempts(t *testing.T, result *Result, covered, inspectParent bool) {
	t.Helper()
	want := map[manifest.NamedResource]uint64{
		testutil.KustomizationID("default", "plex"): 1,
		testutil.HelmReleaseID("default", "plex"):   1,
	}
	if covered || inspectParent {
		want[testutil.KustomizationID("flux-system", "flux-instance")] = 1
	}
	if covered {
		want[testutil.KustomizationID("flux-system", "flux-system")] = 1
		want[testutil.HelmReleaseID("flux-system", "flux-instance")] = 1
	}
	assert.Diff(t, plexApplicationAttempts(result.Work), want)
}

func fluxInstanceChartRegistry(t *testing.T) string {
	t.Helper()
	return testutil.NewChartRegistry(t, []map[string]string{{
		"instance/Chart.yaml":  "apiVersion: v2\nname: instance\nversion: 0.1.0\n",
		"instance/values.yaml": "instance: {sync: {kind: GitRepository}}\n",
		"instance/templates/instance.yaml": `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: flux-system}
spec: {{ toYaml .Values.instance | nindent 2 }}
`,
	}}, false).URL
}

func addOCIChangedOnlySibling(t *testing.T, root string) {
	t.Helper()
	const sibling = "kubernetes/apps/default/sibling/app/"
	testutil.WriteFile(t, root, sibling+"kustomization.yaml", "resources: [release.yaml]\n")
	testutil.WriteFile(t, root, sibling+"release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: sibling, namespace: default}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/sibling
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, root, "charts/sibling/Chart.yaml", "apiVersion: v2\nname: sibling\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "charts/sibling/templates/config.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: sibling, namespace: default}
data: {strategy: sibling}
`)
}
