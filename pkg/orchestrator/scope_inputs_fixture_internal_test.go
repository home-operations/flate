package orchestrator

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/baseline"
	"github.com/home-operations/flate/internal/diff"
	"github.com/home-operations/flate/internal/selection"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRenderTrees_ScopeInputCorpus(t *testing.T) {
	for _, name := range []string{
		"chart-input-template-committed",
		"chart-input-template-directories",
		"chart-input-template-added-directories",
		"chart-input-template-added-committed",
		"chart-input-template-deleted-directories",
		"chart-input-template-deleted-committed",
		"chart-input-default-values",
		"chart-input-packaged-subchart",
		"chart-input-root-chart",
		"chart-input-policy-admits-template",
		"chart-input-policy-removes-template",
		"chart-input-policy-created",
		"chart-input-policy-deleted",
		"chart-input-excluded-payload",
		"chart-input-unchanged",
		"chart-input-remote-chart-excludes-local-payload",
		"optional-changed-directory",
		"optional-changed-committed",
		"optional-sides-appear",
		"optional-sides-remove",
		"optional-sides-projected-appear",
		"optional-sides-projected-remove",
		"cross-source-optional-removal",
	} {
		t.Run(name, func(t *testing.T) {
			fixture := testfixture.Materialize(t, "scope-inputs-"+name, name)
			fixture.Restore(t)
			cache := cachetest.New(t, "", sourcecache.New)
			paths := [2]string{fixture.Resolve(t, "@@baseline@@/flux", ""), filepath.Join(fixture.Repo, "flux")}
			universal, want := scopeInputFullOracle(t, name, paths, cache)
			var resolved *baseline.Result
			var err error
			if strings.Contains(name, "committed") {
				resolved, err = baseline.AutoResolve(
					t.Context(),
					paths[1],
					fixture.Resolve(t, "@@commit:"+name+"-base@@", ""),
					cache,
				)
			} else {
				resolved, err = baseline.ResolvePaths(t.Context(), paths[1], paths[0])
			}
			assert.NoError(t, err)
			var previous []byte
			for run := range 2 {
				trees := scopeInputTrees(resolved, []string{"https://example.test/cluster.git"})
				selected := testScopeInputRun(t, name, trees, cache)
				if strings.HasPrefix(name, "optional-changed-") {
					testScopeInputManifests(t, selected, universal)
				}
				got, _, _, err := diff.Compare(scopeInputSnapshot(selected[0]), scopeInputSnapshot(selected[1]), nil,
					diff.Options{Format: diff.FormatDiff})
				assert.NoError(t, err)
				if !bytes.Equal(got, want) || run == 1 && !bytes.Equal(got, previous) {
					t.Fatalf(
						"scoped output differs from universal oracle: want %q, got %q, previous %q",
						want,
						got,
						previous,
					)
				}
				previous = slices.Clone(got)
			}
		})
	}
}

//nolint:nonamedreturns // gocritic requires names for these distinct oracle results.
func scopeInputFullOracle(
	t *testing.T,
	name string,
	paths [2]string,
	cache *sourcecache.Cache,
) (universal [2]*selection.Result, changes []byte) {
	t.Helper()
	roots := []manifest.NamedResource{
		testutil.KustomizationID("", ""), testutil.HelmReleaseID("", ""),
	}
	for side, path := range paths {
		runs, err := renderTreesForTest(t.Context(), t, []Tree{{
			Path: path, RepoRoot: filepath.Dir(path),
			SelfURLs: []string{"https://example.test/cluster.git"},
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		}}, Config{Cache: cache, Concurrency: 2})
		assert.NoError(t, err)
		projectedResults0, projectionErrors0 := Select(runs, err, roots, nil)
		selected, selectedErr, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
		assert.NoError(t, selectedErr)
		assert.NoError(t, remainder)
		assert.Equal(t, len(selected), 1)
		universal[side] = selected[0]
	}
	changes, _, _, err := diff.Compare(scopeInputSnapshot(universal[0]), scopeInputSnapshot(universal[1]), nil,
		diff.Options{Format: diff.FormatDiff})
	assert.NoError(t, err)
	if strings.HasPrefix(name, "chart-input-") {
		testScopeChartOracle(t, name, universal, changes)
	} else {
		testScopeOptionalOracle(t, name, universal)
	}
	return universal, changes
}

func scopeInputTrees(resolved *baseline.Result, selfURLs []string) []Tree {
	return []Tree{
		{
			Path: resolved.BaseScanPath, RepoRoot: resolved.BaseInput.LocalPath,
			Input: resolved.BaseInput, BaseInput: resolved.Source, ExternalChanges: resolved.Changes,
			SelfURLs: selfURLs, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
		},
		{
			Path: resolved.ScanPath, RepoRoot: resolved.Source.LocalPath,
			Input: resolved.Source, BaseInput: resolved.BaseInput, ExternalChanges: resolved.Changes,
			SelfURLs: selfURLs, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		},
	}
}

func testScopeInputRun(t *testing.T, name string, trees []Tree, cache *sourcecache.Cache) []*selection.Result {
	t.Helper()
	runs, err := renderTreesForTest(t.Context(), t, trees, Config{Cache: cache, Concurrency: 2, RecordWork: true})
	assert.NoError(t, err)
	projectedResults1, projectionErrors1 := Select(runs, err, []manifest.NamedResource{
		testutil.KustomizationID("", ""), testutil.HelmReleaseID("", ""),
	}, nil)
	selected, selectedErr, remainder := projectedResults1, projectionErrors1.Selected, projectionErrors1.Remaining
	assert.NoError(t, selectedErr)
	assert.NoError(t, remainder)
	assert.Equal(t, len(selected), 2)
	for side, rendered := range runs {
		testScopeInputWork(t, name, side, rendered)
	}
	return selected
}

func testScopeInputManifests(t *testing.T, selected []*selection.Result, universal [2]*selection.Result) {
	t.Helper()
	for side := range selected {
		assert.Diff(t, outputByParent(selected[side].Manifests), outputByParent(universal[side].Manifests))
	}
}

func testScopeChartOracle(t *testing.T, name string, universal [2]*selection.Result, body []byte) {
	t.Helper()
	precision := slices.Contains([]string{
		"chart-input-excluded-payload", "chart-input-unchanged", "chart-input-remote-chart-excludes-local-payload",
	}, name)
	if (len(body) == 0) != precision {
		t.Fatalf("full chart oracle changed=%t, want %t: %s", len(body) != 0, !precision, body)
	}
	if strings.Contains(name, "template-added") || strings.Contains(name, "template-deleted") {
		testScopeChartLifecycle(t, name, universal)
	}
}

func testScopeChartLifecycle(t *testing.T, name string, universal [2]*selection.Result) {
	t.Helper()
	document, _, _, err := diff.Compare(scopeInputSnapshot(universal[0]), scopeInputSnapshot(universal[1]), nil,
		diff.Options{Format: diff.FormatJSON})
	assert.NoError(t, err)
	var got struct {
		Changes []diff.Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(document, &got))
	if len(got.Changes) != 1 || got.Changes[0].Kind != manifest.KindConfigMap ||
		got.Changes[0].Name != "chart-input-extra" || got.Changes[0].Namespace != "apps" {
		t.Fatalf("template lifecycle changes another resource: %s", document)
	}
	want := diff.StatusAdded
	if strings.Contains(name, "template-deleted") {
		want = diff.StatusRemoved
	}
	assert.Equal(t, got.Changes[0].Status, want)
}

func testScopeOptionalOracle(t *testing.T, name string, universal [2]*selection.Result) {
	t.Helper()
	app := testutil.KustomizationID("flux-system", "app")
	for side, result := range universal {
		docs := outputByParent(result.Manifests)[app]
		if len(docs) != 1 {
			t.Fatalf("side %d full app output has %d documents", side, len(docs))
		}
		data, ok := docs[0]["data"].(map[string]any)
		if !ok || data["sentinel"] != "active" {
			t.Fatalf("side %d loses its substitution sentinel: %+v", side, docs)
		}
		present := side == 1
		if strings.HasSuffix(name, "remove") || name == "cross-source-optional-removal" {
			present = side == 0
		}
		value := "fallback"
		if present {
			value = "available"
		}
		assert.Equal(t, data["value"], any(value))
		if strings.HasPrefix(name, "optional-sides-projected-") || name == "cross-source-optional-removal" {
			testScopeOptionalProjection(t, result.Manifests, present)
		}
	}
}

func testScopeOptionalProjection(t *testing.T, batches []manifest.DocumentBatch, present bool) {
	t.Helper()
	input := false
	for _, batch := range batches {
		for _, document := range batch.Documents {
			input = input ||
				manifest.DocKind(document) == manifest.KindConfigMap &&
					manifest.DocMetadata(document).Name == "optional"
		}
	}
	assert.Equal(t, input, present)
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRenderTrees_InTreeCacheCorpus(t *testing.T) {
	for _, source := range []string{"implicit", "self"} {
		for _, state := range []string{"unchanged", "changed", "suspended-current", "suspended-orig"} {
			layouts := []string{"external"}
			if source == "self" && state == "changed" {
				layouts = append(layouts, "relative", "absolute")
			}
			for _, layout := range layouts {
				name := "cache-" + source + "-" + state + "-" + layout
				t.Run(name, func(t *testing.T) {
					testScopeInputCacheLifecycle(t, name, layout)
				})
			}
		}
	}
}

func testScopeInputCacheLifecycle(t *testing.T, name, layout string) {
	t.Helper()
	fixture := testfixture.Materialize(t, "scope-selection-"+name, name)
	fixture.Restore(t)
	cachePath := fixture.CachePath("package-proof")
	switch layout {
	case "relative":
		cachePath = filepath.Join(fixture.Repo, ".cache")
	case "absolute":
		cachePath = filepath.Join(fixture.Repo, ".absolute-cache")
	}
	_, err := os.Stat(cachePath)
	assert.Equal(t, os.IsNotExist(err), true)
	var prior []byte
	for run := range 2 {
		cache := cachetest.New(t, cachePath, sourcecache.New)
		resolved, err := baseline.AutoResolve(t.Context(), filepath.Join(fixture.Repo, "flux"),
			fixture.Resolve(t, "@@commit:"+name+"-base@@", ""), cache)
		assert.NoError(t, err)
		trees := scopeInputTrees(resolved, resolved.SelfURLs)
		rendered, renderErr := renderTreesForTest(t.Context(), t, trees, Config{Cache: cache, Concurrency: 2})
		assert.NoError(t, renderErr)
		projectedResults2, projectionErrors2 := Select(rendered, renderErr, []manifest.NamedResource{
			testutil.KustomizationID("", ""), testutil.HelmReleaseID("", ""),
		}, nil)
		selected, selectedErr, remainder := projectedResults2, projectionErrors2.Selected, projectionErrors2.Remaining
		assert.NoError(t, selectedErr)
		assert.NoError(t, remainder)
		body, _, _, err := diff.Compare(
			scopeInputSnapshot(selected[0]),
			scopeInputSnapshot(selected[1]),
			nil,
			diff.Options{Format: diff.FormatDiff},
		)
		assert.NoError(t, err)
		if run == 1 && !bytes.Equal(body, prior) {
			t.Fatalf("cache output changes after reuse: first %q, second %q", prior, body)
		}
		prior = slices.Clone(body)
		_, err = os.Stat(cachePath)
		assert.NoError(t, err)
	}
}

func scopeInputSnapshot(selected *selection.Result) diff.Snapshot {
	return diff.Snapshot{
		Subjects: selected.Subjects, Manifests: selected.Manifests,
		ProducerKeys: selected.ProducerKeys, Unavailable: selected.Unavailable,
	}
}

func testScopeInputWork(t *testing.T, name string, side int, rendered Rendered) {
	t.Helper()
	if strings.HasPrefix(name, "chart-input-") {
		if slices.Contains([]string{
			"chart-input-excluded-payload", "chart-input-unchanged", "chart-input-remote-chart-excludes-local-payload",
		}, name) {
			return
		}
		keep := selectionNamesForTest(rendered.selection, rendered.store, rendered.index)
		for _, id := range []manifest.NamedResource{
			testutil.HelmReleaseID("apps", "demo"),
			{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "flux-system"},
		} {
			if !slices.Contains(keep, id.String()) {
				t.Fatalf("side %d selected set omits %s: %v", side, id, keep)
			}
		}
		if slices.Contains(keep, testutil.KustomizationID("flux-system", "unrelated").String()) {
			t.Fatalf("side %d selects unrelated Kustomization: %v", side, keep)
		}
		return
	}
	keep := selectionNamesForTest(rendered.selection, rendered.store, rendered.index)
	if !slices.Contains(keep, testutil.KustomizationID("flux-system", "app").String()) {
		t.Fatalf("side %d omits optional-data consumer: %v", side, keep)
	}
	if strings.HasPrefix(name, "optional-changed-") {
		source := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Namespace: "flux-system",
			Name:      "cluster",
		}
		if !slices.Contains(keep, source.String()) {
			t.Fatalf("side %d omits the optional-data source: %v", side, keep)
		}
		app := testutil.KustomizationID("flux-system", "app")
		if got := outputByParent(rendered.Result.Manifests)[app]; len(got) != 1 ||
			got[0]["data"].(map[string]any)["value"] != []string{"fallback", "available"}[side] {
			t.Fatalf("side %d loses optional substitution output: %+v", side, got)
		}
	}
}
