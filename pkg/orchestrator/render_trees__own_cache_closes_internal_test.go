package orchestrator

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_OwnCacheCloses(t *testing.T) {
	t.Parallel()

	for _, headOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("head_only_%t", headOnly), func(t *testing.T) {
			t.Parallel()

			baseDir, headDir, root := t.TempDir(), t.TempDir(), t.TempDir()
			writeCluster(t, baseDir, "v1", "same")
			writeCluster(t, headDir, "v2", "same")
			{
				{
					_, err := RenderTrees(
						t.Context(),
						[]Tree{
							{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, RepoRoot: baseDir},
							{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: headDir},
						},
						Config{CacheDir: root, Concurrency: 2, HeadOnly: headOnly},
					)
					assert.FatalIf(t, err != nil, "%v", err)
				}
			}
			res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
			{
				assert.FatalIf(
					t,
					err != nil || len(res.Errors) != 0,
					"own cache retained lock: result=%+v err=%v",
					res,
					err,
				)
			}
			{
				{
					_, localErr := RenderTrees(
						t.Context(),
						[]Tree{
							{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
							{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
						},
						Config{CacheDir: root, Concurrency: 2, HeadOnly: headOnly},
					)
					assert.FatalIf(t, !errors.Is(
						localErr,
						manifest.ErrInput,
					), "missing input error = %v", localErr)
				}
			}
			res, err = sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
			{
				assert.FatalIf(
					t,
					err != nil || len(res.Errors) != 0,
					"input failure retained owner: result=%+v err=%v",
					res,
					err,
				)
			}
		})
	}
}

func TestRenderTrees_SameDirectoryKeepsFullRender(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		for _, tc := range []struct {
			name       string
			base, head bool
		}{
			{name: "directories"},
			{name: "supplied", base: true, head: true},
			{name: "supplied_base", base: true},
			{name: "supplied_head", head: true},
		} {
			for _, external := range []string{"automatic", "nonempty", "empty"} {
				t.Run(fmt.Sprintf("%s/%s/workers_%d", tc.name, external, workers), func(t *testing.T) {
					t.Parallel()
					testRenderTreesSameDirectoryKeepsFullRenderCase(
						t,
						testRenderTreesSameDirectoryKeepsFullRenderCaseInputs{
							workers:  &workers,
							tc:       &tc,
							external: &external,
						},
					)
				},
				)
			}
		}
	}
	t.Run("scan_paths", func(t *testing.T) { t.Parallel(); testRenderTreesSameDirectoryKeepsFullRenderScanPaths(t) })
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_SuppliedViews(t *testing.T) {
	for _, headOnly := range []bool{false, true} {
		for _, scenario := range []string{"differing_options", "filtered_addition", "filtered_deletion", "explicit_changes", "explicit_empty"} {
			t.Run(fmt.Sprintf("%s/head_only_%t", scenario, headOnly), func(t *testing.T) {
				testRenderTreesSuppliedViewsCase(
					t,
					testRenderTreesSuppliedViewsCaseInputs{headOnly: &headOnly, scenario: &scenario},
				)
			},
			)
		}
	}
	t.Run("same_input_override", func(t *testing.T) { ; testRenderTreesSuppliedViewsSameInputOverride(t) })
}

func TestRenderTrees_HeadOnly(t *testing.T) {
	t.Parallel()

	for _, headFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("head_failure_%t", headFailure), func(t *testing.T) {
			t.Parallel()
			testRenderTreesHeadOnlyCase(t, &headFailure)
		},
		)
	}
}

type joinedTreeFS struct {
	fs.FS

	payloadPath string
	armed       atomic.Bool
	once        sync.Once
	acquire     func() error
	err         error
}

func (f *joinedTreeFS) Open(name string) (fs.File, error) {
	if f.payloadPath != "" && (name != f.payloadPath || !f.armed.Load()) {
		return f.FS.Open(name)
	}
	f.once.Do(func() { f.err = f.acquire() })
	return nil, f.err
}

type testRenderTreesSameDirectoryKeepsFullRenderCaseInputs struct {
	workers *int
	tc      *struct {
		name string

		base bool
		head bool
	}
	external *string
}

func testRenderTreesSameDirectoryKeepsFullRenderCase(
	t *testing.T,
	testCaseInput testRenderTreesSameDirectoryKeepsFullRenderCaseInputs,
) {
	t.Helper()

	root := t.TempDir()
	writeCluster(t, root, "same", "same")
	alias := filepath.Join(t.TempDir(), "linked")
	assert.NoError(t, os.Symlink(root, alias))
	cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: (*testCaseInput.workers), Tree: Tree{}}
	if testCaseInput.tc.base || testCaseInput.tc.head {
		resolved, err := baseline.ResolvePaths(t.Context(), alias, root)
		assert.NoError(t, err)
		if testCaseInput.tc.base {
			cfg.BaseInput = resolved.BaseInput
		}
		if testCaseInput.tc.head {
			cfg.Input = resolved.Source
		}
	}
	switch *testCaseInput.external {
	case "nonempty":
		cfg.ExternalChanges = change.NewSet([]string{"apps/app-a/cm.yaml"})
	case "empty":
		cfg.ExternalChanges = change.NewSet(nil)
	}
	renderedTrees7, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				RepoRoot:        root,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				BaseInput:       cfg.Input,
				ExternalChanges: cfg.ExternalChanges,
				Input:           cfg.BaseInput,
			},
			{
				RepoRoot:        alias,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Input:           cfg.Input,
				BaseInput:       cfg.BaseInput,
				ExternalChanges: cfg.ExternalChanges,
			},
		},
		cfg.Config,
	)
	base, head := renderedTrees7[0], renderedTrees7[1]
	assert.NoError(t, err)
	full := testCaseInput.tc.base == testCaseInput.tc.head && (*testCaseInput.external) == "automatic"
	for side, rendered := range []Rendered{base, head} {
		{
			assert.FatalIf(t, rendered.Result == nil, "side %d has no result", side)
		}
		assert.NoError(t, rendered.Err)
		assert.Equal(t, len(rendered.Result.Failed), 0)
		names := configMapNames(rendered.Result)
		assert.Equal(t, names["cfg-a"], full || (*testCaseInput.external) == "nonempty")
		assert.Equal(t, names["cfg-b"], full)
		warnings := rendered.Result.Warnings
		if (*testCaseInput.external) == "nonempty" {
			assert.Equal(t, len(warnings), 0)
			continue
		}
		{
			assert.FatalIf(
				t,
				len(warnings) != 1 || warnings[0].Category != manifest.WarnPathConfig,
				"side %d warnings = %v",
				side,
				warnings,
			)
		}
		message := "no changes detected"
		if full {
			message = "resolve to the same root"
		}
		assert.Equal(t, strings.Contains(warnings[0].Message, message), true)
	}
}

func testRenderTreesSameDirectoryKeepsFullRenderScanPaths(t *testing.T) {
	t.Helper()

	for _, mode := range []string{"directories", "supplied", "head_only"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRenderTreesSameDirectoryKeepsFullRenderScanPathsCase(
				t,
				testRenderTreesSameDirectoryKeepsFullRenderScanPathsCaseInputs{mode: &mode},
			)
		},
		)
	}
}

type testRenderTreesSuppliedViewsCaseInputs struct {
	headOnly *bool
	scenario *string
}

func testRenderTreesSuppliedViewsCase(t *testing.T, testCaseInput testRenderTreesSuppliedViewsCaseInputs) {
	t.Helper()

	root := t.TempDir()
	writeCluster(t, root, "same", "same")
	for _, name := range []string{"ks-a.yaml", "ks-b.yaml"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "flux/"+name, string(data))
	}
	input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
	assert.NoError(t, err)
	cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, HeadOnly: (*testCaseInput.headOnly), Tree: Tree{}}
	testRenderTreesSuppliedViewsCaseSegment(t, &testRenderTreesSuppliedViewsCaseSegmentInputs{testCaseInput: &testCaseInput, root: &root, input: &input, err: &err, cfg: &cfg})
	testRenderTreesSuppliedViewsCaseTail(
		t,
		&testRenderTreesSuppliedViewsCaseTailInputs{testCaseInput: &testCaseInput, cfg: &cfg, root: &root},
	)
}

func testRenderTreesSuppliedViewsCaseTail(t *testing.T, tailCaseInput *testRenderTreesSuppliedViewsCaseTailInputs) {
	t.Helper()
	switch *tailCaseInput.testCaseInput.scenario {
	case "explicit_changes":
		tailCaseInput.cfg.
			ExternalChanges = change.NewSet([]string{"apps/app-a/cm.yaml"})
	case "explicit_empty":
		tailCaseInput.cfg.
			ExternalChanges = change.NewSet(nil)
	}
	renderedTrees9, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				RepoRoot:        (*tailCaseInput.root),
				Path:            filepath.Join((*tailCaseInput.root), "flux"),
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Input:           tailCaseInput.cfg.BaseInput,
				BaseInput:       tailCaseInput.cfg.Input,
				ExternalChanges: tailCaseInput.cfg.ExternalChanges,
			},
			{
				RepoRoot:        (*tailCaseInput.root),
				Path:            filepath.Join((*tailCaseInput.root), "flux"),
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Input:           tailCaseInput.cfg.Input,
				BaseInput:       tailCaseInput.cfg.BaseInput,
				ExternalChanges: tailCaseInput.cfg.ExternalChanges,
			},
		}, tailCaseInput.cfg.
			Config,
	)
	base, head := renderedTrees9[0], renderedTrees9[1]
	assert.NoError(t, err)
	testRenderTreesSuppliedViewsCaseTailSegment(t, &tailCaseInput, &base, &head)
}

func testRenderTreesSuppliedViewsSameInputOverride(t *testing.T) {
	t.Helper()

	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty_%t", empty), func(t *testing.T) {
			t.Parallel()
			testRenderTreesSuppliedViewsSameInputOverrideCase(
				t,
				testRenderTreesSuppliedViewsSameInputOverrideCaseInputs{empty: &empty},
			)
		},
		)
	}
}

func testRenderTreesHeadOnlyCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	baseInput := &store.SourceArtifact{LocalPath: filepath.Join(t.TempDir(), "base")}
	headInput := &store.SourceArtifact{LocalPath: filepath.Join(t.TempDir(), "head")}
	before := fstest.MapFS{
		"flux/hr.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		},
		"charts/demo/Chart.yaml": {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
		"charts/demo/templates/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: surviving}\n"),
		},
		"charts/demo/templates/deleted.yaml": {Data: []byte("{{ fail \"baseline reconciliation ran\" }}\n")},
	}
	after := make(fstest.MapFS, len(before)-1)
	for name, file := range before {
		if name != "charts/demo/templates/deleted.yaml" {
			after[name] = file
		}
	}
	if *testCaseInput {
		after["charts/demo/templates/cm.yaml"] = &fstest.MapFile{
			Data: []byte("{{ fail \"head reconciliation failed\" }}\n"),
		}
	}
	baseInput.FS, headInput.FS = before, after
	cfg := Config{Concurrency: 2, HeadOnly: true}
	baseTree := Tree{
		ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
		Input:    baseInput,
		RepoRoot: baseInput.LocalPath,
		Path:     filepath.Join(baseInput.LocalPath, "flux"),
	}
	headTree := Tree{
		ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		Input:    headInput,
		RepoRoot: headInput.LocalPath,
		Path:     filepath.Join(headInput.LocalPath, "flux"),
	}
	runs, err := RenderTrees(t.Context(), []Tree{baseTree, headTree}, cfg)
	{
		assert.FatalIf(t, len(runs) != 2, "missing results: %v", err)
	}
	base, head := runs[0], runs[1]
	{
		assert.FatalIf(t, base != (Rendered{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}) ||
			head.Orchestrator == nil ||
			head.Result == nil, "head-only completion = base=%+v head=%+v err=%v", base, head, err)
	}
	if *testCaseInput {
		{
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "head reconciliation failed") ||
					len(head.Result.Failed) != 1,
				"head failure lost partial result: %+v %v",
				head.Result,
				err,
			)
		}
	} else {
		assert.NoError(t, err)
		assert.Diff(t, configMapNames(head.Result), map[string]bool{"surviving": true})
		cfg.HeadOnly = false
		_, err = RenderTrees(t.Context(), []Tree{baseTree, headTree}, cfg)
		{
			assert.FatalIf(
				t,
				err == nil || !strings.Contains(err.Error(), "baseline reconciliation ran"),
				"baseline failure control did not reconcile: %v",
				err,
			)
		}
	}
}

type testRenderTreesSameDirectoryKeepsFullRenderScanPathsCaseInputs struct {
	mode *string
}

func testRenderTreesSameDirectoryKeepsFullRenderScanPathsCase(
	t *testing.T,
	testCaseInput testRenderTreesSameDirectoryKeepsFullRenderScanPathsCaseInputs,
) {
	t.Helper()

	root := t.TempDir()
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(
			t,
			root,
			"scans/"+name+"/ks.yaml",
			testutil.KustomizationYAML(
				"app-"+name,
				"flux-system",
				"  path: ./apps/"+name+"\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			),
		)
		testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/"+name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cfg-"+name+"}\n",
		)
	}
	cfg := treeConfig{
		CacheDir:    t.TempDir(),
		Concurrency: 2,
		HeadOnly:    (*testCaseInput.mode) == "head_only",
		Tree:        Tree{},
	}
	if (*testCaseInput.mode) != "directories" {
		input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
		assert.NoError(t, err)
		cfg.BaseInput, cfg.Input = input, input
	}
	renderedTrees8, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				RepoRoot:        root,
				Path:            filepath.Join(root, "scans", "a"),
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Input:           cfg.BaseInput,
				BaseInput:       cfg.Input,
				ExternalChanges: cfg.ExternalChanges,
			},
			{
				RepoRoot:        root,
				Path:            filepath.Join(root, "scans", "b"),
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Input:           cfg.Input,
				BaseInput:       cfg.BaseInput,
				ExternalChanges: cfg.ExternalChanges,
			},
		},
		cfg.Config,
	)
	base, head := renderedTrees8[0], renderedTrees8[1]
	assert.NoError(t, err)
	for side, rendered := range []Rendered{base, head} {
		if cfg.HeadOnly && side == 0 {
			assert.Equal(
				t,
				rendered == (Rendered{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}),
				true,
			)
			continue
		}
		{
			assert.FatalIf(t, rendered.Result == nil, "side %d has no result", side)
		}
		assert.NoError(t, rendered.Err)
		name, other := "a", "b"
		if side == 1 {
			name, other = other, name
		}
		assert.Diff(t, configMapNames(rendered.Result), map[string]bool{"cfg-" + name: true})
		foreign := testutil.KustomizationID("flux-system", "app-"+other)
		{
			assert.FatalIf(
				t,
				rendered.Store().GetObject(foreign) != nil,
				"side %d admitted the other scan's Kustomization as an orphan",
				side,
			)
		}
	}
}

type testRenderTreesSuppliedViewsSameInputOverrideCaseInputs struct {
	empty *bool
}

func testRenderTreesSuppliedViewsSameInputOverrideCase(
	t *testing.T,
	testCaseInput testRenderTreesSuppliedViewsSameInputOverrideCaseInputs,
) {
	t.Helper()

	root := t.TempDir()
	writeCluster(t, root, "same", "same")
	input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
	assert.NoError(t, err)
	paths := []string{"apps/app-a/cm.yaml"}
	if *testCaseInput.empty {
		paths = nil
	}
	cfg := treeConfig{
		CacheDir:        t.TempDir(),
		Concurrency:     2,
		Input:           input,
		BaseInput:       input,
		ExternalChanges: change.NewSet(paths),
	}
	var previous [2]map[manifest.NamedResource][]map[string]any
	for run := range 2 {
		renderedTrees1, err := RenderTrees(
			t.Context(),
			[]Tree{
				{
					RepoRoot:        root,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					ExternalChanges: cfg.ExternalChanges,
					Input:           cfg.BaseInput,
					BaseInput:       cfg.Input,
				},
				{
					RepoRoot:        root,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Input:           cfg.Input,
					BaseInput:       cfg.BaseInput,
					ExternalChanges: cfg.ExternalChanges,
				},
			},
			cfg.Config,
		)
		base, head := renderedTrees1[0], renderedTrees1[1]
		assert.NoError(t, err)
		for side, rendered := range []Rendered{base, head} {
			if run == 0 && side == 0 {
				directCfg := cfg
				directCfg.Path = root
				direct, err := newTestOrchestrator(t, directCfg.Tree, directCfg.Config)
				assert.NoError(t, err)
				result, err := direct.render(t.Context())
				assert.NoError(t, err)
				assert.Diff(t, outputByParent(result.Manifests), outputByParent(rendered.Result.Manifests))
			}
			want := map[string]bool{}
			if !(*testCaseInput.empty) {
				want["cfg-a"] = true
			}
			assert.Diff(t, configMapNames(rendered.Result), want)
			if run == 1 {
				assert.Diff(t, outputByParent(rendered.Result.Manifests), previous[side])
			}
			previous[side] = outputByParent(rendered.Result.Manifests)
		}
	}
}

type testRenderTreesSuppliedViewsCaseTailInputs struct {
	testCaseInput *testRenderTreesSuppliedViewsCaseInputs
	cfg           *treeConfig
	root          *string
}

func testRenderTreesSuppliedViewsCaseTailSegment(t *testing.T, tailCaseInput **testRenderTreesSuppliedViewsCaseTailInputs, base *Rendered, head *Rendered) {
	t.Helper()
	for side, rendered := range []Rendered{(*base), (*head)} {
		if (*(*tailCaseInput).testCaseInput.headOnly) && side == 0 {
			assert.Equal(
				t,
				rendered == (Rendered{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}),
				true,
			)
			continue
		}
		{
			assert.FatalIf(t, rendered.Result == nil, "side %d has no result", side)
		}
		assert.NoError(t, rendered.Err)
		assert.Equal(t, len(rendered.Result.Failed), 0)
		wantA := (*(*tailCaseInput).testCaseInput.scenario) == "explicit_changes" ||
			(*(*tailCaseInput).testCaseInput.scenario) == "filtered_addition" && side == 1 ||
			(*(*tailCaseInput).testCaseInput.scenario) ==
				"filtered_deletion" && side == 0
		names := configMapNames(rendered.Result)
		assert.Equal(t, names["cfg-a"], wantA)
		assert.Equal(t, names["cfg-b"], false)
		filtered := (*(*tailCaseInput).testCaseInput.scenario) == "filtered_addition" && side == 0 ||
			(*(*tailCaseInput).testCaseInput.scenario) ==
				"filtered_deletion" && side == 1
		if filtered {
			id := testutil.KustomizationID("flux-system", "app-a")
			assert.Equal(t, rendered.Store().GetObject(id) == nil, true)
			for _, obj := range rendered.Store().ListObjects("", manifest.KindConfigMap) {
				{
					assert.FatalIf(
						t,
						obj.Named().Name == "cfg-a",
						"%v",
						"filtered side admitted cfg-a as an orphan seed",
					)
				}
			}
		}
		warnings := rendered.Result.Warnings
		if (*(*tailCaseInput).testCaseInput.scenario) == "differing_options" ||
			(*(*tailCaseInput).testCaseInput.scenario) == "explicit_empty" {
			{
				assert.FatalIf(t, len(warnings) != 1 || warnings[0].Category != manifest.WarnPathConfig ||
					!strings.Contains(
						warnings[0].Message,
						"no changes detected",
					), "side %d warnings = %v", side, warnings)
			}
		} else {
			assert.Equal(t, len(warnings), 0)
		}
	}
}

type testRenderTreesSuppliedViewsCaseSegmentInputs struct {
	testCaseInput *testRenderTreesSuppliedViewsCaseInputs
	root          *string
	input         **store.SourceArtifact
	err           *error
	cfg           *treeConfig
}

func testRenderTreesSuppliedViewsCaseSegment(t *testing.T, statementInput *testRenderTreesSuppliedViewsCaseSegmentInputs) {
	t.Helper()
	for side := range 2 {
		current := (*statementInput.input).FS
		filtered := (*(*statementInput.testCaseInput).scenario) == "filtered_addition" && side == 0 || (*(*statementInput.testCaseInput).scenario) ==
			"filtered_deletion" && side == 1
		if filtered {
			current, _, (*statementInput.err) = gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{
				Mode: gittree.Source, Current: current, Ignore: "flux/ks-a.yaml\napps/app-a/\n",
				Sparse: []string{"flux/ks-b.yaml", "apps/app-b"},
			})
			assert.NoError(t, (*statementInput.err))
		}
		opts := gittree.ViewOptions{Mode: gittree.Direct, Current: current}
		if (*(*statementInput.testCaseInput).scenario) == "differing_options" && side == 1 {
			opts.Ignore, opts.Sparse = "flux/ks-a.yaml\napps/app-a/\n", []string{
				"flux/ks-b.yaml",
				"apps/app-b",
			}
		}
		view, key, innerLocalErr := gittree.New(t.Context(), nil, plumbing.ZeroHash, opts)
		assert.NoError(t, innerLocalErr)
		assert.Equal(t, key, "")
		if view.Location() == nil || view.Location().ContentRoot == "" ||
			view.Location().ContentRoot != (*statementInput.input).LocalPath {
			t.Fatalf("side %d location = %+v, want content root %s", side, view.Location(), (*statementInput.input).LocalPath)
		}
		for _, name := range []string{"flux/ks-a.yaml", "apps/app-a/kustomization.yaml", "apps/app-a/cm.yaml", "flux/ks-b.yaml", "apps/app-b/cm.yaml"} {
			body, readErr := fs.ReadFile(view, name)
			if filtered && (name == "flux/ks-a.yaml" || strings.HasPrefix(name, "apps/app-a/")) {
				assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
			} else {
				assert.NoError(t, readErr)
				want, err := os.ReadFile(filepath.Join((*statementInput.root), name))
				assert.NoError(t, err)
				assert.Equal(t, string(body), string(want))
			}
		}
		artifact := *(*statementInput.input)
		artifact.FS, artifact.Key = view, key
		if side == 0 {
			(*statementInput.cfg).BaseInput = &artifact
		} else {
			(*statementInput.cfg).Input = &artifact
		}
	}
}
