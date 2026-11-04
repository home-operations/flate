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

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
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

			res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})

			assert.FatalIf(
				t,
				err != nil || len(res.Errors) != 0,
				"own cache retained lock: result=%+v err=%v",
				res,
				err,
			)

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

			res, err = sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})

			assert.FatalIf(
				t,
				err != nil || len(res.Errors) != 0,
				"input failure retained owner: result=%+v err=%v",
				res,
				err,
			)
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
	cfg := treeConfig{RecordWork: true, CacheDir: t.TempDir(), Concurrency: (*testCaseInput.workers), Tree: Tree{}}
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
		assert.FatalIf(t, rendered.Result == nil, "side %d has no result", side)

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

		assert.FatalIf(
			t,
			len(warnings) != 1 || warnings[0].Category != manifest.WarnPathConfig,
			"side %d warnings = %v",
			side,
			warnings,
		)

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
	cfg := Config{RecordWork: true, Concurrency: 2, HeadOnly: true}
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

	assert.FatalIf(t, len(runs) != 2, "missing results: %v", err)

	base, head := runs[0], runs[1]
	assertHeadOnlyWork(t, base)

	assert.FatalIf(t, head.Orchestrator == nil ||
		head.Result == nil, "head-only completion = base=%+v head=%+v err=%v", base, head, err)

	if *testCaseInput {
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "head reconciliation failed") ||
				len(head.Result.Failed) != 1,
			"head failure lost partial result: %+v %v",
			head.Result,
			err,
		)
	} else {
		assert.NoError(t, err)
		assert.Diff(t, configMapNames(head.Result), map[string]bool{"surviving": true})
		cfg.HeadOnly = false
		_, err = RenderTrees(t.Context(), []Tree{baseTree, headTree}, cfg)

		assert.FatalIf(
			t,
			err == nil || !strings.Contains(err.Error(), "baseline reconciliation ran"),
			"baseline failure control did not reconcile: %v",
			err,
		)
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
		RecordWork:  true,
		CacheDir:    t.TempDir(),
		Concurrency: 2,
		HeadOnly:    (*testCaseInput.mode) == "head_only",
		Tree:        Tree{},
	}
	if (*testCaseInput.mode) != "directories" {
		input, _, err := baseline.AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
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
			assertHeadOnlyWork(t, rendered)
			continue
		}

		assert.FatalIf(t, rendered.Result == nil, "side %d has no result", side)

		assert.NoError(t, rendered.Err)
		name, other := "a", "b"
		if side == 1 {
			name, other = other, name
		}
		assert.Diff(t, configMapNames(rendered.Result), map[string]bool{"cfg-" + name: true})
		foreign := testutil.KustomizationID("flux-system", "app-"+other)

		assert.FatalIf(
			t,
			rendered.Store().GetObject(foreign) != nil,
			"side %d admitted the other scan's Kustomization as an orphan",
			side,
		)
	}
}
