package orchestrator

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

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
	input, _, err := baseline.AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
	assert.NoError(t, err)
	cfg := treeConfig{
		RecordWork:  true,
		CacheDir:    t.TempDir(),
		Concurrency: 2,
		HeadOnly:    (*testCaseInput.headOnly),
		Tree:        Tree{},
	}
	testRenderTreesSuppliedViewsCaseSegment(
		t,
		&testRenderTreesSuppliedViewsCaseSegmentInputs{
			testCaseInput: &testCaseInput,
			root:          &root,
			input:         &input,
			err:           &err,
			cfg:           &cfg,
		},
	)
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
	renderedTrees9, err := renderTreesForTest(t.Context(), t,
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
	testRenderTreesSuppliedViewsCaseTailSegment(t, tailCaseInput, &base, &head)
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
	input, _, err := baseline.AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
	assert.NoError(t, err)
	paths := []string{"apps/app-a/cm.yaml"}
	if *testCaseInput.empty {
		paths = nil
	}
	cfg := treeConfig{
		RecordWork:      true,
		CacheDir:        t.TempDir(),
		Concurrency:     2,
		Input:           input,
		BaseInput:       input,
		ExternalChanges: change.NewSet(paths),
	}
	var previous [2]map[manifest.NamedResource][]map[string]any
	for run := range 2 {
		renderedTrees1, err := renderTreesForTest(t.Context(), t,
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
				result, err := direct.render(t.Context(), t)
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

	cfg *treeConfig

	root *string
}

func testRenderTreesSuppliedViewsCaseTailSegment(
	t *testing.T,
	tailCaseInput *testRenderTreesSuppliedViewsCaseTailInputs,
	base *Rendered,
	head *Rendered,
) {
	t.Helper()
	for side, rendered := range []Rendered{(*base), (*head)} {
		if (*tailCaseInput.testCaseInput.headOnly) && side == 0 {
			assertHeadOnlyWork(t, rendered)
			continue
		}
		assert.FatalIf(t, rendered.Result == nil, "side %d has no result", side)
		assert.NoError(t, rendered.Err)
		assert.Equal(t, len(rendered.Result.Failed), 0)
		wantA := (*tailCaseInput.testCaseInput.scenario) == "explicit_changes" ||
			(*tailCaseInput.testCaseInput.scenario) == "filtered_addition" && side == 1 ||
			(*tailCaseInput.testCaseInput.scenario) ==
				"filtered_deletion" && side == 0
		names := configMapNames(rendered.Result)
		assert.Equal(t, names["cfg-a"], wantA)
		assert.Equal(t, names["cfg-b"], false)
		filtered := (*tailCaseInput.testCaseInput.scenario) == "filtered_addition" && side == 0 ||
			(*tailCaseInput.testCaseInput.scenario) ==
				"filtered_deletion" && side == 1
		if filtered {
			id := testutil.KustomizationID("flux-system", "app-a")
			assert.Equal(t, rendered.Store().GetObject(id) == nil, true)
			for _, obj := range rendered.Store().ListObjects("", manifest.KindConfigMap) {
				assert.FatalIf(
					t,
					obj.Named().Name == "cfg-a",
					"%v",
					"filtered side admitted cfg-a as an orphan seed",
				)
			}
		}
		tailCaseInput.assertWarnings(t, side, &rendered)
	}
}

type testRenderTreesSuppliedViewsCaseSegmentInputs struct {
	testCaseInput *testRenderTreesSuppliedViewsCaseInputs
	root          *string
	input         **store.SourceArtifact
	err           *error
	cfg           *treeConfig
}

func testRenderTreesSuppliedViewsCaseSegment(
	t *testing.T,
	statementInput *testRenderTreesSuppliedViewsCaseSegmentInputs,
) {
	t.Helper()
	for side := range 2 {
		current := (*statementInput.input).FS
		filtered := (*statementInput.testCaseInput.scenario) == "filtered_addition" && side == 0 ||
			(*statementInput.testCaseInput.scenario) ==
				"filtered_deletion" && side == 1
		if filtered {
			current, _, (*statementInput.err) = gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{
				Mode: gittree.Source, Current: current, Ignore: "flux/ks-a.yaml\napps/app-a/\n",
				Sparse: []string{"flux/ks-b.yaml", "apps/app-b"},
			})
			assert.NoError(t, (*statementInput.err))
		}
		opts := gittree.ViewOptions{Mode: gittree.Direct, Current: current}
		if (*statementInput.testCaseInput.scenario) == "differing_options" && side == 1 {
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
			t.Fatalf(
				"side %d location = %+v, want content root %s",
				side,
				view.Location(),
				(*statementInput.input).LocalPath,
			)
		}
		statementInput.assertFiles(t, view, filtered)
		artifact := *(*statementInput.input)
		artifact.FS, artifact.Key = view, key
		if side == 0 {
			statementInput.cfg.BaseInput = &artifact
		} else {
			statementInput.cfg.Input = &artifact
		}
	}
}

func (s *testRenderTreesSuppliedViewsCaseTailInputs) assertWarnings(t *testing.T, side int, rendered *Rendered) {
	t.Helper()
	warnings := rendered.Result.Warnings
	if (*s.testCaseInput.scenario) == "differing_options" ||
		(*s.testCaseInput.scenario) == "explicit_empty" {
		assert.FatalIf(t, len(warnings) != 1 || warnings[0].Category != manifest.WarnPathConfig ||
			!strings.Contains(
				warnings[0].Message,
				"no changes detected",
			), "side %d warnings = %v", side, warnings)
	} else {
		assert.Equal(t, len(warnings), 0)
	}
}

func (s *testRenderTreesSuppliedViewsCaseSegmentInputs) assertFiles(t *testing.T, view fs.FS, filtered bool) {
	t.Helper()
	for _, name := range []string{"flux/ks-a.yaml", "apps/app-a/kustomization.yaml", "apps/app-a/cm.yaml", "flux/ks-b.yaml", "apps/app-b/cm.yaml"} {
		body, readErr := fs.ReadFile(view, name)
		if filtered && (name == "flux/ks-a.yaml" || strings.HasPrefix(name, "apps/app-a/")) {
			assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
		} else {
			assert.NoError(t, readErr)
			want, err := os.ReadFile(filepath.Join((*s.root), name))
			assert.NoError(t, err)
			assert.Equal(t, string(body), string(want))
		}
	}
}
