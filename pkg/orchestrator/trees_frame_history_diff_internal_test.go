package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_FrameHistoryDiffSides(t *testing.T) {
	t.Parallel()
	roots := frameHistoryDiffRoots(t)
	testFrameHistoryDiffSides(t, roots)
}

func testFrameHistoryDiffSides(t *testing.T, roots [2]string) {
	t.Helper()
	for _, direction := range []struct {
		name   string
		roots  [2]string
		status diff.ChangeStatus
	}{
		{"forward", roots, diff.StatusRemoved},
		{"reverse", [2]string{roots[1], roots[0]}, diff.StatusAdded},
	} {
		t.Run(direction.name, func(t *testing.T) {
			t.Parallel()
			trees := make([]Tree, 2)
			for side, root := range direction.roots {
				trees[side] = Tree{
					RepoRoot: root, Path: filepath.Join(root, "entry"),
					ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
				}
			}
			var fullDiff []byte
			for _, changed := range []bool{false, true} {
				name := "full"
				if changed {
					name = "changed-only"
				}
				t.Run(name, func(t *testing.T) {
					body := assertFrameHistoryDiff(t, trees, changed, direction.status)
					if changed {
						assert.Equal(t, string(body), string(fullDiff))
					} else {
						fullDiff = body
					}
				})
			}
		})
	}
}

func assertFrameHistoryDiff(t *testing.T, trees []Tree, changed bool, status diff.ChangeStatus) []byte {
	t.Helper()
	cacheDirs := [2]string{t.TempDir(), t.TempDir()}
	var cold frameHistoryDiffResult
	for repeat, cacheDir := range cacheDirs {
		got := observeFrameHistoryDiff(t, trees, cacheDir, changed, false)
		if repeat == 0 {
			cold = got
		} else {
			assert.Diff(t, got, cold)
		}
	}
	var output struct {
		Changes []diff.Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(cold.Diff, &output))
	assert.Equal(t, len(output.Changes), 1)
	if len(output.Changes) == 1 {
		assert.Equal(t, output.Changes[0].Name, "render-values")
		assert.Equal(t, output.Changes[0].Kind, manifest.KindConfigMap)
		assert.Equal(t, output.Changes[0].Status, status)
	}
	t.Run("reused", func(t *testing.T) {
		t.Parallel()
		if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
			t.Skip(reason)
		}
		for _, cacheDir := range cacheDirs {
			got := observeFrameHistoryDiff(t, trees, cacheDir, changed, true)
			assert.Diff(t, got, cold)
		}
	})
	return cold.Diff
}

func frameHistoryDiffRoots(t *testing.T) [2]string {
	t.Helper()
	roots := [2]string{t.TempDir(), t.TempDir()}
	for side, root := range roots {
		writeCluster(t, root, "unchanged", "same")
		testutil.WriteFile(t, root, "ks-b.yaml", testutil.KustomizationYAML("sibling-parent", "flux-system",
			"  path: ./parents/sibling\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
		testutil.WriteFile(t, root, "parents/sibling/kustomization.yaml", "resources: [child.yaml]\n")
		testutil.WriteFile(t, root, "parents/sibling/child.yaml", testutil.KustomizationYAML("app-b", "flux-system",
			"  path: ./apps/app-b\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
		testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [../ks-a.yaml, ../ks-b.yaml, ../inputs]\n")
		testutil.WriteFile(t, root, "ks-a.yaml", testutil.KustomizationYAML(
			"app-a",
			"flux-system",
			"  path: ./apps/app-a\n  targetNamespace: flux-system\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		))
		testutil.WriteFile(t, root, "apps/app-a/cm.yaml", testutil.KustomizationYAML("consumer-a", "",
			"  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n"+
				"    substituteFrom: [{kind: ConfigMap, name: render-values}, {kind: Secret, name: vars}]\n")+fmt.Sprintf("# snapshot %d\n", side))
		resources := "resources: [vars.yaml]\n"
		if side == 0 {
			resources = "resources: [vars.yaml, values.yaml]\n"
			testutil.WriteFile(
				t,
				root,
				"inputs/values.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: render-values, namespace: flux-system}\ndata: {VALUE: before}\n",
			)
		}
		testutil.WriteFile(t, root, "inputs/kustomization.yaml", resources)
		testutil.WriteFile(t, root, "inputs/vars.yaml", "apiVersion: v1\nkind: Secret\n"+
			"metadata: {name: vars, namespace: flux-system}\n"+
			"stringData: {TOKEN: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n")
		testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\n"+
			"metadata: {name: cfg-a}\ndata: {value: '${VALUE}-${TOKEN}'}\n")
	}
	return roots
}

type frameHistoryDiffSide struct {
	Output                    []byte
	Warnings, ClusterWarnings []manifest.Warning
	Failures                  []frameHistoryDiffFailure
	Blocked                   map[manifest.NamedResource][]manifest.NamedResource
	Orphans                   map[manifest.NamedResource]string
	Error                     string
}

type frameHistoryDiffFailure struct {
	ID         manifest.NamedResource
	Status     store.Status
	Reason     store.StatusReason
	Message    string
	Diagnostic manifest.DiagnosticReason
}

type frameHistoryDiffResult struct {
	Sides                      []frameHistoryDiffSide
	Diff                       []byte
	Warnings                   []manifest.Warning
	Suppressions               []diff.Suppression
	Error, Selected, Remaining string
	DiffError                  string
}

//nolint:revive // Changed selection and reused storage are independent witness axes.
func observeFrameHistoryDiff(
	t *testing.T,
	trees []Tree,
	directory string,
	changed, reused bool,
) frameHistoryDiffResult {
	t.Helper()
	cache, err := sourcecache.New(t.Context(), directory)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, cache.Close()) }()
	app := testutil.KustomizationID("flux-system", "app-a")
	if reused {
		for _, tree := range trees {
			owners, historyErr := cache.ProducersForPaths(t.Context(), []manifest.SourceEvidence{{
				Source: manifest.SourceIdentity{Root: tree.RepoRoot}, Path: "apps/app-a/cm.yaml",
			}})
			assert.NoError(t, historyErr)
			assert.FatalIf(
				t,
				len(owners[app]) == 0,
				"reused cache has no accepted producer inventory for %s",
				tree.RepoRoot,
			)
		}
	}
	cfg := Config{Cache: cache, Concurrency: 2, RecordWork: true, Selection: selection.Request{Changed: changed}}
	var runs []Rendered
	if changed {
		runs, err = renderTreesForTest(t.Context(), t, trees, cfg)
	} else {
		for _, tree := range trees {
			full, failure := renderTreesForTest(t.Context(), t, []Tree{tree}, cfg)
			runs = append(runs, full...)
			err = errors.Join(err, failure)
		}
	}
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	assert.Equal(t, len(runs), 2)
	observed := frameHistoryDiffResult{Error: fmt.Sprint(err)}
	for i := range runs {
		run := &runs[i]
		if reused {
			history := run.controllers.PreviousRender(t.Context(), run.Store().GetObject(app), schedule.Result{})
			if history.Assessment != nil {
				history.Assessment.Close()
			}
			assert.ErrorIf(t, !history.Complete, "reused app has no complete accepted history")
			assert.FatalIf(t, history.Records == nil, "reused app has no accepted inventory")
			_, accepted := maps.Collect(history.Records)[testutil.KustomizationID("flux-system", "consumer-a")]
			assert.Equal(t, accepted, true)
		}
		observed.Sides = append(observed.Sides, observeFrameHistoryDiffSide(t, run, changed, reused))
	}
	selected, failures := Select(runs, err, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
	observed.Selected, observed.Remaining = fmt.Sprint(failures.Selected), fmt.Sprint(failures.Remaining)
	comparison1, preparationErr1 := diff.Compare([]diff.Group{{Orig: diff.Snapshot{
		Subjects: selected[0].Subjects, Manifests: selected[0].Manifests,
		ProducerKeys: selected[0].ProducerKeys, Unavailable: selected[0].Unavailable,
	}, Current: diff.Snapshot{
		Subjects: selected[1].Subjects, Manifests: selected[1].Manifests,
		ProducerKeys: selected[1].ProducerKeys, Unavailable: selected[1].Unavailable,
	}, SelectedErr: failures.Selected}}, diff.Options{Format: diff.FormatJSON})
	var comparisonOutput1 bytes.Buffer
	writeErr1 := comparison1.Write(&comparisonOutput1, nil)
	body, warnings, suppressed, failure := comparisonOutput1.Bytes(), comparison1.Groups[0].Warnings, comparison1.Groups[0].Suppressed, errors.Join(
		preparationErr1,
		writeErr1,
	)
	assert.Equal(t, errors.Is(failure, manifest.ErrFlux), true)
	observed.Diff, observed.Warnings, observed.Suppressions, observed.DiffError = body, warnings, suppressed, fmt.Sprint(
		failure,
	)
	return observed
}

func observeFrameHistoryDiffSide(t *testing.T, run *Rendered, changed, reused bool) frameHistoryDiffSide {
	t.Helper()
	app := testutil.KustomizationID("flux-system", "app-a")
	sibling := testutil.KustomizationID("flux-system", "app-b")
	consumer := testutil.KustomizationID("flux-system", "consumer-a")
	assert.FatalIf(t, run.Result == nil || run.Orchestrator == nil, "missing completed tree: %+v", run)
	_, indexed := run.index.Lookup(sibling)
	assert.FatalIf(t, !indexed, "unchanged producer is absent from the discovery index")
	assert.Equal(t, run.Store().GetObject(sibling) == nil, changed)
	assert.Equal(t, len(outputByParent(run.Result.Manifests)[sibling]) == 0, changed)
	assert.Equal(t, run.selection != nil, true)
	assert.Equal(t, run.selection.Snapshot().Output(manifest.NamedResource{}), !changed)
	if changed {
		assert.ErrorIf(
			t,
			!slices.Contains(run.cfg.ExternalChanges.Paths(), "inputs/values.yaml"),
			"changed input is absent from the diff",
		)
	}
	assert.Equal(t, configMapNames(run.Result)["cfg-b"], !changed)
	assert.ErrorIf(t, len(run.Result.Warnings) == 0, "render omitted its expected warning")
	if reused {
		assert.Equal(t, workCounts(run.Result.Work, app, manifest.WorkKustomize).Attempts, uint64(0))
		assert.ErrorIf(
			t,
			workCounts(run.Result.Work, app, manifest.WorkCache).CacheService == 0,
			"reused app has no cache service work",
		)
	} else {
		assert.Equal(t, workCounts(run.Result.Work, app, manifest.WorkKustomize).Attempts > 0, true)
	}
	output, marshalErr := json.Marshal(run.Result.Manifests)
	assert.NoError(t, marshalErr)
	side := frameHistoryDiffSide{
		Output: output, Warnings: run.Result.Warnings, ClusterWarnings: run.Result.ClusterWarnings,
		Blocked: run.Result.Blocked, Orphans: run.Result.Orphans, Error: fmt.Sprint(run.Err),
	}
	for _, id := range slices.SortedFunc(maps.Keys(run.Result.Failed), manifest.NamedResource.Compare) {
		failure := run.Result.Failed[id]
		side.Failures = append(side.Failures, frameHistoryDiffFailure{
			ID: id, Status: failure.Status, Reason: failure.Reason, Message: failure.Message,
			Diagnostic: failure.DependencyReason(run.diagnostic(id)),
		})
	}
	if run.Err == nil {
		assert.Equal(t, configMapNames(run.Result)["cfg-a"], true)
	} else {
		assert.ErrorIf(t, len(side.Failures) == 0, "failed render has no diagnostic failures")
		assert.ErrorIf(t, len(side.Blocked[consumer]) == 0, "failed render has no blocked consumer")
		assert.Equal(t, configMapNames(run.Result)["cfg-a"], false)
	}
	return side
}

//nolint:tparallel // Source replacement follows the completed parallel comparisons.
func TestRenderTrees_IndexedFrameHistoryDiffSides(t *testing.T) {
	t.Parallel()
	roots := frameHistoryDiffRoots(t)
	replacements := make([]func(), 0, len(roots))
	for _, root := range roots {
		file := filepath.Join(root, "apps", "app-a", "cm.yaml")
		body, err := os.ReadFile(file)
		assert.NoError(t, err)
		testutil.WriteFile(
			t,
			root,
			"apps/app-a/cm.yaml",
			strings.ReplaceAll(string(body), "./consumer", "./must-not-render"),
		)
		testutil.WriteFile(t, root, "apps/app-a/kustomization.yaml", `resources: [cm.yaml]
patches:
- target: {kind: Kustomization}
  patch: |
    - op: replace
      path: /spec/path
      value: ./consumer
`)
		st := store.New()
		input := &store.SourceArtifact{LocalPath: root, Content: "transformed", FS: os.DirFS(root)}
		st.SetArtifact(manifest.BootstrapSourceID, input)
		l := loader.New(st)
		l.SourceRoot = root
		l.Options.Session, err = selection.New(st, l, selection.Request{Changed: true})
		assert.NoError(t, err)
		_, err = l.Load(t.Context(), filepath.Join(root, "apps", "app-a"), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		assert.NoError(t, l.Admit(t.Context(), root))
		id := testutil.KustomizationID("", "consumer-a")
		occurrence, known := l.Occurrence(id)
		assert.Equal(t, known, true)
		assert.Equal(t, occurrence.RequiresBuild, true)
		assert.Equal(t, st.GetDeclaration(id) == nil, true)
		assert.NoError(t, l.Admit(t.Context(), root))
		again, known := l.Occurrence(id)
		assert.Equal(t, known, true)
		assert.Equal(t, again.RequiresBuild, true)
		assert.Equal(t, st.GetDeclaration(id) == nil, true)
		replacements = append(replacements, func() {
			testutil.WriteFile(t, root, "apps/app-a/cm.yaml", string(body))
			testutil.WriteFile(t, root, "apps/app-a/kustomization.yaml", "resources: [cm.yaml]\n")
			replacement := *input
			replacement.Content = "untransformed"
			st.SetArtifact(manifest.BootstrapSourceID, &replacement)
			_, loadErr := l.Load(t.Context(), filepath.Join(root, "apps", "app-a"), manifest.NamedResource{}, nil)
			assert.NoError(t, loadErr)
			assert.NoError(t, l.Admit(t.Context(), root))
			current, exists := l.Occurrence(id)
			assert.Equal(t, exists, true)
			assert.Equal(t, current.RequiresBuild, false)
			l.Retire()
		})
	}
	t.Run("comparison", func(t *testing.T) { testFrameHistoryDiffSides(t, roots) })
	for _, replace := range replacements {
		replace()
	}
}
