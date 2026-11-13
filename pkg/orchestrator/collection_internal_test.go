package orchestrator

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func writeCollectionEntry(t *testing.T, root, entry, value string) {
	t.Helper()
	testutil.WriteFile(t, root, entry+"/kustomization.yaml", "resources: [objects.yaml, ../shared.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"shared.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: apps}\ndata: {shared: content}\n",
	)
	testutil.WriteFile(t, root, entry+"/objects.yaml", fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata: {name: cluster-vars, namespace: apps}
data: {value: %s}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: workload, namespace: apps}
spec:
  inputs: [{value: %s}]
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: output, namespace: apps}
      data: {value: '<< inputs.value >>'}
`, value, value))
}

func TestRenderTrees_CollectionPayloadIsolation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeCollectionEntry(t, root, "alpha", "first")
	writeCollectionEntry(t, root, "omega", "second")
	var first []*Result
	for run := range 2 {
		grouped, err := renderTreesForTest(t.Context(), t,
			[]Tree{
				{
					ID: manifest.TreeID{
						Cluster: "alpha",
						Side:    manifest.TreeHead,
					},
					RepoRoot: root,
					Path: filepath.Join(
						root,
						"alpha",
					),
				},
				{
					ID: manifest.TreeID{
						Cluster: "omega",
						Side:    manifest.TreeHead,
					},
					RepoRoot: root,
					Path: filepath.Join(
						root,
						"omega",
					),
				},
			},
			Config{
				RecordWork:  true,
				Concurrency: 2,
			},
		)
		assert.NoError(t, err)
		assert.Equal(t, len(grouped), 2)
		for i, entry := range []string{"alpha", "omega"} {
			assert.Equal(t, grouped[i].ID, manifest.TreeID{Cluster: manifest.ClusterID(entry), Side: manifest.TreeHead})
			explicit, err := renderTreesForTest(t.Context(), t,
				[]Tree{
					{
						ID: manifest.TreeID{
							Cluster: ".",
							Side:    manifest.TreeHead,
						},
						Path: filepath.Join(
							root,
							entry,
						),
						RepoRoot: root,
					},
				},
				Config{
					RecordWork:  true,
					Concurrency: 2,
				},
			)
			assert.NoError(t, err)
			{
				delta := cmp.Diff(
					grouped[i].Result.Manifests,
					explicit[0].Result.Manifests,
					cmpopts.IgnoreUnexported(
						manifest.Kustomization{},
						manifest.OCIRepository{},
						manifest.HelmChartSource{},
					),
				)
				assert.FatalIf(t, delta != "", "%v", delta)
			}
			assert.Diff(t, grouped[i].Result.Failed, explicit[0].Result.Failed)
			vars := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "cluster-vars"}
			data, ok := grouped[i].Store().Get[*manifest.ConfigMap](vars)
			assert.FatalIf(t, !ok, "%v", "entry lost its native variables")
			assert.Equal(t, data.Data["value"], any([]string{"first", "second"}[i]))
			assert.Equal(
				t,
				workCounts(
					grouped[i].Result.Work,
					manifest.NamedResource{},
					manifest.WorkKustomize,
				),
				manifest.WorkCounts{
					Attempts:  1,
					Completed: 1,
				},
			)
			assert.Equal(
				t,
				workCounts(
					grouped[i].Result.Work,
					manifest.NamedResource{
						Group:     manifest.FluxOperatorDomain,
						Kind:      manifest.KindResourceSet,
						Namespace: "apps",
						Name:      "workload",
					},
					manifest.WorkEvaluation,
				),
				manifest.WorkCounts{
					Attempts:  1,
					Completed: 1,
				},
			)
			for recordIndex := range grouped[i].Result.Work {
				record := &grouped[i].Result.Work[recordIndex]
				assert.Equal(t, record.Key.Tree, grouped[i].ID)
			}
		}
		results := []*Result{grouped[0].Result, grouped[1].Result}
		if run == 0 {
			first = results
		} else {
			{
				delta := cmp.Diff(
					results,
					first,
					cmpopts.IgnoreUnexported(
						Result{},
						manifest.Kustomization{},
						manifest.OCIRepository{},
						manifest.HelmChartSource{},
					),
				)
				assert.FatalIf(t, delta != "", "%v", delta)
			}
		}
	}
}

func TestRenderTrees_ClusterUnion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		before, after []string
	}{
		{"removed", []string{"alpha", "omega"}, []string{"alpha"}},
		{"added", []string{"alpha"}, []string{"alpha", "omega"}},
		{"renamed", []string{"alpha", "omega"}, []string{"alpha", "theta"}},
		{"renamed_back", []string{"alpha", "theta"}, []string{"alpha", "omega"}},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				t.Parallel()
				testRenderTreesClusterUnionCase(t, &tc)
			},
		)
	}
}

func TestRenderTrees_IndependentBootstrapFailure(t *testing.T) {
	t.Parallel()
	t.Run(
		"work_before_failure",
		func(t *testing.T) {
			t.Parallel()
			tree := Tree{ID: manifest.TreeID{Cluster: "failed", Side: manifest.TreeHead}, RepoRoot: t.TempDir()}
			tree.Path = filepath.Join(tree.RepoRoot, "missing")
			runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{RecordWork: true, Concurrency: 2})
			assert.FatalIf(
				t,
				err == nil || len(runs) != 1 || runs[0].Result == nil || runs[0].Orchestrator != nil || runs[0].Absent,
				"preparation failure lost its attempted side: runs=%+v error=%v",
				runs,
				err,
			)
			assert.Equal(
				t,
				workCounts(
					runs[0].Result.Work,
					manifest.NamedResource{},
					manifest.WorkPreparation,
				),
				manifest.WorkCounts{
					Attempts:      1,
					Failures:      1,
					OwnerPrepares: 1,
				},
			)
			assert.Equal(
				t,
				workCounts(runs[0].Result.Work, manifest.NamedResource{}, manifest.WorkKustomize),
				manifest.WorkCounts{},
			)
		},
	)
	failed, healthy := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, failed, "kustomization.yaml", "resources: [missing.yaml]\n")
	testutil.WriteFile(t, healthy, "kustomization.yaml", "resources: [object.yaml]\n")
	testutil.WriteFile(t, healthy, "object.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy}\n")
	runs, err := renderTreesForTest(t.Context(), t, []Tree{
		{ID: manifest.TreeID{Cluster: "failed", Side: manifest.TreeHead}, RepoRoot: failed},
		{ID: manifest.TreeID{Cluster: "healthy", Side: manifest.TreeHead}, RepoRoot: healthy},
	}, Config{RecordWork: true, Concurrency: 2})
	assert.FatalIf(t, err == nil || len(runs) != 2 || runs[0].Orchestrator == nil || runs[0].Result == nil ||
		runs[1].Orchestrator == nil ||
		runs[1].Err != nil ||
		len(runs[1].Result.Manifests) == 0, "bootstrap failure affected sibling: runs=%+v error=%v", runs, err)
}

func TestRenderTrees_DuplicateBeforeLaunch(t *testing.T) {
	t.Parallel()

	var updates atomic.Int32
	tree := Tree{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: t.TempDir()}
	results, err := renderTreesForTest(t.Context(), t,
		[]Tree{tree, tree},
		Config{
			Concurrency:    2,
			StatusListener: func(_ manifest.TreeID, _ manifest.DiagnosticIdentity, _ store.StatusInfo) { updates.Add(1) },
		},
	)
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrInput) || len(results) != 0 || updates.Load() != 0,
		"duplicate launched: %v %v",
		results,
		err,
	)
}

func testRenderTreesClusterUnionCase(t *testing.T, tc *struct {
	name          string
	before, after []string
},
) {
	t.Helper()

	before, after := t.TempDir(), t.TempDir()
	for _, entry := range tc.before {
		writeCollectionEntry(t, before, entry, "old")
	}
	for _, entry := range tc.after {
		writeCollectionEntry(t, after, entry, "new")
	}
	var trees []Tree
	for side, entries := range [][]string{tc.before, tc.after} {
		root := before
		if side == 1 {
			root = after
		}
		for _, entry := range entries {
			trees = append(
				trees,
				Tree{
					ID: manifest.TreeID{
						Cluster: manifest.ClusterID(
							entry,
						),
						Side: manifest.TreeSide(
							side,
						),
					},
					RepoRoot: root,
					Path: filepath.Join(
						root,
						entry,
					),
				},
			)
		}
	}
	for _, order := range []string{"base_first", "current_first"} {
		t.Run(
			order,
			func(t *testing.T) {
				t.Parallel()
				testRenderTreesClusterUnionOrder(t, tc, trees, order)
			},
		)
	}
}

func testRenderTreesClusterUnionOrder(t *testing.T, tc *struct {
	name          string
	before, after []string
}, trees []Tree, order string,
) {
	t.Helper()

	inputs := slices.Clone(trees)
	if order == "current_first" {
		slices.Reverse(inputs)
	}
	runs, err := renderTreesForTest(t.Context(), t, inputs, Config{RecordWork: true, Concurrency: 2})
	assert.NoError(t, err)
	roots := []manifest.NamedResource{testutil.KustomizationID("", "")}
	for i := 0; i < len(runs); i += 2 {
		pair := runs[i : i+2]
		assert.Equal(t, pair[0].ID.Cluster, pair[1].ID.Cluster)
		assert.Equal(t, pair[0].ID.Side, manifest.TreeBase)
		assert.Equal(t, pair[1].ID.Side, manifest.TreeHead)
		for side, entries := range [][]string{tc.before, tc.after} {
			absent := !slices.Contains(entries, string(pair[side].ID.Cluster))
			assert.Equal(t, pair[side].Absent, absent)
			assert.FatalIf(t, absent && pair[side].Orchestrator != nil, "%v", "absent side constructed a tree")
			if absent {
				delta := cmp.Diff(pair[side].Result, &Result{}, cmp.AllowUnexported(Result{}))
				assert.ErrorIf(t, delta != "", "%s", delta)
				assert.NoError(t, pair[side].Err)
				continue
			}
			assert.Equal(
				t,
				workCounts(
					pair[side].Result.Work,
					manifest.NamedResource{},
					manifest.WorkKustomize,
				),
				manifest.WorkCounts{
					Attempts:  1,
					Completed: 1,
				},
			)
			assert.Equal(
				t,
				workCounts(
					pair[side].Result.Work,
					manifest.NamedResource{
						Group:     manifest.FluxOperatorDomain,
						Kind:      manifest.KindResourceSet,
						Namespace: "apps",
						Name:      "workload",
					},
					manifest.WorkEvaluation,
				),
				manifest.WorkCounts{
					Attempts:  1,
					Completed: 1,
				},
			)
			for recordIndex := range pair[side].Result.Work {
				record := &pair[side].Result.Work[recordIndex]
				assert.Equal(t, record.Key.Tree, pair[side].ID)
			}
		}
		projectedResults0, projectionErrors0 := Select(pair, errors.Join(pair[0].Err, pair[1].Err), roots, nil)
		selected, err, _ := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
		assert.NoError(t, err)
		comparison1, preparationErr1 := diff.Compare([]diff.Group{{Orig: diff.Snapshot{
			Subjects:     selected[0].Subjects,
			Manifests:    selected[0].Manifests,
			Unavailable:  selected[0].Unavailable,
			ProducerKeys: selected[0].ProducerKeys,
		}, Current: diff.Snapshot{
			Subjects:     selected[1].Subjects,
			Manifests:    selected[1].Manifests,
			Unavailable:  selected[1].Unavailable,
			ProducerKeys: selected[1].ProducerKeys,
		}, SelectedErr: nil}}, diff.Options{
			Format: diff.FormatJSON,
		})
		var comparisonOutput1 bytes.Buffer
		writeErr1 := comparison1.Write(&comparisonOutput1, nil)
		changes, _, _, err := comparisonOutput1.Bytes(), comparison1.Groups[0].Warnings, comparison1.Groups[0].Suppressed, errors.Join(
			preparationErr1,
			writeErr1,
		)
		assert.NoError(t, err)
		if pair[0].Absent || pair[1].Absent {
			want := "removed"
			if pair[0].Absent {
				want = "added"
			}
			assert.FatalIf(t, !strings.Contains(
				string(
					changes,
				),
				`"status": "`+want+`"`,
			) || len(
				selected[0].Unavailable,
			)+len(
				selected[1].Unavailable,
			) != 0, "absent group lost %s: %s", want, changes)
		}
	}
}
