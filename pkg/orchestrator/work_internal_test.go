package orchestrator

import (
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/helm"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func assertHeadOnlyWork(t *testing.T, rendered Rendered) {
	t.Helper()
	result := rendered.Result
	rendered.Result = nil
	assert.Equal(t, rendered, Rendered{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}})
	if result == nil || len(result.Work) == 0 {
		t.Fatal("head-only preparation lost its work")
	}
	if difference := cmp.Diff(&Result{Work: result.Work}, result, cmp.AllowUnexported(Result{})); difference != "" {
		t.Errorf("head-only result differs (-want +got):\n%s", difference)
	}
	assert.Equal(t, workCounts(result.Work, manifest.NamedResource{}, manifest.WorkKustomize).Attempts, uint64(0))
	assert.Equal(t, workCounts(result.Work, manifest.NamedResource{}, manifest.WorkHelm).Attempts, uint64(0))
	for recordIndex := range result.Work {
		record := result.Work[recordIndex]
		assert.Equal(t, record.Key.Tree, rendered.ID)
	}
}

func TestWork_BorrowedOwners(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", sourcecache.New)
	client := helm.NewClient(owner)
	var trees [2]Tree
	for side := range trees {
		root := t.TempDir()
		writeCluster(t, root, []string{"first", "second"}[side], "same")
		trees[side] = Tree{
			ID: manifest.TreeID{
				Cluster: manifest.ClusterID(
					[]string{
						"first",
						"second",
					}[side],
				),
				Side: manifest.TreeHead,
			},
			RepoRoot: root,
		}
	}
	var previous [2]manifest.WorkSummary
	for run := range 2 {
		var results [2][]Rendered
		var errs [2]error
		var wg sync.WaitGroup
		for side := range trees {
			wg.Go(
				func() {
					results[side], errs[side] = renderTreesForTest(t.Context(), t,
						[]Tree{
							trees[side],
						},
						Config{
							RecordWork:  true,
							Cache:       owner,
							HelmClient:  client,
							Concurrency: 2,
						},
					)
				},
			)
		}
		wg.Wait()
		for side := range results {
			assert.NoError(t, errs[side])
			assert.Equal(t, len(results[side]), 1)
			result := results[side][0].Result
			assert.Diff(t, configMapNames(result), map[string]bool{"cfg-a": true, "cfg-b": true})
			docs := outputByParent(result.Manifests)[testutil.KustomizationID("flux-system", "app-a")]
			assert.Equal(t, len(docs), 1)
			assert.Equal(t, docs[0]["data"].(map[string]any)["k"], any([]string{"first", "second"}[side]))
			for recordIndex := range result.Work {
				record := result.Work[recordIndex]
				assert.Equal(t, record.Key.Tree, trees[side].ID)
			}
			if run == 0 {
				previous[side] = append(manifest.WorkSummary(nil), result.Work...)
				assert.Equal(
					t,
					workCounts(result.Work, manifest.NamedResource{}, manifest.WorkKustomize).Attempts,
					uint64(3),
				)
			} else {
				assert.Equal(
					t,
					workCounts(result.Work, manifest.NamedResource{}, manifest.WorkKustomize).Attempts,
					uint64(0),
				)
				if workCounts(result.Work, manifest.NamedResource{}, manifest.WorkCache).CacheService == 0 {
					t.Fatal("warm invocation lost cache service")
				}
				assert.Diff(t, results[side][0].work, result.Work)
			}
		}
	}
	for side := range previous {
		assert.Equal(
			t,
			workCounts(previous[side], manifest.NamedResource{}, manifest.WorkKustomize).Attempts,
			uint64(3),
		)
	}
}

func workCounts(
	summary manifest.WorkSummary,
	owner manifest.NamedResource,
	phase manifest.WorkPhase,
) manifest.WorkCounts {
	var counts manifest.WorkCounts
	for recordIndex := range summary {
		record := summary[recordIndex]
		if record.Key.Phase == phase && (owner == (manifest.NamedResource{}) || record.Key.Owner == owner) {
			counts.Add(&record.Counts)
		}
	}
	return counts
}
