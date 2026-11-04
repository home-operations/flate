package git

import (
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testFetcherReferencesCase(t *testing.T, src *string, tc *struct {
	name              string
	ref               *sourcev1.GitRepositoryRef
	depth             int
	sparse            []string
	want              string
	files             []string
	absent            []string
	warmCommit        string
	recurse           bool
	wantErr           bool
	errText           string
	localAcquisitions uint64
},
) {
	t.Helper()
	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Depth: tc.depth}
	repo := &manifest.GitRepository{
		Name:              "test",
		Namespace:         "flux-system",
		URL:               "file://" + (*src),
		Reference:         tc.ref,
		SparseCheckout:    tc.sparse,
		RecurseSubmodules: tc.recurse,
	}
	if tc.warmCommit != "" {
		unconstrained := *repo
		unconstrained.Reference = &sourcev1.GitRepositoryRef{Commit: tc.warmCommit}
		_, err := f.Fetch(
			t.Context(),
			&unconstrained,
			(testInputs{}).readSet(&unconstrained),
			&sourcecache.Policy{
				MaxAge: unconstrained.SourceInterval(),
			},
		)
		assert.NoError(t, err)
	}
	work := manifest.WorkRecorder{Key: manifest.WorkKey{
		Tree:   manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead},
		Owner:  testutil.KustomizationID("apps", "consumer"),
		Source: manifest.BootstrapSourceID,
	}}
	art, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), &sourcecache.Policy{
		MaxAge: time.Hour,
		Work:   &manifest.AcquisitionWork{Scope: (&work).Scope()},
	})
	if tc.wantErr {
		if err == nil || tc.errText != "" && !strings.Contains(err.Error(), tc.errText) {
			t.Fatalf("Fetch error = %v, want failure containing %q", err, tc.errText)
		}
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, art.Revision, tc.want)
	for _, file := range tc.files {
		if _, err := fs.Stat(art.FS, file); err != nil {
			t.Errorf("included file %s missing: %v", file, err)
		}
	}
	for _, file := range tc.absent {
		if _, err := fs.Stat(art.FS, file); !os.IsNotExist(err) {
			t.Errorf("excluded file %s present: %v", file, err)
		}
	}
	func() {
		assertFetcherReferenceWork(
			&assertFetcherReferenceWorkInputs{art: &art, f: &f, repo: &repo, t: &t, tc: &tc, work: &work},
		)
	}()
}

type assertFetcherReferenceWorkInputs struct {
	art  **store.SourceArtifact
	f    **Fetcher
	repo **manifest.GitRepository
	t    **testing.T
	tc   **struct {
		name              string
		ref               *sourcev1.GitRepositoryRef
		depth             int
		sparse            []string
		want              string
		files             []string
		absent            []string
		warmCommit        string
		recurse           bool
		wantErr           bool
		errText           string
		localAcquisitions uint64
	}
	work *manifest.WorkRecorder
}

func assertFetcherReferenceWork(captures *assertFetcherReferenceWorkInputs) {
	for _, phase := range []string{"cold", "source-warm"} {
		if phase == "source-warm" {
			(*captures.work) = manifest.WorkRecorder{Key: captures.work.Key}
			warm, err := (*captures.f).Fetch(
				(*captures.t).Context(),
				(*captures.repo),
				(testInputs{}).readSet((*captures.repo)),
				&sourcecache.Policy{
					MaxAge: time.Hour, Work: &manifest.AcquisitionWork{Scope: captures.work.Scope()},
				},
			)
			assert.NoError((*captures.t), err)
			assert.Equal((*captures.t), warm.Key, (*captures.art).Key)
			assert.Equal((*captures.t), warm.Content, (*captures.art).Content)
			assert.Equal((*captures.t), warm.Revision, (*captures.tc).want)
		}
		counts := map[string]manifest.WorkCounts{}
		{
			records := captures.work.Scope().Snapshot(0).Normalize()
			for recordIndex := range records {
				record := &records[recordIndex]
				assert.Equal((*captures.t), record.Key.Tree, captures.work.Key.Tree)
				assert.Equal((*captures.t), record.Key.Owner, captures.work.Key.Owner)
				assert.Equal((*captures.t), record.Key.Source, (*captures.repo).Named())
				if record.Key.Phase == manifest.WorkAcquisition || record.Key.Phase == manifest.WorkCache {
					assert.Equal((*captures.t), record.Key.Input, (*captures.art).Key)
				}
				sum := counts[string(record.Key.Phase)]
				sum.Add(&record.Counts)
				counts[string(record.Key.Phase)] = sum
			}
		}
		wantAcquisition := manifest.WorkCounts{
			Attempts:          1,
			Completed:         1,
			LocalAcquisitions: (*captures.tc).localAcquisitions,
		}
		wantCache := manifest.WorkCounts{Attempts: 1, Completed: 1}
		if phase == "source-warm" {
			wantAcquisition = manifest.WorkCounts{}
			wantCache.CacheService = 1
		}
		assert.Equal((*captures.t), counts[string(manifest.WorkAcquisition)], wantAcquisition)
		assert.Equal((*captures.t), counts[string(manifest.WorkCache)], wantCache)
	}
}
