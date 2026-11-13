package helmchart

import (
	"io/fs"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func assertHTTPChartWork(
	t *testing.T,
	work *[2][2]manifest.WorkRecorder,
	artifacts *[2][2]*store.SourceArtifact,
	wave string,
) {
	t.Helper()
	counts := map[string]manifest.WorkCounts{}
	acquired := map[string]uint64{}
	func() {
		assertHTTPChartConsumerWork(
			&assertHTTPChartConsumerWorkInputs{
				acquired:  &acquired,
				artifacts: &artifacts,
				counts:    &counts,
				t:         &t,
				work:      &work,
			},
		)
	}()
	if artifacts[0][0].Key == artifacts[0][1].Key {
		t.Fatal("distinct charts shared an acquisition identity")
	}
	wantAcquisition := manifest.WorkCounts{Attempts: 3, Completed: 3, RemoteTransfers: 3}
	wantCache := manifest.WorkCounts{Attempts: 6, Completed: 6, CacheService: 3}
	wantDecode := manifest.WorkCounts{Attempts: 1, Completed: 1}
	wantValidation := manifest.WorkCounts{}
	if wave == "source-warm" {
		wantAcquisition, wantDecode = manifest.WorkCounts{}, manifest.WorkCounts{}
		wantCache = manifest.WorkCounts{Attempts: 4, Completed: 4, CacheService: 4}
	} else {
		unique := 3
		if wave == "source-stale" {
			unique = 2
		}
		assert.Equal(t, len(acquired), unique)
		for _, artifact := range artifacts[0] {
			assert.Equal(t, acquired[artifact.Key], uint64(1))
		}
	}
	if wave == "source-stale" {
		wantAcquisition = manifest.WorkCounts{Attempts: 2, Completed: 2}
		wantCache.CacheService = 6
		wantDecode = manifest.WorkCounts{}
		wantValidation = manifest.WorkCounts{Attempts: 2, Completed: 2}
	}
	assert.Equal(t, counts[string(manifest.WorkAcquisition)], wantAcquisition)
	assert.Equal(t, counts[string(manifest.WorkCache)], wantCache)
	assert.Equal(t, counts[string(manifest.WorkIndexDecode)], wantDecode)
	assert.Equal(t, counts[string(manifest.WorkValidation)], wantValidation)
}

func assertHTTPChartArtifact(t *testing.T, art *store.SourceArtifact, name string, expected []byte) {
	t.Helper()
	if art.Content == "" || art.Content != sourcecache.AcquisitionIdentity(art.FS) {
		t.Errorf("%s content generation = %q", name, art.Content)
	}
	if art.Kind != manifest.KindHelmChart || art.Digest != chartDigest(expected) {
		t.Errorf("%s artifact = %+v", name, art)
	}
	if _, err := fs.Stat(art.FS, "chart.tgz"); err != nil {
		t.Errorf("%s chart.tgz: %v", name, err)
	}
	body, err := fs.ReadFile(art.FS, "chart.tgz")
	assert.NoError(t, err)
	assert.Equal(t, string(body), string(expected))
}

type assertHTTPChartConsumerWorkInputs struct {
	acquired  *map[string]uint64
	artifacts **[2][2]*store.SourceArtifact
	counts    *map[string]manifest.WorkCounts
	t         **testing.T
	work      **[2][2]manifest.WorkRecorder
}

func assertHTTPChartConsumerWork(captures *assertHTTPChartConsumerWorkInputs) {
	for cluster := range *captures.work {
		for chart, name := range []string{"app-template", "other"} {
			{
				records := (*captures.work)[cluster][chart].Scope().Snapshot(0).Normalize()
				for recordIndex := range records {
					record := &records[recordIndex]
					assert.Equal((*captures.t), record.Key.Tree, (*captures.work)[cluster][chart].Key.Tree)
					assert.Equal((*captures.t), record.Key.Owner, (*captures.work)[cluster][chart].Key.Owner)
					assert.Equal((*captures.t), record.Key.Source, helmChart("repo", name, "1.0.0").Named())
					if record.Key.Input == "" {
						(*captures.t).Fatal("chart or index work lost its acquisition identity")
					}
					if record.Key.Phase == manifest.WorkAcquisition {
						(*captures.acquired)[record.Key.Input] += record.Counts.Attempts
					}
					sum := (*captures.counts)[string(record.Key.Phase)]
					sum.Add(&record.Counts)
					(*captures.counts)[string(record.Key.Phase)] = sum
				}
			}
			if (*captures.artifacts)[cluster][chart] == nil {
				(*captures.t).Fatal("missing chart artifact")
			}
			assert.Equal((*captures.t), (*captures.artifacts)[cluster][chart].Key, (*captures.artifacts)[0][chart].Key)
		}
	}
}
