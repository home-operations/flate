package helmchart

import (
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

type testFetchHTTPChartCaseInputs struct {
	charts    *map[string][]byte
	indexHits *atomic.Int32
	chartHits *atomic.Int32
	f         **Fetcher
	fInputs   *testInputs
}

func testFetchHTTPChartCase(t *testing.T, testCaseInput testFetchHTTPChartCaseInputs) {
	t.Helper()

	var consumers sync.WaitGroup
	for name, data := range *testCaseInput.charts {
		consumers.Go(func() {
			art, err := (*testCaseInput.f).Fetch(
				t.Context(),
				helmChart("repo", name, "1.0.0"), testCaseInput.fInputs.
					readSet(helmChart("repo", name, "1.0.0")), sourcecache.Policy{},
			)
			if err != nil {
				t.Errorf("Fetch %s: %v", name, err)
				return
			}
			if art.Content == "" || art.Content != sourcecache.AcquisitionIdentity(art.FS) {
				t.Errorf("%s content generation = %q", name, art.Content)
			}
			assert.ErrorIf(
				t,
				art.Kind != manifest.KindHelmChart || art.Digest != chartDigest(data),
				"%s artifact = %+v",
				name,
				art,
			)
			{
				_, err := fs.Stat(art.FS, "chart.tgz")
				assert.ErrorIf(t, err != nil, "%s chart.tgz: %v", name, err)
			}
		})
	}
	consumers.Wait()
	t.Logf("index requests=%d chart requests=%d", testCaseInput.indexHits.Load(), testCaseInput.chartHits.Load())
	if testCaseInput.indexHits.Load() != 1 ||
		testCaseInput.chartHits.Load() != int32(len((*testCaseInput.charts))) {
		t.Errorf(
			"requests: index=%d charts=%d, want 1/%d",
			testCaseInput.indexHits.Load(),
			testCaseInput.chartHits.Load(),
			len((*testCaseInput.charts)),
		)
	}
}
