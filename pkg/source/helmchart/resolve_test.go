package helmchart

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// startHelmRepoCounted is startHelmRepo with a SEPARATE index-fetch counter, so
// a test can assert a warm run skips the live index.yaml fetch (not just the
// chart download).
func startHelmRepoCounted(t *testing.T, chartBytes []byte, indexDigest string) (srv *httptest.Server, indexHits, chartHits *int) {
	t.Helper()
	ih, ch := 0, 0
	mux := http.NewServeMux()
	mux.HandleFunc("/index.yaml", func(w http.ResponseWriter, _ *http.Request) {
		ih++
		w.Header().Set("Content-Type", "application/x-yaml")
		_, _ = w.Write([]byte(helmRepoIndex(indexDigest)))
	})
	mux.HandleFunc("/app-template-1.0.0.tgz", func(w http.ResponseWriter, _ *http.Request) {
		ch++
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(chartBytes)
	})
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &ih, &ch
}

// fetcherWithCache builds a Fetcher over a caller-supplied cache+layout, so a
// test can stand up two Fetchers sharing one on-disk cache — simulating two
// separate `flate` invocations (each with a fresh in-process indexCache).
func fetcherWithCache(t *testing.T, r *manifest.HelmRepository, cache *sourcecache.Cache) (*Fetcher, testInputs) {
	t.Helper()
	fInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return r }}
	f := New(nil, Config{Network: newTestNetwork(t), Cache: cache})

	return f, fInputs
}

func TestResolveChart_DiskCacheSkipsIndexWarm(t *testing.T) {
	t.Parallel()
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexHits, chartHits := startHelmRepoCounted(t, chartBytes, chartDigest(chartBytes))
	r := httpRepo(srv.URL)
	r.Interval = metav1.Duration{Duration: time.Hour} // fresh window

	root := t.TempDir()
	cache := cachetest.New(t, root, sourcecache.New)

	cold, coldInputs := fetcherWithCache(t, r, cache)
	if _, err := cold.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), coldInputs.readSet(helmChart("repo", "app-template", "1.0.0")), sourcecache.Policy{}); err != nil {
		t.Fatalf("cold Fetch: %v", err)
	}
	if *indexHits != 1 || *chartHits != 1 {
		t.Fatalf("cold: indexHits=%d chartHits=%d, want 1/1", *indexHits, *chartHits)
	}

	warm, warmInputs := fetcherWithCache(t, r, cache)
	if _, err := warm.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), warmInputs.readSet(helmChart("repo", "app-template", "1.0.0")), sourcecache.Policy{}); err != nil {
		t.Fatalf("warm Fetch: %v", err)
	}
	if *indexHits != 1 {
		t.Errorf("warm run re-fetched index.yaml: indexHits=%d, want 1 (fresh chart must skip it)", *indexHits)
	}
	if *chartHits != 1 {
		t.Errorf("warm run re-downloaded chart: chartHits=%d, want 1 (fresh chart must skip it)", *chartHits)
	}
}

func TestResolveChart_StaleIntervalRefetches(t *testing.T) {
	t.Parallel()
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexHits, _ := startHelmRepoCounted(t, chartBytes, chartDigest(chartBytes))
	r := httpRepo(srv.URL)
	r.Interval = metav1.Duration{}

	root := t.TempDir()
	cache := cachetest.New(t, root, sourcecache.New)
	for i := range 2 {
		f, fInputs := fetcherWithCache(t, r, cache)
		if _, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), fInputs.readSet(helmChart("repo", "app-template", "1.0.0")), sourcecache.Policy{}); err != nil {
			t.Fatalf("Fetch %d: %v", i, err)
		}
	}
	if *indexHits != 2 {
		t.Errorf("zero-interval must re-fetch index each invocation: indexHits=%d, want 2", *indexHits)
	}
}

func TestResolveChart_DigestlessWarmSkipsIndex(t *testing.T) {
	t.Parallel()
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexHits, chartHits := startHelmRepoCounted(t, chartBytes, "") // digest-less index
	r := httpRepo(srv.URL)
	r.Interval = metav1.Duration{Duration: time.Hour}

	root := t.TempDir()
	cache := cachetest.New(t, root, sourcecache.New)
	cold, coldInputs := fetcherWithCache(t, r, cache)
	if _, err := cold.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), coldInputs.readSet(helmChart("repo", "app-template", "1.0.0")), sourcecache.Policy{}); err != nil {
		t.Fatalf("cold Fetch: %v", err)
	}
	warm, warmInputs := fetcherWithCache(t, r, cache)
	if _, err := warm.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), warmInputs.readSet(helmChart("repo", "app-template", "1.0.0")), sourcecache.Policy{}); err != nil {
		t.Fatalf("warm Fetch: %v", err)
	}
	if *indexHits != 1 {
		t.Errorf("digest-less warm re-fetched index: indexHits=%d, want 1", *indexHits)
	}
	if *chartHits != 1 {
		t.Errorf("digest-less chart hits=%d, want 1 (fresh requested chart)", *chartHits)
	}
}

func TestResolveChart_ExactVersionForever(t *testing.T) {
	t.Parallel()
	data := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexes, charts := startHelmRepoCounted(t, data, chartDigest(data))
	repo := httpRepo(srv.URL)
	repo.Interval = metav1.Duration{}
	c := cachetest.New(t, t.TempDir(), sourcecache.New)
	for range 2 {
		f, fInputs := fetcherWithCache(t, repo, c)
		f.disableChartDigestTracking = true
		if _, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), fInputs.readSet(helmChart("repo", "app-template", "1.0.0")), sourcecache.Policy{}); err != nil {
			t.Fatal(err)
		}
	}
	if *indexes != 1 || *charts != 1 {
		t.Fatalf("requests: index=%d chart=%d, want 1/1", *indexes, *charts)
	}
	f, fInputs := fetcherWithCache(t, repo, c)
	f.disableChartDigestTracking = true
	if _, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "*"), fInputs.readSet(helmChart("repo", "app-template", "*")), sourcecache.Policy{}); err != nil {
		t.Fatal(err)
	}
	if *indexes != 2 || *charts != 2 {
		t.Fatalf("mutable sibling requests: index=%d chart=%d, want 2/2", *indexes, *charts)
	}
	if repo.Interval != (metav1.Duration{}) {
		t.Fatalf("repository interval mutated: %v", repo.Interval)
	}
}
