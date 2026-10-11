package helmchart

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/cacheroot"
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
func fetcherWithCache(t *testing.T, r *manifest.HelmRepository, cache *sourcecache.Cache) *Fetcher {
	t.Helper()
	return New(nil, func(_, _ string) *manifest.HelmRepository { return r }, nil, Config{Cache: cache})
}

func TestResolveChart_DiskCacheSkipsIndexWarm(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexHits, chartHits := startHelmRepoCounted(t, chartBytes, chartDigest(chartBytes))
	r := httpRepo(srv.URL)
	r.Interval = metav1.Duration{Duration: time.Hour} // fresh window

	layout := cacheroot.New(t.TempDir())
	cache, err := sourcecache.New(t.Context(), layout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	cold := fetcherWithCache(t, r, cache)
	if _, err := cold.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0")); err != nil {
		t.Fatalf("cold Fetch: %v", err)
	}
	if *indexHits != 1 || *chartHits != 1 {
		t.Fatalf("cold: indexHits=%d chartHits=%d, want 1/1", *indexHits, *chartHits)
	}

	warm := fetcherWithCache(t, r, cache)
	if _, err := warm.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0")); err != nil {
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
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexHits, _ := startHelmRepoCounted(t, chartBytes, chartDigest(chartBytes))
	r := httpRepo(srv.URL)
	r.Interval = metav1.Duration{}

	layout := cacheroot.New(t.TempDir())
	cache, err := sourcecache.New(t.Context(), layout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	for i := range 2 {
		f := fetcherWithCache(t, r, cache)
		if _, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0")); err != nil {
			t.Fatalf("Fetch %d: %v", i, err)
		}
	}
	if *indexHits != 2 {
		t.Errorf("zero-interval must re-fetch index each invocation: indexHits=%d, want 2", *indexHits)
	}
}

func TestResolveChart_DigestlessWarmSkipsIndex(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexHits, chartHits := startHelmRepoCounted(t, chartBytes, "") // digest-less index
	r := httpRepo(srv.URL)
	r.Interval = metav1.Duration{Duration: time.Hour}

	layout := cacheroot.New(t.TempDir())
	cache, err := sourcecache.New(t.Context(), layout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	cold := fetcherWithCache(t, r, cache)
	if _, err := cold.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0")); err != nil {
		t.Fatalf("cold Fetch: %v", err)
	}
	warm := fetcherWithCache(t, r, cache)
	if _, err := warm.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0")); err != nil {
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
	data := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexes, charts := startHelmRepoCounted(t, data, chartDigest(data))
	repo := httpRepo(srv.URL)
	repo.Interval = metav1.Duration{}
	c, err := sourcecache.New(t.Context(), cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	for range 2 {
		f := fetcherWithCache(t, repo, c)
		f.disableChartDigestTracking = true
		if _, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0")); err != nil {
			t.Fatal(err)
		}
	}
	if *indexes != 1 || *charts != 1 {
		t.Fatalf("requests: index=%d chart=%d, want 1/1", *indexes, *charts)
	}
}
