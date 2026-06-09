package helmchart

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
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
func fetcherWithCache(t *testing.T, r *manifest.HelmRepository, cache *source.Cache, layout cacheroot.Layout) *Fetcher {
	t.Helper()
	f, err := New(nil, func(_, _ string) *manifest.HelmRepository { return r }, nil, cache, layout)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

// TestResolveChart_DiskCacheSkipsIndexWarm pins the fix for the warm==cold
// regression: a SECOND Fetcher sharing the same on-disk source.Cache (its
// in-process indexCache empty, as a fresh `flate` invocation has) resolves and
// returns the chart with ZERO index.yaml and ZERO chart hits — the live index
// fetch that made warm runs as slow as cold is gone.
func TestResolveChart_DiskCacheSkipsIndexWarm(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexHits, chartHits := startHelmRepoCounted(t, chartBytes, chartDigest(chartBytes))
	r := httpRepo(srv.URL)
	r.Interval = metav1.Duration{Duration: time.Hour} // fresh window

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)

	cold := fetcherWithCache(t, r, cache, layout)
	if _, err := cold.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0")); err != nil {
		t.Fatalf("cold Fetch: %v", err)
	}
	if *indexHits != 1 || *chartHits != 1 {
		t.Fatalf("cold: indexHits=%d chartHits=%d, want 1/1", *indexHits, *chartHits)
	}

	warm := fetcherWithCache(t, r, cache, layout)
	if _, err := warm.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0")); err != nil {
		t.Fatalf("warm Fetch: %v", err)
	}
	if *indexHits != 1 {
		t.Errorf("warm run re-fetched index.yaml: indexHits=%d, want 1 (disk resolve cache must skip it)", *indexHits)
	}
	if *chartHits != 1 {
		t.Errorf("warm run re-downloaded chart: chartHits=%d, want 1 (blob CAS dedup)", *chartHits)
	}
}

// TestResolveChart_StaleIntervalRefetches pins the freshness gate: a zero
// spec.interval makes the on-disk resolution always stale, so each fresh
// Fetcher re-fetches the index (the safe default for an interval-less repo).
func TestResolveChart_StaleIntervalRefetches(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexHits, _ := startHelmRepoCounted(t, chartBytes, chartDigest(chartBytes))
	r := httpRepo(srv.URL) // Interval == 0 → always stale

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	for i := range 2 {
		f := fetcherWithCache(t, r, cache, layout)
		if _, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0")); err != nil {
			t.Fatalf("Fetch %d: %v", i, err)
		}
	}
	if *indexHits != 2 {
		t.Errorf("zero-interval must re-fetch index each invocation: indexHits=%d, want 2", *indexHits)
	}
}

// TestResolveChart_DigestlessWarmSkipsIndex pins the digest-less edge: a mutable
// index entry (no digest) still skips the expensive index fetch on a warm run,
// but the chart .tgz is re-downloaded (no digest → no content-addressed dedup),
// which is the conservative, correct behavior.
func TestResolveChart_DigestlessWarmSkipsIndex(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, indexHits, chartHits := startHelmRepoCounted(t, chartBytes, "") // digest-less index
	r := httpRepo(srv.URL)
	r.Interval = metav1.Duration{Duration: time.Hour}

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	cold := fetcherWithCache(t, r, cache, layout)
	if _, err := cold.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0")); err != nil {
		t.Fatalf("cold Fetch: %v", err)
	}
	warm := fetcherWithCache(t, r, cache, layout)
	if _, err := warm.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0")); err != nil {
		t.Fatalf("warm Fetch: %v", err)
	}
	if *indexHits != 1 {
		t.Errorf("digest-less warm re-fetched index: indexHits=%d, want 1", *indexHits)
	}
	if *chartHits != 2 {
		t.Errorf("digest-less chart hits=%d, want 2 (no digest → no CAS dedup, re-download)", *chartHits)
	}
}
