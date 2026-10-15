package helmchart

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func httpRepo(url string) *manifest.HelmRepository {
	return &manifest.HelmRepository{
		Name: "repo", Namespace: "flux-system",
		URL:      url,
		Interval: metav1.Duration{Duration: time.Hour},
	}
}

func newHTTPFetcher(t *testing.T, r *manifest.HelmRepository) *Fetcher {
	t.Helper()
	return newHTTPFetcherWithSecrets(t, r, nil)
}

func newHTTPFetcherWithSecrets(t *testing.T, r *manifest.HelmRepository, secrets source.SecretGetter) *Fetcher {
	t.Helper()
	root := t.TempDir()
	c, err := sourcecache.New(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return New(secrets, func(_, _ string) *manifest.HelmRepository { return r }, nil, Config{Cache: c})
}

func TestFetchHTTPChart(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, hits := startHelmRepo(t, chartBytes, chartDigest(chartBytes))
	f := newHTTPFetcher(t, httpRepo(srv.URL))

	art, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if art.Kind != manifest.KindHelmChart {
		t.Errorf("art.Kind = %q, want HelmChart", art.Kind)
	}
	if art.Digest != chartDigest(chartBytes) {
		t.Errorf("art.Digest = %q, want %q", art.Digest, chartDigest(chartBytes))
	}
	if _, err := fs.Stat(art.FS, "chart.tgz"); err != nil {
		t.Errorf("chart.tgz missing from artifact %s: %v", art.Key, err)
	}

	if _, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{}); err != nil {
		t.Fatalf("second Fetch: %v", err)
	}
	if *hits != 1 {
		t.Errorf("chart downloaded %d times, want 1 (second fetch should reuse the cached chart)", *hits)
	}
}

func TestFetchIndex_InvalidResponseDoesNotPublish(t *testing.T) {
	for _, tc := range []struct{ name, data string }{
		{"empty", ""},
		{"malformed", "["},
		{"no api version", "entries: {}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if attempts.Add(1) == 1 {
					_, _ = io.WriteString(w, tc.data)
					return
				}
				_, _ = io.WriteString(w, helmRepoIndex(""))
			}))
			t.Cleanup(srv.Close)
			r := httpRepo(srv.URL)
			f := newHTTPFetcher(t, r)
			if _, err := f.fetchIndex(t.Context(), r, sourcecache.RetryConfig{}); err == nil {
				t.Fatal("accepted invalid index")
			}
			idx, err := f.fetchIndex(t.Context(), r, sourcecache.RetryConfig{})
			if err != nil {
				t.Fatalf("retry within repository interval: %v", err)
			}
			if _, err := idx.Get("app-template", "1.0.0"); err != nil {
				t.Fatal(err)
			}
			warm := New(nil, nil, nil, Config{Cache: f.cache})
			if _, err := warm.fetchIndex(t.Context(), r, sourcecache.RetryConfig{}); err != nil {
				t.Fatalf("cached index: %v", err)
			}
			if got := attempts.Load(); got != 2 {
				t.Fatalf("index fetched %d times, want 2", got)
			}
		})
	}
}

func TestFetchHTTPChart_DigestMismatch(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, _ := startHelmRepo(t, chartBytes, "deadbeefdeadbeef") // index advertises a wrong digest
	f := newHTTPFetcher(t, httpRepo(srv.URL))

	if _, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{}); err == nil {
		t.Fatal("expected digest-mismatch error, got nil")
	}
}

func TestFetchHTTPChart_ChartNotFound(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, _ := startHelmRepo(t, chartBytes, "")
	repo := httpRepo(srv.URL)
	f := newHTTPFetcher(t, repo)

	_, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "9.9.9"), sourcecache.RetryConfig{})
	if !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("err = %v, want ErrObjectNotFound for a missing version", err)
	}
	if !strings.HasPrefix(err.Error(), helmID(repo)) {
		t.Fatalf("failure identity: %v", err)
	}
}

func TestFetchHTTPChart_MissingSecret(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	f := newHTTPFetcher(t, r) // Secrets is nil
	_, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Fatalf("err = %v, want ErrMissingSecret", err)
	}
}

// TestFetchHTTPChart_CertSecretFailsLoud pins the security scope of
// ErrMissingSecret: certSecretRef carries TLS trust material, so an unwired
// SecretGetter (a config/wiring bug) must fail LOUD, NOT wrap ErrMissingSecret
// — otherwise --allow-missing-secrets would silently soft-skip a TLS failure.
// Mirrors source.ResolveCertSecret. (Contrast TestFetchHTTPChart_MissingSecret,
// where an auth secretRef legitimately soft-skips.)
func TestFetchHTTPChart_CertSecretFailsLoud(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	f := newHTTPFetcher(t, r) // Secrets is nil → wiring bug
	_, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
	if err == nil {
		t.Fatal("expected a loud certSecretRef error, got nil")
	}
	if errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("certSecretRef nil-getter wrapped ErrMissingSecret (would soft-skip); want loud: %v", err)
	}
}

// TestFetchHTTPChart_CertSecretNotFoundSoftSkips covers the other half: a
// certSecretRef whose Secret is genuinely absent IS the --allow-missing-secrets
// case (cert materialized live, not in git), so it wraps ErrMissingSecret —
// same sentinel git/oci/bucket use via source.ResolveCertSecret.
func TestFetchHTTPChart_CertSecretNotFoundSoftSkips(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return nil }) // getter wired, secret absent
	if _, ferr := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{}); !errors.Is(ferr, manifest.ErrMissingSecret) {
		t.Errorf("certSecretRef not-found should soft-skip (ErrMissingSecret); got: %v", ferr)
	}
}

// startHelmRepo serves a single-version index.yaml + the chart tarball.
// Returns the server and a download-hit counter.
func startHelmRepo(t *testing.T, chartBytes []byte, indexDigest string) (*httptest.Server, *int) {
	t.Helper()
	hits := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/index.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-yaml")
		_, _ = w.Write([]byte(helmRepoIndex(indexDigest)))
	})
	mux.HandleFunc("/app-template-1.0.0.tgz", func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(chartBytes)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &hits
}

func helmRepoIndex(digest string) string {
	digestLine := ""
	if digest != "" {
		digestLine = fmt.Sprintf("      digest: %s\n", digest)
	}
	return fmt.Sprintf(`apiVersion: v1
entries:
  app-template:
    - name: app-template
      version: 1.0.0
%s      urls:
        - app-template-1.0.0.tgz
`, digestLine)
}

// startHelmRepoSubpath serves the index.yaml and chart .tgz under a non-root
// path prefix (e.g. /charts), mirroring real repos like https://repos.emqx.io/charts
// and https://helm.ngc.nvidia.com/nvidia whose index entries carry RELATIVE urls.
// A relative url must resolve against the repo's base directory, so the prefix
// must be preserved — anything served at host root 404s.
func startHelmRepoSubpath(t *testing.T, prefix string, chartBytes []byte, indexDigest string) (*httptest.Server, *int) {
	t.Helper()
	hits := 0
	mux := http.NewServeMux()
	mux.HandleFunc(prefix+"/index.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-yaml")
		_, _ = w.Write([]byte(helmRepoIndex(indexDigest)))
	})
	mux.HandleFunc(prefix+"/app-template-1.0.0.tgz", func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(chartBytes)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &hits
}

// TestFetchHTTPChart_SubpathRelativeURL reproduces issue #796: a HelmRepository
// whose index lives under a path prefix (.../charts) and lists relative chart
// urls. Without a trailing slash on spec.url, a naive ResolveReference drops the
// last path segment and fetches from host root (404). Helm and Flux force a
// trailing slash before resolving; flate must match.
func TestFetchHTTPChart_SubpathRelativeURL(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")

	t.Run("no trailing slash", func(t *testing.T) {
		srv, hits := startHelmRepoSubpath(t, "/charts", chartBytes, chartDigest(chartBytes))
		f := newHTTPFetcher(t, httpRepo(srv.URL+"/charts"))
		art, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if *hits != 1 {
			t.Errorf("chart fetched %d times from subpath, want 1 (url resolved against host root?)", *hits)
		}
		if _, err := fs.Stat(art.FS, "chart.tgz"); err != nil {
			t.Errorf("chart.tgz missing from artifact %s: %v", art.Key, err)
		}
	})

	t.Run("trailing slash", func(t *testing.T) {
		srv, hits := startHelmRepoSubpath(t, "/charts", chartBytes, chartDigest(chartBytes))
		f := newHTTPFetcher(t, httpRepo(srv.URL+"/charts/"))
		if _, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{}); err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if *hits != 1 {
			t.Errorf("chart fetched %d times from subpath, want 1", *hits)
		}
	})
}

func chartDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func buildChartTarGz(t *testing.T, name, version string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	files := map[string]string{
		name + "/Chart.yaml":             "apiVersion: v2\nname: " + name + "\nversion: " + version + "\n",
		name + "/templates/_helpers.tpl": "",
	}
	for path, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: path, Typeflag: tar.TypeReg, Size: int64(len(body)), Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tw, body); err != nil {
			t.Fatal(err)
		}
	}
	_ = tw.Close()
	_ = gw.Close()
	return buf.Bytes()
}

func TestFetchHTTPChart_BasicAuth(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	requireAuth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if u, p, ok := r.BasicAuth(); !ok || u != "alice" || p != "hunter2" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			next(w, r)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/index.yaml", requireAuth(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(helmRepoIndex(chartDigest(chartBytes))))
	}))
	mux.HandleFunc("/app-template-1.0.0.tgz", requireAuth(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(chartBytes)
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	r := httpRepo(srv.URL)
	r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "hunter2"}}
	})

	if _, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
}

func TestFetchHTTPChart_BasicAuthOtherHost(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	for _, tc := range []struct {
		name            string
		passCredentials bool
		wantAuth        bool
	}{
		{name: "withheld by default", passCredentials: false, wantAuth: false},
		{name: "forwarded with passCredentials", passCredentials: true, wantAuth: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotAuth bool
			chartSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _, gotAuth = r.BasicAuth()
				_, _ = w.Write(chartBytes)
			}))
			t.Cleanup(chartSrv.Close)
			repoSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprintf(w, `apiVersion: v1
entries:
  app-template:
    - name: app-template
      version: 1.0.0
      digest: %s
      urls:
        - %s/app-template-1.0.0.tgz
`, chartDigest(chartBytes), chartSrv.URL)
			}))
			t.Cleanup(repoSrv.Close)

			r := httpRepo(repoSrv.URL)
			r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
			r.PassCredentials = tc.passCredentials
			f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "hunter2"}}
			})

			if _, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{}); err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			if gotAuth != tc.wantAuth {
				t.Errorf("chart host got basic auth = %v, want %v", gotAuth, tc.wantAuth)
			}
		})
	}
}

func TestFetchHTTPChart_CancellationDoesNotPublish(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		for _, endpoint := range []string{"/index.yaml", "/app-template-1.0.0.tgz"} {
			t.Run(backing+endpoint, func(t *testing.T) {
				chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
				started, canceled := make(chan struct{}), make(chan struct{})
				var attempts atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == endpoint && attempts.Add(1) == 1 {
						_, _ = io.WriteString(w, "partial")
						w.(http.Flusher).Flush()
						close(started)
						<-r.Context().Done()
						close(canceled)
						return
					}
					if r.URL.Path == "/index.yaml" {
						_, _ = io.WriteString(w, helmRepoIndex(chartDigest(chartBytes)))
						return
					}
					_, _ = w.Write(chartBytes)
				}))
				t.Cleanup(srv.Close)
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c, err := sourcecache.New(t.Context(), root)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = c.Close() })
				r := httpRepo(srv.URL)
				f := New(nil, func(_, _ string) *manifest.HelmRepository { return r }, nil, Config{Cache: c})
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				result := make(chan error, 1)
				go func() {
					_, err := f.Fetch(ctx, helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
					result <- err
				}()
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("download did not start")
				}
				cancel()
				select {
				case err := <-result:
					if !errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux) {
						t.Fatalf("canceled fetch: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("fetch ignored cancellation")
				}
				select {
				case <-canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("HTTP request outlived fetch")
				}
				art, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
				if err != nil {
					t.Fatal(err)
				}
				data, err := fs.ReadFile(art.FS, "chart.tgz")
				if err != nil || !bytes.Equal(data, chartBytes) || attempts.Load() != 2 {
					t.Fatalf("retry: bytes=%d attempts=%d err=%v", len(data), attempts.Load(), err)
				}
			})
		}
	}
}

func TestFetchIndex_FreshnessAndDigest(t *testing.T) {
	var hits atomic.Int32
	var version atomic.Int32
	version.Store(1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, strings.ReplaceAll(helmRepoIndex(""), "1.0.0", fmt.Sprintf("%d.0.0", version.Load())))
	}))
	defer srv.Close()
	r := httpRepo(srv.URL)
	f := newHTTPFetcher(t, r)
	first, err := f.fetchIndex(t.Context(), r, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.fetchIndex(t.Context(), r, sourcecache.RetryConfig{})
	if err != nil || second != first || hits.Load() != 1 {
		t.Fatalf("fresh memo=%p/%p, hits=%d, error=%v", first, second, hits.Load(), err)
	}
	strict := *r
	strict.Interval.Duration = 0
	same, err := f.fetchIndex(t.Context(), &strict, sourcecache.RetryConfig{})
	if err != nil || same != first || hits.Load() != 2 {
		t.Fatalf("same digest=%p/%p, hits=%d, error=%v", first, same, hits.Load(), err)
	}
	version.Store(2)
	updated, err := f.fetchIndex(t.Context(), &strict, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := updated.Get("app-template", "2.0.0"); err != nil || updated == first || hits.Load() != 3 {
		t.Fatalf("expired index not refreshed: hits=%d, error=%v", hits.Load(), err)
	}
}

func TestFetchHTTPChart_NestedIndexExhaustion(t *testing.T) {
	for _, attempts := range []int{1, 3} {
		t.Run(fmt.Sprint(attempts), func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.Header().Set("Content-Length", "20")
				_, _ = io.WriteString(w, "partial")
			}))
			defer srv.Close()
			f := newHTTPFetcher(t, httpRepo(srv.URL))
			_, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond})
			terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || !terminal.Attempted || hits.Load() != int32(attempts) || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("hits=%d, terminal=%+v, error=%v", hits.Load(), terminal, err)
			}
		})
	}
}

func TestFetchIndex_ConcurrentRefreshDuringBackoff(t *testing.T) {
	failed := make(chan struct{})
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Content-Length", "20")
			_, _ = io.WriteString(w, "partial")
			close(failed)
			return
		}
		_, _ = io.WriteString(w, strings.ReplaceAll(helmRepoIndex(""), "1.0.0", "2.0.0"))
	}))
	defer srv.Close()
	r := httpRepo(srv.URL)
	f := newHTTPFetcher(t, r)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.fetchIndex(ctx, r, sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour, MaxWait: time.Hour})
		done <- err
	}()
	<-failed
	updated, err := f.fetchIndex(t.Context(), r, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("interrupted refresh: %v", err)
	}
	if _, err := updated.Get("app-template", "2.0.0"); err != nil {
		t.Fatal(err)
	}
	memo, err := f.fetchIndex(t.Context(), r, sourcecache.RetryConfig{})
	if err != nil || memo != updated || hits.Load() != 2 {
		t.Fatalf("digest-bound memo=%p/%p, hits=%d, error=%v", updated, memo, hits.Load(), err)
	}
}
