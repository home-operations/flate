package helmchart

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
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
	f, cleanup := New(secrets, func(_, _ string) *manifest.HelmRepository { return r }, nil, Config{Cache: c})
	t.Cleanup(cleanup)
	return f
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
			root := t.TempDir()
			c, err := sourcecache.New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			f, cleanup := New(nil, nil, nil, Config{Cache: c})
			t.Cleanup(cleanup)
			if _, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, r), sourcecache.RetryConfig{}); err == nil {
				t.Fatal("accepted invalid index")
			}
			if _, ok := f.indexCache.Load("sha256:" + chartDigest([]byte(tc.data))); ok {
				t.Fatal("invalid index entered parsed memo")
			}
			idx, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, r), sourcecache.RetryConfig{})
			if err != nil {
				t.Fatalf("retry within repository interval: %v", err)
			}
			if _, err := idx.Get("app-template", "1.0.0"); err != nil {
				t.Fatal(err)
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			c, err = sourcecache.New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			warm, cleanup := New(nil, nil, nil, Config{Cache: c})
			t.Cleanup(cleanup)
			cached, err := warm.fetchIndex(t.Context(), preparedHTTP(t, warm, r), sourcecache.RetryConfig{})
			if err != nil {
				t.Fatalf("disk-cached index: %v", err)
			}
			if memo, ok := warm.indexCache.Load("sha256:" + chartDigest([]byte(helmRepoIndex("")))); !ok || memo != cached {
				t.Fatal("disk-cached index did not populate digest-bound memo")
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
	for _, state := range []string{"unwired", "absent", "wiped", "partial", "malformed"} {
		t.Run(state, func(t *testing.T) {
			r := httpRepo("https://charts.example")
			r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
			secret := &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "first"}}
			f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return secret })
			preparedHTTP(t, f, r)
			switch state {
			case "unwired":
				f.secrets = nil
			case "absent":
				secret = nil
			case "wiped":
				secret = &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "..PLACEHOLDER_password.."}}
			case "partial":
				secret = &manifest.Secret{StringData: map[string]any{"username": "alice"}}
			case "malformed":
				secret = &manifest.Secret{Data: map[string]any{"username": "invalid base64", "password": "invalid base64"}}
			}
			_, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
			if !errors.Is(err, manifest.ErrMissingSecret) {
				t.Fatalf("err = %v, want ErrMissingSecret after warming the client memo", err)
			}
		})
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
	var hits atomic.Int32
	requireAuth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if u, p, ok := r.BasicAuth(); !ok || u != "alice" || p != "hunter2" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			hits.Add(1)
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

	base := *r
	keys := make(map[string]string)
	for i, tc := range []struct {
		name   string
		change func(*manifest.HelmRepository)
	}{
		{"original", func(*manifest.HelmRepository) {}},
		{"secret name", func(r *manifest.HelmRepository) { r.SecretRef = &manifest.LocalObjectReference{Name: "other"} }},
		{"auth namespace", func(r *manifest.HelmRepository) { r.Namespace = "other" }},
		{"pass credentials", func(r *manifest.HelmRepository) { r.PassCredentials = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := base
			tc.change(&current)
			art, err := f.fetchHTTPChart(t.Context(), preparedHTTP(t, f, &current), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if previous, ok := keys[art.Key]; ok {
				t.Fatalf("%s reused %s acquisition", tc.name, previous)
			}
			keys[art.Key] = tc.name
			if got, want := hits.Load(), int32(2*(i+1)); got != want {
				t.Fatalf("index/chart requests=%d, want %d", got, want)
			}
		})
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
				f, cleanup := New(nil, func(_, _ string) *manifest.HelmRepository { return r }, nil, Config{Cache: c, Network: new(source.Network)})
				t.Cleanup(cleanup)
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
	first, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, r), sourcecache.RetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, r), sourcecache.RetryConfig{})
	if err != nil || second != first || hits.Load() != 1 {
		t.Fatalf("fresh memo=%p/%p, hits=%d, error=%v", first, second, hits.Load(), err)
	}
	strict := *r
	strict.Interval.Duration = 0
	same, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, &strict), sourcecache.RetryConfig{})
	if err != nil || same != first || hits.Load() != 2 {
		t.Fatalf("same digest=%p/%p, hits=%d, error=%v", first, same, hits.Load(), err)
	}
	version.Store(2)
	updated, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, &strict), sourcecache.RetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := updated.Get("app-template", "2.0.0"); err != nil || updated == first || hits.Load() != 3 {
		t.Fatalf("expired index not refreshed: hits=%d, error=%v", hits.Load(), err)
	}
	other := *r
	other.URL += "/other"
	shared, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, &other), sourcecache.RetryConfig{})
	if err != nil || shared != updated || hits.Load() != 4 {
		t.Fatalf("equal response digest across URLs=%p/%p, hits=%d, error=%v", updated, shared, hits.Load(), err)
	}
	key := "sha256:" + chartDigest([]byte(strings.ReplaceAll(helmRepoIndex(""), "1.0.0", "2.0.0")))
	if memo, ok := f.indexCache.Load(key); !ok || memo != updated {
		t.Fatal("index memo is not keyed by response digest")
	}
}

func TestHTTPRepositoryGet_ResponseRules(t *testing.T) {
	for _, status := range []int{200, 201, 204, 299, 300, 400, 401, 403, 404, 429, 499, 500, 599, 600} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			r := &httpRepository{HelmRepository: httpRepo("https://charts.invalid"), client: &http.Client{Transport: chartTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Body: io.NopCloser(strings.NewReader("content"))}, nil
			})}}
			var body bytes.Buffer
			_, err := r.get(t.Context(), "https://charts.invalid/chart.tgz", &body)
			if status == http.StatusOK {
				if err != nil || body.String() != "content" {
					t.Fatalf("successful response: body=%q error=%v", body.String(), err)
				}
				return
			}
			response, ok := errors.AsType[*fetcherr.ResponseError](err)
			want := fmt.Sprintf("failed to fetch https://charts.invalid/chart.tgz : %d %s", status, http.StatusText(status))
			if !ok || response.StatusCode != status || response.Op != http.MethodGet || err.Error() != want || body.Len() != 0 {
				t.Fatalf("response=%+v body=%q error=%v want=%q", response, body.String(), err, want)
			}
		})
	}
}

func TestFetchHTTPChart_TruncatedOutcomeReplay(t *testing.T) {
	for _, status := range []int{0, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		for _, target := range []string{"/index.yaml", "/app-template-1.0.0.tgz"} {
			for _, attempts := range []int{1, 3} {
				t.Run(fmt.Sprintf("%s/%d/status-%d", strings.TrimPrefix(target, "/"), attempts, status), func(t *testing.T) {
					var indexHits, chartHits atomic.Int32
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						if req.URL.Path == "/index.yaml" {
							indexHits.Add(1)
							if target != "/index.yaml" {
								_, _ = io.WriteString(w, helmRepoIndex(""))
								return
							}
						} else {
							chartHits.Add(1)
						}
						if status != 0 {
							w.WriteHeader(status)
							return
						}
						w.Header().Set("Content-Length", "20")
						_, _ = io.WriteString(w, "partial")
					}))
					defer srv.Close()
					r := httpRepo(srv.URL)
					lookup := func(_, _ string) *manifest.HelmRepository { return r }
					root := t.TempDir()
					c, err := sourcecache.New(t.Context(), root)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = c.Close() })
					f, cleanup := New(nil, lookup, nil, Config{Cache: c})
					t.Cleanup(cleanup)
					hc := helmChart("repo", "app-template", "1.0.0")
					retry := sourcecache.RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
					_, err = f.Fetch(t.Context(), hc, retry)
					terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
					if !ok || !terminal.Attempted || !errors.Is(err, manifest.ErrFlux) {
						t.Fatalf("initial terminal=%+v, error=%v", terminal, err)
					}
					if status == 0 {
						if transport, ok := errors.AsType[*url.Error](err); !ok || transport.Op != "Get" || transport.URL != srv.URL+target || !errors.Is(transport.Err, io.ErrUnexpectedEOF) {
							t.Errorf("initial transport=%+v, error=%v", transport, err)
						}
					} else if response, ok := errors.AsType[*fetcherr.ResponseError](err); !ok || response.StatusCode != status || response.Op != http.MethodGet || !strings.Contains(response.Error(), srv.URL+target) {
						t.Errorf("initial response=%+v, error=%v", response, err)
					}
					wantIndex, wantChart := int32(attempts), int32(0)
					if target != "/index.yaml" {
						wantIndex, wantChart = 1, int32(attempts)
					}
					if indexHits.Load() != wantIndex || chartHits.Load() != wantChart {
						t.Fatalf("initial requests: index=%d chart=%d, want index=%d chart=%d", indexHits.Load(), chartHits.Load(), wantIndex, wantChart)
					}
					cleanup()
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					warmCache, err := sourcecache.New(t.Context(), root)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = warmCache.Close() })
					if warmCache == c {
						t.Fatal("reopened cache reused the closed owner")
					}
					warm, cleanup := New(nil, lookup, nil, Config{Cache: warmCache})
					t.Cleanup(cleanup)
					_, err = warm.Fetch(t.Context(), hc, retry)
					terminal, ok = errors.AsType[*sourcecache.TerminalError](err)
					if !ok || terminal.Attempted || !errors.Is(err, manifest.ErrFlux) {
						t.Errorf("replayed terminal=%+v, error=%v", terminal, err)
					}
					if status == 0 {
						if transport, ok := errors.AsType[*url.Error](err); !ok || transport.Op != "Get" || transport.URL != "" || !errors.Is(transport.Err, io.ErrUnexpectedEOF) {
							t.Errorf("replayed transport=%+v, error=%v", transport, err)
						}
					} else if response, ok := errors.AsType[*fetcherr.ResponseError](err); !ok || response.StatusCode != status || response.Op != http.MethodGet || strings.Contains(response.Error(), srv.URL) {
						t.Errorf("replayed response=%+v, error=%v", response, err)
					}
					if indexHits.Load() != wantIndex || chartHits.Load() != wantChart {
						t.Errorf("replayed requests: index=%d chart=%d, want index=%d chart=%d", indexHits.Load(), chartHits.Load(), wantIndex, wantChart)
					}
				})
			}
		}
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
		_, err := f.fetchIndex(ctx, preparedHTTP(t, f, r), sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour, MaxWait: time.Hour})
		done <- err
	}()
	<-failed
	updated, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, r), sourcecache.RetryConfig{})
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
	memo, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, r), sourcecache.RetryConfig{})
	if err != nil || memo != updated || hits.Load() != 2 {
		t.Fatalf("digest-bound memo=%p/%p, hits=%d, error=%v", updated, memo, hits.Load(), err)
	}
	key := "sha256:" + chartDigest([]byte(strings.ReplaceAll(helmRepoIndex(""), "1.0.0", "2.0.0")))
	if cached, ok := f.indexCache.Load(key); !ok || cached != updated {
		t.Fatal("concurrent refresh did not publish its response digest")
	}
}

func TestFetchHTTPChart_NetworkRoutes(t *testing.T) {
	for _, route := range []string{"index", "chart"} {
		for _, authenticated := range []bool{false, true} {
			for _, customTLS := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/auth-%t/tls-%t", route, authenticated, customTLS), func(t *testing.T) {
					var network source.Network
					var indexHits, chartHits, dials atomic.Int32
					cause := &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}
					var target string
					latch := func() error {
						tr, _ := source.NewHTTPTransport(nil, nil)
						tr.Proxy = nil
						tr.DialContext = func(context.Context, string, string) (net.Conn, error) { dials.Add(1); return nil, cause }
						req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
						_, err := (&http.Client{Transport: network.HTTPTransport(tr)}).Do(req)
						terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
						if !ok || !terminal.Attempted {
							return fmt.Errorf("seed: %w", err)
						}
						return nil
					}
					handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if authenticated {
							user, password, ok := r.BasicAuth()
							if !ok || user != "user" || password != "pass" {
								t.Error("missing repository auth")
							}
						}
						if r.URL.Path != "/index.yaml" {
							chartHits.Add(1)
							w.WriteHeader(http.StatusInternalServerError)
							return
						}
						indexHits.Add(1)
						if err := latch(); err != nil {
							t.Error(err)
						}
						_, _ = io.WriteString(w, "apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    version: 1.0.0\n    apiVersion: v2\n    urls: [chart.tgz]\n")
					})
					var srv *httptest.Server
					if customTLS {
						srv = httptest.NewTLSServer(handler)
					} else {
						srv = httptest.NewServer(handler)
					}
					t.Cleanup(srv.Close)
					target = srv.URL
					r := httpRepo(target)
					secret := &manifest.Secret{StringData: map[string]any{"username": "user", "password": "pass"}}
					if authenticated {
						r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
					}
					if customTLS {
						r.CertSecretRef = &manifest.LocalObjectReference{Name: "auth"}
						secret.StringData["ca.crt"] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
					}
					c, err := sourcecache.New(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = c.Close() })
					f, cleanup := New(func(_, _ string) *manifest.Secret { return secret }, func(_, _ string) *manifest.HelmRepository { return r }, nil, Config{Cache: c, Network: &network})
					t.Cleanup(cleanup)
					if route == "index" {
						if err := latch(); err != nil {
							t.Fatal(err)
						}
					}
					_, err = f.Fetch(t.Context(), helmChart("repo", "chart", "1.0.0"), sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour})
					terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
					wantIndex := int32(0)
					if route == "chart" {
						wantIndex = 1
					}
					if !ok || terminal.Attempted || !errors.Is(err, cause) || dials.Load() != 1 || indexHits.Load() != wantIndex || chartHits.Load() != 0 {
						t.Fatalf("error=%v terminal=%+v dials=%d index=%d chart=%d", err, terminal, dials.Load(), indexHits.Load(), chartHits.Load())
					}
				})
			}
		}
	}
}

func preparedHTTP(t *testing.T, f *Fetcher, r *manifest.HelmRepository) *httpRepository {
	t.Helper()
	prepared, err := f.prepareHTTP(r)
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

func TestFetchHTTPChart_BasicAuth_SnapshotRotation(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	var fail atomic.Bool
	var hits, lookups atomic.Int32
	var password atomic.Value
	password.Store("first")
	var proxyPassword atomic.Value
	proxyPassword.Store("first")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.IsAbs() && r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("proxy:"+proxyPassword.Load().(string))) {
			t.Error("transport used a different proxy snapshot")
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		hits.Add(1)
		user, pass, _ := r.BasicAuth()
		if user != "alice" || pass != password.Load().(string) {
			t.Error("chart/index credential snapshot changed")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if fail.Load() {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			if err := conn.(*net.TCPConn).SetLinger(0); err != nil {
				t.Error(err)
			}
			_ = conn.Close()
			return
		}
		if strings.HasSuffix(r.URL.Path, "index.yaml") {
			_, _ = io.WriteString(w, helmRepoIndex(chartDigest(chartBytes)))
		} else {
			_, _ = w.Write(chartBytes)
		}
	})
	srv := httptest.NewUnstartedServer(handler)
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Listener = listener
	srv.Start()
	t.Cleanup(srv.Close)
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	r := httpRepo(srv.URL)
	r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "creds"}
	root := t.TempDir()
	ca := testutil.SelfSignedCA(t)
	var previous string
	for _, wave := range []string{"first", "password", "certificate", "proxy", "direct"} {
		t.Run(wave, func(t *testing.T) {
			if wave == "password" {
				password.Store("second")
			}
			if wave == "certificate" {
				ca = testutil.SelfSignedCA(t)
			}
			proxyURL := strings.Replace(proxy.URL, "://", "://proxy:first@", 1)
			if wave == "proxy" || wave == "direct" {
				proxyURL = strings.Replace(proxy.URL, "://", "://proxy:second@", 1)
				proxyPassword.Store("second")
			}
			t.Setenv("http_proxy", proxyURL)
			if wave == "direct" {
				t.Setenv("no_proxy", "*")
			}
			c, err := sourcecache.New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			f, cleanup := New(func(_, _ string) *manifest.Secret {
				lookups.Add(1)
				return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": password.Load().(string), "ca.crt": ca}}
			}, func(_, _ string) *manifest.HelmRepository { return r }, nil, Config{Cache: c})
			t.Cleanup(cleanup)
			before := hits.Load()
			art, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{Attempts: 3})
			if err != nil {
				t.Fatal(err)
			}
			if art.Key == previous || hits.Load()-before != 2 {
				t.Fatalf("rotation reused chart/index: requests=%d", hits.Load()-before)
			}
			previous = art.Key
			before = hits.Load()
			if _, err := f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{}); err != nil || hits.Load() != before {
				t.Fatalf("warm chart missed: %v", err)
			}
			fail.Store(true)
			failed := *r
			failed.Interval.Duration = 0
			_, err = f.fetchHTTPChart(t.Context(), preparedHTTP(t, f, &failed), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
			terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || !terminal.Attempted {
				t.Fatalf("failed snapshot: %v", err)
			}
			fail.Store(false)
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			c, err = sourcecache.New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			f.cache = c
			before = hits.Load()
			_, err = f.Fetch(t.Context(), helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{})
			terminal, ok = errors.AsType[*sourcecache.TerminalError](err)
			if !ok || terminal.Attempted || hits.Load() != before {
				t.Fatalf("reopened snapshot replay: %v", err)
			}

		})
	}
	if lookups.Load() != 20 {
		t.Fatalf("chart/index reread material: %d", lookups.Load())
	}
	if err := fs.WalkDir(os.DirFS(root), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		data, err := fs.ReadFile(os.DirFS(root), path)
		if err != nil {
			return err
		}
		for _, secret := range []string{"proxy:first@", "proxy:second@", "BEGIN CERTIFICATE"} {
			if strings.Contains(string(data), secret) {
				t.Errorf("credential material persisted in %s", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type chartTransport func(*http.Request) (*http.Response, error)

func (f chartTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestFetchHTTPChart_NestedIndexExhaustion_DueWave(t *testing.T) {
	t.Setenv("NO_PROXY", "*")
	t.Setenv("no_proxy", "*")
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	for _, callers := range []int{2, 4} {
		for _, result := range []string{"success", "failure", "cold child", "cached child"} {
			t.Run(fmt.Sprintf("callers-%d/%s", callers, result), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					root := t.TempDir()
					c, err := sourcecache.New(t.Context(), root)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = c.Close() }()
					repo := httpRepo("https://charts.invalid")
					repo.Interval.Duration = 0
					f, cleanup := New(nil, nil, nil, Config{Cache: c})
					t.Cleanup(cleanup)
					prepared := preparedHTTP(t, f, repo)
					outer := struct{ Repository, Chart, Transport any }{repo.Content(), helmChart("repo", "app-template", "1.0.0").Content(), prepared.material}
					inner := struct{ Repository, Transport any }{repo.Content(), prepared.material}
					cause := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
					seed := func(content any) {
						_, _, _, err := c.Blob(t.Context(), sourcecache.BlobKey{Content: content}, sourcecache.Policy{}, func(context.Context, io.Writer) (ocispec.Descriptor, error) { return ocispec.Descriptor{}, cause })
						terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
						if !ok || !terminal.Attempted {
							t.Fatalf("seed: %v", err)
						}
					}
					seed(outer)
					if result == "cached child" {
						time.Sleep(100 * time.Millisecond)
					}
					if result != "cold child" {
						seed(inner)
					}
					before, err := os.ReadFile(filepath.Join(root, "oci", "index.json"))
					if err != nil {
						t.Fatal(err)
					}
					delay := 750 * time.Millisecond
					if result == "cached child" {
						delay -= 100 * time.Millisecond
					}
					time.Sleep(delay)
					gate := make(chan struct{})
					var indexCalls, chartCalls atomic.Int32
					prepared.client.Transport = chartTransport(func(req *http.Request) (*http.Response, error) {
						body := string(chartBytes)
						if strings.HasSuffix(req.URL.Path, "index.yaml") {
							indexCalls.Add(1)
							<-gate
							if result == "failure" || result == "cold child" {
								return nil, cause
							}
							body = helmRepoIndex(chartDigest(chartBytes))
						} else {
							chartCalls.Add(1)
						}
						return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
					})
					var wg sync.WaitGroup
					results := make(chan error, callers)
					for range callers {
						wg.Go(func() {
							_, err := f.fetchHTTPChart(t.Context(), prepared, helmChart("repo", "app-template", "1.0.0"), sourcecache.RetryConfig{Attempts: 5, MinWait: time.Nanosecond, MaxWait: time.Nanosecond})
							results <- err
						})
					}
					synctest.Wait()
					close(gate)
					wg.Wait()
					close(results)
					attempted := 0
					for err := range results {
						if result == "success" {
							if err != nil {
								t.Fatal(err)
							}
							continue
						}
						terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
						if !ok || !errors.Is(err, syscall.ECONNRESET) {
							t.Fatalf("nested cause: %v", err)
						}
						if terminal.Attempted {
							attempted++
						}
					}
					wantIndex, wantChart, wantAttempted := int32(1), int32(0), 1
					if result == "success" {
						wantChart, wantAttempted = 1, 0
					}
					if result == "cached child" {
						wantIndex, wantAttempted = 0, 0
					}
					if indexCalls.Load() != wantIndex || chartCalls.Load() != wantChart || attempted != wantAttempted {
						t.Fatalf("index=%d chart=%d attempted=%d", indexCalls.Load(), chartCalls.Load(), attempted)
					}
					if result == "cached child" {
						after, err := os.ReadFile(filepath.Join(root, "oci", "index.json"))
						if err != nil || !bytes.Equal(before, after) {
							t.Fatal("cached child advanced outer sequence")
						}
					}
				})
			})
		}
	}
}
