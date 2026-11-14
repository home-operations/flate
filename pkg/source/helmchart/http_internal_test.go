package helmchart

import (
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func newTestNetwork(tb testing.TB) *source.Network {
	tb.Helper()
	network := new(source.Network)
	tb.Cleanup(network.Close)
	return network
}

func httpRepo(url string) *manifest.HelmRepository {
	return &manifest.HelmRepository{
		Name: "repo", Namespace: "flux-system",
		URL:      url,
		Interval: metav1.Duration{Duration: time.Hour},
	}
}

func newHTTPFetcher(t *testing.T, r *manifest.HelmRepository) (*Fetcher, testInputs) {
	t.Helper()
	return newHTTPFetcherWithSecrets(t, r, nil)
}

func newHTTPFetcherWithSecrets(
	t *testing.T,
	r *manifest.HelmRepository,
	secrets source.SecretGetter,
) (*Fetcher, testInputs) {
	t.Helper()
	root := t.TempDir()
	c := cachetest.New(t, root, sourcecache.New)
	fInputs := testInputs{secrets: secrets, repos: func(_, _ string) *manifest.HelmRepository { return r }}
	f := New(nil, Config{Network: newTestNetwork(t), Cache: c})

	return f, fInputs
}

func TestFetchHTTPChart(t *testing.T) {
	t.Parallel()

	charts := map[string][]byte{
		"app-template": buildChartTarGz(t, "app-template", "1.0.0"),
		"other":        buildChartTarGz(t, "other", "1.0.0"),
	}
	var indexHits, chartHits atomic.Int32
	var index strings.Builder
	index.WriteString("apiVersion: v1\nentries:\n")
	for _, name := range []string{"app-template", "other"} {
		fmt.Fprintf(&index,
			"  %s:\n    - name: %s\n      version: 1.0.0\n      digest: %s\n      urls: [%s-1.0.0.tgz]\n",
			name, name, chartDigest(charts[name]), name)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/index.yaml" {
			indexHits.Add(1)
			_, _ = io.WriteString(w, index.String())
			return
		}
		name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), "-1.0.0.tgz")
		data, ok := charts[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		chartHits.Add(1)
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)
	f, fInputs := newHTTPFetcher(t, httpRepo(srv.URL))
	for _, wave := range []string{"cold", "source-warm"} {
		testFetchHTTPChartWave(
			t,
			&testFetchHTTPChartWaveInputs{
				chartHits: &chartHits,
				charts:    &charts,
				f:         &f,
				fInputs:   &fInputs,
				indexHits: &indexHits,
				wave:      &wave,
			},
		)
	}
}

func TestFetchIndex_InvalidResponseDoesNotPublish(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, data string }{
		{"empty", ""},
		{"malformed", "["},
		{"no api version", "entries: {}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetchIndexInvalidResponseDoesNotPublishCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestFetchHTTPChart_DigestMismatch(t *testing.T) {
	t.Parallel()
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	repository := startHelmRepo(t, chartBytes, "deadbeefdeadbeef")
	// index advertises a wrong digest
	f, fInputs := newHTTPFetcher(t, httpRepo(repository.server.URL))
	{
		_, err := fetchtest.Fetch(
			context.Background(),
			f.Fetch,
			helmChart("repo", "app-template", "1.0.0"),
			fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
			&sourcecache.Policy{},
			nil,
		)
		assert.FatalIf(t, err == nil, "%s", "expected digest-mismatch error, got nil")
	}
}

//nolint:paralleltest // The test replaces the process HTTP transport and restores it afterward.
//nolint:paralleltest // The test replaces the process HTTP transport and restores it afterward.
func TestFetchHTTPChart_ChartNotFound(t *testing.T) {
	helmPipe(t, func(request *http.Request) (*http.Response, error) {
		return helmReply(request, http.StatusOK, []byte("apiVersion: v1\nentries: {}\n")), nil
	})
	repository := httpRepo("https://charts.example.com")
	f, inputs := newHTTPFetcher(t, repository)
	chart := helmChart("repo", "nginx", "1.2.3")
	_, err := fetchtest.Fetch(t.Context(), f.Fetch, chart, inputs.readSet(chart), &sourcecache.Policy{}, nil)
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrObjectNotFound) || !errors.Is(err, manifest.ErrFlux),
		"missing chart classification: %v",
		err,
	)
	assert.FatalIf(
		t,
		err.Error() != "chart nginx 1.2.3 not found in https://charts.example.com",
		"chart cause: %v",
		err,
	)
	assert.FatalIf(t, !strings.Contains(manifest.ErrorDetails(err), "no chart name found"),
		"lost lookup detail: %s", manifest.ErrorDetails(err))
}

func TestFetchHTTPChart_SecretReferences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, state string
		cert        bool
		missing     bool
		input       bool
	}{
		{name: "auth unwired", state: "unwired", missing: true},
		{name: "auth absent", state: "absent", missing: true},
		{name: "auth empty", state: "empty", missing: true},
		{name: "auth partial", state: "partial", missing: true},
		{name: "auth malformed", state: "malformed", input: true},
		{name: "auth malformed password", state: "malformed password", input: true},
		{name: "auth nonscalar", state: "nonscalar", input: true},
		{name: "auth empty username malformed password", state: "empty malformed", input: true},
		{name: "auth malformed username empty password", state: "malformed empty", input: true},
		{name: "cert unwired", state: "unwired", cert: true},
		{name: "cert absent", state: "absent", cert: true, missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetchHTTPChartSecretReferencesCase1(t, &tc)
		})
	}
}

func testFetchHTTPChartSecretReferencesCase1(t *testing.T, tc *struct {
	name    string
	state   string
	cert    bool
	missing bool
	input   bool
},
) {
	t.Helper()

	r := httpRepo("https://charts.example")
	secret := &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "first"}}
	f, fInputs := newHTTPFetcherWithSecrets(t, r, func(_, _ string) store.ReferenceFacts {
		secretInput := secret
		if secretInput == nil {
			return store.ReferenceFacts{}
		}
		return store.ReferenceFacts{Data: secretInput, Original: secretInput}
	})
	if tc.cert {
		r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	} else {
		r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
		preparedHTTP(t, f, fInputs, r)
	}
	switch tc.state {
	case "unwired":
		fInputs.secrets = nil
	case "absent":
		secret = nil
	case "empty":
		secret = &manifest.Secret{StringData: map[string]any{"username": "alice", "password": ""}}
	case "partial":
		secret = &manifest.Secret{StringData: map[string]any{"username": "alice"}}
	case "malformed":
		secret = &manifest.Secret{
			Data:       map[string]any{"username": "credential-payload!"},
			StringData: map[string]any{"password": "first"},
		}
	case "malformed password":
		secret = &manifest.Secret{
			Data:       map[string]any{"password": "credential-payload!"},
			StringData: map[string]any{"username": "alice"},
		}
	case "nonscalar":
		secret = &manifest.Secret{
			StringData: map[string]any{"username": []any{"credential-payload"}, "password": "first"},
		}
	case "empty malformed":
		secret = &manifest.Secret{
			StringData: map[string]any{"username": ""},
			Data:       map[string]any{"password": "credential-payload!"},
		}
	case "malformed empty":
		secret = &manifest.Secret{
			Data:       map[string]any{"username": "credential-payload!"},
			StringData: map[string]any{"password": ""},
		}
	}
	f.cache = nil
	var artifact *store.SourceArtifact
	var err error
	if tc.state == "unwired" {
		_, err = (&operation{Fetcher: f}).resolvedHTTP(r)
	} else {
		artifact, err = fetchtest.Fetch(
			t.Context(),
			f.Fetch,
			helmChart("repo", "app-template", "1.0.0"),
			fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
			&sourcecache.Policy{},
			nil,
		)
	}
	assert.FatalIf(t, err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing || artifact != nil,
		"missing=%t, artifact=%v, error=%v", tc.missing, artifact, err)

	if tc.input {
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
			"expected hard input error, got %v", err)
		assert.ErrorIf(t, strings.Contains(err.Error(), "HelmRepository flux-system/repo") ||
			!strings.Contains(err.Error(), "secret flux-system/creds"),
			"input error lost source or Secret identity: %v", err)
		assert.ErrorIf(
			t,
			strings.Contains(err.Error(), "credential-payload"),
			"%s",
			"input error exposed credential payload",
		)
	}
	if tc.missing && tc.state != "unwired" && !tc.cert {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok || missing.Owner != r.Named() || missing.Secret.Name != "creds" ||
			missing.Secret.Namespace != r.Namespace ||
			missing.RetryAmbient == nil,
			"missing error lost source identity or ambient continuation: %v", err)
	}
}

// startHelmRepo serves a single-version index.yaml + the chart tarball.
// Returns the server and a download-hit counter.
func startHelmRepo(t *testing.T, chartBytes []byte, indexDigest string) helmRepository {
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
	return helmRepository{server: srv, hits: &hits}
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
func startHelmRepoSubpath(t *testing.T, prefix string, chartBytes []byte, indexDigest string) helmRepository {
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
	return helmRepository{server: srv, hits: &hits}
}

// TestFetchHTTPChart_SubpathRelativeURL reproduces issue #796: a HelmRepository
// whose index lives under a path prefix (.../charts) and lists relative chart
// urls. Without a trailing slash on spec.url, a naive ResolveReference drops the
// last path segment and fetches from host root (404). Helm and Flux force a
// trailing slash before resolving; flate must match.
func TestFetchHTTPChart_SubpathRelativeURL(t *testing.T) {
	t.Parallel()
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")

	t.Run("no trailing slash", func(t *testing.T) {
		t.Parallel()
		repository := startHelmRepoSubpath(t, "/charts", chartBytes, chartDigest(chartBytes))

		f, fInputs := newHTTPFetcher(t, httpRepo(repository.server.URL+"/charts"))
		art, err := fetchtest.Fetch(
			context.Background(),
			f.Fetch,
			helmChart("repo", "app-template", "1.0.0"),
			fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
			&sourcecache.Policy{},
			nil,
		)
		assert.FatalIf(t, err != nil, "Fetch: %v", err)
		assert.ErrorIf(t, *repository.hits != 1,
			"chart fetched %d times from subpath, want 1 (url resolved against host root?)", *repository.hits)

		if _, err := fs.Stat(art.FS, "chart.tgz"); err != nil {
			t.Errorf("chart.tgz missing from artifact %s: %v", art.Key, err)
		}
	})

	t.Run("trailing slash", func(t *testing.T) {
		t.Parallel()
		repository2 := startHelmRepoSubpath(t, "/charts", chartBytes, chartDigest(chartBytes))

		f, fInputs := newHTTPFetcher(t, httpRepo(repository2.server.URL+"/charts/"))
		{
			_, err := fetchtest.Fetch(
				context.Background(),
				f.Fetch,
				helmChart("repo", "app-template", "1.0.0"),
				fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
				&sourcecache.Policy{},
				nil,
			)
			assert.FatalIf(t, err != nil, "Fetch: %v", err)
		}
		assert.ErrorIf(t, *repository2.hits != 1,
			"chart fetched %d times from subpath, want 1", *repository2.hits)
	})
}

func chartDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func buildChartTarGz(t *testing.T, name, version string) []byte {
	t.Helper()
	return testutil.TarGz(t, map[string]string{
		name + "/Chart.yaml":             "apiVersion: v2\nname: " + name + "\nversion: " + version + "\n",
		name + "/templates/_helpers.tpl": "",
	}, 0o644, gzip.DefaultCompression)
}

//nolint:paralleltest,tparallel // Subtests compare cumulative request counts and acquisition keys in order.
func TestFetchHTTPChart_BasicAuth(t *testing.T) {
	t.Parallel()

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
	f, fInputs := newHTTPFetcherWithSecrets(t, r, func(_, _ string) store.ReferenceFacts {
		secretInput := &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "hunter2"}}

		return store.ReferenceFacts{Data: secretInput, Original: secretInput}
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
			art, err := f.fetchHTTPChart(
				t.Context(),
				preparedHTTP(t, f, fInputs, &current),
				helmChart("repo", "app-template", "1.0.0"),
				&sourcecache.Policy{},
			)
			assert.NoError(t, err)

			if previous, ok := keys[art.Key]; ok {
				t.Fatalf("%s reused %s acquisition", tc.name, previous)
			}
			keys[art.Key] = tc.name
			{
				got, want := hits.Load(), int32(2*(i+1))
				assert.FatalIf(t, got != want, "index/chart requests=%d, want %d", got, want)
			}
		})
	}
}

func TestFetchHTTPChart_BasicAuthOtherHost(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
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
			f, fInputs := newHTTPFetcherWithSecrets(t, r, func(_, _ string) store.ReferenceFacts {
				secretInput := &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "hunter2"}}

				return store.ReferenceFacts{Data: secretInput, Original: secretInput}
			})

			{
				_, err := fetchtest.Fetch(
					context.Background(),
					f.Fetch,
					helmChart("repo", "app-template", "1.0.0"),
					fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
					&sourcecache.Policy{},
					nil,
				)
				assert.FatalIf(t, err != nil, "Fetch: %v", err)
			}
			assert.ErrorIf(t, gotAuth != tc.wantAuth,
				"chart host got basic auth = %v, want %v", gotAuth, tc.wantAuth)
		})
	}
}

func TestFetchHTTPChart_CancellationDoesNotPublish(t *testing.T) {
	t.Parallel()

	for _, backing := range []string{"memory", "disk"} {
		for _, endpoint := range []string{"/index.yaml", "/app-template-1.0.0.tgz"} {
			t.Run(backing+endpoint, func(t *testing.T) {
				t.Parallel()

				testFetchHTTPChartCancellationDoesNotPublishCase(
					t,
					&backing, &endpoint,
				)
			},
			)
		}
	}
}

func TestFetchIndex_FreshnessAndDigest(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	var version atomic.Int32
	version.Store(1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, strings.ReplaceAll(helmRepoIndex(""), "1.0.0", fmt.Sprintf("%d.0.0", version.Load())))
	}))
	defer srv.Close()
	r := httpRepo(srv.URL)
	f, fInputs := newHTTPFetcher(t, r)
	first, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, fInputs, r), &sourcecache.Policy{})
	assert.NoError(t, err)

	second, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, fInputs, r), &sourcecache.Policy{})
	assert.FatalIf(t, err != nil || second != first || hits.Load() != 1,
		"fresh memo=%p/%p, hits=%d, error=%v", first, second, hits.Load(), err)

	strict := *r
	strict.Interval.Duration = 0
	same, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, fInputs, &strict), &sourcecache.Policy{})
	assert.FatalIf(t, err != nil || same != first || hits.Load() != 2,
		"same digest=%p/%p, hits=%d, error=%v", first, same, hits.Load(), err)

	version.Store(2)
	updated, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, fInputs, &strict), &sourcecache.Policy{})
	assert.NoError(t, err)

	if _, getErr := updated.Get("app-template", "2.0.0"); getErr != nil || updated == first || hits.Load() != 3 {
		t.Fatalf("expired index not refreshed: hits=%d, error=%v", hits.Load(), getErr)
	}
	other := *r
	other.URL += "/other"
	shared, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, fInputs, &other), &sourcecache.Policy{})
	assert.FatalIf(t, err != nil || shared != updated || hits.Load() != 4,
		"equal response digest across URLs=%p/%p, hits=%d, error=%v", updated, shared, hits.Load(), err)

	key := "sha256:" + chartDigest([]byte(strings.ReplaceAll(helmRepoIndex(""), "1.0.0", "2.0.0")))
	{
		memo, ok := f.indexCache.Load(key)
		assert.FatalIf(t, !ok || memo != updated, "%s", "index memo is not keyed by response digest")
	}
}

func TestHTTPRepositoryGet_ResponseRules(t *testing.T) {
	t.Parallel()
	for _, status := range []int{200, 201, 204, 206, 299, 300, 304, 400, 401, 403, 404, 429, 499, 500, 599, 600} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()
			r := &httpRepository{
				HelmRepository: httpRepo("https://charts.invalid"),
				client: &http.Client{Transport: chartTransport(func(*http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: status,
						Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
						Body:       io.NopCloser(strings.NewReader("content")),
					}, nil
				})},
			}
			var body bytes.Buffer
			_, err := r.get(t.Context(), "https://charts.invalid/chart.tgz", httpDownload{Writer: &body})
			if status == http.StatusOK {
				assert.FatalIf(t, err != nil || body.String() != "content",
					"successful response: body=%q error=%v", body.String(), err)

				return
			}
			response, ok := errors.AsType[*fetcherr.ResponseError](err)
			want := fmt.Sprintf(
				"failed to fetch https://charts.invalid/chart.tgz : %d %s",
				status,
				http.StatusText(status),
			)
			assert.FatalIf(
				t,
				!ok || response.StatusCode != status || response.Op != http.MethodGet || err.Error() != want ||
					body.Len() != 0,
				"response=%+v body=%q error=%v want=%q",
				response,
				body.String(),
				err,
				want,
			)
		})
	}
}

func TestFetchHTTPChart_TruncatedOutcomeReplay(t *testing.T) {
	t.Parallel()

	for _, status := range []int{0, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		for _, target := range []string{"/index.yaml", "/app-template-1.0.0.tgz"} {
			for _, attempts := range []int{1, 3} {
				t.Run(
					fmt.Sprintf("%s/%d/status-%d", strings.TrimPrefix(target, "/"), attempts, status),
					func(t *testing.T) {
						t.Parallel()

						testFetchHTTPChartTruncatedOutcomeReplayCase(
							t,

							&status,
							&target,
							&attempts,
						)
					},
				)
			}
		}
	}
}

func TestFetchIndex_ConcurrentRefreshDuringBackoff(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		failed := make(chan struct{})
		var hits atomic.Int32
		r := httpRepo("https://charts.invalid")
		f, fInputs := newHTTPFetcher(t, r)
		prepared := preparedHTTP(t, f, fInputs, r)
		prepared.client.Transport = chartTransport(func(req *http.Request) (*http.Response, error) {
			body := io.Reader(strings.NewReader(strings.ReplaceAll(helmRepoIndex(""), "1.0.0", "2.0.0")))
			if hits.Add(1) == 1 {
				body = io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(io.ErrUnexpectedEOF))
				close(failed)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(body),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := f.fetchIndex(
				ctx,
				prepared,
				&sourcecache.Policy{
					Retry: sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour, MaxWait: time.Hour},
				},
			)
			done <- err
		}()
		<-failed
		synctest.Wait()
		updated, err := f.fetchIndex(t.Context(), prepared, &sourcecache.Policy{})
		assert.NoError(t, err)

		cancel()
		{
			innerErr := <-done
			assert.FatalIf(
				t,
				!errors.Is(innerErr, context.Canceled) || !errors.Is(innerErr, io.ErrUnexpectedEOF),
				"interrupted refresh: %v",
				innerErr,
			)
		}
		{
			_, getErr2 := updated.Get("app-template", "2.0.0")
			assert.NoError(t, getErr2)
		}

		memo, err := f.fetchIndex(t.Context(), prepared, &sourcecache.Policy{})
		assert.FatalIf(t, err != nil || memo != updated || hits.Load() != 2,
			"digest-bound memo=%p/%p, hits=%d, error=%v", updated, memo, hits.Load(), err)

		key := "sha256:" + chartDigest([]byte(strings.ReplaceAll(helmRepoIndex(""), "1.0.0", "2.0.0")))
		{
			cached, ok := f.indexCache.Load(key)
			assert.FatalIf(t, !ok || cached != updated, "%s", "concurrent refresh did not publish its response digest")
		}
	})
}

type helmRepository struct {
	server *httptest.Server
	hits   *int
}

type testFetchHTTPChartWaveInputs struct {
	chartHits *atomic.Int32
	charts    *map[string][]byte
	f         **Fetcher
	fInputs   *testInputs
	indexHits *atomic.Int32
	wave      *string
}

func testFetchHTTPChartWave(t *testing.T, captures *testFetchHTTPChartWaveInputs) {
	t.Helper()
	var work [2][2]manifest.WorkRecorder
	var artifacts [2][2]*store.SourceArtifact
	var consumers sync.WaitGroup
	for cluster, clusterName := range []manifest.ClusterID{"alpha", "omega"} {
		for chart, name := range []string{"app-template", "other"} {
			work[cluster][chart].Key = manifest.WorkKey{
				Tree:   manifest.TreeID{Cluster: clusterName, Side: manifest.TreeHead},
				Owner:  testutil.HelmReleaseID("flux-system", name),
				Source: manifest.BootstrapSourceID,
			}
			consumers.Go(func() {
				ctx := t.Context()
				accounting := manifest.AcquisitionWork{Scope: work[cluster][chart].Scope()}
				art, err := fetchtest.Fetch(
					ctx,
					(*captures.f).Fetch,
					helmChart("repo", name, "1.0.0"),
					captures.fInputs.readSet(
						helmChart("repo", name, "1.0.0"),
					),
					&sourcecache.Policy{Work: &accounting},
					nil,
				)
				if err != nil {
					t.Errorf("Fetch %s: %v", name, err)
					return
				}
				assertHTTPChartArtifact(t, art, name, (*captures.charts)[name])
				artifacts[cluster][chart] = art
			})
		}
	}
	consumers.Wait()
	assertHTTPChartWork(t, &work, &artifacts, (*captures.wave))
	t.Logf("index requests=%d chart requests=%d", captures.indexHits.Load(), captures.chartHits.Load())
	if captures.indexHits.Load() != 1 || captures.chartHits.Load() != int32(len((*captures.charts))) {
		t.Errorf(
			"requests: index=%d charts=%d, want 1/%d",
			captures.indexHits.Load(),
			captures.chartHits.Load(),
			len((*captures.charts)),
		)
	}
}
