package helmchart

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testFetchHTTPChartSecretReferencesCase(t *testing.T, tc *struct {
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
	f, fInputs := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return secret })
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
	case "wiped":
		secret = &manifest.Secret{
			StringData: map[string]any{"username": "alice", "password": "..PLACEHOLDER_password.."},
		}
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
	case "wiped malformed":
		secret = &manifest.Secret{
			StringData: map[string]any{"username": "..PLACEHOLDER_username.."},
			Data:       map[string]any{"password": "credential-payload!"},
		}
	case "malformed wiped":
		secret = &manifest.Secret{
			Data:       map[string]any{"username": "credential-payload!"},
			StringData: map[string]any{"password": "..PLACEHOLDER_password.."},
		}
	}
	f.cache = nil
	var artifact *store.SourceArtifact
	var err error
	if tc.state == "unwired" {
		_, err = (&operation{Fetcher: f}).prepareHTTP(r)
	} else {
		artifact, err = f.Fetch(
			t.Context(),
			helmChart("repo", "app-template", "1.0.0"),
			fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
			sourcecache.Policy{},
		)
	}
	if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing || artifact != nil {
		t.Fatalf("missing=%t, artifact=%v, error=%v", tc.missing, artifact, err)
	}
	if tc.input {
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
			"expected hard input error, got %v",
			err,
		)
		assert.ErrorIf(t, strings.Contains(err.Error(), "HelmRepository flux-system/repo") ||
			!strings.Contains(
				err.Error(),
				"secret flux-system/creds",
			), "input error lost source or Secret identity: %v", err)
		if strings.Contains(err.Error(), "credential-payload") {
			t.Error("input error exposed credential payload")
		}
	}
	if tc.missing && tc.state != "unwired" && !tc.cert {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok || missing.Owner != r.Named() || missing.Secret.Name != "creds" ||
			missing.Secret.Namespace != r.Namespace ||
			missing.RetryAmbient == nil, "missing error lost source identity or ambient continuation: %v", err)
	}
}

func testFetchHTTPChartCancellationDoesNotPublishCase(
	t *testing.T, backing *string,
	endpoint *string,
) {
	t.Helper()
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	started, canceled := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == (*endpoint) && attempts.Add(1) == 1 {
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
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, sourcecache.New)
	r := httpRepo(srv.URL)
	fInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return r }}
	f := New(nil, Config{Cache: c, Network: newTestNetwork(t)})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := f.Fetch(
			ctx,
			helmChart("repo", "app-template", "1.0.0"),
			fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
			sourcecache.Policy{},
		)
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
		assert.FatalIf(
			t,
			!errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux),
			"canceled fetch: %v",
			err,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("fetch ignored cancellation")
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request outlived fetch")
	}
	art, err := f.Fetch(
		t.Context(),
		helmChart("repo", "app-template", "1.0.0"),
		fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
		sourcecache.Policy{},
	)
	assert.NoError(t, err)

	data, err := fs.ReadFile(art.FS, "chart.tgz")
	if err != nil || !bytes.Equal(data, chartBytes) || attempts.Load() != 2 {
		t.Fatalf("retry: bytes=%d attempts=%d err=%v", len(data), attempts.Load(), err)
	}
}

func testFetchHTTPChartTruncatedOutcomeReplayCase(
	t *testing.T, status *int,
	target *string,
	attempts *int,
) {
	t.Helper()
	var indexHits, chartHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		testFetchHTTPChartTruncatedOutcomeReplayCaseCallback(
			w,
			req,
			testFetchHTTPChartTruncatedOutcomeReplayCaseCallbackInputs{
				status:    &status,
				target:    &target,
				indexHits: &indexHits,
				chartHits: &chartHits,
			},
		)
	},
	))
	defer srv.Close()
	r := httpRepo(srv.URL)
	lookup := func(_, _ string) *manifest.HelmRepository { return r }
	root := t.TempDir()
	c, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = c.Close() })
	fInputs := testInputs{repos: lookup}
	f := New(nil, Config{Network: newTestNetwork(t), Cache: c})

	hc := helmChart("repo", "app-template", "1.0.0")
	retry := sourcecache.RetryConfig{
		Attempts: (*attempts),
		MinWait:  time.Nanosecond,
		MaxWait:  time.Nanosecond,
	}
	_, err = f.Fetch(t.Context(), hc, fInputs.readSet(hc), sourcecache.Policy{Retry: retry})
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(
		t,
		!ok || !terminal.Attempted || !errors.Is(err, manifest.ErrFlux),
		"initial terminal=%+v, error=%v",
		terminal,
		err,
	)
	if (*status) == 0 {
		{
			transport, innerOk := errors.AsType[*url.Error](
				err,
			)
			assert.ErrorIf(t, !innerOk || transport.Op != "Get" || transport.URL != srv.URL+(*target) ||
				!errors.Is(
					transport.Err,
					io.ErrUnexpectedEOF,
				), "initial transport=%+v, error=%v", transport, err)
		}
	} else {
		response, innerOk2 := errors.AsType[*fetcherr.ResponseError](err)
		assert.ErrorIf(
			t,
			!innerOk2 || response.StatusCode != (*status) || response.Op != http.MethodGet ||
				!strings.Contains(response.Error(), srv.URL+(*target)),
			"initial response=%+v, error=%v",
			response,
			err,
		)
	}
	wantIndex, wantChart := int32((*attempts)), int32(0)
	if (*target) != "/index.yaml" {
		wantIndex, wantChart = 1, int32((*attempts))
	}
	if indexHits.Load() != wantIndex || chartHits.Load() != wantChart {
		t.Fatalf(
			"initial requests: index=%d chart=%d, want index=%d chart=%d",
			indexHits.Load(),
			chartHits.Load(),
			wantIndex,
			wantChart,
		)
	}
	{
		closeErr2 := c.Close()
		assert.NoError(t, closeErr2)
	}

	warmCache, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = warmCache.Close() })
	assert.FatalIf(t, warmCache == c, "%s", "reopened cache reused the closed owner")

	warmInputs := testInputs{repos: lookup}
	warm := New(nil, Config{Network: newTestNetwork(t), Cache: warmCache})

	_, err = warm.Fetch(t.Context(), hc, warmInputs.readSet(hc), sourcecache.Policy{Retry: retry})
	terminal, ok = errors.AsType[*sourcecache.TerminalError](err)
	assert.ErrorIf(
		t,
		!ok || terminal.Attempted || !errors.Is(err, manifest.ErrFlux),
		"replayed terminal=%+v, error=%v",
		terminal,
		err,
	)
	if (*status) == 0 {
		{
			transport, ok := errors.AsType[*url.Error](
				err,
			)
			assert.ErrorIf(t, !ok || transport.Op != "Get" || transport.URL != "" ||
				!errors.Is(
					transport.Err,
					io.ErrUnexpectedEOF,
				), "replayed transport=%+v, error=%v", transport, err)
		}
	} else {
		response, ok := errors.AsType[*fetcherr.ResponseError](err)
		assert.ErrorIf(
			t,
			!ok || response.StatusCode != (*status) || response.Op != http.MethodGet ||
				strings.Contains(response.Error(), srv.URL),
			"replayed response=%+v, error=%v",
			response,
			err,
		)
	}
	if indexHits.Load() != wantIndex || chartHits.Load() != wantChart {
		t.Errorf(
			"replayed requests: index=%d chart=%d, want index=%d chart=%d",
			indexHits.Load(),
			chartHits.Load(),
			wantIndex,
			wantChart,
		)
	}
}

func testFetchHTTPChartNetworkRoutesCase(t *testing.T, route *string,
	authenticated *bool,
	customTLS *bool,
) {
	t.Helper()
	var network source.Network
	var indexHits, chartHits, dials atomic.Int32
	var cause error
	var failing atomic.Bool
	testutil.StubDNS(t, func(context.Context, string) error {
		if failing.Load() {
			dials.Add(1)
			return os.ErrDeadlineExceeded
		}
		return nil
	})
	var target string
	latch := func() error {
		failing.Store(true)
		tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
		if err != nil {
			return err
		}
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
		_, err = (&http.Client{Transport: tr}).Do(req)
		failing.Store(false)
		terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
		if !ok || !terminal.Attempted {
			return fmt.Errorf("seed: %w", err)
		}
		cause = terminal.Err
		return nil
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetchHTTPChartNetworkRoutesCaseCallback(
			w,
			r,
			testFetchHTTPChartNetworkRoutesCaseCallbackInputs{
				t:             &t,
				authenticated: &authenticated,
				indexHits:     &indexHits,
				chartHits:     &chartHits,
				latch:         &latch,
			},
		)
	},
	)
	var srv *httptest.Server
	if *customTLS {
		srv = httptest.NewTLSServer(handler)
	} else {
		srv = httptest.NewServer(handler)
	}
	t.Cleanup(srv.Close)
	target = strings.Replace(srv.URL, "127.0.0.1", "example.com", 1)
	r := httpRepo(target)
	secret := &manifest.Secret{StringData: map[string]any{"username": "user", "password": "pass"}}
	if *authenticated {
		r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
	}
	if *customTLS {
		r.CertSecretRef = &manifest.LocalObjectReference{Name: "auth"}
		secret.StringData["ca.crt"] = string(
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}),
		)
	}
	c := cachetest.New(t, "", sourcecache.New)
	fInputs := testInputs{
		secrets: func(_, _ string) *manifest.Secret { return secret },
		repos:   func(_, _ string) *manifest.HelmRepository { return r },
	}
	f := New(nil, Config{Cache: c, Network: &network})

	if (*route) == "index" {
		{
			err := latch()
			assert.NoError(t, err)
		}
	}
	_, err := f.Fetch(
		t.Context(),
		helmChart("repo", "chart", "1.0.0"),
		fInputs.readSet(helmChart("repo", "chart", "1.0.0")),
		sourcecache.Policy{Retry: sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}},
	)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	wantIndex := int32(0)
	if (*route) == "chart" {
		wantIndex = 1
	}
	if !ok || terminal.Attempted || !errors.Is(err, cause) || dials.Load() == 0 ||
		indexHits.Load() != wantIndex ||
		chartHits.Load() != 0 {
		t.Fatalf(
			"error=%v terminal=%+v dials=%d index=%d chart=%d",
			err,
			terminal,
			dials.Load(),
			indexHits.Load(),
			chartHits.Load(),
		)
	}
}

type testFetchHTTPChartBasicAuthSnapshotRotationCaseInputs struct {
	fail          *atomic.Bool
	hits          *atomic.Int32
	lookups       *atomic.Int32
	password      *atomic.Value
	proxyPassword *atomic.Value
	proxy         **httptest.Server
	r             **manifest.HelmRepository
	root          *string
	ca            *string
	previous      *string
	wave          *string
}

func testFetchHTTPChartBasicAuthSnapshotRotationCase(
	t *testing.T, testCaseInputValue *testFetchHTTPChartBasicAuthSnapshotRotationCaseInputs,
) {
	testCaseInput := *testCaseInputValue
	t.Helper()
	if (*testCaseInput.wave) == "password" {
		testCaseInput.password.
			Store("second")
	}
	if (*testCaseInput.wave) == "certificate" {
		(*testCaseInput.ca) = testutil.SelfSignedCA(t)
	}
	proxyURL := strings.Replace((*testCaseInput.proxy).URL, "://", "://proxy:first@", 1)
	if (*testCaseInput.wave) == "proxy" || (*testCaseInput.wave) == "direct" {
		proxyURL = strings.Replace((*testCaseInput.proxy).URL, "://", "://proxy:second@", 1)
		testCaseInput.proxyPassword.
			Store("second")
	}
	t.Setenv("http_proxy", proxyURL)
	if (*testCaseInput.wave) == "direct" {
		t.Setenv("no_proxy", "*")
	}
	c, err := sourcecache.New(t.Context(), (*testCaseInput.root))
	assert.NoError(t, err)

	defer func() { _ = c.Close() }()
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		testCaseInput.lookups.
			Add(1)
		return &manifest.Secret{
			StringData: map[string]any{
				"username": "alice",
				"password": testCaseInput.password.Load().(string),
				"ca.crt":   (*testCaseInput.ca),
			},
		}
	}, repos: func(_, _ string) *manifest.HelmRepository { return (*testCaseInput.r) }}
	f := New(nil, Config{Network: newTestNetwork(t), Cache: c})

	before := testCaseInput.hits.Load()
	art, err := f.Fetch(
		t.Context(),
		helmChart("repo", "app-template", "1.0.0"),
		fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
		sourcecache.Policy{Retry: sourcecache.RetryConfig{Attempts: 3}},
	)
	assert.NoError(t, err)

	if art.Key == (*testCaseInput.previous) || testCaseInput.hits.Load()-before != 2 {
		t.Fatalf("rotation reused chart/index: requests=%d", testCaseInput.hits.Load()-before)
	}
	(*testCaseInput.previous) = art.Key
	before = testCaseInput.hits.Load()
	{
		_, fetchErr := f.Fetch(
			t.Context(),
			helmChart("repo", "app-template", "1.0.0"),
			fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
			sourcecache.Policy{},
		)
		assert.FatalIf(t, fetchErr != nil || testCaseInput.hits.
			Load() != before, "warm chart missed: %v", fetchErr)
	}
	testCaseInput.fail.
		Store(true)
	failed := *(*testCaseInput.r)
	failed.Interval.Duration = 0
	_, err = f.fetchHTTPChart(
		t.Context(),
		preparedHTTP(t, f, fInputs, &failed),
		helmChart("repo", "app-template", "1.0.0"),
		sourcecache.Policy{},
	)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !terminal.Attempted, "failed snapshot: %v", err)
	testCaseInput.fail.
		Store(false)
}

type testFetchHTTPChartTruncatedOutcomeReplayCaseCallbackInputs struct {
	status    **int
	target    **string
	indexHits *atomic.Int32
	chartHits *atomic.Int32
}

func testFetchHTTPChartTruncatedOutcomeReplayCaseCallback(
	w http.ResponseWriter,
	req *http.Request,
	testCaseInput testFetchHTTPChartTruncatedOutcomeReplayCaseCallbackInputs,
) {
	if req.URL.Path == "/index.yaml" {
		testCaseInput.indexHits.
			Add(1)
		if (*(*testCaseInput.target)) != "/index.yaml" {
			_, _ = io.WriteString(w, helmRepoIndex(""))
			return
		}
	} else {
		testCaseInput.chartHits.
			Add(1)
	}
	if (*(*testCaseInput.status)) != 0 {
		w.WriteHeader((*(*testCaseInput.status)))
		return
	}
	w.Header().Set("Content-Length", "20")
	_, _ = io.WriteString(w, "partial")
}

type testFetchHTTPChartNetworkRoutesCaseCallbackInputs struct {
	t             **testing.T
	authenticated **bool
	indexHits     *atomic.Int32
	chartHits     *atomic.Int32
	latch         *func() error
}

func testFetchHTTPChartNetworkRoutesCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetchHTTPChartNetworkRoutesCaseCallbackInputs,
) {
	if *(*testCaseInput.authenticated) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "user" || password != "pass" {
			(*testCaseInput.t).
				Error("missing repository auth")
		}
	}
	if r.URL.Path != "/index.yaml" {
		testCaseInput.chartHits.
			Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	testCaseInput.indexHits.
		Add(1)
	if err := (*testCaseInput.latch)(); err != nil {
		(*testCaseInput.t).
			Error(err)
	}
	_, _ = io.WriteString(
		w,
		"apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    version: 1.0.0\n    apiVersion: v2\n    urls: [chart.tgz]\n",
	)
}
