package helmchart

import (
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetchHTTPChart_BasicAuth_SnapshotRotation(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	var fail atomic.Bool
	var hits, lookups atomic.Int32
	var password atomic.Value
	password.Store("first")
	var proxyPassword atomic.Value
	proxyPassword.Store("first")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetchHTTPChartBasicAuthSnapshotRotationCallback(
			w,
			r,
			testFetchHTTPChartBasicAuthSnapshotRotationCallbackInputs{
				t:             &t,
				chartBytes:    &chartBytes,
				fail:          &fail,
				hits:          &hits,
				password:      &password,
				proxyPassword: &proxyPassword,
			},
		)
	},
	)
	srv := httptest.NewUnstartedServer(handler)
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)

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
			testFetchHTTPChartBasicAuthSnapshotRotationCase(
				t, &testFetchHTTPChartBasicAuthSnapshotRotationCaseInputs{
					fail:          &fail,
					hits:          &hits,
					lookups:       &lookups,
					password:      &password,
					proxyPassword: &proxyPassword,
					proxy:         &proxy,
					r:             &r,
					root:          &root,
					ca:            &ca,
					previous:      &previous,
					wave:          &wave,
				},
			)
		},
		)
	}
	if lookups.Load() != 15 {
		t.Fatalf("chart/index reread material: %d", lookups.Load())
	}
}

type testFetchHTTPChartBasicAuthSnapshotRotationCallbackInputs struct {
	t             **testing.T
	chartBytes    *[]byte
	fail          *atomic.Bool
	hits          *atomic.Int32
	password      *atomic.Value
	proxyPassword *atomic.Value
}

func testFetchHTTPChartBasicAuthSnapshotRotationCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetchHTTPChartBasicAuthSnapshotRotationCallbackInputs,
) {
	if r.URL.IsAbs() &&
		r.Header.Get(
			"Proxy-Authorization",
		) != "Basic "+base64.StdEncoding.EncodeToString(
			[]byte("proxy:"+testCaseInput.proxyPassword.Load().(string)),
		) {
		(*testCaseInput.t).
			Error("transport used a different proxy snapshot")
		w.WriteHeader(http.StatusProxyAuthRequired)
		return
	}
	testCaseInput.hits.
		Add(1)
	user, pass, _ := r.BasicAuth()
	if user != "alice" || pass != testCaseInput.password.Load().(string) {
		(*testCaseInput.t).
			Error("chart/index credential snapshot changed")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if testCaseInput.fail.Load() {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			(*testCaseInput.t).
				Error(err)
			return
		}
		if err := conn.(*net.TCPConn).SetLinger(0); err != nil {
			(*testCaseInput.t).
				Error(err)
		}
		_ = conn.Close()
		return
	}
	if strings.HasSuffix(r.URL.Path, "index.yaml") {
		_, _ = io.WriteString(w, helmRepoIndex(chartDigest((*testCaseInput.chartBytes))))
	} else {
		_, _ = w.Write((*testCaseInput.chartBytes))
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
	t.Helper()
	testCaseInput := *testCaseInputValue
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
		&sourcecache.Policy{Retry: sourcecache.RetryConfig{Attempts: 3}},
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
			&sourcecache.Policy{},
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
		&sourcecache.Policy{},
	)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !terminal.Attempted, "failed snapshot: %v", err)
	testCaseInput.fail.
		Store(false)
}
