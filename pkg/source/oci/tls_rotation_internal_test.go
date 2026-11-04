package oci

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_ResolveTLS_SnapshotRotation(t *testing.T) {
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	var fail atomic.Bool
	var requests, lookups atomic.Int32
	var password atomic.Value
	password.Store("first")
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		user, pass, ok := r.BasicAuth()
		if !ok || user != "alice" || pass != password.Load().(string) {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)

	srv.Listener = listener
	srv.StartTLS()
	t.Cleanup(srv.Close)
	var proxyPassword atomic.Value
	proxyPassword.Store("first")
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherResolveTLSSnapshotRotationCallback(
			w,
			r,
			testFetcherResolveTLSSnapshotRotationCallbackInputs{t: &t, proxyPassword: &proxyPassword},
		)
	},
	))
	t.Cleanup(proxy.Close)
	t.Setenv("HTTPS_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	repo := ociRepo("snapshot", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://" + mustURL(t, srv.URL).Host + "/app"
		s.Insecure = true
		s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
		s.CertSecretRef = &manifest.LocalObjectReference{Name: "creds"}
		s.Interval = &metav1.Duration{Duration: time.Hour}
	})
	root := t.TempDir()
	ca := testutil.SelfSignedCA(t)
	var previous string
	for _, wave := range []string{"first", "password", "certificate", "proxy", "direct"} {
		t.Run(wave, func(t *testing.T) {
			testFetcherResolveTLSSnapshotRotationCase(
				t, &testFetcherResolveTLSSnapshotRotationCaseInputs{
					fail:          &fail,
					requests:      &requests,
					lookups:       &lookups,
					password:      &password,
					srv:           &srv,
					proxyPassword: &proxyPassword,
					proxy:         &proxy,
					repo:          &repo,
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
		t.Fatalf("auth and cert reread Secrets: %d", lookups.Load())
	}
}

type testFetcherResolveTLSSnapshotRotationCallbackInputs struct {
	t             **testing.T
	proxyPassword *atomic.Value
}

func testFetcherResolveTLSSnapshotRotationCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherResolveTLSSnapshotRotationCallbackInputs,
) {
	if r.Header.Get(
		"Proxy-Authorization",
	) != "Basic "+base64.StdEncoding.EncodeToString(
		[]byte("proxy:"+testCaseInput.proxyPassword.Load().(string)),
	) {
		(*testCaseInput.t).
			Error("transport used a different proxy snapshot")
		w.WriteHeader(http.StatusProxyAuthRequired)
		return
	}
	remote, err := (&net.Dialer{}).DialContext(r.Context(), "tcp", r.Host)
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	defer remote.Close()
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		return
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
	go func() { _, _ = io.Copy(remote, conn); _ = remote.Close() }()
	_, _ = io.Copy(conn, remote)
}

type testFetcherResolveTLSSnapshotRotationCaseInputs struct {
	fail          *atomic.Bool
	requests      *atomic.Int32
	lookups       *atomic.Int32
	password      *atomic.Value
	srv           **httptest.Server
	proxyPassword *atomic.Value
	proxy         **httptest.Server
	repo          **manifest.OCIRepository
	root          *string
	ca            *string
	previous      *string
	wave          *string
}

func testFetcherResolveTLSSnapshotRotationCase(
	t *testing.T, testCaseInputValue *testFetcherResolveTLSSnapshotRotationCaseInputs,
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
	t.Setenv("https_proxy", proxyURL)
	if (*testCaseInput.wave) == "direct" {
		t.Setenv("no_proxy", "*")
	}
	c := cachetest.New(t, (*testCaseInput.root), sourcecache.New)
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		testCaseInput.lookups.
			Add(1)
		config := fmt.Sprintf(
			`{"auths":{%q:{"username":"alice","password":%q}}}`,
			mustURL(t, (*testCaseInput.srv).URL).Host, testCaseInput.password.
				Load().(string),
		)
		return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": config, "ca.crt": (*testCaseInput.ca)}}
	}}
	f := &Fetcher{Network: newTestNetwork(t), Cache: c}
	before := testCaseInput.requests.Load()
	art, err := f.Fetch(
		t.Context(), (*testCaseInput.repo), fInputs.readSet((*testCaseInput.repo)),
		sourcecache.Policy{MaxAge: (*testCaseInput.repo).SourceInterval(), Retry: sourcecache.RetryConfig{Attempts: 3}},
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		(*testCaseInput.previous) == art.Key || testCaseInput.requests.Load() <= before,
		"%s",
		"changed material reused acquisition",
	)

	(*testCaseInput.previous) = art.Key
	before = testCaseInput.requests.Load()
	{
		_, fetchErr := f.Fetch(
			t.Context(), (*testCaseInput.repo), fInputs.readSet((*testCaseInput.repo)),
			sourcecache.Policy{MaxAge: (*testCaseInput.repo).SourceInterval()},
		)
		assert.FatalIf(t, fetchErr != nil || testCaseInput.requests.
			Load() != before, "warm snapshot missed: %v", fetchErr)
	}
	testCaseInput.fail.
		Store(true)
	failed := *(*testCaseInput.repo)
	failed.Interval = &metav1.Duration{}
	_, err = f.Fetch(
		t.Context(),
		&failed,
		fInputs.readSet(&failed),
		sourcecache.Policy{MaxAge: failed.SourceInterval()},
	)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !terminal.Attempted, "failed snapshot: %v", err)
	testCaseInput.fail.
		Store(false)
}
