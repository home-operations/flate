package git

import (
	"crypto/tls"
	"crypto/x509"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetcherTLSOperationsCaseCase(t *testing.T, testCaseInput testFetcherTLSOperationsCaseCaseInputs) {
	t.Helper()

	cert, ca := tlsGitCertificate(t)
	crt, key := testutil.SelfSignedClientCert(t)
	clients := x509.NewCertPool()
	clients.AppendCertsFromPEM([]byte(crt))
	handler := gitHTTPHandler(t, (*(*testCaseInput.root)))
	var requests, advertisements, connections atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			advertisements.Add(1)
		}
		if r.Method == http.MethodPost && requests.Add(1) == 1 && testCaseInput.tc.refuse {
			http.Error(w, "exact request refused", http.StatusBadRequest)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clients,
	}
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	proxy, connects := tlsGitProxy(t, (*(*testCaseInput.route)) == "authenticated-proxy")
	fInputs := testInputs{secrets: func(_, name string) *manifest.Secret {
		if name == "proxy" {
			data := map[string]any{"address": proxy.URL}
			if (*(*testCaseInput.route)) == "authenticated-proxy" {
				data["username"], data["password"] = "proxy", "password"
			}
			return &manifest.Secret{StringData: data}
		}
		return &manifest.Secret{
			StringData: map[string]any{
				"username": "alice",
				"password": "p",
				"ca.crt":   ca,
				"tls.crt":  crt,
				"tls.key":  key,
			},
		}
	}}
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	repo := gitRepoWithSecret("tls", srv.URL, "tls")
	repo.Reference, repo.RecurseSubmodules = testCaseInput.tc.ref, testCaseInput.tc.recurse
	if (*(*testCaseInput.route)) != "direct" {
		repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
	}
	art, err := f.Fetch(
		t.Context(),
		repo,
		fInputs.readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	if err != nil {
		_, fallback := f.fallback.Load(srv.URL)
		t.Fatalf("fetch: %v (requests=%d, fallback=%t)", err, requests.Load(), fallback)
	}
	testFetcherTLSOperationsCaseCaseCompletion(
		t,
		&testFetcherTLSOperationsCaseCaseCompletionInputs{
			art:            &art,
			testCaseInput:  &testCaseInput,
			requests:       &requests,
			advertisements: &advertisements,
			connections:    &connections,
			connects:       &connects,
			err:            &err,
			f:              &f,
			repo:           &repo,
			fInputs:        &fInputs,
		},
	)
}

func testFetcherTLSOperationsCaseCaseCompletion(
	t *testing.T,
	captures *testFetcherTLSOperationsCaseCaseCompletionInputs,
) {
	t.Helper()
	{
		data, readFileErr := fs.ReadFile((*captures.art).FS, "hello.txt")
		assert.FatalIf(
			t,
			readFileErr != nil || string(data) != "hi",
			"content=%q, error=%v",
			data,
			readFileErr,
		)
	}
	want := int32(1)
	if captures.testCaseInput.tc.refuse {
		want = 2
	}
	if captures.requests.Load() != want {
		t.Fatalf("upload requests=%d, want %d", captures.requests.Load(), want)
	}
	wantAdvertisements := int32(1)
	if captures.testCaseInput.tc.recurse || (*(*captures.testCaseInput).tc).refuse {
		wantAdvertisements = 2
	}
	if captures.advertisements.Load() != wantAdvertisements {
		t.Errorf("initial advertisements=%d, want %d", captures.advertisements.Load(), wantAdvertisements)
	}
	t.Logf(
		"initial: advertisements=%d upload_pack=%d accepted_connections=%d proxy_CONNECT=%d", captures.advertisements.
			Load(), captures.requests.
			Load(), captures.connections.
			Load(), (*captures.connects).
			Load(),
	)
	_, (*captures.err) = (*captures.f).Fetch(
		t.Context(), (*captures.repo), captures.fInputs.
			readSet((*captures.repo)), sourcecache.Policy{MaxAge: (*captures.repo).SourceInterval()},
	)
	assert.NoError(t, (*captures.err))

	t.Logf(
		"after compatible operation: advertisements=%d upload_pack=%d accepted_connections=%d proxy_CONNECT=%d",
		captures.advertisements.
			Load(),
		captures.requests.
			Load(),
		captures.connections.
			Load(),
		(*captures.connects).
			Load(),
	)
	if captures.testCaseInput.tc.ref == nil || (*(*captures.testCaseInput).tc).ref.Commit == "" {
		wantAdvertisements *= 2
		want *= 2
	}
	if captures.advertisements.Load() != wantAdvertisements || (*captures.requests).Load() != want {
		t.Errorf(
			"two operations: advertisements=%d upload_pack=%d, want %d and %d", captures.advertisements.
				Load(), captures.requests.
				Load(), wantAdvertisements,
			want,
		)
	}
}

type testFetcherTLSOperationsCaseCaseCompletionInputs struct {
	art            **store.SourceArtifact
	testCaseInput  *testFetcherTLSOperationsCaseCaseInputs
	requests       *atomic.Int32
	advertisements *atomic.Int32
	connections    *atomic.Int32
	connects       **atomic.Int32
	err            *error
	f              **Fetcher
	repo           **manifest.GitRepository
	fInputs        *testInputs
}
