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

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testFetcherTLSOperationsCase(t *testing.T, root *string,
	head *string,
	route *string,
) {
	t.Helper()

	for _, tc := range []struct {
		name            string
		ref             *sourcev1.GitRepositoryRef
		recurse, refuse bool
	}{
		{name: "clone"},
		{name: "branch", ref: &sourcev1.GitRepositoryRef{Branch: "master"}},
		{name: "tag", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}},
		{name: "semver", ref: &sourcev1.GitRepositoryRef{SemVer: ">=1.0.0"}},
		{name: "named", ref: &sourcev1.GitRepositoryRef{Name: "refs/tags/v1.0.0"}},
		{name: "clone-fetch", ref: &sourcev1.GitRepositoryRef{Name: "refs/tags/v1.0.0"}, recurse: true},
		{name: "exact", ref: &sourcev1.GitRepositoryRef{Commit: (*head)}},
		{name: "fallback", ref: &sourcev1.GitRepositoryRef{Commit: (*head)}, refuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherTLSOperationsCaseCase(
				t,
				testFetcherTLSOperationsCaseCaseInputs{root: &root, route: &route, tc: &tc},
			)
		},
		)
	}
}

type testFetcherTLSOperationsCaseCaseInputs struct {
	root  **string
	route **string
	tc    *struct {
		name string
		ref  *sourcev1.
			GitRepositoryRef
		recurse bool
		refuse  bool
	}
}

func testFetcherTLSOperationsCaseCase(t *testing.T, testCaseInput testFetcherTLSOperationsCaseCaseInputs) {
	t.Helper()

	certificate2 := tlsGitCertificate(t)

	certificate3 := testutil.SelfSignedClientCert(t)

	clients := x509.NewCertPool()
	clients.AppendCertsFromPEM([]byte(certificate3.Cert))
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
		Certificates: []tls.Certificate{certificate2.certificate},
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
				"ca.crt":   certificate2.pem,
				"tls.crt":  certificate3.Cert,
				"tls.key":  certificate3.Key,
			},
		}
	}}
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	repo := gitRepoWithSecret("tls", srv.URL, "tls")
	repo.Reference, repo.RecurseSubmodules = testCaseInput.tc.ref, testCaseInput.tc.recurse
	if (*(*testCaseInput.route)) != "direct" {
		repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
	}
	art, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		repo,
		fInputs.readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
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
	if captures.testCaseInput.tc.recurse || captures.testCaseInput.tc.refuse {
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
	_, (*captures.err) = fetchtest.Fetch(t.Context(), (*captures.f).Fetch, (*captures.repo), captures.fInputs.
		readSet((*captures.repo)), &sourcecache.Policy{MaxAge: (*captures.repo).SourceInterval()}, nil)
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
	if captures.testCaseInput.tc.ref == nil || captures.testCaseInput.tc.ref.Commit == "" {
		wantAdvertisements *= 2
		want *= 2
	}
	if captures.advertisements.Load() != wantAdvertisements || captures.requests.Load() != want {
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
