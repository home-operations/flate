package oci

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_BearerTokenReuse(t *testing.T) {
	t.Parallel()
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprintf("authenticated=%t", authenticated), func(t *testing.T) {
			t.Parallel()
			testFetcherBearerTokenReuseCase(t, &authenticated)
		},
		)
	}
}

func testFetcherBearerTokenReuseCase(t *testing.T, authenticated *bool,
) {
	t.Helper()

	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	upstream, _ := newMutableFakeRegistry(t, artifact)
	var challenges, exchanges, wrongContext atomic.Int32
	var generation atomic.Int32
	var failTokens atomic.Bool
	var password atomic.Value
	password.Store("")
	if *authenticated {
		password.Store("first")
	}
	token := func(host, scope, secret string) string {
		return base64.RawURLEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s|%s|%s|%d", host, scope, secret, generation.Load())),
		)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherBearerTokenReuseCaseCallback(
			w,
			r,
			testFetcherBearerTokenReuseCaseCallbackInputs{
				upstream:     &upstream,
				challenges:   &challenges,
				exchanges:    &exchanges,
				wrongContext: &wrongContext,
				failTokens:   &failTokens,
				password:     &password,
				token:        &token,
			},
		)
	},
	)
	srv := httptest.NewTLSServer(handler)
	t.Cleanup(srv.Close)
	repo := &manifest.OCIRepository{
		Name: "bearer", Namespace: "test",
		URL: "oci://" + mustURL(t, srv.URL).Host + "/app", Insecure: true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	if *authenticated {
		repo.SecretRef = &manifest.LocalObjectReference{Name: "registry"}
	}
	registryHost := mustURL(t, srv.URL).Host
	inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		config := fmt.Sprintf(
			`{"auths":{%q:{"username":"user","password":%q}}}`,
			registryHost,
			password.Load().(string),
		)
		return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": config}}
	}}

	owner := cachetest.New(t, t.TempDir(), sourcecache.New)
	network := newTestNetwork(t)
	fetch := func(request *manifest.OCIRepository, n *source.Network) {
		t.Helper()
		f := &Fetcher{Network: n, Cache: owner}
		got, err := f.Fetch(t.Context(), request, inputs.readSet(request), sourcecache.Policy{})
		assert.NoError(t, err)
		assert.Equal(t, mustReadFile(t, got.FS, "app.yaml"), "version: v1\n")
	}
	fetch(repo, network)
	assert.Equal(t, challenges.Load(), int32(1))
	assert.Equal(t, exchanges.Load(), int32(1))
	fetch(repo, network)
	assert.Equal(t, challenges.Load(), int32(1))
	assert.Equal(t, exchanges.Load(), int32(1))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		f := &Fetcher{Network: network, Cache: cachetest.New(t, "", sourcecache.New)}
		wg.Go(func() {
			<-start
			got, err := f.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{})
			if err != nil {
				t.Errorf("concurrent acquisition: %v", err)
				return
			}
			data, err := fs.ReadFile(got.FS, "app.yaml")
			assert.ErrorIf(
				t,
				err != nil || string(data) != "version: v1\n",
				"concurrent content=%q error=%v",
				data,
				err,
			)
		})
	}
	close(start)
	wg.Wait()
	assert.Equal(t, challenges.Load(), int32(1))
	assert.Equal(t, exchanges.Load(), int32(1))

	fetch(repo, newTestNetwork(t))
	assert.Equal(t, challenges.Load(), int32(2))
	assert.Equal(t, exchanges.Load(), int32(2))
	password.Store("rotated")
	repo.SecretRef = &manifest.LocalObjectReference{Name: "registry"}
	fetch(repo, network)
	assert.Equal(t, challenges.Load(), int32(3))
	assert.Equal(t, exchanges.Load(), int32(3))

	otherScope := *repo
	otherScope.URL = "oci://" + mustURL(t, srv.URL).Host + "/other"
	fetch(&otherScope, network)
	assert.Equal(t, challenges.Load(), int32(4))
	assert.Equal(t, exchanges.Load(), int32(4))
	otherRegistry := httptest.NewTLSServer(handler)
	t.Cleanup(otherRegistry.Close)
	other := *repo
	other.URL = "oci://" + mustURL(t, otherRegistry.URL).Host + "/app"
	registryHost = mustURL(t, otherRegistry.URL).Host
	fetch(&other, network)
	registryHost = mustURL(t, srv.URL).Host
	assert.Equal(t, challenges.Load(), int32(5))
	assert.Equal(t, exchanges.Load(), int32(5))

	generation.Add(1)
	fetch(repo, network)
	assert.Equal(t, challenges.Load(), int32(6))
	assert.Equal(t, exchanges.Load(), int32(6))
	generation.Add(1)
	failTokens.Store(true)
	_, err := (&Fetcher{Network: network, Cache: owner}).Fetch(
		t.Context(),
		repo,
		inputs.readSet(repo),
		sourcecache.Policy{},
	)
	assert.FatalIf(
		t,
		err == nil || !errors.Is(err, manifest.ErrFlux),
		"token rejection did not propagate: %v",
		err,
	)
	response, ok := errors.AsType[*fetcherr.ResponseError](err)
	assert.FatalIf(
		t,
		!ok || response.StatusCode != http.StatusForbidden,
		"token endpoint failure lost response: %v",
		err,
	)
	assert.Equal(t, challenges.Load(), int32(7))
	assert.Equal(t, exchanges.Load(), int32(7))
	assert.Equal(t, wrongContext.Load(), int32(0))
	t.Logf(
		"initial, warm, concurrent, invocation, credential, scope, registry and rejection stages: challenges=%d exchanges=%d wrong_context=%d",
		challenges.Load(),
		exchanges.Load(),
		wrongContext.Load(),
	)
}

type testFetcherBearerTokenReuseCaseCallbackInputs struct {
	upstream     **httptest.Server
	challenges   *atomic.Int32
	exchanges    *atomic.Int32
	wrongContext *atomic.Int32
	failTokens   *atomic.Bool
	password     *atomic.Value
	token        *func(host string, scope string,

		secret string) string
}

func testFetcherBearerTokenReuseCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherBearerTokenReuseCaseCallbackInputs,
) {
	secret := testCaseInput.password.Load().(string)
	if r.URL.Path == "/token" {
		testCaseInput.exchanges.
			Add(1)
		user, pass, present := r.BasicAuth()
		if testCaseInput.failTokens.Load() || secret != "" && (!present || user != "user" || pass != secret) ||
			secret == "" && present {
			http.Error(w, "token denied", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		//nolint:errchkjson // Fixture payloads are encodable; client cancellation may fail their response writes.
		_ = json.NewEncoder(w).
			Encode(map[string]string{"token": (*testCaseInput.token)(r.Host, r.URL.Query().Get("scope"), secret)})
		return
	}
	name, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/v2/"), "/")
	scope := "repository:" + name + ":pull"
	header := r.Header.Get("Authorization")
	if header != "Bearer "+(*testCaseInput.token)(r.Host, scope, secret) {
		testCaseInput.challenges.
			Add(1)
		if header != "" {
			decoded, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(header, "Bearer "))
			if !strings.HasPrefix(string(decoded), r.Host+"|"+scope+"|"+secret+"|") {
				testCaseInput.wrongContext.
					Add(1)
			}
		}
		w.Header().
			Set("WWW-Authenticate", fmt.Sprintf("Bearer realm=%q,service=%q,scope=%q", "https://"+r.Host+"/token", "registry", scope))
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	(*testCaseInput.upstream).
		Config.Handler.ServeHTTP(w, r)
}
