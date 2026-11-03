package oci

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testFetcherResolveTLSSnapshotRotationCase(
	t *testing.T, testCaseInputValue *testFetcherResolveTLSSnapshotRotationCaseInputs,
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

func testFetcherResolveConfigRegistryFallbackSnapshotCase(
	t *testing.T, callers *int,
) {
	t.Helper()

	ca := testutil.SelfSignedCA(t)
	artifact := newFakeOCIArtifact(t, map[string]string{"data.yaml": "concurrent"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(
		t,
		config,
		fmt.Sprintf(`{"auths":{%q:{"username":"alice","password":"test"}}}`, mustURL(t, upstream.URL).Host),
	)
	c := cachetest.New(t, t.TempDir(), sourcecache.New)
	var lookups atomic.Int32
	fInputs := testInputs{secrets: func(_, name string) *manifest.Secret {
		lookups.Add(1)
		if name == "tls" {
			return &manifest.Secret{StringData: map[string]any{"ca.crt": ca}}
		}
		return nil
	}}
	f := &Fetcher{Network: newTestNetwork(t), Cache: c, RegistryConfig: config}
	repo := ociRepo("fallback", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://" + mustURL(t, upstream.URL).Host + "/app"
		s.Insecure = true
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
		s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	})
	_, err := f.Fetch(
		t.Context(),
		repo,
		fInputs.readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryAmbient == nil || lookups.Load() != 2 {
		t.Fatalf("snapshot before handoff: reads=%d error=%v", lookups.Load(), err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var wg sync.WaitGroup
	for range *callers {
		wg.Go(func() {
			_, err := missing.RetryAmbient(ctx)
			assert.ErrorIf(t, !errors.Is(err, context.Canceled), "canceled fallback: %v", err)
		})
	}

	wg.Wait()
	start := make(chan struct{})
	artifacts := make([]*store.SourceArtifact, (*callers))
	failures := make([]error, (*callers))
	for i := range *callers {
		wg.Go(func() { <-start; artifacts[i], failures[i] = missing.RetryMachine(t.Context(), nil) })
	}
	close(start)
	wg.Wait()
	for i, artifact := range artifacts {
		if failures[i] != nil || artifact == nil || artifact.Key != artifacts[0].Key ||
			mustReadFile(t, artifact.FS, "data.yaml") != "concurrent" {
			t.Fatalf("concurrent prepared acquisition: artifact=%v error=%v", artifact, failures[i])
		}
	}
	if lookups.Load() != 2 {
		t.Fatalf("retained fallback reread mutable Secrets: %d", lookups.Load())
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

func testFetcherSyntheticCredentialContextCase(
	t *testing.T, generated *bool,
	material *string,
) {
	t.Helper()

	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "sources"}
	chart.Chart = "app"
	expected := chart.Named()
	if *generated {
		release := &manifest.HelmRelease{
			Name:      "release",
			Namespace: "apps",
			HelmReleaseSpec: helmv2.HelmReleaseSpec{
				Chart: &helmv2.HelmChartTemplate{
					Spec: helmv2.HelmChartTemplateSpec{
						Chart: "app",
						SourceRef: &helmv2.CrossNamespaceObjectReference{
							Kind:      manifest.KindHelmRepository,
							Name:      "charts",
							Namespace: "sources",
						},
					},
				},
			},
		}
		children, err := release.GeneratedObjects()
		assert.NoError(t, err)

		chart = children[0].(*manifest.HelmChartSource)
		expected = release.Named()
	}
	repo := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
	var sec *manifest.Secret
	ref := &manifest.LocalObjectReference{Name: "material"}
	missing := !strings.HasSuffix((*material), "malformed")
	switch {
	case strings.HasPrefix((*material), "auth"):
		repo.SecretRef = ref
		if (*material) == "auth wiped" {
			sec = &manifest.Secret{
				StringData: map[string]any{".dockerconfigjson": manifest.ValuePlaceholder("config")},
			}
		}
		if (*material) == "auth malformed" {
			sec = &manifest.Secret{StringData: map[string]any{".dockerconfigjson": "{"}}
		}
	case strings.HasPrefix((*material), "cert"):
		repo.CertSecretRef = ref
		if (*material) == "cert wiped" {
			sec = &manifest.Secret{StringData: map[string]any{"ca.crt": manifest.ValuePlaceholder("ca")}}
		}
		if (*material) == "cert malformed" {
			sec = &manifest.Secret{StringData: map[string]any{"ca.crt": "invalid"}}
		}
	case strings.HasPrefix((*material), "proxy"):
		repo.ProxySecretRef = ref
		if (*material) == "proxy wiped" {
			sec = &manifest.Secret{
				StringData: map[string]any{"address": manifest.ValuePlaceholder("proxy")},
			}
		}
		if (*material) == "proxy malformed" {
			sec = &manifest.Secret{StringData: map[string]any{"address": "http://invalid/%"}}
		}
	default:
		repo.Provider = "aws"
	}
	testFetcherSyntheticCredentialContextCaseCompletion(
		t,
		&testFetcherSyntheticCredentialContextCaseCompletionInputs{
			ref:      &ref,
			sec:      &sec,
			repo:     &repo,
			expected: &expected,
			material: &material,
			missing:  &missing,
		},
	)
}

func testFetcherSyntheticCredentialContextCaseCompletion(
	t *testing.T,
	captures *testFetcherSyntheticCredentialContextCaseCompletionInputs,
) {
	t.Helper()
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, "{}")
	op := &operation{
		Fetcher: &Fetcher{Network: newTestNetwork(t), RegistryConfig: config},
		secrets: func(namespace, name string) *manifest.Secret {
			assert.FatalIf(
				t,
				namespace != "sources" || name != (*captures.ref).Name,
				"Secret lookup %s/%s used display owner",
				namespace,
				name,
			)
			return (*captures.sec)
		},
	}
	_, err := op.fetchSource(t.Context(), (*captures.repo), sourcecache.Policy{})
	label := captures.expected.Kind + " " + (*captures.expected).NamespacedName()
	if (*(*captures.material)) == "provider" {
		label = captures.expected.String()
	}
	assert.FatalIf(t, err == nil || strings.Contains(err.Error(), label) ||
		strings.Contains(err.Error(), "OCIRepository"), "early diagnostic=%v, owner=%s", err, (*captures.expected))
	assert.FatalIf(
		t,
		errors.Is(err, manifest.ErrMissingSecret) != (*captures.missing),
		"material classification changed: %v",
		err,
	)
	if failure, ok := errors.AsType[*source.MissingSecretError](err); ok {
		assert.FatalIf(
			t,
			failure.Owner != (*captures.repo).Named() || failure.Subject.Resource != (*captures.expected),
			"raw/display owner=%+v",
			failure,
		)
		if (*(*captures.material)) != "provider" && failure.Secret.Namespace != "sources" {
			t.Fatalf("Secret identity=%s", failure.Secret)
		}
	}
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

type testFetcherSyntheticCredentialContextCaseCompletionInputs struct {
	ref      **manifest.LocalObjectReference
	sec      **manifest.Secret
	repo     **manifest.OCIRepository
	expected *manifest.NamedResource
	material **string
	missing  *bool
}
