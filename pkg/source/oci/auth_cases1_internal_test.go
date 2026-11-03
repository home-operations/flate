package oci

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oras.land/oras-go/v2/registry/remote/auth"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
)

func TestFetcher_OCIHelmRepositoryCredentials(t *testing.T) {
	var failing atomic.Bool
	testutil.StubDNS(t, func(context.Context, string) error {
		if failing.Load() {
			return os.ErrDeadlineExceeded
		}
		return nil
	})
	const user, pass = "alice", "hunter2"
	const chart = "name: app\nversion: 1.0.0\n"
	art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": chart})
	art.manifest = mustManifestJSON(t,
		manifestBlob{data: art.config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: art.layer, mediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip"},
	)
	upstream := startFakeRegistry(t, art.manifest, art.config, art.layer)
	var failNext atomic.Bool
	var failures atomic.Int32
	var requests atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if u, p, ok := r.BasicAuth(); !ok || u != user || p != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if failNext.CompareAndSwap(true, false) {
			failures.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	targetURL := strings.Replace(srv.URL, "127.0.0.1", "example.com", 1)
	host := mustURL(t, targetURL).Host
	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	configJSON := fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, host, auth)
	cache := cachetest.New(t, "", sourcecache.New)
	var network source.Network
	ociFetcherInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": configJSON}}
	}}
	ociFetcher := &Fetcher{
		Cache: cache, Network: &network,
	}
	repo := &manifest.HelmRepository{
		Name: "charts", Namespace: "ns",
		URL: "oci://" + host + "/charts", Type: sourcev1.HelmRepositoryTypeOCI, Insecure: true,
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
		Interval:  metav1.Duration{Duration: time.Hour},
	}
	chartInput := manifest.NewHelmChartSource(
		sourcev1.HelmChart{
			Name:      "app",
			Namespace: repo.Namespace,
			Spec: sourcev1.HelmChartSpec{
				Chart:     "app",
				Version:   "1.0.0",
				Interval:  repo.Interval,
				SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: repo.Name},
			},
		},
	)
	chartsInputs := testInputs{
		secrets: ociFetcherInputs.secrets,
		repos:   func(_, _ string) *manifest.HelmRepository { return repo },
	}
	charts := helmchart.New(ociFetcher, helmchart.Config{Cache: cache, Network: &network})

	got, err := charts.Fetch(t.Context(), chartInput, chartsInputs.readSet(chartInput), sourcecache.Policy{})
	assert.NoError(t, err)

	if got.Kind != manifest.KindHelmChart {
		t.Errorf("artifact Kind = %q, want %q", got.Kind, manifest.KindHelmChart)
	}
	{
		b := mustReadFile(t, got.FS, "Chart.yaml")
		assert.ErrorIf(t, b != chart, "Chart.yaml = %q, want %q", b, chart)
	}
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	helmPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("HELM_REGISTRY_CONFIG", helmPath)
	testutil.WriteFileAt(t, helmPath, configJSON)
	helmRepo := *repo
	helmRepo.SecretRef = nil
	helmInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return &helmRepo }}
	helmCharts := helmchart.New(
		&Fetcher{Cache: cache, Network: &network},
		helmchart.Config{Cache: cache, Network: &network},
	)
	helmArtifact, err := helmCharts.Fetch(t.Context(), chartInput, helmInputs.readSet(chartInput), sourcecache.Policy{})
	assert.NoError(t, err)
	assert.Equal(t, helmArtifact.Kind, manifest.KindHelmChart)
	assert.Equal(t, mustReadFile(t, helmArtifact.FS, "Chart.yaml"), chart)
	initialKey := got.Key
	configPath := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, configPath, configJSON)
	fallbackFetcherInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	fallbackFetcher := &Fetcher{Cache: cache, Network: &network, RegistryConfig: configPath}
	fallbackChartsInputs := testInputs{
		secrets: fallbackFetcherInputs.secrets,
		repos:   func(_, _ string) *manifest.HelmRepository { return repo },
	}
	fallbackCharts := helmchart.New(fallbackFetcher, helmchart.Config{Cache: cache, Network: &network})

	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	_, err = fallbackCharts.Fetch(
		t.Context(),
		chartInput,
		fallbackChartsInputs.readSet(chartInput),
		sourcecache.Policy{Retry: retry},
	)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing fallback: %v", err)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	{
		_, retryAmbientErr := missing.RetryAmbient(cancelled)
		assert.FatalIf(t, !errors.Is(retryAmbientErr, context.Canceled), "fallback context: %v", retryAmbientErr)
	}
	failNext.Store(true)
	got, err = missing.RetryAmbient(t.Context())
	if err != nil || failures.Load() != 1 {
		t.Fatalf("fallback recovery: %v, failures=%d", err, failures.Load())
	}
	assert.FatalIf(
		t,
		got.Key == initialKey || got.Kind != manifest.KindHelmChart || mustReadFile(t, got.FS, "Chart.yaml") != chart,
		"fallback artifact=%+v",
		got,
	)
	beforeWarm := requests.Load()
	warm, err := missing.RetryAmbient(t.Context())
	if err != nil || warm == nil || warm.Key != got.Key || warm.Kind != manifest.KindHelmChart ||
		requests.Load() != beforeWarm {
		t.Fatalf(
			"fallback lost freshness or chart identity: artifact=%+v, err=%v, new requests=%d",
			warm,
			err,
			requests.Load()-beforeWarm,
		)
	}
	refreshRepo := *repo
	refreshRepo.Interval = metav1.Duration{}
	refreshInput := *chartInput
	refreshInput.Interval = refreshRepo.Interval
	refreshChartsInputs := testInputs{
		secrets: fallbackFetcherInputs.secrets,
		repos:   func(_, _ string) *manifest.HelmRepository { return &refreshRepo },
	}
	refreshCharts := helmchart.New(fallbackFetcher, helmchart.Config{Cache: cache, Network: &network})
	_, err = refreshCharts.Fetch(
		t.Context(),
		&refreshInput,
		refreshChartsInputs.readSet(&refreshInput),
		sourcecache.Policy{Retry: retry},
	)
	missing, ok = errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "refresh fallback: %v", err)
	failing.Store(true)
	tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
	assert.NoError(t, err)

	req, _ := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		strings.Replace(srv.URL, "127.0.0.1", "example.com", 1),
		nil,
	)
	_, err = (&http.Client{Transport: tr}).Do(req)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !terminal.Attempted, "seed: %v", err)
	cause := terminal.Err
	before := requests.Load()
	_, err = missing.RetryAmbient(t.Context())
	if terminal, ok := errors.AsType[*sourcecache.TerminalError](
		err,
	); !ok || terminal.Attempted || !errors.Is(err, cause) ||
		requests.Load() != before {
		t.Fatalf("suppressed fallback: %v, additional requests=%d", err, requests.Load()-before)
	}
}

func TestFetcher_ResolveConfig_CredentialSnapshot(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.json")
	write := func(password string) {
		t.Helper()
		testutil.WriteFileAt(
			t,
			path,
			fmt.Sprintf(`{"auths":{"https://index.docker.io/v1/":{"username":"fixture","password":%q}}}`, password),
		)
	}
	write("first")
	op := &operation{Fetcher: &Fetcher{Network: newTestNetwork(t), RegistryConfig: path}}
	repo := ociRepo("snapshot", func(s *sourcev1.OCIRepositorySpec) { s.URL = "oci://docker.io/library/test" })
	selected, err := newTestNetwork(t).RegistryCredentials(t.Context(), path, "registry-1.docker.io")
	assert.NoError(t, err)
	request, err := op.newRepoClient(t.Context(), new(repo.RegistryAccess()))
	if err == nil {
		err = op.configure(new(repo.RegistryAccess()), &request, *selected)
	}
	assert.NoError(t, err)
	write("second")
	cred, err := request.client.Client.(*auth.Client).Credential(t.Context(), "registry-1.docker.io")
	assert.FatalIf(t, err != nil || cred.Password != "first", "credential was not frozen: %v", err)
	selected, err = newTestNetwork(t).RegistryCredentials(t.Context(), path, "registry-1.docker.io")
	assert.NoError(t, err)
	next, err := op.newRepoClient(t.Context(), new(repo.RegistryAccess()))
	if err == nil {
		err = op.configure(new(repo.RegistryAccess()), &next, *selected)
	}
	assert.FatalIf(
		t,
		err != nil || cmp.Equal(request.material, next.material),
		"next acquisition did not resolve rotated credential: %v",
		err,
	)
	testutil.WriteFileAt(t, path, `{"auths":{"https://index.docker.io/v1/":{"auth":"!!!"}}}`)
	_, err = newTestNetwork(t).RegistryCredentials(t.Context(), path, "registry-1.docker.io")
	assert.FatalIf(t, err == nil, "failed lookup admitted acquisition: %v", err)
	assert.FatalIf(
		t,
		!strings.Contains(err.Error(), "failed to decode auth field"),
		"credential decode detail lost: %v",
		err,
	)
	op.RegistryConfig = filepath.Join(path, "config.json")
	_, err = newTestNetwork(t).RegistryCredentials(t.Context(), op.RegistryConfig, "registry-1.docker.io")
	{
		_, ok := errors.AsType[*os.PathError](err)
		assert.FatalIf(t, !ok, "file lookup error chain lost: %v", err)
	}
}

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

func TestFetcher_ResolveConfig_RegistryFallbackSnapshot(t *testing.T) {
	t.Parallel()
	for _, callers := range []int{2, 4} {
		t.Run(strconv.Itoa(callers), func(t *testing.T) {
			t.Parallel()
			testFetcherResolveConfigRegistryFallbackSnapshotCase(
				t,
				&callers,
			)
		},
		)
	}
}

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

func TestFetcher_ProviderTransportValidation(t *testing.T) {
	t.Parallel()
	for _, use := range []string{"cert", "proxy"} {
		t.Run(use, func(t *testing.T) {
			t.Parallel()
			repo := ociRepo("provider", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://fixture.invalid/image"
				s.Provider = sourcev1.AmazonOCIProvider
				if use == "cert" {
					s.CertSecretRef = &manifest.LocalObjectReference{Name: "transport"}
				} else {
					s.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
				}
			})
			inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{
					StringData: map[string]any{"ca.crt": "invalid", "address": "http://proxy.invalid/%"},
				}
			}}
			fetcher := &Fetcher{Network: newTestNetwork(t)}
			_, err := fetcher.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{})
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrMissingSecret),
				"provider hid malformed transport: %v",
				err,
			)
		})
	}
}

func TestFetcher_ProviderRegistryMatching(t *testing.T) {
	t.Parallel()
	for _, matching := range []bool{false, true} {
		t.Run(strconv.FormatBool(matching), func(t *testing.T) {
			t.Parallel()
			registry := "other.invalid"
			if matching {
				registry = "fixture.invalid"
			}
			config := filepath.Join(t.TempDir(), "config.json")
			testutil.WriteFileAt(
				t,
				config,
				fmt.Sprintf(`{"auths":{%q:{"username":"ambient","password":"fixture"}}}`, registry),
			)
			repo := ociRepo("provider", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://fixture.invalid/image"
				s.Provider = sourcev1.AmazonOCIProvider
				s.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
			})
			inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{
					StringData: map[string]any{
						".dockerconfigjson": `{"auths":{"unrelated.invalid":{"username":"repository","password":"fixture"}}}`,
					},
				}
			}}
			fetcher := &Fetcher{Network: newTestNetwork(t), RegistryConfig: config}
			_, err := fetcher.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(
				t,
				!ok || missing.Secret.Name != "auth" || missing.RetryAmbient == nil,
				"actual-registry continuation: %v",
				err,
			)
		})
	}
}

func TestFetcher_SyntheticCredentialContext(t *testing.T) {
	t.Parallel()
	for _, generated := range []bool{false, true} {
		for _, material := range []string{"auth absent", "auth wiped", "auth malformed", "cert absent", "cert wiped", "cert malformed", "proxy absent", "proxy wiped", "proxy malformed"} {
			t.Run(fmt.Sprintf("generated=%t/%s", generated, material), func(t *testing.T) {
				t.Parallel()
				testFetcherSyntheticCredentialContextCase(
					t,
					&generated, &material,
				)
			},
			)
		}
	}
}

func testFetcherSecretReferencesCase(t *testing.T, tt *struct {
	name        string
	secret      string
	wantText    string
	material    map[string]any
	cert        bool
	proxy       bool
	unwired     bool
	wantMissing bool
	wantOwner   bool
},
) {
	t.Helper()
	fInputs := testInputs{}
	f := &Fetcher{Network: newTestNetwork(t)}
	if !tt.unwired {
		fInputs.secrets = func(_, _ string) *manifest.Secret {
			if tt.material == nil {
				return nil
			}
			return &manifest.Secret{StringData: tt.material}
		}
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		testFetcherSecretReferencesCaseCallback(s, testFetcherSecretReferencesCaseCallbackInputs{tt: &tt})
	},
	)
	if tt.wantOwner {
		repo.Name = "repo"
	}
	var err error
	switch {
	case tt.proxy:
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(
			t.Context(), new(repo.RegistryAccess()),
		)
	case tt.cert:
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(
			t.Context(), new(repo.RegistryAccess()),
		)
	default:
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveRegistryConfig(
			t.Context(), new(repo.RegistryAccess()), "ghcr.io",
		)
	}
	if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tt.wantMissing ||
		!strings.Contains(err.Error(), tt.wantText) {
		t.Fatalf(
			"error = %v, want missing=%t containing %q",
			err, tt.
				wantMissing, tt.
				wantText,
		)
	}
	if tt.wantOwner {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok, "expected MissingSecretError; got %T", err)
		assert.Equal(
			t,
			missing.Owner,
			manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "ns",
				Name:      "repo",
			},
		)
		assert.Equal(
			t,
			missing.Secret,
			manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "missing"},
		)
		assert.Equal(t, err.Error(), "secret ns/missing not found")
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

type testFetcherSecretReferencesCaseCallbackInputs struct {
	tt **struct {
		name   string
		secret string

		wantText    string
		material    map[string]any
		cert        bool
		proxy       bool
		unwired     bool
		wantMissing bool
		wantOwner   bool
	}
}

func testFetcherSecretReferencesCaseCallback(
	s *sourcev1.OCIRepositorySpec,
	testCaseInput testFetcherSecretReferencesCaseCallbackInputs,
) {
	s.URL = "oci://ghcr.io/x/y"
	ref := &manifest.LocalObjectReference{Name: (*testCaseInput.tt).secret}
	switch {
	case (*testCaseInput.tt).proxy:
		s.ProxySecretRef = ref
	case (*testCaseInput.tt).cert:
		s.CertSecretRef = ref
	default:
		s.SecretRef = ref
	}
}
