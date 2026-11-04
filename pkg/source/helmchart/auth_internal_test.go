package helmchart

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestHelmRepoTransport_NoCertRef(t *testing.T) {
	t.Parallel()
	r := httpRepo("https://charts.example")
	f, fInputs := newHTTPFetcherWithSecrets(t, r, nil)

	prepared, err := (&operation{Fetcher: f, secrets: fInputs.lookup(r)}).prepareHTTP(r)
	assert.FatalIf(t, err != nil, "prepareHTTP: %v", err)
	expected, err := f.network.HTTPTransport(source.HTTPTransportConfig{DisableCompression: true})
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		prepared.client.Transport != expected,
		"%s",
		"transport applied customization without a certificate reference",
	)
}

func TestHelmRepoTransport_CertSecret(t *testing.T) {
	t.Parallel()
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	ca := testutil.SelfSignedCA(t)
	f, fInputs := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"ca.crt": ca}}
	})

	prepared, err := (&operation{Fetcher: f, secrets: fInputs.lookup(r)}).prepareHTTP(r)
	assert.FatalIf(t, err != nil, "prepareHTTP: %v", err)
	material, err := source.ResolveTransport(
		fInputs.secrets,
		manifest.DiagnosticFor(r.Named(), r),
		[]source.TransportInput{{Reference: r.CertSecretRef, Use: source.TransportCert}},
	)
	assert.NoError(t, err)

	expected, err := f.network.HTTPTransport(source.HTTPTransportConfig{Material: material, DisableCompression: true})
	assert.NoError(t, err)
	assert.FatalIf(t, prepared.client.Transport != expected, "%s", "transport did not apply the certificate reference")
}

func TestHelmRepoTransport_MissingSecretIsSoftSkippable(t *testing.T) {
	t.Parallel()
	r := httpRepo("https://charts.example")
	r.Name, r.Namespace = "charts", "ns"
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	secret := &manifest.Secret{StringData: map[string]any{"ca.crt": testutil.SelfSignedCA(t)}}
	f, fInputs := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return secret })
	preparedHTTP(t, f, fInputs, r)
	secret = nil

	prepared, err := (&operation{Fetcher: f, secrets: fInputs.lookup(r)}).prepareHTTP(r)
	assert.FatalIf(t, err == nil, "%s", "expected an error for an absent certSecretRef Secret")

	assert.ErrorIf(
		t,
		!errors.Is(err, manifest.ErrMissingSecret),
		"missing certSecretRef Secret must be the ErrMissingSecret sentinel; got %v",
		err,
	)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok, "expected MissingSecretError; got %T", err)
	assert.Equal(
		t,
		missing.Owner,
		manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindHelmRepository,
			Namespace: "ns",
			Name:      "charts",
		},
	)
	assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "tls"})
	assert.Equal(t, err.Error(), "secret ns/tls not found")
	assert.ErrorIf(t, prepared != nil, "%s", "repository should be nil on error")
}

func TestHelmRepoTransport_MissingMaterialSoftSkips(t *testing.T) {
	t.Parallel()
	ca := testutil.SelfSignedCA(t)
	for _, tc := range []struct {
		name string
		data map[string]any
		soft bool
	}{
		{"unrelated", map[string]any{"unrelated": "x"}, true},
		{"empty", map[string]any{"ca.crt": ""}, true},
		{"malformed", map[string]any{"ca.crt": "broken"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httpRepo("https://charts.example")
			r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
			secret := &manifest.Secret{StringData: map[string]any{"ca.crt": ca}}
			f, fInputs := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return secret })
			preparedHTTP(t, f, fInputs, r)
			secret = &manifest.Secret{StringData: tc.data}
			prepared, err := (&operation{Fetcher: f, secrets: fInputs.lookup(r)}).prepareHTTP(r)
			assert.FatalIf(t, err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.soft || prepared != nil,
				"warm memo bypassed material policy: repository=%v, error=%v", prepared, err)
		})
	}
}

func TestHelmRepoTransport_ProxySnapshot(t *testing.T) {
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("HTTPS_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("http_proxy", "http://proxy:first@proxy.invalid:8080")
	t.Setenv("https_proxy", "")
	t.Setenv("no_proxy", "bypass.invalid")
	r := httpRepo("http://bypass.invalid")
	f, fInputs := newHTTPFetcher(t, r)
	first := preparedHTTP(t, f, fInputs, r)
	t.Setenv("http_proxy", "http://proxy:second@proxy.invalid:8080")
	second := preparedHTTP(t, f, fInputs, r)
	assert.FatalIf(
		t,
		manifest.Fingerprint(first.material) == manifest.Fingerprint(second.material),
		"%s",
		"cross-host proxy rotation reused identity",
	)

	for i, password := range []string{"first", "second"} {
		t.Setenv("http_proxy", "http://proxy:"+password+"@proxy.invalid:8080")
		material, err := source.ResolveTransport(nil, manifest.DiagnosticFor(r.Named(), r), nil)
		assert.NoError(t, err)
		expected, err := f.network.HTTPTransport(
			source.HTTPTransportConfig{DisableCompression: true, Material: material},
		)
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			[]*httpRepository{first, second}[i].client.Transport != expected,
			"%s",
			"transport did not retain its captured proxy environment",
		)
	}
}

//nolint:paralleltest // Credential fixtures change the process environment and resolver state.
func TestHelmRepoTransport_ClientReuse(t *testing.T) {
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"} {
		t.Setenv(name, "")
	}
	ca := testutil.SelfSignedCA(t)
	otherCA := testutil.SelfSignedCA(t)
	for _, settings := range []string{"anonymous", "credentials", "ambient", "cert", "proxy"} {
		t.Run(settings, func(t *testing.T) {
			testHelmRepoTransportClientReuseCase(
				t,
				&ca, &otherCA, &settings,
			)
		},
		)
	}
}

func TestHelmRepoTransport_OwnedCleanup(t *testing.T) {
	t.Parallel()
	idle, closed := make(chan struct{}, 1), make(chan struct{}, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "chart")
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew, http.StateActive, http.StateHijacked:
		case http.StateIdle:
			idle <- struct{}{}
		case http.StateClosed:
			closed <- struct{}{}
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	network := newTestNetwork(t)
	r := httpRepo(srv.URL)
	fInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return r }}
	f := New(nil, Config{Cache: cachetest.New(t, "", sourcecache.New), Network: network})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := f.Fetch(
		ctx,
		helmChart("repo", "app", "1.0.0"),
		fInputs.readSet(helmChart("repo", "app", "1.0.0")),
		&sourcecache.Policy{},
	)
	assert.FatalIf(t, err == nil, "%s", "malformed index unexpectedly succeeded")

	select {
	case <-idle:
	case <-ctx.Done():
		t.Fatal("request did not leave an idle connection")
	}
	select {
	case <-closed:
		t.Fatal("borrower closed the transport")
	default:
	}
	network.Close()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("owner cleanup did not close the guarded transport's idle connection")
	}
}

type timeoutChartBody struct{ ctx context.Context } //nolint:containedctx // The test stores the producer context to assert deadline and cancellation propagation.

func (b timeoutChartBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (timeoutChartBody) Close() error { return nil }

func TestHelmRepoTransport_SourceTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		timeout, caller time.Duration
	}{
		{name: "default"},
		{name: "shorter", timeout: 10 * time.Second},
		{name: "longer", timeout: 180 * time.Second},
		{name: "caller", timeout: 180 * time.Second, caller: 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testHelmRepoTransportSourceTimeoutCase(t, &tc)
		},
		)
	}
}

func testHelmRepoTransportClientReuseCase(t *testing.T, ca *string,
	otherCA *string,
	settings *string,
) {
	t.Helper()
	r := httpRepo("http://charts.invalid")
	secret := &manifest.Secret{
		StringData: map[string]any{"username": "alice", "password": "first", "ca.crt": (*ca)},
	}
	if (*settings) == "credentials" || (*settings) == "ambient" {
		r.SecretRef = &manifest.LocalObjectReference{Name: "first"}
	}
	if (*settings) == "ambient" {
		secret = nil
	}
	if (*settings) == "cert" {
		r.CertSecretRef = &manifest.LocalObjectReference{Name: "first"}
	}
	if (*settings) == "proxy" {
		t.Setenv("http_proxy", "http://proxy:first@proxy.invalid:8080")
	}
	f, fInputs := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return secret })
	other := *r
	other.Name = "other"
	if r.SecretRef != nil {
		other.SecretRef = &manifest.LocalObjectReference{Name: "equivalent"}
	}
	if r.CertSecretRef != nil {
		other.CertSecretRef = &manifest.LocalObjectReference{Name: "equivalent"}
	}
	var prepared [2]*httpRepository
	var errs [2]error
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, repo := range []*manifest.HelmRepository{r, &other} {
		wg.Go(func() {
			<-start
			prepared[i], errs[i] = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).prepareHTTP(repo)
		})
	}
	testHelmRepoTransportClientReuseCaseCompletion(
		t,
		&testHelmRepoTransportClientReuseCaseCompletionInputs{
			start:    &start,
			wg:       &wg,
			errs:     &errs,
			settings: &settings,
			prepared: &prepared,
			f:        &f,
			fInputs:  &fInputs,
			r:        &r,
			secret:   &secret,
			ca:       &ca,
			otherCA:  &otherCA,
		},
	)
}

func testHelmRepoTransportClientReuseCaseCompletion(
	t *testing.T,
	captures *testHelmRepoTransportClientReuseCaseCompletionInputs,
) {
	t.Helper()
	close((*captures.start))
	captures.wg.
		Wait()
	for i, err := range *captures.errs {
		if (*(*captures.settings)) == "ambient" {
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrMissingSecret) || captures.prepared[i] == nil,
				"missing auth lost its prepared transport: %v",
				err,
			)
		} else {
			assert.NoError(t, err)
		}
	}
	first, second := captures.prepared[0], captures.prepared[1]
	assert.FatalIf(t, manifest.Fingerprint(first.material) != manifest.Fingerprint(second.material) ||
		first.client.Transport != second.client.Transport, "%s", "equal captured identities did not share a transport")
	assert.FatalIf(
		t,
		((*(*captures.settings)) == "anonymous" || (*(*captures.settings)) == "ambient") && first.material != nil,
		"%s",
		"credential-free identity must be empty",
	)

	warm, err := (&operation{Fetcher: (*captures.f), secrets: captures.fInputs.lookup((*captures.r))}).prepareHTTP(
		(*captures.r),
	)
	assert.FatalIf(t, (*(*captures.settings)) != "ambient" && err != nil || (*(*captures.settings)) ==
		"ambient" &&
		!errors.Is(err, manifest.ErrMissingSecret), "warm preparation lost auth state: %v", err)
	assert.FatalIf(t, warm.client.Transport != first.client.Transport, "%s", "warm preparation replaced the transport")

	switch *(*captures.settings) {
	case "anonymous":
		(*captures.r) = httpRepo("http://other.invalid")
	case "credentials", "ambient":
		(*captures.secret) = &manifest.Secret{
			StringData: map[string]any{"username": "alice", "password": "second", "ca.crt": (*(*captures.ca))},
		}
	case "cert":
		(*captures.secret) = &manifest.Secret{
			StringData: map[string]any{"username": "alice", "password": "first", "ca.crt": (*(*captures.otherCA))},
		}
	case "proxy":
		t.Setenv("http_proxy", "http://proxy:second@proxy.invalid:8080")
	}
	changed := preparedHTTP(t, (*captures.f), (*captures.fInputs), (*captures.r))
	equal := (*(*captures.settings)) == "anonymous"
	assert.FatalIf(t, (manifest.Fingerprint(first.material) == manifest.Fingerprint(changed.material)) != equal ||
		(first.client.Transport == changed.client.Transport) != equal, "%s", "client isolation did not follow captured identity")
}

func testHelmRepoTransportSourceTimeoutCase(t *testing.T, tc *struct {
	name    string
	timeout time.Duration
	caller  time.
		Duration
},
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		r := httpRepo("https://charts.invalid")
		if tc.timeout != 0 {
			r.Timeout = &metav1.Duration{Duration: tc.timeout}
		}
		want := (sourcev1.HelmRepository{Spec: r.HelmRepositorySpec}).GetTimeout()
		ctx := t.Context()
		if tc.caller != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, tc.caller)
			defer cancel()
			want = min(want, tc.caller)
		}
		f, inputs := newHTTPFetcher(t, r)
		prepared := preparedHTTP(t, f, inputs, r)
		prepared.client.Transport = chartTransport(func(req *http.Request) (*http.Response, error) {
			deadline, ok := req.Context().Deadline()
			assert.ErrorIf(t, !ok || time.Until(deadline) != want,
				"request deadline remaining=%v present=%t, want %v", time.Until(deadline), ok, want)

			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       timeoutChartBody{req.Context()},
				Request:    req,
			}, nil
		})
		start := time.Now()
		_, err := f.fetchHTTPChart(
			ctx,
			prepared,
			helmChart(r.Name, "app-template", "1.0.0"),
			&sourcecache.Policy{},
		)
		assert.ErrorIf(t, !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != want,

			"body deadline: elapsed=%v error=%v, want %v and DeadlineExceeded",
			time.Since(start),
			err,
			want)
	})
}

type testHelmRepoTransportClientReuseCaseCompletionInputs struct {
	start    *chan struct{}
	wg       *sync.WaitGroup
	errs     *[2]error
	settings **string
	prepared *[2]*httpRepository
	f        **Fetcher
	fInputs  *testInputs
	r        **manifest.HelmRepository
	secret   **manifest.Secret
	ca       **string
	otherCA  **string
}
