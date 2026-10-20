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
	"time"

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
	f := newHTTPFetcherWithSecrets(t, r, nil)

	prepared, err := f.prepareHTTP(r)
	if err != nil {
		t.Fatalf("prepareHTTP: %v", err)
	}
	expected, err := f.network.HTTPTransport(source.HTTPTransportConfig{DisableCompression: true})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.client.Transport != expected {
		t.Fatal("transport applied customization without a certificate reference")
	}
}

func TestHelmRepoTransport_CertSecret(t *testing.T) {
	t.Parallel()
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	ca := testutil.SelfSignedCA(t)
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"ca.crt": ca}}
	})

	prepared, err := f.prepareHTTP(r)
	if err != nil {
		t.Fatalf("prepareHTTP: %v", err)
	}
	material, err := source.ResolveTransport(f.secrets, r.Named(), []source.TransportInput{{Reference: r.CertSecretRef, Use: source.TransportCert}})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := f.network.HTTPTransport(source.HTTPTransportConfig{Material: material, DisableCompression: true})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.client.Transport != expected {
		t.Fatal("transport did not apply the certificate reference")
	}
}

func TestHelmRepoTransport_MissingSecretIsSoftSkippable(t *testing.T) {
	t.Parallel()
	r := httpRepo("https://charts.example")
	r.Name, r.Namespace = "charts", "ns"
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	secret := &manifest.Secret{StringData: map[string]any{"ca.crt": testutil.SelfSignedCA(t)}}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return secret })
	preparedHTTP(t, f, r)
	secret = nil

	prepared, err := f.prepareHTTP(r)
	if err == nil {
		t.Fatal("expected an error for an absent certSecretRef Secret")
	}
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("missing certSecretRef Secret must be the ErrMissingSecret sentinel; got %v", err)
	}
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok {
		t.Fatalf("expected MissingSecretError; got %T", err)
	}
	assert.Equal(t, missing.Owner, manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "ns", Name: "HelmRepository ns/charts"})
	assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "tls"})
	assert.Equal(t, err.Error(), manifest.ErrMissingSecret.Error()+": HelmRepository ns/HelmRepository ns/charts: secret ns/tls not found")
	if prepared != nil {
		t.Error("repository should be nil on error")
	}
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
		{"wiped", map[string]any{"ca.crt": "..PLACEHOLDER_ca.crt.."}, true},
		{"malformed", map[string]any{"ca.crt": "broken"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httpRepo("https://charts.example")
			r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
			secret := &manifest.Secret{StringData: map[string]any{"ca.crt": ca}}
			f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return secret })
			preparedHTTP(t, f, r)
			secret = &manifest.Secret{StringData: tc.data}
			prepared, err := f.prepareHTTP(r)
			if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.soft || prepared != nil {
				t.Fatalf("warm memo bypassed material policy: repository=%v, error=%v", prepared, err)
			}
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
	f := newHTTPFetcher(t, r)
	first := preparedHTTP(t, f, r)
	t.Setenv("http_proxy", "http://proxy:second@proxy.invalid:8080")
	second := preparedHTTP(t, f, r)
	if manifest.Fingerprint(first.material) == manifest.Fingerprint(second.material) {
		t.Fatal("cross-host proxy rotation reused identity")
	}
	for i, password := range []string{"first", "second"} {
		t.Setenv("http_proxy", "http://proxy:"+password+"@proxy.invalid:8080")
		material, err := source.ResolveTransport(nil, r.Named(), nil)
		assert.NoError(t, err)
		expected, err := f.network.HTTPTransport(source.HTTPTransportConfig{DisableCompression: true, Material: material})
		if err != nil {
			t.Fatal(err)
		}
		if []*httpRepository{first, second}[i].client.Transport != expected {
			t.Fatal("transport did not retain its captured proxy environment")
		}
	}
}

func TestHelmRepoTransport_ClientReuse(t *testing.T) {
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"} {
		t.Setenv(name, "")
	}
	ca := testutil.SelfSignedCA(t)
	otherCA := testutil.SelfSignedCA(t)
	for _, settings := range []string{"anonymous", "credentials", "cert", "proxy"} {
		t.Run(settings, func(t *testing.T) {
			r := httpRepo("http://charts.invalid")
			secret := &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "first", "ca.crt": ca}}
			if settings == "credentials" {
				r.SecretRef = &manifest.LocalObjectReference{Name: "first"}
			}
			if settings == "cert" {
				r.CertSecretRef = &manifest.LocalObjectReference{Name: "first"}
			}
			if settings == "proxy" {
				t.Setenv("http_proxy", "http://proxy:first@proxy.invalid:8080")
			}
			f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return secret })
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
					prepared[i], errs[i] = f.prepareHTTP(repo)
				})
			}
			close(start)
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			first, second := prepared[0], prepared[1]
			if manifest.Fingerprint(first.material) != manifest.Fingerprint(second.material) || first.client.Transport != second.client.Transport {
				t.Fatal("equal captured identities did not share a transport")
			}
			if settings == "anonymous" && first.material != nil {
				t.Fatal("anonymous identity must be empty")
			}
			if warm := preparedHTTP(t, f, r); warm.client.Transport != first.client.Transport {
				t.Fatal("warm preparation replaced the transport")
			}
			switch settings {
			case "anonymous":
				r = httpRepo("http://other.invalid")
			case "credentials":
				secret = &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "second", "ca.crt": ca}}
			case "cert":
				secret = &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "first", "ca.crt": otherCA}}
			case "proxy":
				t.Setenv("http_proxy", "http://proxy:second@proxy.invalid:8080")
			}
			changed := preparedHTTP(t, f, r)
			equal := settings == "anonymous"
			if (manifest.Fingerprint(first.material) == manifest.Fingerprint(changed.material)) != equal || (first.client.Transport == changed.client.Transport) != equal {
				t.Fatal("client isolation did not follow captured identity")
			}
		})
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
	f := New(nil, func(_, _ string) *manifest.HelmRepository { return r }, nil, Config{Cache: cachetest.New(t, "", sourcecache.New), Network: network})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := f.Fetch(ctx, helmChart("repo", "app", "1.0.0"), sourcecache.Policy{})
	if err == nil {
		t.Fatal("malformed index unexpectedly succeeded")
	}
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
