package helmchart

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

func TestHelmRepoTransport_NoCertRef(t *testing.T) {
	r := httpRepo("https://charts.example")
	f := newHTTPFetcherWithSecrets(t, r, nil)

	prepared, err := f.prepareHTTP(r)
	if err != nil {
		t.Fatalf("prepareHTTP: %v", err)
	}
	tr := prepared.client.Transport.(*http.Transport)
	if tr.TLSClientConfig != nil && tr.TLSClientConfig.RootCAs != nil {
		t.Fatal("unexpected custom CA roots")
	}
	if tr.DialContext == nil {
		t.Fatal("the default helm transport must be egress-guarded (DialContext set)")
	}
}

// TestHelmRepoTransport_CertSecretYieldsGuardedTransport: a certSecretRef with
// CA material yields a non-nil transport that is BOTH egress-guarded
// (DialContext set) and TLS-configured (RootCAs from the CA), passed to helm via
// WithTransport. Uses a real CA PEM so source.BuildTLSConfig accepts it.
func TestHelmRepoTransport_CertSecretYieldsGuardedTransport(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"ca.crt": testutil.SelfSignedCA(t)}}
	})

	prepared, err := f.prepareHTTP(r)
	if err != nil {
		t.Fatalf("prepareHTTP: %v", err)
	}
	tr := prepared.client.Transport.(*http.Transport)
	if tr == nil {
		t.Fatal("certSecretRef should yield a per-repo transport")
	}
	if tr.DialContext == nil {
		t.Error("per-repo helm transport must be egress-guarded (DialContext set)")
	}
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.RootCAs == nil {
		t.Error("certSecretRef CA must populate the transport's RootCAs")
	}
}

// TestHelmRepoTransport_MissingSecretIsSoftSkippable: an absent certSecretRef
// Secret surfaces the shared missing-secret sentinel (so --allow-missing-secrets
// covers it), matching git/OCI/bucket via source.ResolveCertSecret.
func TestHelmRepoTransport_MissingSecretIsSoftSkippable(t *testing.T) {
	r := httpRepo("https://charts.example")
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
	if prepared != nil {
		t.Error("repository should be nil on error")
	}
}

func TestHelmRepoTransport_MissingMaterialSoftSkips(t *testing.T) {
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
	if first.identity == second.identity {
		t.Fatal("cross-host proxy rotation reused identity")
	}
	for _, host := range []string{"chart.invalid", "bypass.invalid"} {
		req := &http.Request{URL: &url.URL{Scheme: "http", Host: host}}
		old, err := first.client.Transport.(*http.Transport).Proxy(req)
		if err != nil {
			t.Fatal(err)
		}
		current, err := second.client.Transport.(*http.Transport).Proxy(req)
		if err != nil {
			t.Fatal(err)
		}
		if host == "bypass.invalid" {
			if old != nil || current != nil {
				t.Fatal("captured bypass changed")
			}
		} else {
			if old == nil || old.User.String() != "proxy:first" || current == nil || current.User.String() != "proxy:second" {
				t.Fatal("captured proxy changed")
			}
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
			if first.identity != second.identity || first.client != second.client || first.client.Transport != second.client.Transport {
				t.Fatal("equal captured identities did not share a client and transport")
			}
			if settings == "anonymous" && first.identity != "" {
				t.Fatal("anonymous identity must be empty")
			}
			if warm := preparedHTTP(t, f, r); warm.client != first.client {
				t.Fatal("warm preparation replaced the client")
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
			if (first.identity == changed.identity) != equal || (first.client == changed.client) != equal || (first.client.Transport == changed.client.Transport) != equal {
				t.Fatal("client isolation did not follow captured identity")
			}
		})
	}
}

func TestHelmRepoTransport_OwnedCleanup(t *testing.T) {
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
	f, cleanup := New(nil, nil, nil, Config{Network: new(source.Network)})
	t.Cleanup(cleanup)
	cleanup()
	f.clients.Range(func(_, _ any) bool {
		t.Error("unused cleanup constructed a client")
		return true
	})
	r := httpRepo(srv.URL)
	prepared := preparedHTTP(t, f, r)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := prepared.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, resp.Body)
	if err := errors.Join(readErr, resp.Body.Close()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-idle:
	case <-ctx.Done():
		t.Fatal("request did not leave an idle connection")
	}
	select {
	case <-closed:
		t.Fatal("connection closed before owner cleanup")
	default:
	}
	cleanup()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("owner cleanup did not close the guarded transport's idle connection")
	}
}
