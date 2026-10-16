package bucket

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_ResolveTransport_NoCertSecretStaysBounded(t *testing.T) {
	f := &Fetcher{}
	b := &manifest.Bucket{Name: "b", Namespace: "ns"}
	tr, err := f.resolveTransport(b)
	if err != nil {
		t.Fatalf("resolveTransport: %v", err)
	}
	// NewHTTPTransport now always returns a bounded transport (never nil), so
	// even an anonymous bucket inherits the ResponseHeaderTimeout backstop.
	if tr == nil {
		t.Fatal("expected a bounded transport, got nil")
	}
	if tr.ResponseHeaderTimeout != source.ResponseHeaderTimeout {
		t.Errorf("ResponseHeaderTimeout = %v; want %v", tr.ResponseHeaderTimeout, source.ResponseHeaderTimeout)
	}
}

func TestFetcher_ResolveTransport_FromSecret(t *testing.T) {
	certPEM, keyPEM := testutil.SelfSignedServerCert(t)
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{
				StringData: map[string]any{
					"tls.crt": certPEM,
					"tls.key": keyPEM,
					"ca.crt":  certPEM,
				},
			}
		},
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
	}
	tr, err := f.resolveTransport(b)
	if err != nil {
		t.Fatalf("resolveTransport: %v", err)
	}
	if tr == nil {
		t.Fatalf("expected non-nil transport")
	}
	if tr.TLSClientConfig == nil {
		t.Fatalf("expected TLSClientConfig set")
	}
	if len(tr.TLSClientConfig.Certificates) != 1 {
		t.Errorf("expected 1 client cert, got %d", len(tr.TLSClientConfig.Certificates))
	}
	if tr.TLSClientConfig.RootCAs == nil {
		t.Errorf("expected RootCAs populated from ca.crt")
	}
}

func TestFetcher_ResolveTransport_PartialCertKey(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"tls.crt": "-only-cert-"}}
		},
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
	}
	_, err := f.resolveTransport(b)
	if err == nil || !strings.Contains(err.Error(), "must provide both") {
		t.Errorf("expected partial cert/key error; got %v", err)
	}
}

func TestFetcher_ResolveTransport_AllKeysMissing(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"unrelated": "x"}}
		},
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
	}
	_, err := f.resolveTransport(b)
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("expected missing-keys error; got %v", err)
	}
}

func TestFetcher_ResolveTransport_SecretNotFound(t *testing.T) {
	for _, tc := range []struct {
		name        string
		proxy, cert *manifest.LocalObjectReference
	}{
		{name: "proxy", proxy: &manifest.LocalObjectReference{Name: "missing"}},
		{name: "cert", cert: &manifest.LocalObjectReference{Name: "missing"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &Fetcher{Secrets: func(_, _ string) *manifest.Secret { return nil }}
			b := &manifest.Bucket{
				Name: "b", Namespace: "ns", ProxySecretRef: tc.proxy, CertSecretRef: tc.cert,
			}
			_, err := f.resolveTransport(b)
			if !errors.Is(err, manifest.ErrMissingSecret) {
				t.Fatalf("expected missing-secret error; got %v", err)
			}
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok {
				t.Fatalf("expected MissingSecretError; got %T", err)
			}
			assert.Equal(t, missing.Owner, manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindBucket, Namespace: "ns", Name: "ns/b"})
			assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "missing"})
			assert.Equal(t, err.Error(), manifest.ErrMissingSecret.Error()+": Bucket ns/ns/b: secret ns/missing not found")
		})
	}
}

func TestFetcher_ResolveTransport_CertSecretRefWithoutGetter(t *testing.T) {
	f := &Fetcher{} // no Secrets
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
	}
	_, err := f.resolveTransport(b)
	if err == nil || !strings.Contains(err.Error(), "source.SecretGetter") {
		t.Errorf("expected source.SecretGetter error; got %v", err)
	}
}

func TestFetcher_NetworkSuppressionSkipsBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var network source.Network
		tr, _ := source.NewHTTPTransport(nil, nil)
		tr.Proxy = nil
		cause := &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}
		calls := 0
		tr.DialContext = func(context.Context, string, string) (net.Conn, error) {
			calls++
			return nil, cause
		}
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://bucket.test", nil)
		_, err := (&http.Client{Transport: network.HTTPTransport(tr)}).Do(req)
		terminal, ok := errors.AsType[*cache.TerminalError](err)
		if !ok || !terminal.Attempted {
			t.Fatalf("seed: %v", err)
		}
		c, err := cache.New(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		start := time.Now()
		_, err = (&Fetcher{Cache: c, Network: &network}).Fetch(t.Context(), &manifest.Bucket{Name: "network", Namespace: "test", BucketName: "data", Endpoint: "bucket.test", Insecure: true, Region: "us-east-1"}, cache.RetryConfig{Attempts: 3, MinWait: time.Hour})
		terminal, ok = errors.AsType[*cache.TerminalError](err)
		if !ok || terminal.Attempted || !errors.Is(err, cause) || calls != 1 || time.Since(start) > minio.DefaultRetryUnit {
			t.Fatalf("suppression: %v calls=%d elapsed=%v", err, calls, time.Since(start))
		}
	})
}

func TestFetcher_ResolveTransport_SnapshotRotation(t *testing.T) {
	var fail atomic.Bool
	var requests, lookups atomic.Int32
	var proxyPassword atomic.Value
	proxyPassword.Store("first")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.IsAbs() && r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("proxy:"+proxyPassword.Load().(string))) {
			t.Error("transport used a different proxy snapshot")
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		requests.Add(1)
		if !strings.Contains(r.Header.Get("Authorization"), "Credential=access/") {
			t.Error("static credentials lost")
		}
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
	})
	srv := httptest.NewUnstartedServer(handler)
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Listener = listener
	srv.Start()
	t.Cleanup(srv.Close)
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	root := t.TempDir()
	b := &manifest.Bucket{Name: "snapshot", Namespace: "ns", Endpoint: srv.URL, Insecure: true, Region: "us-east-1", BucketName: "data",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"}, CertSecretRef: &manifest.LocalObjectReference{Name: "creds"}}
	b.Interval.Duration = time.Hour
	password, ca := "first", testutil.SelfSignedCA(t)
	var previous string
	for _, wave := range []string{"first", "password", "certificate", "proxy", "direct"} {
		t.Run(wave, func(t *testing.T) {
			if wave == "password" {
				password = "second"
			}
			if wave == "certificate" {
				ca = testutil.SelfSignedCA(t)
			}
			proxyURL := strings.Replace(proxy.URL, "://", "://proxy:first@", 1)
			if wave == "proxy" || wave == "direct" {
				proxyURL = strings.Replace(proxy.URL, "://", "://proxy:second@", 1)
				proxyPassword.Store("second")
			}
			t.Setenv("http_proxy", proxyURL)
			if wave == "direct" {
				t.Setenv("no_proxy", "*")
			}
			c, err := cache.New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			f := &Fetcher{Cache: c, Secrets: func(_, _ string) *manifest.Secret {
				lookups.Add(1)
				return &manifest.Secret{StringData: map[string]any{"accesskey": "access", "secretkey": password, "ca.crt": ca}}
			}}
			before := requests.Load()
			art, err := f.Fetch(t.Context(), b, cache.RetryConfig{Attempts: 3})
			if err != nil {
				t.Fatal(err)
			}
			if previous == art.Key || requests.Load() != before+1 {
				t.Fatal("changed material reused acquisition")
			}
			previous = art.Key
			before = requests.Load()
			if _, err := f.Fetch(t.Context(), b, cache.RetryConfig{}); err != nil || requests.Load() != before {
				t.Fatalf("warm snapshot missed: %v", err)
			}
			fail.Store(true)
			failed := *b
			failed.Interval.Duration = 0
			_, err = f.Fetch(t.Context(), &failed, cache.RetryConfig{})
			terminal, ok := errors.AsType[*cache.TerminalError](err)
			if !ok || !terminal.Attempted {
				t.Fatalf("failed snapshot: %v", err)
			}
			fail.Store(false)
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			c, err = cache.New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			f.Cache = c
			before = requests.Load()
			_, err = f.Fetch(t.Context(), b, cache.RetryConfig{})
			terminal, ok = errors.AsType[*cache.TerminalError](err)
			if !ok || terminal.Attempted || requests.Load() != before {
				t.Fatalf("reopened snapshot replay: %v", err)
			}

		})
	}
	if lookups.Load() != 20 {
		t.Fatalf("credentials and cert reread Secrets: %d", lookups.Load())
	}
	if err := fs.WalkDir(os.DirFS(root), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		data, err := fs.ReadFile(os.DirFS(root), path)
		if err != nil {
			return err
		}
		for _, secret := range []string{"proxy:first@", "proxy:second@", "BEGIN CERTIFICATE"} {
			if strings.Contains(string(data), secret) {
				t.Errorf("credential material persisted in %s", path)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
