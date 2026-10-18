package bucket

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
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
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_ResolveTransport_NoCertSecretStaysBounded(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func TestFetcher_SecretReferences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, ref, secret, wantError string
		data                         map[string]any
		unwired, encoded, missing    bool
	}{
		{name: "auth unwired", ref: "auth", secret: "creds", unwired: true, wantError: "SecretGetter"},
		{name: "auth missing key", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "k"}, missing: true, wantError: "missing accesskey/secretkey"},
		{name: "accesskey/encoded=false", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "..PLACEHOLDER_accesskey..", "secretkey": "secret"}, missing: true},
		{name: "accesskey/encoded=true", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "..PLACEHOLDER_accesskey..", "secretkey": "secret"}, encoded: true, missing: true},
		{name: "secretkey/encoded=false", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "access", "secretkey": "..PLACEHOLDER_secretkey.."}, missing: true},
		{name: "secretkey/encoded=true", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "access", "secretkey": "..PLACEHOLDER_secretkey.."}, encoded: true, missing: true},
		{name: "auth absent", ref: "auth", secret: "creds", missing: true, wantError: "secret ns/creds not found"},
		{name: "cert partial pair", ref: "cert", secret: "tls", data: map[string]any{"tls.crt": "-only-cert-"}, wantError: "must provide both"},
		{name: "cert empty", ref: "cert", secret: "tls", data: map[string]any{"unrelated": "x"}, missing: true},
		{name: "cert absent", ref: "cert", secret: "missing", missing: true},
		{name: "proxy absent", ref: "proxy", secret: "missing", missing: true},
		{name: "cert unwired", ref: "cert", secret: "tls", unwired: true, wantError: "source.SecretGetter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &Fetcher{}
			if !tc.unwired {
				f.Secrets = func(_, _ string) *manifest.Secret {
					if tc.data == nil {
						return nil
					}
					sec := &manifest.Secret{StringData: tc.data}
					if tc.encoded {
						sec.StringData, sec.Data = nil, map[string]any{}
						for k, v := range tc.data {
							sec.Data[k] = base64.StdEncoding.EncodeToString([]byte(v.(string)))
						}
					}
					return sec
				}
			}
			b := &manifest.Bucket{Name: "b", Namespace: "ns", BucketName: "fixture", Endpoint: "127.0.0.1:1", Insecure: true}
			ref := &manifest.LocalObjectReference{Name: tc.secret}
			var err error
			switch tc.ref {
			case "auth":
				b.SecretRef = ref
				_, err = f.Fetch(t.Context(), b, cache.RetryConfig{})
			case "cert":
				b.CertSecretRef = ref
				_, err = f.resolveTransport(b)
			case "proxy":
				b.ProxySecretRef = ref
				_, err = f.resolveTransport(b)
			}
			if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("missing=%t, error=%v, want %q", tc.missing, err, tc.wantError)
			}
			if tc.missing {
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok {
					t.Fatalf("expected MissingSecretError; got %T", err)
				}
				owner := b.Named()
				if tc.ref != "auth" {
					owner.Name = "ns/b"
				}
				assert.Equal(t, missing.Owner, owner)
				assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: tc.secret})
				if tc.ref != "auth" && tc.data == nil {
					assert.Equal(t, err.Error(), manifest.ErrMissingSecret.Error()+": Bucket ns/ns/b: secret ns/missing not found")
				}
			}
		})
	}
}

func TestFetcher_NetworkSuppressionSkipsBackoff(t *testing.T) {
	t.Parallel()
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
		c := cachetest.New(t, "", cache.New)
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
			c := cachetest.New(t, root, cache.New)
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
		})
	}
	if lookups.Load() != 15 {
		t.Fatalf("credentials and cert reread Secrets: %d", lookups.Load())
	}
}
