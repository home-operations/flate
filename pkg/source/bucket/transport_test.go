package bucket

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/minio/minio-go/v7"

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
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret { return nil },
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "missing"},
	}
	_, err := f.resolveTransport(b)
	if err == nil || !strings.Contains(err.Error(), "secret ns/missing not found") {
		t.Errorf("expected secret-not-found error; got %v", err)
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
