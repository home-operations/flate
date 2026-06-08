package bucket

import (
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
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
		BucketSpec: sourcev1.BucketSpec{
			CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
		},
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
		BucketSpec: sourcev1.BucketSpec{
			CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
		},
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
		BucketSpec: sourcev1.BucketSpec{
			CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
		},
	}
	_, err := f.resolveTransport(b)
	if err == nil || !strings.Contains(err.Error(), "tls.crt") {
		t.Errorf("expected missing-keys error; got %v", err)
	}
}

func TestFetcher_ResolveTransport_SecretNotFound(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret { return nil },
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		BucketSpec: sourcev1.BucketSpec{
			CertSecretRef: &manifest.LocalObjectReference{Name: "missing"},
		},
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
		BucketSpec: sourcev1.BucketSpec{
			CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
		},
	}
	_, err := f.resolveTransport(b)
	if err == nil || !strings.Contains(err.Error(), "source.SecretGetter") {
		t.Errorf("expected source.SecretGetter error; got %v", err)
	}
}
