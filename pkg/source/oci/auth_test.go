package oci

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFetcher_ResolveTLS_NoCertSecretIsNil(t *testing.T) {
	f := &Fetcher{}
	repo := &manifest.OCIRepository{Name: "o", Namespace: "ns"}
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg != nil {
		t.Errorf("expected nil TLS config when no CertSecretRef + Insecure=false")
	}
}

func TestFetcher_ResolveTLS_Insecure(t *testing.T) {
	f := &Fetcher{}
	repo := &manifest.OCIRepository{Name: "o", Namespace: "ns", Insecure: true}
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg == nil || !cfg.InsecureSkipVerify {
		t.Errorf("expected Insecure to set InsecureSkipVerify: %+v", cfg)
	}
}

// TestFetcher_ResolveTLS_FromSecret uses a real ephemeral cert/key
// pair — tls.X509KeyPair actually parses it so we can't hardcode.
func TestFetcher_ResolveTLS_FromSecret(t *testing.T) {
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
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
	}
	cfg, err := f.resolveTLS(repo)
	if err != nil {
		t.Fatalf("resolveTLS: %v", err)
	}
	if cfg == nil {
		t.Fatalf("expected non-nil TLS config")
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Errorf("expected RootCAs populated from ca.crt")
	}
}

func TestFetcher_ResolveTLS_PartialCertKey(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"tls.crt": "-only-cert-"}}
		},
	}
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
	}
	_, err := f.resolveTLS(repo)
	if err == nil || !strings.Contains(err.Error(), "must provide both") {
		t.Errorf("expected partial cert/key error; got %v", err)
	}
}

func TestFetcher_ResolveTLS_AllKeysMissing(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"unrelated": "x"}}
		},
	}
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
	}
	_, err := f.resolveTLS(repo)
	if err == nil || !strings.Contains(err.Error(), "tls.crt") {
		t.Errorf("expected missing-keys error; got %v", err)
	}
}


func TestFetcher_NonGenericProvider(t *testing.T) {
	f := &Fetcher{}
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		URL:      "oci://ghcr.io/x/y",
		Provider: manifest.OCIProviderAmazon,
	}
	_, err := f.Fetch(context.Background(), repo)
	if err == nil {
		t.Fatalf("expected error for unimplemented provider")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should say 'not implemented'; got %v", err)
	}
}

func TestFetcher_ResolveConfig_NoSecretFallsBackToGlobal(t *testing.T) {
	f := &Fetcher{RegistryConfig: "/etc/docker/config.json"}
	repo := &manifest.OCIRepository{Name: "o", Namespace: "ns"}
	path, cleanup, err := f.resolveRegistryConfig(repo)
	defer cleanup()
	if err != nil {
		t.Fatalf("resolveRegistryConfig: %v", err)
	}
	if path != "/etc/docker/config.json" {
		t.Errorf("path = %q, want /etc/docker/config.json", path)
	}
}

func TestFetcher_ResolveConfig_SecretWritesTempFile(t *testing.T) {
	dockerJSON := `{"auths":{"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{
				StringData: map[string]any{".dockerconfigjson": dockerJSON},
			}
		},
	}
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		URL:       "oci://ghcr.io/x/y",
		SecretRef: &manifest.LocalObjectReference{Name: "ghcr-creds"},
	}
	path, cleanup, err := f.resolveRegistryConfig(repo)
	defer cleanup()
	if err != nil {
		t.Fatalf("resolveRegistryConfig: %v", err)
	}
	if path == "" {
		t.Fatalf("expected temp file path, got empty")
	}
	data, err := os.ReadFile(path) //nolint:gosec // path is a temp file produced by the fetcher under test
	if err != nil {
		t.Fatalf("read temp file: %v", err)
	}
	if string(data) != dockerJSON {
		t.Errorf("temp file content mismatch")
	}
	// cleanup should remove the file.
	cleanup()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("temp file not removed by cleanup: stat err = %v", err)
	}
}

func TestFetcher_ResolveConfig_SecretMissingDockerConfigJSON(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{
				StringData: map[string]any{"username": "alice"}, // wrong shape
			}
		},
	}
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "wrong-shape"},
	}
	_, cleanup, err := f.resolveRegistryConfig(repo)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), ".dockerconfigjson") {
		t.Errorf("expected missing-.dockerconfigjson error; got %v", err)
	}
}

func TestFetcher_ResolveConfig_SecretRefWithoutGetter(t *testing.T) {
	f := &Fetcher{} // no Secrets
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, cleanup, err := f.resolveRegistryConfig(repo)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "source.SecretGetter") {
		t.Errorf("expected source.SecretGetter error; got %v", err)
	}
}

func TestFetcher_ResolveConfig_SecretNotFound(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret { return nil },
	}
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "missing"},
	}
	_, cleanup, err := f.resolveRegistryConfig(repo)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "secret ns/missing not found") {
		t.Errorf("expected secret-not-found error; got %v", err)
	}
}
