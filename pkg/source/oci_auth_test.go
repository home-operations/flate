package source

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestOCIFetcher_NonGenericProvider(t *testing.T) {
	f := &OCIFetcher{}
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

func TestOCIFetcher_ResolveConfig_NoSecretFallsBackToGlobal(t *testing.T) {
	f := &OCIFetcher{RegistryConfig: "/etc/docker/config.json"}
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

func TestOCIFetcher_ResolveConfig_SecretWritesTempFile(t *testing.T) {
	dockerJSON := `{"auths":{"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`
	f := &OCIFetcher{
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

func TestOCIFetcher_ResolveConfig_SecretMissingDockerConfigJSON(t *testing.T) {
	f := &OCIFetcher{
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

func TestOCIFetcher_ResolveConfig_SecretRefWithoutGetter(t *testing.T) {
	f := &OCIFetcher{} // no Secrets
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, cleanup, err := f.resolveRegistryConfig(repo)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "SecretGetter") {
		t.Errorf("expected SecretGetter error; got %v", err)
	}
}

func TestOCIFetcher_ResolveConfig_SecretNotFound(t *testing.T) {
	f := &OCIFetcher{
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
