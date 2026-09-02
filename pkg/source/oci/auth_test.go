package oci

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func ociRepo(name string, set func(s *sourcev1.OCIRepositorySpec)) *manifest.OCIRepository {
	r := &manifest.OCIRepository{Name: name, Namespace: "ns"}
	if set != nil {
		set(&r.OCIRepositorySpec)
	}
	return r
}

func TestFetcher_ResolveTLS_NoCertSecretIsNil(t *testing.T) {
	f := &Fetcher{}
	repo := ociRepo("o", nil)
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
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) { s.Insecure = true })
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
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	})
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
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	})
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
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	})
	_, err := f.resolveTLS(repo)
	if err == nil || !strings.Contains(err.Error(), "tls.crt") {
		t.Errorf("expected missing-keys error; got %v", err)
	}
}

func TestFetcher_NonGenericProvider(t *testing.T) {
	// No credential source configured anywhere: no SecretRef, no
	// --registry-config, and the docker default lookup finds nothing
	// (DOCKER_CONFIG points at an empty dir, isolating this from whatever
	// docker config the machine running the test happens to have).
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	f := &Fetcher{}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.Provider = sourcev1.AmazonOCIProvider
	})
	_, err := f.Fetch(context.Background(), repo)
	if err == nil {
		t.Fatalf("expected error for unimplemented provider")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should say 'not implemented'; got %v", err)
	}
}

func TestFetcher_NonGenericProvider_RegistryConfigFallsBack(t *testing.T) {
	layerBytes := mustTarGz(t, map[string]string{"Chart.yaml": "apiVersion: v2\nname: x\nversion: 0.1.0\n"})
	configBytes := []byte(`{}`)
	manifestBytes := mustManifestJSON(t, configBytes, layerBytes,
		"application/vnd.cncf.flux.config.v1+json",
		"application/vnd.cncf.helm.chart.content.v1.tar+gzip",
	)
	srv := startFakeRegistry(t, manifestBytes, configBytes, layerBytes)

	// The fake registry doesn't check auth, so a config.json with no
	// matching host entry is enough to prove the provider check let the
	// fetch through to --registry-config instead of refusing outright.
	configPath := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(configPath, []byte(`{"auths":{}}`), 0o600); err != nil {
		t.Fatalf("write docker config: %v", err)
	}

	f := &Fetcher{RegistryConfig: configPath, Cache: source.NewCache(cacheroot.New(t.TempDir()))}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = fmt.Sprintf("oci://%s/x/y", mustURL(t, srv.URL).Host)
		s.Provider = sourcev1.GoogleOCIProvider
		s.Insecure = true
		s.Reference = &sourcev1.OCIRepositoryRef{Tag: "v1"}
	})
	if _, err := f.Fetch(context.Background(), repo); err != nil {
		t.Fatalf("Fetch: expected the --registry-config fallback to be used, got %v", err)
	}
}

func TestFetcher_NonGenericProvider_SecretRefFallsBack(t *testing.T) {
	layerBytes := mustTarGz(t, map[string]string{"Chart.yaml": "apiVersion: v2\nname: x\nversion: 0.1.0\n"})
	configBytes := []byte(`{}`)
	manifestBytes := mustManifestJSON(t, configBytes, layerBytes,
		"application/vnd.cncf.flux.config.v1+json",
		"application/vnd.cncf.helm.chart.content.v1.tar+gzip",
	)
	srv := startFakeRegistry(t, manifestBytes, configBytes, layerBytes)

	f := &Fetcher{
		Cache: source.NewCache(cacheroot.New(t.TempDir())),
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": `{"auths":{}}`}}
		},
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = fmt.Sprintf("oci://%s/x/y", mustURL(t, srv.URL).Host)
		s.Provider = sourcev1.GoogleOCIProvider
		s.Insecure = true
		s.Reference = &sourcev1.OCIRepositoryRef{Tag: "v1"}
		s.SecretRef = &manifest.LocalObjectReference{Name: "registry-creds"}
	})
	if _, err := f.Fetch(context.Background(), repo); err != nil {
		t.Fatalf("Fetch: expected the SecretRef fallback to be used, got %v", err)
	}
}

func TestFetcher_ResolveConfig_NoSecretFallsBackToGlobal(t *testing.T) {
	f := &Fetcher{RegistryConfig: "/etc/docker/config.json"}
	repo := ociRepo("o", nil)
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
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "ghcr-creds"}
	})
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
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.SecretRef = &manifest.LocalObjectReference{Name: "wrong-shape"}
	})
	_, cleanup, err := f.resolveRegistryConfig(repo)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), ".dockerconfigjson") {
		t.Errorf("expected missing-.dockerconfigjson error; got %v", err)
	}
	// The wrong-shape / placeholder case must wrap ErrMissingSecret so
	// --allow-missing-secrets covers it. This is the actual #190 case:
	// an ExternalSecret materializes the Secret manifest in-tree but
	// the values get PLACEHOLDER-wiped, so StringFromSecret returns ""
	// and we land here, not in the "not found" branch.
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("expected ErrMissingSecret wrap so --allow-missing-secrets handles ExternalSecret/placeholder case; got %v", err)
	}
}

func TestFetcher_ResolveConfig_SecretRefWithoutGetter(t *testing.T) {
	f := &Fetcher{} // no Secrets
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	})
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
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
	})
	_, cleanup, err := f.resolveRegistryConfig(repo)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "secret ns/missing not found") {
		t.Errorf("expected secret-not-found error; got %v", err)
	}
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("expected ErrMissingSecret wrap; got %v", err)
	}
}
