package oci

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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

func TestFetcher_ResolveConfig_NoSecretFallsBackToGlobal(t *testing.T) {
	f := &Fetcher{RegistryConfig: "/etc/docker/config.json"}
	repo := ociRepo("o", nil)
	path, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
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
	path, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
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

// `data:`-form Secrets carry base64-encoded values; StringFromSecret
// decodes them, so resolution must produce the same temp file as the
// `stringData:` form.
func TestFetcher_ResolveConfig_SecretDataForm(t *testing.T) {
	dockerJSON := `{"auths":{"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{
				Data: map[string]any{".dockerconfigjson": base64.StdEncoding.EncodeToString([]byte(dockerJSON))},
			}
		},
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	})
	path, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
	defer cleanup()
	if err != nil {
		t.Fatalf("resolveRegistryConfig: %v", err)
	}
	data, err := os.ReadFile(path) //nolint:gosec // temp file produced by the fetcher under test
	if err != nil {
		t.Fatalf("read temp file: %v", err)
	}
	if string(data) != dockerJSON {
		t.Errorf("temp file content mismatch")
	}
}

func TestFetcher_ResolveConfig_SecretMissingDockerConfigJSON(t *testing.T) {
	// Pin an empty docker default so the fallback probe below can't pick
	// up credentials from the developer's / CI runner's real
	// ~/.docker/config.json — this test asserts the sentinel when NO
	// fallback credential exists.
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{
				StringData: map[string]any{"username": "alice"}, // wrong shape
			}
		},
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "wrong-shape"}
	})
	_, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
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
	_, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "source.SecretGetter") {
		t.Errorf("expected source.SecretGetter error; got %v", err)
	}
}

func TestFetcher_ResolveConfig_SecretNotFound(t *testing.T) {
	// Pin an empty docker default: the sentinel is only correct when no
	// fallback credential exists anywhere.
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret { return nil },
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
	})
	_, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "secret ns/missing not found") {
		t.Errorf("expected secret-not-found error; got %v", err)
	}
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("expected ErrMissingSecret wrap; got %v", err)
	}
}

// writeDockerConfig materializes a docker-style config.json with the given
// auths entries and returns its path. The #999 fallback probes these files,
// so the positive cases need a real config on disk, not just a path.
func writeDockerConfig(t *testing.T, auths string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"auths":{`+auths+`}}`), 0o600); err != nil {
		t.Fatalf("write docker config: %v", err)
	}
	return path
}

func TestFetcher_ResolveConfig_SecretMissingFallsBackToGlobal(t *testing.T) {
	cfg := writeDockerConfig(t, `"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}`)
	f := &Fetcher{
		RegistryConfig: cfg,
		Secrets:        func(_, _ string) *manifest.Secret { return nil },
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
	})
	path, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
	defer cleanup()
	if err != nil {
		t.Fatalf("resolveRegistryConfig: %v", err)
	}
	if path != cfg {
		t.Errorf("path = %q, want the --registry-config fallback %q", path, cfg)
	}
}

// The #190 ExternalSecret shape — the Secret exists but its
// .dockerconfigjson is absent or PLACEHOLDER-wiped — must fall back the
// same way a not-found Secret does.
func TestFetcher_ResolveConfig_MissingKeyFallsBackToGlobal(t *testing.T) {
	cfg := writeDockerConfig(t, `"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}`)
	cases := []struct {
		name   string
		secret *manifest.Secret
	}{
		{"wrong shape", &manifest.Secret{StringData: map[string]any{"username": "alice"}}},
		{"placeholder-wiped key", &manifest.Secret{StringData: map[string]any{
			".dockerconfigjson": "..PLACEHOLDER_.dockerconfigjson..",
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &Fetcher{
				RegistryConfig: cfg,
				Secrets:        func(_, _ string) *manifest.Secret { return tc.secret },
			}
			repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://ghcr.io/x/y"
				s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
			})
			path, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
			defer cleanup()
			if err != nil {
				t.Fatalf("resolveRegistryConfig: %v", err)
			}
			if path != cfg {
				t.Errorf("path = %q, want the --registry-config fallback %q", path, cfg)
			}
		})
	}
}

// The global config is only a fallback when it actually authenticates the
// repo's registry: a config covering some other registry must NOT mask the
// ErrMissingSecret sentinel — that's what --allow-missing-secrets and the
// producer-backed skip depend on.
func TestFetcher_ResolveConfig_GlobalWithoutRegistryCredKeepsSentinel(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	cfg := writeDockerConfig(t, `"other.registry.io":{"auth":"YWxpY2U6aHVudGVyMg=="}`)
	f := &Fetcher{
		RegistryConfig: cfg,
		Secrets:        func(_, _ string) *manifest.Secret { return nil },
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
	})
	_, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
	cleanup()
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("expected ErrMissingSecret when the global config lacks the registry's credential; got %v", err)
	}
}

func TestFetcher_ResolveConfig_FallsBackToDockerDefault(t *testing.T) {
	dir := t.TempDir()
	dockerJSON := `{"auths":{"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(dockerJSON), 0o600); err != nil {
		t.Fatalf("write docker config: %v", err)
	}
	t.Setenv("DOCKER_CONFIG", dir)
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret { return nil },
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
	})
	path, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
	defer cleanup()
	if err != nil {
		t.Fatalf("resolveRegistryConfig: %v", err)
	}
	// An empty path is the documented docker-default signal: newRepoClient's
	// RegistryCredentialStore("") rebuilds the same store for the pull.
	if path != "" {
		t.Errorf("path = %q, want empty (docker-default lookup)", path)
	}
}

// Complement of GlobalWithoutRegistryCredKeepsSentinel: the global config
// covers some other registry, but the docker default lookup does hold the
// repo's credential, so the ordering must land on the docker default.
func TestFetcher_ResolveConfig_GlobalNonMatchingFallsToDockerDefault(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"auths":{"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`), 0o600); err != nil {
		t.Fatalf("write docker config: %v", err)
	}
	t.Setenv("DOCKER_CONFIG", dir)
	cfg := writeDockerConfig(t, `"other.registry.io":{"auth":"YWxpY2U6aHVudGVyMg=="}`)
	f := &Fetcher{
		RegistryConfig: cfg,
		Secrets:        func(_, _ string) *manifest.Secret { return nil },
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
	})
	path, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
	defer cleanup()
	if err != nil {
		t.Fatalf("resolveRegistryConfig: %v", err)
	}
	if path != "" {
		t.Errorf("path = %q, want empty (docker-default lookup)", path)
	}
}

// A corrupt --registry-config fails loudly rather than degrading to the
// sentinel: the operator pointed flate at that file explicitly, so a
// silent skip would hide the broken config behind --allow-missing-secrets.
func TestFetcher_ResolveConfig_CorruptGlobalConfigFails(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"auths":`), 0o600); err != nil {
		t.Fatalf("write corrupt docker config: %v", err)
	}
	f := &Fetcher{
		RegistryConfig: path,
		Secrets:        func(_, _ string) *manifest.Secret { return nil },
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
	})
	_, cleanup, err := f.resolveRegistryConfig(context.Background(), repo)
	cleanup()
	if err == nil {
		t.Fatalf("expected corrupt --registry-config to fail loudly")
	}
	if errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("corrupt config must not degrade to ErrMissingSecret; got %v", err)
	}
}

func TestFetcher_ForceGenericProvider(t *testing.T) {
	// Gate bypassed: the generic path runs and fails on the absent secret.
	// Pin an empty docker default so the #999 fallback can't authenticate
	// from the runner's own docker config — the sentinel is the point.
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	f := &Fetcher{
		ForceGeneric: true,
		Secrets:      func(_, _ string) *manifest.Secret { return nil },
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.Provider = sourcev1.AmazonOCIProvider
		s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	})
	_, err := f.Fetch(context.Background(), repo)
	if err == nil {
		t.Fatalf("expected missing-secret error")
	}
	if strings.Contains(err.Error(), "not implemented") {
		t.Errorf("provider gate should be bypassed with ForceGeneric; got %v", err)
	}
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("want ErrMissingSecret from the generic path; got %v", err)
	}
}
