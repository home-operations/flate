package oci

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/source/helmchart"
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
	_, err := f.Fetch(t.Context(), repo)
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
	path, cleanup, err := f.resolveRegistryConfig(t.Context(), repo)
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
	path, cleanup, err := f.resolveRegistryConfig(t.Context(), repo)
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
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "wrong-shape"}
	})
	_, cleanup, err := f.resolveRegistryConfig(t.Context(), repo)
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
	_, cleanup, err := f.resolveRegistryConfig(t.Context(), repo)
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
		s.URL = "oci://ghcr.io/x/y"
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
	})
	_, cleanup, err := f.resolveRegistryConfig(t.Context(), repo)
	cleanup()
	if err == nil || !strings.Contains(err.Error(), "secret ns/missing not found") {
		t.Errorf("expected secret-not-found error; got %v", err)
	}
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("expected ErrMissingSecret wrap; got %v", err)
	}
}

func TestFetcher_ForceGenericProvider(t *testing.T) {
	// Gate bypassed: the generic path runs and fails on the absent secret.
	f := &Fetcher{
		ForceGeneric: true,
		Secrets:      func(_, _ string) *manifest.Secret { return nil },
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.Provider = sourcev1.AmazonOCIProvider
		s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	})
	_, err := f.Fetch(t.Context(), repo)
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

func TestFetcher_ResolveConfig_RegistryFallback(t *testing.T) {
	writeConfig := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	covering := `{"auths":{"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`
	tests := []struct {
		name     string
		url      string
		secret   *manifest.Secret
		config   string // registry config content; "" leaves --registry-config unset
		docker   string // docker default config content
		wantPath bool
		wantErr  error // nil with wantPath false means any non-sentinel error
	}{
		{name: "not found, fallback", config: covering, wantPath: true},
		{name: "docker Hub login", url: "oci://docker.io/library/x", config: `{"auths":{"https://index.docker.io/v1/":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`, wantPath: true},
		{name: "not found, docker default fallback", docker: covering, wantPath: true},
		{
			name:   "placeholder-wiped, fallback",
			secret: &manifest.Secret{StringData: map[string]any{".dockerconfigjson": "..PLACEHOLDER_.dockerconfigjson.."}},
			config: covering, wantPath: true,
		},
		{name: "first fetch keeps the sentinel", config: covering, wantErr: manifest.ErrMissingSecret},
		{name: "config covers another registry", config: `{"auths":{"quay.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`, wantErr: manifest.ErrMissingSecret},
		{name: "no config at all", wantErr: manifest.ErrMissingSecret},
		{name: "corrupt config fails loud", config: `{"auths":`},
		{name: "invalid ORAS repository", url: "oci://ghcr.io/UpperCase", config: covering, wantErr: manifest.ErrInput},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dockerDir := t.TempDir() // keeps the host's docker config out
			if tt.docker != "" {
				if err := os.WriteFile(filepath.Join(dockerDir, "config.json"), []byte(tt.docker), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("DOCKER_CONFIG", dockerDir)
			f := &Fetcher{Secrets: func(_, _ string) *manifest.Secret { return tt.secret }}
			if tt.config != "" {
				f.RegistryConfig = writeConfig(t, tt.config)
			}
			repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://ghcr.io/x/y"
				if tt.url != "" {
					s.URL = tt.url
				}
				s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
			})
			path, cleanup, err := f.resolveRegistryConfig(t.Context(), repo)
			cleanup()
			switch {
			case tt.wantPath:
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || !errors.Is(err, manifest.ErrMissingSecret) || path != "" || missing.RegistryConfig != f.RegistryConfig || missing.RetryWithRegistryConfig == nil {
					t.Fatalf("got (%q, %v), want missing Secret with retry using %q", path, err, f.RegistryConfig)
				}
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				if tt.wantErr == manifest.ErrMissingSecret && tt.config != covering {
					if missing, ok := errors.AsType[*source.MissingSecretError](err); !ok || missing.RetryWithRegistryConfig != nil {
						t.Fatalf("err = %v, want missing Secret without registry retry", err)
					}
				}
			default:
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || missing.RetryWithRegistryConfig == nil {
					t.Fatalf("err = %v, want missing Secret with deferred config error", err)
				}
				if _, err := missing.RetryWithRegistryConfig(t.Context(), missing.RegistryConfig); !errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrMissingSecret) {
					t.Fatalf("retry err = %v, want a loud input error", err)
				}
			}
		})
	}
}

// An OCI HelmRepository's chart is fetched through a synthesized
// OCIRepository, so its secretRef takes the same registry fallback.
func TestFetcher_OCIHelmRepositoryRegistryFallback(t *testing.T) {
	const user, pass = "alice", "hunter2"
	art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": "name: app\nversion: 1.0.0\n"})
	upstream := startFakeRegistry(t, art.manifest, art.config, art.layer)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != user || p != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	host := mustURL(t, srv.URL).Host

	t.Setenv("DOCKER_CONFIG", t.TempDir()) // keeps the host's docker config out
	config := filepath.Join(t.TempDir(), "config.json")
	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	if err := os.WriteFile(config, fmt.Appendf(nil, `{"auths":{%q:{"auth":%q}}}`, host, auth), 0o600); err != nil {
		t.Fatal(err)
	}

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	ociFetcher := &Fetcher{
		Cache:          cache,
		RegistryConfig: config,
		Secrets:        func(_, _ string) *manifest.Secret { return nil },
	}
	repo := &manifest.HelmRepository{
		Name: "charts", Namespace: "ns",
		URL: "oci://" + host + "/charts", Type: manifest.RepoTypeOCI, Insecure: true,
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	charts, err := helmchart.New(nil, func(_, _ string) *manifest.HelmRepository { return repo }, ociFetcher, cache, layout)
	if err != nil {
		t.Fatal(err)
	}
	hc := helmchart.Synthesize(repo, "app", "1.0.0")

	_, err = charts.Fetch(t.Context(), hc)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok || !errors.Is(err, manifest.ErrMissingSecret) || missing.RetryWithRegistryConfig == nil {
		t.Fatalf("first fetch err = %v, want ErrMissingSecret with registry retry", err)
	}
	got, err := missing.RetryWithRegistryConfig(t.Context(), missing.RegistryConfig)
	if err != nil {
		t.Fatalf("fallback fetch: %v", err)
	}
	if got.Kind != manifest.KindHelmChart {
		t.Errorf("artifact Kind = %q, want %q", got.Kind, manifest.KindHelmChart)
	}
	if b := mustReadFile(t, filepath.Join(got.LocalPath, "Chart.yaml")); !strings.Contains(b, "name: app") {
		t.Errorf("Chart.yaml = %q", b)
	}
}
