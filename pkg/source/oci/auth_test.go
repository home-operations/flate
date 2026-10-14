package oci

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
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
	if !errors.Is(err, manifest.ErrMissingSecret) {
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

func TestFetcher_ResolveConfig_CredentialPrecedence(t *testing.T) {
	const secretJSON = `{"auths":{"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`
	const explicitJSON = `{"auths":{"ghcr.io":{"username":"explicit","password":"explicit"}}}`
	tests := []struct {
		name, registry, secret, config, user, pass string
	}{
		{name: "Secret wins", registry: "ghcr.io", secret: secretJSON, config: explicitJSON, user: "alice", pass: "hunter2"},
		{name: "Secret wins malformed fallback", registry: "ghcr.io", secret: secretJSON, config: `{"auths":`, user: "alice", pass: "hunter2"},
		{name: "explicit wins Docker defaults", registry: "ghcr.io", config: explicitJSON, user: "explicit", pass: "explicit"},
		{name: "Docker fallback", registry: "ghcr.io", user: "default", pass: "default"},
		{name: "Docker Hub alias", registry: "registry-1.docker.io", secret: `{"auths":{"index.docker.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`, user: "alice", pass: "hunter2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dockerDir := t.TempDir()
			t.Setenv("DOCKER_CONFIG", dockerDir)
			testutil.WriteFile(t, dockerDir, "config.json", `{"auths":{"ghcr.io":{"username":"default","password":"default"}}}`)
			f := &Fetcher{}
			if tt.config != "" {
				f.RegistryConfig = filepath.Join(t.TempDir(), "config.json")
				testutil.WriteFileAt(t, f.RegistryConfig, tt.config)
			}
			f.Secrets = func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": tt.secret}}
			}
			repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://" + tt.registry + "/x/y"
				if tt.secret != "" {
					s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
				}
			})
			credStore, err := f.resolveRegistryConfig(t.Context(), repo)
			if err != nil {
				t.Fatal(err)
			}
			got, err := credentials.Credential(credStore)(t.Context(), tt.registry)
			want := auth.Credential{Username: tt.user, Password: tt.pass}
			if err != nil || got != want {
				t.Fatalf("credentials = %+v, %v; want %+v", got, err, want)
			}
		})
	}
}

func TestFetcher_SecretCredentialsAcquisitionPolicy(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	tests := []struct {
		name                             string
		cert, proxy, encoded             bool
		material                         map[string]any
		wantMissing, wantInput, canceled bool
	}{
		{name: "missing auth", wantMissing: true},
		{name: "wiped auth", material: map[string]any{".dockerconfigjson": "..PLACEHOLDER_.dockerconfigjson.."}, wantMissing: true},
		{name: "malformed auth", material: map[string]any{".dockerconfigjson": `{"auths":`}, wantInput: true},
		{name: "malformed encoded auth", encoded: true, material: map[string]any{".dockerconfigjson": "!!!"}, wantInput: true},
		{name: "missing TLS", cert: true, wantMissing: true},
		{name: "wiped TLS", cert: true, material: map[string]any{"ca.crt": "..PLACEHOLDER_ca.crt.."}, wantMissing: true},
		{name: "malformed TLS", cert: true, material: map[string]any{"ca.crt": "invalid PEM"}},
		{name: "missing proxy", proxy: true, wantMissing: true},
		{name: "wiped proxy", proxy: true, material: map[string]any{"address": "..PLACEHOLDER_address.."}, wantMissing: true},
		{name: "malformed proxy", proxy: true, material: map[string]any{"address": "%"}},
		{name: "canceled", canceled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requests atomic.Int64
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			t.Cleanup(srv.Close)
			cache, err := sourcecache.New(t.Context(), cacheroot.Layout{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cache.Close() })
			f := &Fetcher{Cache: cache, Secrets: func(_, name string) *manifest.Secret {
				if name == "creds" && (tt.cert || tt.proxy || tt.canceled) {
					return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": `{"auths":{"ghcr.io":{"username":"alice","password":"hunter2"}}}`}}
				}
				if tt.material != nil {
					if tt.encoded {
						return &manifest.Secret{Data: tt.material}
					}
					return &manifest.Secret{StringData: tt.material}
				}
				return nil
			}}
			repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
				s.URL, s.Insecure = "oci://"+mustURL(t, srv.URL).Host+"/app", true
				s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
				if tt.cert {
					s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
				}
				if tt.proxy {
					s.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
				}
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.canceled {
				cancel()
			}
			if _, err := f.Fetch(ctx, repo); err == nil || errors.Is(err, manifest.ErrMissingSecret) != tt.wantMissing || errors.Is(err, context.Canceled) != tt.canceled {
				t.Fatalf("Fetch error = %v, want missing=%t canceled=%t", err, tt.wantMissing, tt.canceled)
			} else if tt.wantInput && !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("Fetch error = %v, want malformed material to be an input error", err)
			}
			if got := requests.Load(); got != 0 {
				t.Fatalf("registry requests = %d, want 0", got)
			}
		})
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
	_, err := f.resolveRegistryConfig(t.Context(), repo)
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
	_, err := f.resolveRegistryConfig(t.Context(), repo)
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
	_, err := f.resolveRegistryConfig(t.Context(), repo)
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
	covering := `{"auths":{"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`
	tests := []struct {
		name      string
		secret    *manifest.Secret
		config    string // registry config content; "" leaves --registry-config unset
		docker    string // docker default config content
		wantRetry bool
		wantErr   error // nil with wantRetry false means any non-sentinel error
	}{
		{name: "not found, fallback", config: covering, wantRetry: true},
		{name: "not found, docker default fallback", docker: covering, wantRetry: true},
		{
			name:   "placeholder-wiped, fallback",
			secret: &manifest.Secret{StringData: map[string]any{".dockerconfigjson": "..PLACEHOLDER_.dockerconfigjson.."}},
			config: covering, wantRetry: true,
		},
		{name: "config covers another registry", config: `{"auths":{"quay.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`, wantErr: manifest.ErrMissingSecret},
		{name: "no config at all", wantErr: manifest.ErrMissingSecret},
		{name: "corrupt config fails loud", config: `{"auths":`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dockerDir := t.TempDir() // keeps the host's docker config out
			if tt.docker != "" {
				testutil.WriteFile(t, dockerDir, "config.json", tt.docker)
			}
			t.Setenv("DOCKER_CONFIG", dockerDir)
			f := &Fetcher{Secrets: func(_, _ string) *manifest.Secret { return tt.secret }}
			if tt.config != "" {
				f.RegistryConfig = filepath.Join(t.TempDir(), "config.json")
				testutil.WriteFileAt(t, f.RegistryConfig, tt.config)
			}
			repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://ghcr.io/x/y"
				s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
			})
			credStore, err := f.resolveRegistryConfig(t.Context(), repo)
			switch {
			case tt.wantRetry:
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || !errors.Is(err, manifest.ErrMissingSecret) || credStore != nil || missing.RegistryConfig != f.RegistryConfig || missing.RetryWithRegistryConfig == nil {
					t.Fatalf("got (%v, %v), want missing Secret with retry using %q", credStore, err, f.RegistryConfig)
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

func TestFetcher_OCIHelmRepositoryCredentials(t *testing.T) {
	const user, pass = "alice", "hunter2"
	const chart = "name: app\nversion: 1.0.0\n"
	art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": chart})
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
	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	configJSON := fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, host, auth)
	cache, err := sourcecache.New(t.Context(), cacheroot.Layout{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	ociFetcher := &Fetcher{
		Cache: cache,
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": configJSON}}
		},
	}
	repo := &manifest.HelmRepository{
		Name: "charts", Namespace: "ns",
		URL: "oci://" + host + "/charts", Type: sourcev1.HelmRepositoryTypeOCI, Insecure: true,
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	charts := helmchart.New(nil, func(_, _ string) *manifest.HelmRepository { return repo }, ociFetcher, helmchart.Config{Cache: cache})
	got, err := charts.Fetch(t.Context(), helmchart.Synthesize(repo, "app", "1.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != manifest.KindHelmChart {
		t.Errorf("artifact Kind = %q, want %q", got.Kind, manifest.KindHelmChart)
	}
	if b := mustReadFile(t, got.FS, "Chart.yaml"); b != chart {
		t.Errorf("Chart.yaml = %q, want %q", b, chart)
	}
}
