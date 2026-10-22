package oci

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
)

func ociRepo(name string, set func(s *sourcev1.OCIRepositorySpec)) *manifest.OCIRepository {
	r := &manifest.OCIRepository{Name: name, Namespace: "ns"}
	if set != nil {
		set(&r.OCIRepositorySpec)
	}
	return r
}

func TestFetcher_SecretReferences(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	tests := []struct {
		name, secret, wantText string
		material               map[string]any
		cert, proxy, unwired   bool
		wantMissing, wantOwner bool
	}{
		{name: "missing proxy owner", secret: "missing", proxy: true, wantMissing: true, wantOwner: true},
		{name: "missing cert owner", secret: "missing", cert: true, wantMissing: true, wantOwner: true},
		{name: "partial cert key", secret: "tls", cert: true, material: map[string]any{"tls.crt": "-only-cert-"}, wantText: "must provide both"},
		{name: "empty cert material", secret: "tls", cert: true, material: map[string]any{"unrelated": "x"}, wantMissing: true},
		{name: "missing Docker JSON", secret: "wrong-shape", material: map[string]any{"username": "alice"}, wantText: ".dockerconfigjson", wantMissing: true},
		{name: "unwired getter", secret: "creds", unwired: true, wantText: "source.SecretGetter"},
		{name: "missing auth secret", secret: "missing", wantText: "secret ns/missing not found", wantMissing: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fInputs := testInputs{}
			f := &Fetcher{Network: newTestNetwork(t)}
			if !tt.unwired {
				fInputs.secrets = func(_, _ string) *manifest.Secret {
					if tt.material == nil {
						return nil
					}
					return &manifest.Secret{StringData: tt.material}
				}
			}
			repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://ghcr.io/x/y"
				ref := &manifest.LocalObjectReference{Name: tt.secret}
				switch {
				case tt.proxy:
					s.ProxySecretRef = ref
				case tt.cert:
					s.CertSecretRef = ref
				default:
					s.SecretRef = ref
				}
			})
			if tt.wantOwner {
				repo.Name = "repo"
			}
			var err error
			switch {
			case tt.proxy:
				_, _, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo, nil)
			case tt.cert:
				_, _, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo, nil)
			default:
				_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveRegistryConfig(t.Context(), repo, sourcecache.Policy{})
			}
			if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tt.wantMissing || !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("error = %v, want missing=%t containing %q", err, tt.wantMissing, tt.wantText)
			}
			if tt.wantOwner {
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok {
					t.Fatalf("expected MissingSecretError; got %T", err)
				}
				assert.Equal(t, missing.Owner, manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "ns/repo"})
				assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "missing"})
				assert.Equal(t, err.Error(), manifest.ErrMissingSecret.Error()+": OCIRepository ns/ns/repo: secret ns/missing not found")
			}
		})
	}
}

func TestFetcher_ResolveTLS_NoCertSecret(t *testing.T) {
	t.Parallel()
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", nil)
	repo.URL = "oci://registry.example/test"
	client, _, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := f.Network.HTTPTransport(source.HTTPTransportConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if client.Client.(*auth.Client).Client.Transport != expected {
		t.Fatal("transport applied customization without a certificate reference")
	}
}

func TestFetcher_ResolveTLS_Insecure(t *testing.T) {
	t.Parallel()
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) { s.Insecure = true })
	repo.URL = "oci://registry.example/test"
	client, _, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if client.Client.(*auth.Client).Client.Transport != expected {
		t.Fatal("transport did not apply insecure policy")
	}
}

func TestFetcher_ResolveTLS_FromSecret(t *testing.T) {
	t.Parallel()
	certPEM, keyPEM := testutil.SelfSignedServerCert(t)
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{
			StringData: map[string]any{
				"tls.crt": certPEM,
				"tls.key": keyPEM,
				"ca.crt":  certPEM,
			},
		}
	}}
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	})
	repo.URL = "oci://registry.example/test"
	client, _, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	material, err := source.ResolveTransport(fInputs.secrets, repo.Named(), []source.TransportInput{{Reference: repo.CertSecretRef, Use: source.TransportCert}})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: material})
	if err != nil {
		t.Fatal(err)
	}
	if client.Client.(*auth.Client).Client.Transport != expected {
		t.Fatal("transport did not apply the certificate reference")
	}
}

func TestFetcher_NonGenericProvider(t *testing.T) {
	t.Parallel()
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.Provider = sourcev1.AmazonOCIProvider
	})
	_, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
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
		{name: "Docker Hub docker.io", registry: "docker.io", secret: `{"auths":{"https://index.docker.io/v1/":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`, user: "alice", pass: "hunter2"},
		{name: "Docker Hub alias", registry: "registry-1.docker.io", secret: `{"auths":{"index.docker.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`, user: "alice", pass: "hunter2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dockerDir := t.TempDir()
			t.Setenv("DOCKER_CONFIG", dockerDir)
			testutil.WriteFile(t, dockerDir, "config.json", `{"auths":{"ghcr.io":{"username":"default","password":"default"}}}`)
			fInputs := testInputs{}
			f := &Fetcher{Network: newTestNetwork(t)}
			if tt.config != "" {
				f.RegistryConfig = filepath.Join(t.TempDir(), "config.json")
				testutil.WriteFileAt(t, f.RegistryConfig, tt.config)
			}
			fInputs.secrets = func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": tt.secret}}
			}
			repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://" + tt.registry + "/x/y"
				if tt.secret != "" {
					s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
				}
			})
			credStore, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveRegistryConfig(t.Context(), repo, sourcecache.Policy{})
			if err != nil {
				t.Fatal(err)
			}
			client, identity, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo, credStore)
			if err != nil {
				t.Fatal(err)
			}
			got, err := client.Client.(*auth.Client).Credential(t.Context(), client.Reference.Host())
			want := auth.Credential{Username: tt.user, Password: tt.pass}
			if err != nil || got != want {
				t.Fatalf("credentials = %+v, %v; want %+v", got, err, want)
			}
			if identity == nil {
				t.Fatal("authenticated acquisition has anonymous identity")
			}
			if tt.secret != "" {
				if err := credStore.Put(t.Context(), credentials.ServerAddressFromHostname(client.Reference.Host()), auth.Credential{Username: "rotated", Password: "rotated"}); err != nil {
					t.Fatal(err)
				}
				captured, err := client.Client.(*auth.Client).Credential(t.Context(), client.Reference.Host())
				if err != nil || captured != want {
					t.Fatal("producer reread mutable credentials")
				}
				_, rotated, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo, credStore)
				if err != nil || cmp.Equal(identity, rotated) {
					t.Fatal("credential rotation reused identity")
				}
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
			cache := cachetest.New(t, "", sourcecache.New)
			fInputs := testInputs{secrets: func(_, name string) *manifest.Secret {
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
			f := &Fetcher{Network: newTestNetwork(t), Cache: cache}
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
			if _, err := f.Fetch(ctx, repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()}); err == nil || errors.Is(err, manifest.ErrMissingSecret) != tt.wantMissing || errors.Is(err, context.Canceled) != tt.canceled {
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

func TestFetcher_ForceGenericProvider(t *testing.T) {
	t.Parallel()
	// Gate bypassed: the generic path runs and fails on the absent secret.
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	f := &Fetcher{Network: newTestNetwork(t),
		ForceGeneric: true,
	}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.Provider = sourcev1.AmazonOCIProvider
		s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	})
	_, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
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
			fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return tt.secret }}
			f := &Fetcher{Network: newTestNetwork(t)}
			if tt.config != "" {
				f.RegistryConfig = filepath.Join(t.TempDir(), "config.json")
				testutil.WriteFileAt(t, f.RegistryConfig, tt.config)
			}
			repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://ghcr.io/x/y"
				s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
			})
			credStore, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveRegistryConfig(t.Context(), repo, sourcecache.Policy{})
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
				if errors.Is(tt.wantErr, manifest.ErrMissingSecret) && tt.config != covering {
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

	var failing atomic.Bool
	testutil.StubDNS(t, func(context.Context, string) error {
		if failing.Load() {
			return os.ErrDeadlineExceeded
		}
		return nil
	})
	const user, pass = "alice", "hunter2"
	const chart = "name: app\nversion: 1.0.0\n"
	art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": chart})
	upstream := startFakeRegistry(t, art.manifest, art.config, art.layer)
	var failNext atomic.Bool
	var failures atomic.Int32
	var requests atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if u, p, ok := r.BasicAuth(); !ok || u != user || p != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if failNext.CompareAndSwap(true, false) {
			failures.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	targetURL := strings.Replace(srv.URL, "127.0.0.1", "example.com", 1)
	host := mustURL(t, targetURL).Host
	auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	configJSON := fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, host, auth)
	cache := cachetest.New(t, "", sourcecache.New)
	var network source.Network
	ociFetcherInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": configJSON}}
	}}
	ociFetcher := &Fetcher{
		Cache: cache, Network: &network,
	}
	repo := &manifest.HelmRepository{
		Name: "charts", Namespace: "ns",
		URL: "oci://" + host + "/charts", Type: sourcev1.HelmRepositoryTypeOCI, Insecure: true,
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
		Interval:  metav1.Duration{Duration: time.Hour},
	}
	chartsInputs := testInputs{secrets: ociFetcherInputs.secrets, repos: func(_, _ string) *manifest.HelmRepository { return repo }}
	charts := helmchart.New(ociFetcher, helmchart.Config{Cache: cache, Network: &network})

	got, err := charts.Fetch(t.Context(), helmchart.Synthesize(repo, "app", "1.0.0"), chartsInputs.readSet(helmchart.Synthesize(repo, "app", "1.0.0")), sourcecache.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != manifest.KindHelmChart {
		t.Errorf("artifact Kind = %q, want %q", got.Kind, manifest.KindHelmChart)
	}
	if b := mustReadFile(t, got.FS, "Chart.yaml"); b != chart {
		t.Errorf("Chart.yaml = %q, want %q", b, chart)
	}
	initialKey := got.Key
	configPath := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, configPath, configJSON)
	fallbackFetcherInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	fallbackFetcher := &Fetcher{Cache: cache, Network: &network, RegistryConfig: configPath}
	fallbackChartsInputs := testInputs{secrets: fallbackFetcherInputs.secrets, repos: func(_, _ string) *manifest.HelmRepository { return repo }}
	fallbackCharts := helmchart.New(fallbackFetcher, helmchart.Config{Cache: cache, Network: &network})

	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	_, err = fallbackCharts.Fetch(t.Context(), helmchart.Synthesize(repo, "app", "1.0.0"), fallbackChartsInputs.readSet(helmchart.Synthesize(repo, "app", "1.0.0")), sourcecache.Policy{Retry: retry})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryWithRegistryConfig == nil {
		t.Fatalf("missing fallback: %v", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := missing.RetryWithRegistryConfig(cancelled, configPath); !errors.Is(err, context.Canceled) {
		t.Fatalf("fallback context: %v", err)
	}
	failNext.Store(true)
	got, err = missing.RetryWithRegistryConfig(t.Context(), configPath)
	if err != nil || failures.Load() != 1 {
		t.Fatalf("fallback recovery: %v, failures=%d", err, failures.Load())
	}
	if got.Key == initialKey || got.Kind != manifest.KindHelmChart || mustReadFile(t, got.FS, "Chart.yaml") != chart {
		t.Fatalf("fallback artifact=%+v", got)
	}
	beforeWarm := requests.Load()
	warm, err := missing.RetryWithRegistryConfig(t.Context(), configPath)
	if err != nil || warm == nil || warm.Key != got.Key || warm.Kind != manifest.KindHelmChart || requests.Load() != beforeWarm {
		t.Fatalf("fallback lost freshness or chart identity: artifact=%+v, err=%v, new requests=%d", warm, err, requests.Load()-beforeWarm)
	}
	refreshRepo := *repo
	refreshRepo.Interval = metav1.Duration{}
	refreshChartsInputs := testInputs{secrets: fallbackFetcherInputs.secrets, repos: func(_, _ string) *manifest.HelmRepository { return &refreshRepo }}
	refreshCharts := helmchart.New(fallbackFetcher, helmchart.Config{Cache: cache, Network: &network})
	_, err = refreshCharts.Fetch(t.Context(), helmchart.Synthesize(&refreshRepo, "app", "1.0.0"), refreshChartsInputs.readSet(helmchart.Synthesize(&refreshRepo, "app", "1.0.0")), sourcecache.Policy{Retry: retry})
	missing, ok = errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryWithRegistryConfig == nil {
		t.Fatalf("refresh fallback: %v", err)
	}
	failing.Store(true)
	tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, strings.Replace(srv.URL, "127.0.0.1", "example.com", 1), nil)
	_, err = (&http.Client{Transport: tr}).Do(req)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	if !ok || !terminal.Attempted {
		t.Fatalf("seed: %v", err)
	}
	cause := terminal.Err
	before := requests.Load()
	_, err = missing.RetryWithRegistryConfig(t.Context(), configPath)
	if terminal, ok := errors.AsType[*sourcecache.TerminalError](err); !ok || terminal.Attempted || !errors.Is(err, cause) || requests.Load() != before {
		t.Fatalf("suppressed fallback: %v, additional requests=%d", err, requests.Load()-before)
	}
}

type countedCredentials struct {
	credentials.Store
	calls   int
	failure error
}

func (s *countedCredentials) Get(context.Context, string) (auth.Credential, error) {
	s.calls++
	if s.failure != nil {
		return auth.EmptyCredential, s.failure
	}
	return auth.Credential{Username: "helper", Password: fmt.Sprint(s.calls)}, nil
}

func TestFetcher_ResolveConfig_HelperSnapshot(t *testing.T) {
	t.Setenv("NO_PROXY", "*")
	t.Setenv("no_proxy", "*")
	store := &countedCredentials{}
	repo := ociRepo("helper", func(s *sourcev1.OCIRepositorySpec) { s.URL = "oci://docker.io/library/test" })
	client, first, err := (&operation{Fetcher: &Fetcher{Network: newTestNetwork(t)}}).newRepoClient(t.Context(), repo, store)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		cred, err := client.Client.(*auth.Client).Credential(t.Context(), "registry-1.docker.io")
		if err != nil || cred.Password != "1" || store.calls != 1 {
			t.Fatalf("helper was not frozen: calls=%d error=%v", store.calls, err)
		}
	}
	_, second, err := (&operation{Fetcher: &Fetcher{Network: newTestNetwork(t)}}).newRepoClient(t.Context(), repo, store)
	if err != nil || cmp.Equal(first, second) || store.calls != 2 {
		t.Fatalf("next acquisition did not resolve rotated helper: calls=%d error=%v", store.calls, err)
	}
	store.failure = os.ErrPermission
	failed, identity, err := (&operation{Fetcher: &Fetcher{Network: newTestNetwork(t)}}).newRepoClient(t.Context(), repo, store)
	if failed != nil || identity != nil || !errors.Is(err, os.ErrPermission) || !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("failed helper admitted acquisition: %v", err)
	}
}

func TestFetcher_ResolveTLS_SnapshotRotation(t *testing.T) {
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	var fail atomic.Bool
	var requests, lookups atomic.Int32
	var password atomic.Value
	password.Store("first")
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		user, pass, ok := r.BasicAuth()
		if !ok || user != "alice" || pass != password.Load().(string) {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Listener = listener
	srv.StartTLS()
	t.Cleanup(srv.Close)
	var proxyPassword atomic.Value
	proxyPassword.Store("first")
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("proxy:"+proxyPassword.Load().(string))) {
			t.Error("transport used a different proxy snapshot")
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		remote, err := (&net.Dialer{}).DialContext(r.Context(), "tcp", r.Host)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer remote.Close()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		go func() { _, _ = io.Copy(remote, conn); _ = remote.Close() }()
		_, _ = io.Copy(conn, remote)
	}))
	t.Cleanup(proxy.Close)
	t.Setenv("HTTPS_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	repo := ociRepo("snapshot", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://" + mustURL(t, srv.URL).Host + "/app"
		s.Insecure = true
		s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
		s.CertSecretRef = &manifest.LocalObjectReference{Name: "creds"}
		s.Interval.Duration = time.Hour
	})
	root := t.TempDir()
	ca := testutil.SelfSignedCA(t)
	var previous string
	for _, wave := range []string{"first", "password", "certificate", "proxy", "direct"} {
		t.Run(wave, func(t *testing.T) {
			if wave == "password" {
				password.Store("second")
			}
			if wave == "certificate" {
				ca = testutil.SelfSignedCA(t)
			}
			proxyURL := strings.Replace(proxy.URL, "://", "://proxy:first@", 1)
			if wave == "proxy" || wave == "direct" {
				proxyURL = strings.Replace(proxy.URL, "://", "://proxy:second@", 1)
				proxyPassword.Store("second")
			}
			t.Setenv("https_proxy", proxyURL)
			if wave == "direct" {
				t.Setenv("no_proxy", "*")
			}
			c := cachetest.New(t, root, sourcecache.New)
			fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				lookups.Add(1)
				config := fmt.Sprintf(`{"auths":{%q:{"username":"alice","password":%q}}}`, mustURL(t, srv.URL).Host, password.Load().(string))
				return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": config, "ca.crt": ca}}
			}}
			f := &Fetcher{Network: newTestNetwork(t), Cache: c}
			before := requests.Load()
			art, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval(), Retry: sourcecache.RetryConfig{Attempts: 3}})
			if err != nil {
				t.Fatal(err)
			}
			if previous == art.Key || requests.Load() <= before {
				t.Fatal("changed material reused acquisition")
			}
			previous = art.Key
			before = requests.Load()
			if _, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()}); err != nil || requests.Load() != before {
				t.Fatalf("warm snapshot missed: %v", err)
			}
			fail.Store(true)
			failed := *repo
			failed.Interval.Duration = 0
			_, err = f.Fetch(t.Context(), &failed, fInputs.readSet(&failed), sourcecache.Policy{MaxAge: failed.SourceInterval()})
			terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || !terminal.Attempted {
				t.Fatalf("failed snapshot: %v", err)
			}
			fail.Store(false)
		})
	}
	if lookups.Load() != 15 {
		t.Fatalf("auth and cert reread Secrets: %d", lookups.Load())
	}
}

func TestFetcher_ResolveConfig_RegistryFallbackSnapshot(t *testing.T) {
	t.Parallel()
	for _, callers := range []int{2, 4} {
		t.Run(fmt.Sprint(callers), func(t *testing.T) {
			t.Parallel()
			ca := testutil.SelfSignedCA(t)
			config := filepath.Join(t.TempDir(), "config.json")
			testutil.WriteFileAt(t, config, `{"auths":{"ghcr.io":{"username":"alice","password":"test"}}}`)
			c := cachetest.New(t, t.TempDir(), sourcecache.New)
			var lookups atomic.Int32
			fInputs := testInputs{secrets: func(_, name string) *manifest.Secret {
				lookups.Add(1)
				if name == "tls" {
					return &manifest.Secret{StringData: map[string]any{"ca.crt": ca}}
				}
				return nil
			}}
			f := &Fetcher{Network: newTestNetwork(t), Cache: c, RegistryConfig: config}
			repo := ociRepo("fallback", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://ghcr.io/app"
				s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
				s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
			})
			_, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryWithRegistryConfig == nil || lookups.Load() != 2 {
				t.Fatalf("snapshot before handoff: reads=%d error=%v", lookups.Load(), err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var wg sync.WaitGroup
			for range callers {
				wg.Go(func() {
					_, err := missing.RetryWithRegistryConfig(ctx, config)
					if !errors.Is(err, context.Canceled) {
						t.Errorf("canceled fallback: %v", err)
					}
				})
			}
			wg.Wait()
			if lookups.Load() != 2 {
				t.Fatalf("retained fallback reread mutable Secrets: %d", lookups.Load())
			}
		})
	}
}

func TestFetcher_BearerTokenReuse(t *testing.T) {
	t.Parallel()
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprintf("authenticated=%t", authenticated), func(t *testing.T) {
			t.Parallel()
			artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
			upstream, _ := newMutableFakeRegistry(t, artifact)
			var challenges, exchanges, wrongContext atomic.Int32
			var generation atomic.Int32
			var failTokens atomic.Bool
			var password atomic.Value
			password.Store("")
			if authenticated {
				password.Store("first")
			}
			token := func(host, scope, secret string) string {
				return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s|%s|%s|%d", host, scope, secret, generation.Load())))
			}
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				secret := password.Load().(string)
				if r.URL.Path == "/token" {
					exchanges.Add(1)
					user, pass, present := r.BasicAuth()
					if failTokens.Load() || secret != "" && (!present || user != "user" || pass != secret) || secret == "" && present {
						http.Error(w, "token denied", http.StatusForbidden)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]string{"token": token(r.Host, r.URL.Query().Get("scope"), secret)})
					return
				}
				name, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/v2/"), "/")
				scope := "repository:" + name + ":pull"
				header := r.Header.Get("Authorization")
				if header != "Bearer "+token(r.Host, scope, secret) {
					challenges.Add(1)
					if header != "" {
						decoded, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(header, "Bearer "))
						if !strings.HasPrefix(string(decoded), r.Host+"|"+scope+"|"+secret+"|") {
							wrongContext.Add(1)
						}
					}
					w.Header().Set("WWW-Authenticate", fmt.Sprintf("Bearer realm=%q,service=%q,scope=%q", "https://"+r.Host+"/token", "registry", scope))
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				upstream.Config.Handler.ServeHTTP(w, r)
			})
			srv := httptest.NewTLSServer(handler)
			t.Cleanup(srv.Close)
			repo := &manifest.OCIRepository{Name: "bearer", Namespace: "test",
				URL: "oci://" + mustURL(t, srv.URL).Host + "/app", Insecure: true,
				Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"}}
			if authenticated {
				repo.SecretRef = &manifest.LocalObjectReference{Name: "registry"}
			}
			registryHost := mustURL(t, srv.URL).Host
			inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				config := fmt.Sprintf(`{"auths":{%q:{"username":"user","password":%q}}}`, registryHost, password.Load().(string))
				return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": config}}
			}}

			owner := cachetest.New(t, t.TempDir(), sourcecache.New)
			network := newTestNetwork(t)
			fetch := func(request *manifest.OCIRepository, n *source.Network) {
				t.Helper()
				f := &Fetcher{Network: n, Cache: owner}
				got, err := f.Fetch(t.Context(), request, inputs.readSet(request), sourcecache.Policy{})
				assert.NoError(t, err)
				assert.Equal(t, mustReadFile(t, got.FS, "app.yaml"), "version: v1\n")
			}
			fetch(repo, network)
			assert.Equal(t, challenges.Load(), int32(1))
			assert.Equal(t, exchanges.Load(), int32(1))
			fetch(repo, network)
			assert.Equal(t, challenges.Load(), int32(1))
			assert.Equal(t, exchanges.Load(), int32(1))

			start := make(chan struct{})
			var wg sync.WaitGroup
			for range 2 {
				f := &Fetcher{Network: network, Cache: cachetest.New(t, "", sourcecache.New)}
				wg.Go(func() {
					<-start
					got, err := f.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{})
					if err != nil {
						t.Errorf("concurrent acquisition: %v", err)
						return
					}
					data, err := fs.ReadFile(got.FS, "app.yaml")
					if err != nil || string(data) != "version: v1\n" {
						t.Errorf("concurrent content=%q error=%v", data, err)
					}
				})
			}
			close(start)
			wg.Wait()
			assert.Equal(t, challenges.Load(), int32(1))
			assert.Equal(t, exchanges.Load(), int32(1))

			fetch(repo, newTestNetwork(t))
			assert.Equal(t, challenges.Load(), int32(2))
			assert.Equal(t, exchanges.Load(), int32(2))
			password.Store("rotated")
			repo.SecretRef = &manifest.LocalObjectReference{Name: "registry"}
			fetch(repo, network)
			assert.Equal(t, challenges.Load(), int32(3))
			assert.Equal(t, exchanges.Load(), int32(3))

			otherScope := *repo
			otherScope.URL = "oci://" + mustURL(t, srv.URL).Host + "/other"
			fetch(&otherScope, network)
			assert.Equal(t, challenges.Load(), int32(4))
			assert.Equal(t, exchanges.Load(), int32(4))
			otherRegistry := httptest.NewTLSServer(handler)
			t.Cleanup(otherRegistry.Close)
			other := *repo
			other.URL = "oci://" + mustURL(t, otherRegistry.URL).Host + "/app"
			registryHost = mustURL(t, otherRegistry.URL).Host
			fetch(&other, network)
			registryHost = mustURL(t, srv.URL).Host
			assert.Equal(t, challenges.Load(), int32(5))
			assert.Equal(t, exchanges.Load(), int32(5))

			generation.Add(1)
			fetch(repo, network)
			assert.Equal(t, challenges.Load(), int32(6))
			assert.Equal(t, exchanges.Load(), int32(6))
			generation.Add(1)
			failTokens.Store(true)
			_, err := (&Fetcher{Network: network, Cache: owner}).Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{})
			if err == nil || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("token rejection did not propagate: %v", err)
			}
			response, ok := errors.AsType[*fetcherr.ResponseError](err)
			if !ok || response.StatusCode != http.StatusForbidden {
				t.Fatalf("token endpoint failure lost response: %v", err)
			}
			assert.Equal(t, challenges.Load(), int32(7))
			assert.Equal(t, exchanges.Load(), int32(7))
			assert.Equal(t, wrongContext.Load(), int32(0))
			t.Logf("initial, warm, concurrent, invocation, credential, scope, registry and rejection stages: challenges=%d exchanges=%d wrong_context=%d", challenges.Load(), exchanges.Load(), wrongContext.Load())
		})
	}
}
