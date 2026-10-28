package oci

import (
	"bufio"
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
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/errcode"

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
	t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
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
				_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
			case tt.cert:
				_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
			default:
				_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveRegistryConfig(t.Context(), repo, "ghcr.io", sourcecache.Policy{})
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
	request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := f.Network.HTTPTransport(source.HTTPTransportConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if request.client.Client.(*auth.Client).Client.Transport != expected {
		t.Fatal("transport applied customization without a certificate reference")
	}
}

func TestFetcher_ResolveTLS_Insecure(t *testing.T) {
	t.Parallel()
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) { s.Insecure = true })
	repo.URL = "oci://registry.example/test"
	request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if request.client.Client.(*auth.Client).Client.Transport != expected {
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
	request, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
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
	if request.client.Client.(*auth.Client).Client.Transport != expected {
		t.Fatal("transport did not apply the certificate reference")
	}
}

func TestFetcher_ProviderUnavailable(t *testing.T) {
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
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("expected missing provider credentials; got %v", err)
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
			t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
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
				s.Provider = sourcev1.AmazonOCIProvider
				if tt.secret != "" {
					s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
				}
			})
			op := &operation{Fetcher: f, secrets: fInputs.lookup(repo)}
			request, err := op.newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := request.client.Client.(*auth.Client).Credential(t.Context(), request.client.Reference.Host())
			want := auth.Credential{Username: tt.user, Password: tt.pass}
			if err != nil || got != want {
				t.Fatalf("credentials = %+v, %v; want %+v", got, err, want)
			}
			if request.material == nil {
				t.Fatal("authenticated acquisition has anonymous identity")
			}
			if tt.secret != "" {
				op.secrets = func(_, _ string) *manifest.Secret {
					config := fmt.Sprintf(`{"auths":{%q:{"username":"rotated","password":"rotated"}}}`, credentials.ServerAddressFromRegistry(tt.registry))
					return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": config}}
				}
				captured, err := request.client.Client.(*auth.Client).Credential(t.Context(), request.client.Reference.Host())
				if err != nil || captured != want {
					t.Fatal("producer reread mutable credentials")
				}
				rotated, err := op.newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
				if err != nil || cmp.Equal(request.material, rotated.material) {
					t.Fatal("credential rotation reused identity")
				}
			}
		})
	}
}

func TestFetcher_AnonymousWithCorruptDockerConfig(t *testing.T) {
	dockerDir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dockerDir)
	t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	testutil.WriteFile(t, dockerDir, "config.json", `{"auths":`)
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap\n"})
	registry := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	repo := ociRepo("anonymous", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://" + mustURL(t, registry.URL).Host + "/app"
		s.Provider = sourcev1.GenericOCIProvider
		s.Insecure = true
		s.Reference = &sourcev1.OCIRepositoryRef{Tag: "v1"}
	})
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New), Network: newTestNetwork(t)}
	got, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
	assert.NoError(t, err)
	assert.Equal(t, mustReadFile(t, got.FS, "app.yaml"), "kind: ConfigMap\n")
}

func TestFetcher_MachineCredentials(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "helper-ran")
	bin := t.TempDir()
	t.Setenv("PATH", bin)
	t.Setenv("FLATE_HELPER_MARKER", marker)
	for _, helper := range []string{"global", "target", "unrelated"} {
		path := filepath.Join(bin, "docker-credential-"+helper)
		testutil.WriteFileAt(t, path, "#!/bin/sh\necho invoked >> \"$FLATE_HELPER_MARKER\"\nexit 1\n")
		assert.NoError(t, os.Chmod(path, 0o755))
	}
	const docker = `{"auths":{"ghcr.io":{"username":"docker","password":"docker"}}}`
	const helm = `{"auths":{"ghcr.io":{"username":"helm","password":"helm"}}}`
	const helper = `{"credsStore":"global","credHelpers":{"ghcr.io":"target","quay.io":"unrelated"}}`
	for _, tc := range []struct {
		name, docker, helm, explicit, secret, user string
		wantInput                                  bool
	}{
		{name: "Helm only", helm: helm, user: "helm"},
		{name: "Docker wins", docker: docker, helm: helm, user: "docker"},
		{name: "unrelated Docker falls through", docker: `{"auths":{"quay.io":{"username":"other","password":"other"}}}`, helm: helm, user: "helm"},
		{name: "corrupt Docker falls through", docker: `{`, helm: helm, user: "helm"},
		{name: "Docker avoids malformed Helm", docker: docker, helm: `{`, user: "docker"},
		{name: "consulted malformed Helm", helm: `{`, wantInput: true},
		{name: "Docker target decode fails", docker: `{"auths":{"ghcr.io":{"auth":"!!!"}}}`, helm: helm, wantInput: true},
		{name: "Helm target decode fails", helm: `{"auths":{"ghcr.io":{"auth":"!!!"}}}`, wantInput: true},
		{name: "explicit target excludes ambient", docker: `{`, helm: `{`, explicit: docker, user: "docker"},
		{name: "explicit miss excludes ambient", docker: docker, helm: helm, explicit: `{}`},
		{name: "explicit malformed excludes ambient", docker: docker, helm: helm, explicit: `{`, wantInput: true},
		{name: "Secret excludes malformed files", docker: `{`, helm: `{`, explicit: `{`, secret: helm, user: "helm"},
		{name: "malformed Secret is authoritative", docker: docker, helm: helm, secret: `{`, wantInput: true},
		{name: "Docker helper only", docker: helper},
		{name: "Helm helper only", helm: helper},
		{name: "explicit helper excludes ambient", docker: helm, helm: helm, explicit: helper},
		{name: "Helm plain follows Docker helper", docker: helper, helm: helm, user: "helm"},
		{name: "Docker plain plus helper", docker: `{"credsStore":"global","auths":{"ghcr.io":{"username":"docker","password":"docker"}}}`, user: "docker"},
		{name: "Helm plain plus helper", helm: `{"credsStore":"global","auths":{"ghcr.io":{"username":"helm","password":"helm"}}}`, user: "helm"},
		{name: "explicit plain plus helper", explicit: `{"credsStore":"global","auths":{"ghcr.io":{"username":"docker","password":"docker"}}}`, user: "docker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dockerDir, helmDir := t.TempDir(), t.TempDir()
			t.Setenv("DOCKER_CONFIG", dockerDir)
			t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(helmDir, "config.json"))
			if tc.docker != "" {
				testutil.WriteFile(t, dockerDir, "config.json", tc.docker)
			}
			if tc.helm != "" {
				testutil.WriteFile(t, helmDir, "config.json", tc.helm)
			}
			f := &Fetcher{Network: newTestNetwork(t)}
			if tc.explicit != "" {
				f.RegistryConfig = filepath.Join(t.TempDir(), "config.json")
				testutil.WriteFileAt(t, f.RegistryConfig, tc.explicit)
			}
			repo := ociRepo("config", func(s *sourcev1.OCIRepositorySpec) { s.URL = "oci://ghcr.io/app" })
			op := &operation{Fetcher: f}
			if tc.secret != "" {
				repo.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
				op.secrets = func(_, _ string) *manifest.Secret {
					return &manifest.Secret{StringData: map[string]any{dockerConfigJSONKey: tc.secret}}
				}
			}
			request, err := op.newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
			if tc.wantInput {
				if !errors.Is(err, manifest.ErrInput) || request.client != nil {
					t.Fatalf("lookup = %v, want authoritative input failure", err)
				}
				return
			}
			assert.NoError(t, err)
			got, err := request.client.Client.(*auth.Client).Credential(t.Context(), request.client.Reference.Host())
			assert.NoError(t, err)
			assert.Equal(t, got, auth.Credential{Username: tc.user, Password: tc.user})
		})
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("credential helper ran: %v", err)
	}
}

func TestFetcher_HelmConfigAvailability(t *testing.T) {
	for _, mode := range []string{"absent", "open failure", "read failure", "malformed", "default", "override"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("DOCKER_CONFIG", t.TempDir())
			home := t.TempDir()
			t.Setenv("HELM_CONFIG_HOME", home)
			t.Setenv("HELM_REGISTRY_CONFIG", "")
			path := filepath.Join(home, "registry", "config.json")
			wantUser := ""
			switch mode {
			case "open failure":
				testutil.WriteFile(t, home, "registry", "regular file")
			case "read failure":
				assert.NoError(t, os.MkdirAll(path, 0o755))
			case "malformed":
				testutil.WriteFileAt(t, path, `{`)
			case "default", "override":
				wantUser = "default"
				testutil.WriteFileAt(t, path, `{"auths":{"https://index.docker.io/v1/":{"username":"default","password":"default"}}}`)
				if mode == "override" {
					wantUser = "override"
					path = filepath.Join(t.TempDir(), "config.json")
					t.Setenv("HELM_REGISTRY_CONFIG", path)
					testutil.WriteFileAt(t, path, `{"auths":{"https://index.docker.io/v1/":{"username":"override","password":"override"}}}`)
				}
			}
			selected, err := loadCredentials(t.Context(), "", "registry-1.docker.io")
			if mode == "malformed" {
				if !errors.Is(err, manifest.ErrInput) {
					t.Fatalf("readable malformed Helm config = %v", err)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, *selected, auth.Credential{Username: wantUser, Password: wantUser})
		})
	}
}

func TestFetcher_MachineCredentialsAcquisition(t *testing.T) {
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap\n"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	for _, name := range []string{"Helm only", "Docker wins", "unrelated Docker", "wrong Docker is authoritative", "explicit miss", "anonymous helper success", "unreadable Helm public success"} {
		t.Run(name, func(t *testing.T) {
			var accepted atomic.Int32
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if name != "anonymous helper success" && name != "unreadable Helm public success" {
					user, password, ok := r.BasicAuth()
					want := "helm"
					if name == "Docker wins" {
						want = "docker"
					}
					if !ok || user != want || password != want {
						w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
				}
				accepted.Add(1)
				upstream.Config.Handler.ServeHTTP(w, r)
			}))
			t.Cleanup(srv.Close)
			host := mustURL(t, srv.URL).Host
			dockerDir := t.TempDir()
			t.Setenv("DOCKER_CONFIG", dockerDir)
			helmPath := filepath.Join(t.TempDir(), "config.json")
			t.Setenv("HELM_REGISTRY_CONFIG", helmPath)
			config := func(host, user string) string {
				return fmt.Sprintf(`{"auths":{%q:{"username":%q,"password":%q}}}`, host, user, user)
			}
			testutil.WriteFileAt(t, helmPath, config(host, "helm"))
			f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, "", sourcecache.New)}
			switch name {
			case "Docker wins", "wrong Docker is authoritative":
				testutil.WriteFile(t, dockerDir, "config.json", config(host, "docker"))
			case "unrelated Docker":
				testutil.WriteFile(t, dockerDir, "config.json", config("unrelated.example", "docker"))
			case "explicit miss":
				f.RegistryConfig = filepath.Join(t.TempDir(), "missing.json")
			case "anonymous helper success":
				testutil.WriteFileAt(t, helmPath, `{"credsStore":"unused"}`)
			case "unreadable Helm public success":
				assert.NoError(t, os.Remove(helmPath))
				assert.NoError(t, os.Mkdir(helmPath, 0o755))
			}
			repo := ociRepo("machine", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://" + host + "/app"
				s.Insecure = true
			})
			got, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
			if name == "wrong Docker is authoritative" || name == "explicit miss" {
				if err == nil || got != nil || accepted.Load() != 0 {
					t.Fatalf("unexpected fallback: artifact=%v error=%v accepted=%d", got, err, accepted.Load())
				}
				if strings.Contains(err.Error(), "no login entry for "+host) != (name == "explicit miss") {
					t.Fatalf("unexpected missing-login advice: %v", err)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, mustReadFile(t, got.FS, "app.yaml"), "kind: ConfigMap\n")
			if accepted.Load() == 0 {
				t.Fatal("acquisition did not reach registry")
			}
		})
	}
}

func TestFetcher_MissingLoginAdvice(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Basic absence", "bearer absence", "helper absence", "rejected credential", "forbidden", "missing content", "malformed content", "transport", "canceled"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch name {
				case "Basic absence", "helper absence", "rejected credential":
					w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
					w.WriteHeader(http.StatusUnauthorized)
				case "bearer absence":
					if r.URL.Path != "/token" {
						w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q,service="registry",scope="repository:app:pull"`, "https://"+r.Host+"/token"))
					}
					w.WriteHeader(http.StatusUnauthorized)
				case "forbidden":
					http.Error(w, "denied", http.StatusForbidden)
				case "missing content":
					http.NotFound(w, r)
				case "malformed content":
					w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
					_, _ = io.WriteString(w, "malformed manifest")
				case "transport":
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
				}
			}))
			t.Cleanup(srv.Close)
			path := filepath.Join(t.TempDir(), "config.json")
			config := `{}`
			if name == "helper absence" {
				config = `{"credsStore":"fixture-helper"}`
			}
			if name == "rejected credential" {
				config = fmt.Sprintf(`{"credsStore":"fixture-helper","auths":{%q:{"username":"fixture-user","password":"private-password"}}}`, mustURL(t, srv.URL).Host)
			}
			testutil.WriteFileAt(t, path, config)
			repo := ociRepo("advice", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://" + mustURL(t, srv.URL).Host + "/app"
				s.Insecure = true
			})
			op := &operation{Fetcher: &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, "", sourcecache.New), RegistryConfig: path}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if name == "canceled" {
				cancel()
			}
			artifact, err := op.fetchWithCredentials(ctx, repo, nil, sourcecache.Policy{})
			if err == nil || artifact != nil || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failure lost source classification: %v", err)
			}
			wantAdvice := name == "Basic absence" || name == "bearer absence" || name == "helper absence"
			for _, text := range []string{"no login entry for " + mustURL(t, srv.URL).Host, "Docker config", "Helm's registry file", "credential helpers are not run", "--registry-config", "plain auths entry", "supplied Secret"} {
				if strings.Contains(err.Error(), text) != wantAdvice {
					t.Errorf("advice %q: %v", text, err)
				}
			}
			if !strings.Contains(err.Error(), "OCIRepository ns/advice") || !strings.Contains(err.Error(), "resolve latest") && name != "canceled" {
				t.Errorf("original source/operation detail lost: %v", err)
			}
			if (name == "Basic absence" || name == "helper absence") && !errors.Is(err, auth.ErrBasicCredentialNotFound) {
				t.Errorf("Basic absence sentinel lost: %v", err)
			}
			if name == "bearer absence" || name == "rejected credential" || name == "forbidden" {
				response, ok := errors.AsType[*errcode.ErrorResponse](err)
				if !ok || response.StatusCode == 0 {
					t.Errorf("ORAS response chain lost: %v", err)
				}
				if _, ok := errors.AsType[*fetcherr.ResponseError](err); !ok {
					t.Errorf("neutral response chain lost: %v", err)
				}
			}
			if name == "missing content" && !errors.Is(err, errdef.ErrNotFound) {
				t.Errorf("missing content sentinel lost: %v", err)
			}
			if name == "canceled" && !errors.Is(err, context.Canceled) {
				t.Errorf("cancellation lost: %v", err)
			}
			for _, secret := range []string{"fixture-helper", "fixture-user", "private-password", base64.StdEncoding.EncodeToString([]byte("fixture-user:private-password"))} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("credential leaked in error")
				}
			}
		})
	}
}

func TestFetcher_SharedFailureMissingLoginAdvice(t *testing.T) {
	previous := http.DefaultTransport
	transport := previous.(*http.Transport).Clone()
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = previous })
	synctest.Test(t, func(t *testing.T) {
		var requests atomic.Int32
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		transport.DialTLSContext = func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				r, err := http.ReadRequest(bufio.NewReader(server))
				if err != nil {
					t.Error(err)
					return
				}
				_ = r.Body.Close()
				if requests.Add(1) == 1 {
					_, _ = io.WriteString(server, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
					return
				}
				close(entered)
				<-release
				_, _ = io.WriteString(server, "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"fixture\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			}()
			return client, nil
		}
		path := filepath.Join(t.TempDir(), "config.json")
		write := func(helper string) {
			t.Helper()
			testutil.WriteFileAt(t, path, fmt.Sprintf(`{"credsStore":%q}`, helper))
		}
		write("first")
		op := &operation{Fetcher: &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New), RegistryConfig: path}}
		repo := ociRepo("shared", func(s *sourcev1.OCIRepositorySpec) { s.URL = "oci://registry.invalid/app" })
		for range 2 {
			_, err := op.fetchWithCredentials(t.Context(), repo, nil, sourcecache.Policy{})
			response, ok := errors.AsType[*fetcherr.ResponseError](err)
			if !ok || response.StatusCode != http.StatusServiceUnavailable || strings.Contains(err.Error(), "credential helper") || requests.Load() != 1 {
				t.Fatalf("transient replay gained advice or retried: %v, requests=%d", err, requests.Load())
			}
		}
		time.Sleep(time.Second)
		done := make(chan error, 2)
		go func() {
			_, err := op.fetchWithCredentials(t.Context(), repo, nil, sourcecache.Policy{})
			done <- err
		}()
		<-entered
		write("second")
		go func() {
			_, err := op.fetchWithCredentials(t.Context(), repo, nil, sourcecache.Policy{})
			done <- err
		}()
		synctest.Wait()
		unblock()
		for range 2 {
			err := <-done
			if !errors.Is(err, auth.ErrBasicCredentialNotFound) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "--registry-config") || !strings.Contains(err.Error(), "supplied Secret") {
				t.Fatalf("shared error lost chain or remedies: %v", err)
			}
			for _, text := range []string{"no login entry for registry.invalid", "Docker config", "Helm's registry file", "credential helpers are not run", "plain auths entry"} {
				if !strings.Contains(err.Error(), text) {
					t.Errorf("shared error lost advice %q: %v", text, err)
				}
			}
			for _, helper := range []string{"first", "second"} {
				if strings.Contains(err.Error(), helper) {
					t.Errorf("shared error named helper %q: %v", helper, err)
				}
			}
		}
		if requests.Load() != 2 {
			t.Fatalf("shared acquisition duplicated work: requests=%d", requests.Load())
		}
	})
}

func TestFetcher_SecretCredentialsAcquisitionPolicy(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
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

func TestFetcher_ProviderMissingSecret(t *testing.T) {
	t.Parallel()
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://ghcr.io/x/y"
		s.Provider = sourcev1.AmazonOCIProvider
		s.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	})
	_, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if err == nil {
		t.Fatalf("expected missing-secret error")
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
		config    string
		docker    string
		helm      string
		wantRetry bool
		wantErr   error // nil with wantRetry false means any non-sentinel error
	}{
		{name: "not found, fallback", config: covering, wantRetry: true},
		{name: "not found, docker default fallback", docker: covering, wantRetry: true},
		{name: "not found, Helm fallback", helm: covering, wantRetry: true},
		{name: "wiped, Helm fallback", secret: &manifest.Secret{StringData: map[string]any{".dockerconfigjson": "..PLACEHOLDER_.dockerconfigjson.."}}, helm: covering, wantRetry: true},
		{name: "Helm unrelated host", helm: `{"auths":{"quay.io":{"username":"other","password":"other"}}}`, wantErr: manifest.ErrMissingSecret},
		{name: "Helm helper only", helm: `{"credsStore":"unused"}`, wantErr: manifest.ErrMissingSecret},
		{name: "Docker helper only", docker: `{"credsStore":"unused"}`, wantErr: manifest.ErrMissingSecret},
		{name: "malformed Helm deferred", helm: `{`},
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
			dockerDir := t.TempDir()
			if tt.docker != "" {
				testutil.WriteFile(t, dockerDir, "config.json", tt.docker)
			}
			t.Setenv("DOCKER_CONFIG", dockerDir)
			t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
			if tt.helm != "" {
				testutil.WriteFileAt(t, os.Getenv("HELM_REGISTRY_CONFIG"), tt.helm)
			}
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
			credStore, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveRegistryConfig(t.Context(), repo, "ghcr.io", sourcecache.Policy{})
			switch {
			case tt.wantRetry:
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || !errors.Is(err, manifest.ErrMissingSecret) || credStore != nil || missing.RetryAmbient == nil {
					t.Fatalf("got (%v, %v), want missing Secret with retry using %q", credStore, err, f.RegistryConfig)
				}
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				if errors.Is(tt.wantErr, manifest.ErrMissingSecret) && tt.config != covering {
					if missing, ok := errors.AsType[*source.MissingSecretError](err); !ok || missing.RetryAmbient != nil {
						t.Fatalf("err = %v, want missing Secret without registry retry", err)
					}
				}
			default:
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || missing.RetryAmbient == nil {
					t.Fatalf("err = %v, want missing Secret with deferred config error", err)
				}
				if _, err := missing.RetryAmbient(t.Context()); !errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrMissingSecret) {
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
	art.manifest = mustManifestJSON(t,
		manifestBlob{data: art.config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: art.layer, mediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip"},
	)
	art.manifestDigest = sha256Digest(art.manifest)
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
	chartInput := manifest.NewHelmChartSource(sourcev1.HelmChart{Name: "app", Namespace: repo.Namespace, Spec: sourcev1.HelmChartSpec{Chart: "app", Version: "1.0.0", Interval: repo.Interval, SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: repo.Name}}})
	chartsInputs := testInputs{secrets: ociFetcherInputs.secrets, repos: func(_, _ string) *manifest.HelmRepository { return repo }}
	charts := helmchart.New(ociFetcher, helmchart.Config{Cache: cache, Network: &network})

	got, err := charts.Fetch(t.Context(), chartInput, chartsInputs.readSet(chartInput), sourcecache.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != manifest.KindHelmChart {
		t.Errorf("artifact Kind = %q, want %q", got.Kind, manifest.KindHelmChart)
	}
	if b := mustReadFile(t, got.FS, "Chart.yaml"); b != chart {
		t.Errorf("Chart.yaml = %q, want %q", b, chart)
	}
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	helmPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("HELM_REGISTRY_CONFIG", helmPath)
	testutil.WriteFileAt(t, helmPath, configJSON)
	helmRepo := *repo
	helmRepo.SecretRef = nil
	helmInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return &helmRepo }}
	helmCharts := helmchart.New(&Fetcher{Cache: cache, Network: &network}, helmchart.Config{Cache: cache, Network: &network})
	helmArtifact, err := helmCharts.Fetch(t.Context(), chartInput, helmInputs.readSet(chartInput), sourcecache.Policy{})
	assert.NoError(t, err)
	assert.Equal(t, helmArtifact.Kind, manifest.KindHelmChart)
	assert.Equal(t, mustReadFile(t, helmArtifact.FS, "Chart.yaml"), chart)
	initialKey := got.Key
	configPath := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, configPath, configJSON)
	fallbackFetcherInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	fallbackFetcher := &Fetcher{Cache: cache, Network: &network, RegistryConfig: configPath}
	fallbackChartsInputs := testInputs{secrets: fallbackFetcherInputs.secrets, repos: func(_, _ string) *manifest.HelmRepository { return repo }}
	fallbackCharts := helmchart.New(fallbackFetcher, helmchart.Config{Cache: cache, Network: &network})

	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	_, err = fallbackCharts.Fetch(t.Context(), chartInput, fallbackChartsInputs.readSet(chartInput), sourcecache.Policy{Retry: retry})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryAmbient == nil {
		t.Fatalf("missing fallback: %v", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := missing.RetryAmbient(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("fallback context: %v", err)
	}
	failNext.Store(true)
	got, err = missing.RetryAmbient(t.Context())
	if err != nil || failures.Load() != 1 {
		t.Fatalf("fallback recovery: %v, failures=%d", err, failures.Load())
	}
	if got.Key == initialKey || got.Kind != manifest.KindHelmChart || mustReadFile(t, got.FS, "Chart.yaml") != chart {
		t.Fatalf("fallback artifact=%+v", got)
	}
	beforeWarm := requests.Load()
	warm, err := missing.RetryAmbient(t.Context())
	if err != nil || warm == nil || warm.Key != got.Key || warm.Kind != manifest.KindHelmChart || requests.Load() != beforeWarm {
		t.Fatalf("fallback lost freshness or chart identity: artifact=%+v, err=%v, new requests=%d", warm, err, requests.Load()-beforeWarm)
	}
	refreshRepo := *repo
	refreshRepo.Interval = metav1.Duration{}
	refreshInput := *chartInput
	refreshInput.Interval = refreshRepo.Interval
	refreshChartsInputs := testInputs{secrets: fallbackFetcherInputs.secrets, repos: func(_, _ string) *manifest.HelmRepository { return &refreshRepo }}
	refreshCharts := helmchart.New(fallbackFetcher, helmchart.Config{Cache: cache, Network: &network})
	_, err = refreshCharts.Fetch(t.Context(), &refreshInput, refreshChartsInputs.readSet(&refreshInput), sourcecache.Policy{Retry: retry})
	missing, ok = errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryAmbient == nil {
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
	_, err = missing.RetryAmbient(t.Context())
	if terminal, ok := errors.AsType[*sourcecache.TerminalError](err); !ok || terminal.Attempted || !errors.Is(err, cause) || requests.Load() != before {
		t.Fatalf("suppressed fallback: %v, additional requests=%d", err, requests.Load()-before)
	}
}

func TestFetcher_ResolveConfig_CredentialSnapshot(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.json")
	write := func(password string) {
		t.Helper()
		testutil.WriteFileAt(t, path, fmt.Sprintf(`{"auths":{"https://index.docker.io/v1/":{"username":"fixture","password":%q}}}`, password))
	}
	write("first")
	op := &operation{Fetcher: &Fetcher{Network: newTestNetwork(t), RegistryConfig: path}}
	repo := ociRepo("snapshot", func(s *sourcev1.OCIRepositorySpec) { s.URL = "oci://docker.io/library/test" })
	request, err := op.newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
	assert.NoError(t, err)
	write("second")
	cred, err := request.client.Client.(*auth.Client).Credential(t.Context(), "registry-1.docker.io")
	if err != nil || cred.Password != "first" {
		t.Fatalf("credential was not frozen: %v", err)
	}
	next, err := op.newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
	if err != nil || cmp.Equal(request.material, next.material) {
		t.Fatalf("next acquisition did not resolve rotated credential: %v", err)
	}
	testutil.WriteFileAt(t, path, `{"auths":{"https://index.docker.io/v1/":{"auth":"!!!"}}}`)
	failed, err := op.newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
	if failed.client != nil || failed.material != nil || !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("failed lookup admitted acquisition: %v", err)
	}
	if !strings.Contains(err.Error(), "failed to decode auth field") {
		t.Fatalf("credential decode detail lost: %v", err)
	}
	op.RegistryConfig = filepath.Join(path, "config.json")
	failed, err = op.newRepoClient(t.Context(), repo, nil, sourcecache.Policy{})
	if _, ok := errors.AsType[*os.PathError](err); !ok || failed.client != nil || !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("file lookup error chain lost: %v", err)
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
		s.Interval = &metav1.Duration{Duration: time.Hour}
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
			failed.Interval = &metav1.Duration{}
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
			if !ok || missing.RetryAmbient == nil || lookups.Load() != 2 {
				t.Fatalf("snapshot before handoff: reads=%d error=%v", lookups.Load(), err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var wg sync.WaitGroup
			for range callers {
				wg.Go(func() {
					_, err := missing.RetryAmbient(ctx)
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

func TestFetcher_ProviderTransportValidation(t *testing.T) {
	t.Parallel()
	for _, use := range []string{"cert", "proxy"} {
		t.Run(use, func(t *testing.T) {
			t.Parallel()
			repo := ociRepo("provider", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://fixture.invalid/image"
				s.Provider = sourcev1.AmazonOCIProvider
				if use == "cert" {
					s.CertSecretRef = &manifest.LocalObjectReference{Name: "transport"}
				} else {
					s.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
				}
			})
			inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{"ca.crt": "invalid", "address": "http://proxy.invalid/%"}}
			}}
			fetcher := &Fetcher{Network: newTestNetwork(t)}
			_, err := fetcher.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{})
			if !errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrMissingSecret) {
				t.Fatalf("provider hid malformed transport: %v", err)
			}
		})
	}
}

func TestFetcher_ProviderRegistryMatching(t *testing.T) {
	t.Parallel()
	for _, matching := range []bool{false, true} {
		t.Run(fmt.Sprint(matching), func(t *testing.T) {
			t.Parallel()
			registry := "other.invalid"
			if matching {
				registry = "fixture.invalid"
			}
			config := filepath.Join(t.TempDir(), "config.json")
			testutil.WriteFileAt(t, config, fmt.Sprintf(`{"auths":{%q:{"username":"ambient","password":"fixture"}}}`, registry))
			repo := ociRepo("provider", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://fixture.invalid/image"
				s.Provider = sourcev1.AmazonOCIProvider
				s.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
			})
			inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": `{"auths":{"unrelated.invalid":{"username":"repository","password":"fixture"}}}`}}
			}}
			fetcher := &Fetcher{Network: newTestNetwork(t), RegistryConfig: config}
			_, err := fetcher.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.Secret.Name != "auth" || (missing.RetryAmbient != nil) != matching {
				t.Fatalf("actual-registry continuation: %v", err)
			}
		})
	}
}
