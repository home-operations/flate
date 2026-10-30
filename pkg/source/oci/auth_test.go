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

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
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
	"github.com/home-operations/flate/pkg/store"
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
				_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo.RegistryAccess())
			case tt.cert:
				_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo.RegistryAccess())
			default:
				_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveRegistryConfig(t.Context(), repo.RegistryAccess(), "ghcr.io")
			}
			if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tt.wantMissing || !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("error = %v, want missing=%t containing %q", err, tt.wantMissing, tt.wantText)
			}
			if tt.wantOwner {
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok {
					t.Fatalf("expected MissingSecretError; got %T", err)
				}
				assert.Equal(t, missing.Owner, manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "repo"})
				assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "missing"})
				assert.Equal(t, err.Error(), "secret ns/missing not found")
			}
		})
	}
}

func TestFetcher_ResolveTLS_NoCertSecret(t *testing.T) {
	t.Parallel()
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", nil)
	repo.URL = "oci://registry.example/test"
	request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), repo.RegistryAccess())
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
	request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), repo.RegistryAccess())
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
	request, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(t.Context(), repo.RegistryAccess())
	if err != nil {
		t.Fatal(err)
	}
	material, err := source.ResolveTransport(fInputs.secrets, manifest.DiagnosticFor(repo.Named(), repo), []source.TransportInput{{Reference: repo.CertSecretRef, Use: source.TransportCert}})
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
			var selected *auth.Credential
			var err error
			if repo.SecretRef == nil {
				selected, err = newTestNetwork(t).RegistryCredentials(t.Context(), f.RegistryConfig, tt.registry)
				assert.NoError(t, err)
			}
			request, err := op.newRepoClient(t.Context(), repo.RegistryAccess())
			if err == nil && selected != nil {
				err = op.configure(repo.RegistryAccess(), &request, *selected)
			}
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
				rotated, err := op.newRepoClient(t.Context(), repo.RegistryAccess())
				if err != nil || cmp.Equal(request.material, rotated.material) {
					t.Fatal("credential rotation reused identity")
				}
			}
		})
	}
}

func TestFetcher_CorruptDockerConfig(t *testing.T) {
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
	if got != nil || err == nil || !errors.Is(err, manifest.ErrFlux) || strings.Contains(err.Error(), "auths") {
		t.Fatalf("malformed machine config admitted acquisition or lost classification: %v", err)
	}
}

func TestFetcher_MachineCredentials(t *testing.T) {
	const docker = `{"auths":{"ghcr.io":{"username":"docker","password":"docker"}}}`
	const helm = `{"auths":{"ghcr.io":{"username":"helm","password":"helm"}}}`
	for _, tc := range []struct {
		name, docker, helm, explicit, helper, user string
		fail                                       bool
		calls                                      int
	}{
		{name: "Helm", helm: helm, user: "helm"},
		{name: "Docker first", docker: docker, helm: helm, user: "docker"},
		{name: "unrelated Docker", docker: `{"auths":{"quay.io":{"username":"other","password":"other"}}}`, helm: helm, user: "helm"},
		{name: "malformed Docker", docker: `{`, helm: helm, fail: true},
		{name: "malformed Helm", docker: docker, helm: `{`, fail: true},
		{name: "explicit excludes defaults", docker: `{`, helm: `{`, explicit: docker, user: "docker"},
		{name: "explicit anonymous", docker: docker, explicit: `{}`},
		{name: "delegated", docker: `{"credHelpers":{"ghcr.io":"fixture"}}`, helper: "success", user: "native", calls: 1},
		{name: "plaintext", docker: docker, helper: "success", user: "docker"},
		{name: "unrelated delegation", docker: `{"credHelpers":{"quay.io":"fixture"}}`, helper: "success"},
		{name: "native not found", docker: `{"credsStore":"fixture"}`, helper: "not-found", calls: 1},
		{name: "missing executable", docker: `{"credsStore":"absent"}`, fail: true},
		{name: "default helper", helper: "default", user: "native", calls: 1},
		{name: "Secret delegation", docker: `{"credHelpers":{"ghcr.io":"fixture"}}`, helper: "secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, bin := t.TempDir(), t.TempDir()
			t.Setenv("HOME", root)
			t.Setenv("PATH", bin)
			t.Setenv("DOCKER_CONFIG", filepath.Join(root, "docker"))
			helmPath := filepath.Join(root, "helm.json")
			t.Setenv("HELM_REGISTRY_CONFIG", helmPath)
			marker := filepath.Join(root, "calls")
			t.Setenv("FLATE_HELPER_MARKER", marker)
			if tc.helper != "" {
				program := "#!/bin/sh\nprintf called >> \"$FLATE_HELPER_MARKER\"\nprintf '%s' '{\"Username\":\"native\",\"Secret\":\"native\"}'\n"
				if tc.helper == "not-found" {
					program = "#!/bin/sh\nprintf called >> \"$FLATE_HELPER_MARKER\"\nprintf '%s' 'credentials not found in native keychain'\nexit 1\n"
				}
				names := []string{"fixture"}
				if tc.helper == "default" {
					names = []string{"pass", "secretservice", "osxkeychain", "wincred"}
				}
				for _, name := range names {
					path := filepath.Join(bin, "docker-credential-"+name)
					testutil.WriteFileAt(t, path, program)
					assert.NoError(t, os.Chmod(path, 0o700))
				}
			}
			if tc.docker != "" {
				testutil.WriteFile(t, os.Getenv("DOCKER_CONFIG"), "config.json", tc.docker)
			}
			if tc.helm != "" {
				testutil.WriteFileAt(t, helmPath, tc.helm)
			}
			explicit := ""
			if tc.explicit != "" {
				explicit = filepath.Join(root, "explicit.json")
				testutil.WriteFileAt(t, explicit, tc.explicit)
			}
			var selected *auth.Credential
			var err error
			if tc.helper != "secret" {
				selected, err = newTestNetwork(t).RegistryCredentials(t.Context(), explicit, "ghcr.io")
			}
			if tc.helper == "secret" {
				repo := ociRepo("secret", func(s *sourcev1.OCIRepositorySpec) {
					s.URL = "oci://ghcr.io/app"
					s.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
				})
				op := &operation{Fetcher: &Fetcher{Network: newTestNetwork(t)}, secrets: func(string, string) *manifest.Secret {
					return &manifest.Secret{StringData: map[string]any{dockerConfigJSONKey: tc.docker}}
				}}
				selected, err = op.resolveRegistryConfig(t.Context(), repo.RegistryAccess(), "ghcr.io")
			}
			if (err != nil) != tc.fail {
				t.Fatalf("native lookup: %v", err)
			}
			if !tc.fail {
				assert.Equal(t, *selected, auth.Credential{Username: tc.user, Password: tc.user})
			}
			data, readErr := os.ReadFile(marker)
			if tc.calls == 0 {
				if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatalf("unexpected helper invocation: %q %v", data, readErr)
				}
			} else if readErr != nil || string(data) != "called" {
				t.Fatalf("helper calls: %q %v", data, readErr)
			}
			if tc.docker != "" {
				data, err := os.ReadFile(filepath.Join(os.Getenv("DOCKER_CONFIG"), "config.json"))
				assert.NoError(t, err)
				assert.Equal(t, string(data), tc.docker)
			}
			if tc.helm != "" {
				data, err := os.ReadFile(helmPath)
				assert.NoError(t, err)
				assert.Equal(t, string(data), tc.helm)
			}
		})
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
			selected, err := newTestNetwork(t).RegistryCredentials(t.Context(), "", "registry-1.docker.io")
			if mode == "malformed" || mode == "open failure" || mode == "read failure" {
				if err == nil {
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
	for _, name := range []string{"Helm only", "Docker wins", "unrelated Docker", "wrong Docker is authoritative", "explicit miss", "missing helper", "unreadable Helm", "helper rotation"} {
		t.Run(name, func(t *testing.T) {
			var accepted atomic.Int32
			var expectedPassword atomic.Value
			expectedPassword.Store("helm")
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if name != "missing helper" && name != "unreadable Helm" {
					user, password, ok := r.BasicAuth()
					want := "helm"
					if name == "Docker wins" {
						want = "docker"
					}
					wantPassword := want
					if name == "helper rotation" {
						wantPassword = expectedPassword.Load().(string)
					}
					if !ok || user != want || password != wantPassword {
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
			t.Setenv("PATH", t.TempDir())
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
			case "missing helper":
				testutil.WriteFileAt(t, helmPath, `{"credsStore":"unused"}`)
			case "unreadable Helm":
				assert.NoError(t, os.Remove(helmPath))
				assert.NoError(t, os.Mkdir(helmPath, 0o755))
			}

			marker := filepath.Join(t.TempDir(), "called")
			if name == "helper rotation" {
				bin := t.TempDir()
				t.Setenv("PATH", bin)
				t.Setenv("FLATE_HELPER_MARKER", marker)
				t.Setenv("FLATE_HELPER_SECRET", "helm")
				program := filepath.Join(bin, "docker-credential-fixture")
				testutil.WriteFileAt(t, program, "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$FLATE_HELPER_MARKER.args\"\n/bin/cat > \"$FLATE_HELPER_MARKER.stdin\"\n/usr/bin/env > \"$FLATE_HELPER_MARKER.env\"\nprintf called >> \"$FLATE_HELPER_MARKER\"\nprintf '{\"Username\":\"helm\",\"Secret\":\"%s\"}' \"$FLATE_HELPER_SECRET\"\n")
				assert.NoError(t, os.Chmod(program, 0o700))
				testutil.WriteFile(t, dockerDir, "config.json", `{"credsStore":"fixture"}`)
			}
			repo := ociRepo("machine", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://" + host + "/app"
				s.Insecure = true
			})
			inputs := testInputs{}
			ca, proxyAddress := "", ""
			var proxied atomic.Int32
			if name == "helper rotation" {
				proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodConnect || r.Host != host {
						http.Error(w, "unexpected proxy destination", http.StatusBadGateway)
						return
					}
					proxied.Add(1)
					remote, err := (&net.Dialer{}).DialContext(r.Context(), "tcp", host)
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
				proxyAddress = proxy.URL
				ca = testutil.SelfSignedCA(t)
				repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
				repo.CertSecretRef = repo.ProxySecretRef
				inputs.secrets = func(string, string) *manifest.Secret {
					return &manifest.Secret{StringData: map[string]any{
						"address": proxyAddress, "username": "repository-proxy-user", "password": "repository-proxy-password", "ca.crt": ca,
					}}
				}
			}
			got, err := f.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{MaxAge: time.Hour})
			if name == "wrong Docker is authoritative" || name == "explicit miss" || name == "missing helper" || name == "unreadable Helm" {
				if err == nil || got != nil || accepted.Load() != 0 {
					t.Fatalf("unexpected fallback: artifact=%v error=%v accepted=%d", got, err, accepted.Load())
				}
				if !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("machine failure lost domain: %v", err)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, mustReadFile(t, got.FS, "app.yaml"), "kind: ConfigMap\n")
			if accepted.Load() == 0 {
				t.Fatal("acquisition did not reach registry")
			}
			if name == "helper rotation" {
				before := accepted.Load()
				warm, err := f.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{MaxAge: time.Hour})
				assert.NoError(t, err)
				if warm.Key != got.Key || accepted.Load() != before {
					t.Fatal("unchanged helper credential missed warm generation")
				}
				expectedPassword.Store("rotated")
				t.Setenv("FLATE_HELPER_SECRET", "rotated")
				rotated, err := f.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{MaxAge: time.Hour})
				assert.NoError(t, err)
				if rotated.Key == got.Key || accepted.Load() == before {
					t.Fatal("rotated helper credential reused identity")
				}
				calls, err := os.ReadFile(marker)
				assert.NoError(t, err)
				assert.Equal(t, string(calls), "calledcalledcalled")
				args, err := os.ReadFile(marker + ".args")
				assert.NoError(t, err)
				assert.Equal(t, string(args), "get\n")
				stdin, err := os.ReadFile(marker + ".stdin")
				assert.NoError(t, err)
				assert.Equal(t, strings.TrimSpace(string(stdin)), host)
				environment, err := os.ReadFile(marker + ".env")
				assert.NoError(t, err)
				if !strings.Contains(string(environment), "FLATE_HELPER_SECRET=rotated") || proxied.Load() == 0 {
					t.Fatal("helper lost the machine environment or source proxy")
				}
				for _, canary := range []string{proxyAddress, "repository-proxy-user", "repository-proxy-password", ca, "-----BEGIN CERTIFICATE-----"} {
					if strings.Contains(string(args)+string(stdin)+string(environment), canary) {
						t.Fatal("repository transport material reached registry helper inputs")
					}
				}
			}

		})
	}
}

func TestFetcher_MachineFailureCauses(t *testing.T) {
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
				config = fmt.Sprintf(`{"auths":{%q:{"username":"fixture-user","password":"private-password"}}}`, mustURL(t, srv.URL).Host)
			}
			testutil.WriteFileAt(t, path, config)
			repo := ociRepo("advice", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://" + mustURL(t, srv.URL).Host + "/app"
				s.Insecure = true
			})
			op := &operation{Fetcher: &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, "", sourcecache.New), RegistryConfig: path}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			policy := sourcecache.Policy{}
			if name == "canceled" {
				policy.Admit = func(context.Context) (func(), error) {
					cancel()
					return func() {}, nil
				}
			}
			artifact, err := op.fetchSource(ctx, repo, policy)
			if err == nil || artifact != nil || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failure lost source classification: %v", err)
			}
			wantAdvice := false
			for _, text := range []string{"no login entry for " + mustURL(t, srv.URL).Host, "Docker config", "Helm's registry file", "credential helpers are not run", "--registry-config", "plain auths entry", "supplied Secret"} {
				if strings.Contains(err.Error(), text) != wantAdvice {
					t.Errorf("advice %q: %v", text, err)
				}
			}
			if strings.Contains(err.Error(), "OCIRepository ns/advice") {
				t.Errorf("source identity repeated: %v", err)
			}
			if name == "Basic absence" && !errors.Is(err, auth.ErrBasicCredentialNotFound) {
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

func TestFetcher_SharedMachineFailure(t *testing.T) {
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
		testutil.WriteFileAt(t, path, `{}`)
		op := &operation{Fetcher: &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New), RegistryConfig: path}}
		repo := ociRepo("shared", func(s *sourcev1.OCIRepositorySpec) { s.URL = "oci://registry.invalid/app" })
		for range 2 {
			_, err := op.fetchSource(t.Context(), repo, sourcecache.Policy{})
			response, ok := errors.AsType[*fetcherr.ResponseError](err)
			if !ok || response.StatusCode != http.StatusServiceUnavailable || strings.Contains(err.Error(), "credential helper") || requests.Load() != 1 {
				t.Fatalf("transient replay gained advice or retried: %v, requests=%d", err, requests.Load())
			}
		}
		time.Sleep(time.Second)
		done := make(chan error, 2)
		go func() {
			_, err := op.fetchSource(t.Context(), repo, sourcecache.Policy{})
			done <- err
		}()
		<-entered
		testutil.WriteFileAt(t, path, `{}`)
		go func() {
			_, err := op.fetchSource(t.Context(), repo, sourcecache.Policy{})
			done <- err
		}()
		synctest.Wait()
		unblock()
		for range 2 {
			err := <-done
			if !errors.Is(err, auth.ErrBasicCredentialNotFound) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("shared error lost chain or remedies: %v", err)
			}
			for _, text := range []string{"no login entry for registry.invalid", "Docker config", "Helm's registry file", "credential helpers are not run", "plain auths entry"} {
				if strings.Contains(err.Error(), text) {
					t.Errorf("shared error retained obsolete advice %q: %v", text, err)
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

			_, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).fetchSource(t.Context(), repo, sourcecache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil || missing.Secret.Name != "creds" {
				t.Fatalf("unavailable auth lost lazy continuation: %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := missing.RetryMachine(ctx, nil); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled continuation ran preparation: %v", err)
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
	selected, err := newTestNetwork(t).RegistryCredentials(t.Context(), path, "registry-1.docker.io")
	assert.NoError(t, err)
	request, err := op.newRepoClient(t.Context(), repo.RegistryAccess())
	if err == nil {
		err = op.configure(repo.RegistryAccess(), &request, *selected)
	}
	assert.NoError(t, err)
	write("second")
	cred, err := request.client.Client.(*auth.Client).Credential(t.Context(), "registry-1.docker.io")
	if err != nil || cred.Password != "first" {
		t.Fatalf("credential was not frozen: %v", err)
	}
	selected, err = newTestNetwork(t).RegistryCredentials(t.Context(), path, "registry-1.docker.io")
	assert.NoError(t, err)
	next, err := op.newRepoClient(t.Context(), repo.RegistryAccess())
	if err == nil {
		err = op.configure(repo.RegistryAccess(), &next, *selected)
	}
	if err != nil || cmp.Equal(request.material, next.material) {
		t.Fatalf("next acquisition did not resolve rotated credential: %v", err)
	}
	testutil.WriteFileAt(t, path, `{"auths":{"https://index.docker.io/v1/":{"auth":"!!!"}}}`)
	_, err = newTestNetwork(t).RegistryCredentials(t.Context(), path, "registry-1.docker.io")
	if err == nil {
		t.Fatalf("failed lookup admitted acquisition: %v", err)
	}
	if !strings.Contains(err.Error(), "failed to decode auth field") {
		t.Fatalf("credential decode detail lost: %v", err)
	}
	op.RegistryConfig = filepath.Join(path, "config.json")
	_, err = newTestNetwork(t).RegistryCredentials(t.Context(), op.RegistryConfig, "registry-1.docker.io")
	if _, ok := errors.AsType[*os.PathError](err); !ok {
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
			artifact := newFakeOCIArtifact(t, map[string]string{"data.yaml": "concurrent"})
			upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
			config := filepath.Join(t.TempDir(), "config.json")
			testutil.WriteFileAt(t, config, fmt.Sprintf(`{"auths":{%q:{"username":"alice","password":"test"}}}`, mustURL(t, upstream.URL).Host))
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
				s.URL = "oci://" + mustURL(t, upstream.URL).Host + "/app"
				s.Insecure = true
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
			start := make(chan struct{})
			artifacts := make([]*store.SourceArtifact, callers)
			failures := make([]error, callers)
			for i := range callers {
				wg.Go(func() { <-start; artifacts[i], failures[i] = missing.RetryMachine(t.Context(), nil) })
			}
			close(start)
			wg.Wait()
			for i, artifact := range artifacts {
				if failures[i] != nil || artifact == nil || artifact.Key != artifacts[0].Key || mustReadFile(t, artifact.FS, "data.yaml") != "concurrent" {
					t.Fatalf("concurrent prepared acquisition: artifact=%v error=%v", artifact, failures[i])
				}
			}
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
			if !ok || missing.Secret.Name != "auth" || missing.RetryAmbient == nil {
				t.Fatalf("actual-registry continuation: %v", err)
			}
		})
	}
}

func TestFetcher_SyntheticCredentialContext(t *testing.T) {
	t.Parallel()
	for _, generated := range []bool{false, true} {
		for _, material := range []string{"auth absent", "auth wiped", "auth malformed", "cert absent", "cert wiped", "cert malformed", "proxy absent", "proxy wiped", "proxy malformed"} {
			t.Run(fmt.Sprintf("generated=%t/%s", generated, material), func(t *testing.T) {
				t.Parallel()
				chart := &manifest.HelmChartSource{Name: "chart", Namespace: "sources"}
				chart.Chart = "app"
				expected := chart.Named()
				if generated {
					release := &manifest.HelmRelease{Name: "release", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{Chart: "app", SourceRef: &helmv2.CrossNamespaceObjectReference{Kind: manifest.KindHelmRepository, Name: "charts", Namespace: "sources"}}}}}
					children, err := release.GeneratedObjects()
					if err != nil {
						t.Fatal(err)
					}
					chart = children[0].(*manifest.HelmChartSource)
					expected = release.Named()
				}
				repo := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
				var sec *manifest.Secret
				ref := &manifest.LocalObjectReference{Name: "material"}
				missing := !strings.HasSuffix(material, "malformed")
				switch {
				case strings.HasPrefix(material, "auth"):
					repo.SecretRef = ref
					if material == "auth wiped" {
						sec = &manifest.Secret{StringData: map[string]any{".dockerconfigjson": manifest.ValuePlaceholder("config")}}
					}
					if material == "auth malformed" {
						sec = &manifest.Secret{StringData: map[string]any{".dockerconfigjson": "{"}}
					}
				case strings.HasPrefix(material, "cert"):
					repo.CertSecretRef = ref
					if material == "cert wiped" {
						sec = &manifest.Secret{StringData: map[string]any{"ca.crt": manifest.ValuePlaceholder("ca")}}
					}
					if material == "cert malformed" {
						sec = &manifest.Secret{StringData: map[string]any{"ca.crt": "invalid"}}
					}
				case strings.HasPrefix(material, "proxy"):
					repo.ProxySecretRef = ref
					if material == "proxy wiped" {
						sec = &manifest.Secret{StringData: map[string]any{"address": manifest.ValuePlaceholder("proxy")}}
					}
					if material == "proxy malformed" {
						sec = &manifest.Secret{StringData: map[string]any{"address": "http://invalid/%"}}
					}
				default:
					repo.Provider = "aws"
				}
				config := filepath.Join(t.TempDir(), "config.json")
				testutil.WriteFileAt(t, config, "{}")
				op := &operation{Fetcher: &Fetcher{Network: newTestNetwork(t), RegistryConfig: config}, secrets: func(namespace, name string) *manifest.Secret {
					if namespace != "sources" || name != ref.Name {
						t.Fatalf("Secret lookup %s/%s used display owner", namespace, name)
					}
					return sec
				}}
				_, err := op.fetchSource(t.Context(), repo, sourcecache.Policy{})
				label := expected.Kind + " " + expected.NamespacedName()
				if material == "provider" {
					label = expected.String()
				}
				if err == nil || strings.Contains(err.Error(), label) || strings.Contains(err.Error(), "OCIRepository") {
					t.Fatalf("early diagnostic=%v, owner=%s", err, expected)
				}
				if errors.Is(err, manifest.ErrMissingSecret) != missing {
					t.Fatalf("material classification changed: %v", err)
				}
				if failure, ok := errors.AsType[*source.MissingSecretError](err); ok {
					if failure.Owner != repo.Named() || failure.Subject.Resource != expected {
						t.Fatalf("raw/display owner=%+v", failure)
					}
					if material != "provider" && failure.Secret.Namespace != "sources" {
						t.Fatalf("Secret identity=%s", failure.Secret)
					}
				}
			})
		}
	}
}
