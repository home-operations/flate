package oci

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"oras.land/oras-go/v2/registry/remote/auth"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func ociRepo(name string, set func(s *sourcev1.OCIRepositorySpec)) *manifest.OCIRepository {
	r := &manifest.OCIRepository{Name: name, Namespace: "ns"}
	if set != nil {
		set(&r.OCIRepositorySpec)
	}
	return r
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
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
		{
			name:     "partial cert key",
			secret:   "tls",
			cert:     true,
			material: map[string]any{"tls.crt": "-only-cert-"},
			wantText: "must provide both",
		},
		{
			name:        "empty cert material",
			secret:      "tls",
			cert:        true,
			material:    map[string]any{"unrelated": "x"},
			wantMissing: true,
		},
		{
			name:        "missing Docker JSON",
			secret:      "wrong-shape",
			material:    map[string]any{"username": "alice"},
			wantText:    ".dockerconfigjson",
			wantMissing: true,
		},
		{name: "unwired getter", secret: "creds", unwired: true, wantText: "source.SecretGetter"},
		{name: "missing auth secret", secret: "missing", wantText: "secret ns/missing not found", wantMissing: true},
	}
	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) { testFetcherSecretReferencesCase(t, &tt) },
		)
	}
}

func TestFetcher_ResolveTLS_NoCertSecret(t *testing.T) {
	t.Parallel()
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", nil)
	repo.URL = "oci://registry.example/test"
	request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), new(repo.RegistryAccess()))
	assert.NoError(t, err)

	expected, err := f.Network.HTTPTransport(source.HTTPTransportConfig{})
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		request.client.Client.(*auth.Client).Client.Transport != expected,
		"%s",
		"transport applied customization without a certificate reference",
	)
}

func TestFetcher_ResolveTLS_Insecure(t *testing.T) {
	t.Parallel()
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) { s.Insecure = true })
	repo.URL = "oci://registry.example/test"
	request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), new(repo.RegistryAccess()))
	assert.NoError(t, err)

	expected, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		request.client.Client.(*auth.Client).Client.Transport != expected,
		"%s",
		"transport did not apply insecure policy",
	)
}

func TestFetcher_ResolveTLS_FromSecret(t *testing.T) {
	t.Parallel()
	certificate := testutil.SelfSignedServerCert(t)

	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{
			StringData: map[string]any{
				"tls.crt": certificate.Cert,
				"tls.key": certificate.Key,
				"ca.crt":  certificate.Cert,
			},
		}
	}}
	f := &Fetcher{Network: newTestNetwork(t)}
	repo := ociRepo("o", func(s *sourcev1.OCIRepositorySpec) {
		s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	})
	repo.URL = "oci://registry.example/test"
	request, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).newRepoClient(
		t.Context(), new(repo.RegistryAccess()),
	)
	assert.NoError(t, err)

	material, err := source.ResolveTransport(
		fInputs.secrets,
		manifest.DiagnosticFor(repo.Named(), repo),
		[]source.TransportInput{{Reference: repo.CertSecretRef, Use: source.TransportCert}},
	)
	assert.NoError(t, err)

	expected, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: material})
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		request.client.Client.(*auth.Client).Client.Transport != expected,
		"%s",
		"transport did not apply the certificate reference",
	)
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_ResolveConfig_CredentialPrecedence(t *testing.T) {
	const secretJSON = `{"auths":{"ghcr.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`
	const explicitJSON = `{"auths":{"ghcr.io":{"username":"explicit","password":"explicit"}}}`
	tests := []struct {
		name, registry, secret, config, user, pass string
	}{
		{
			name:     "Secret wins",
			registry: "ghcr.io",
			secret:   secretJSON,
			config:   explicitJSON,
			user:     "alice",
			pass:     "hunter2",
		},
		{
			name:     "Secret wins malformed fallback",
			registry: "ghcr.io",
			secret:   secretJSON,
			config:   `{"auths":`,
			user:     "alice",
			pass:     "hunter2",
		},
		{
			name:     "explicit wins Docker defaults",
			registry: "ghcr.io",
			config:   explicitJSON,
			user:     "explicit",
			pass:     "explicit",
		},
		{name: "Docker fallback", registry: "ghcr.io", user: "default", pass: "default"},
		{
			name:     "Docker Hub docker.io",
			registry: "docker.io",
			secret:   `{"auths":{"https://index.docker.io/v1/":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`,
			user:     "alice",
			pass:     "hunter2",
		},
		{
			name:     "Docker Hub alias",
			registry: "registry-1.docker.io",
			secret:   `{"auths":{"index.docker.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`,
			user:     "alice",
			pass:     "hunter2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFetcherResolveConfigCredentialPrecedenceCase(
				t,
				&tt,
			)
		},
		)
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
	assert.FatalIf(
		t,
		got != nil || err == nil || !errors.Is(err, manifest.ErrFlux) || strings.Contains(err.Error(), "auths"),
		"malformed machine config admitted acquisition or lost classification: %v",
		err,
	)
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
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
			testFetcherMachineCredentialsCase(t, &tc)
		},
		)
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_HelmConfigAvailability(t *testing.T) {
	for _, mode := range []string{"absent", "open failure", "read failure", "malformed", "default", "override"} {
		t.Run(mode, func(t *testing.T) {
			testFetcherHelmConfigAvailabilityCase(t, &mode)
		},
		)
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_MachineCredentialsAcquisition(t *testing.T) {
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap\n"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	for _, name := range []string{"Helm only", "Docker wins", "unrelated Docker", "wrong Docker is authoritative", "explicit miss", "missing helper", "unreadable Helm", "helper rotation"} {
		t.Run(name, func(t *testing.T) {
			testFetcherMachineCredentialsAcquisitionCase(
				t,
				&upstream, &name,
			)
		},
		)
	}
}

func TestFetcher_MachineFailureCauses(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Basic absence", "bearer absence", "helper absence", "rejected credential", "forbidden", "missing content", "malformed content", "transport", "canceled"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testFetcherMachineFailureCausesCase(t, &name)
		},
		)
	}
}

//nolint:paralleltest // The test replaces the process HTTP transport and restores it afterward.
//nolint:paralleltest // The test replaces the process HTTP transport and restores it afterward.
func TestFetcher_SharedMachineFailure(t *testing.T) {
	previous := http.DefaultTransport
	transport := previous.(*http.Transport).Clone()
	http.DefaultTransport = transport //nolint:reassign // The test replaces the process transport to exercise offline acquisition and restores it afterward.
	t.Cleanup(
		func() {
			http.DefaultTransport = previous //nolint:reassign // The test must restore the process HTTP transport after credential acquisition.
		},
	)
	synctest.Test(t, func(t *testing.T) {
		testFetcherSharedMachineFailureCallback(t, testFetcherSharedMachineFailureCallbackInputs{transport: &transport})
	},
	)
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
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
		{
			name:        "wiped auth",
			material:    map[string]any{".dockerconfigjson": "..PLACEHOLDER_.dockerconfigjson.."},
			wantMissing: true,
		},
		{name: "malformed auth", material: map[string]any{".dockerconfigjson": `{"auths":`}, wantInput: true},
		{
			name:      "malformed encoded auth",
			encoded:   true,
			material:  map[string]any{".dockerconfigjson": "!!!"},
			wantInput: true,
		},
		{name: "missing TLS", cert: true, wantMissing: true},
		{
			name:        "wiped TLS",
			cert:        true,
			material:    map[string]any{"ca.crt": "..PLACEHOLDER_ca.crt.."},
			wantMissing: true,
		},
		{name: "malformed TLS", cert: true, material: map[string]any{"ca.crt": "invalid PEM"}},
		{name: "missing proxy", proxy: true, wantMissing: true},
		{
			name:        "wiped proxy",
			proxy:       true,
			material:    map[string]any{"address": "..PLACEHOLDER_address.."},
			wantMissing: true,
		},
		{name: "malformed proxy", proxy: true, material: map[string]any{"address": "%"}},
		{name: "canceled", canceled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFetcherSecretCredentialsAcquisitionPolicyCase(
				t,
				&tt,
			)
		},
		)
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
	assert.FatalIf(t, err == nil, "expected missing-secret error")
	assert.ErrorIf(
		t,
		!errors.Is(err, manifest.ErrMissingSecret),
		"want ErrMissingSecret from the generic path; got %v",
		err,
	)
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
		{
			name: "wiped, Helm fallback",
			secret: &manifest.Secret{
				StringData: map[string]any{".dockerconfigjson": "..PLACEHOLDER_.dockerconfigjson.."},
			},
			helm:      covering,
			wantRetry: true,
		},
		{
			name:    "Helm unrelated host",
			helm:    `{"auths":{"quay.io":{"username":"other","password":"other"}}}`,
			wantErr: manifest.ErrMissingSecret,
		},
		{name: "Helm helper only", helm: `{"credsStore":"unused"}`, wantErr: manifest.ErrMissingSecret},
		{name: "Docker helper only", docker: `{"credsStore":"unused"}`, wantErr: manifest.ErrMissingSecret},
		{name: "malformed Helm deferred", helm: `{`},
		{
			name: "placeholder-wiped, fallback",
			secret: &manifest.Secret{
				StringData: map[string]any{".dockerconfigjson": "..PLACEHOLDER_.dockerconfigjson.."},
			},
			config:    covering,
			wantRetry: true,
		},
		{
			name:    "config covers another registry",
			config:  `{"auths":{"quay.io":{"auth":"YWxpY2U6aHVudGVyMg=="}}}`,
			wantErr: manifest.ErrMissingSecret,
		},
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

			_, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).fetchSource(
				t.Context(),
				repo,
				sourcecache.Policy{},
			)
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(
				t,
				!ok || missing.RetryAmbient == nil || missing.Secret.Name != "creds",
				"unavailable auth lost lazy continuation: %v",
				err,
			)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			{
				_, err := missing.RetryMachine(ctx, nil)
				assert.FatalIf(t, !errors.Is(err, context.Canceled), "canceled continuation ran preparation: %v", err)
			}
		})
	}
}

type testFetcherSharedMachineFailureCallbackInputs struct {
	transport **http.Transport
}

func testFetcherSharedMachineFailureCallback(
	t *testing.T,
	testCaseInput testFetcherSharedMachineFailureCallbackInputs,
) {
	t.Helper()
	var requests atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	(*testCaseInput.transport).
		DialTLSContext = func(context.Context, string, string) (net.Conn, error) {
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
				_, _ = io.WriteString(
					server,
					"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
				)
				return
			}
			close(entered)
			<-release
			_, _ = io.WriteString(
				server,
				"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"fixture\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
			)
		}()
		return client, nil
	}
	path := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, path, `{}`)
	op := &operation{
		Fetcher: &Fetcher{
			Network:        newTestNetwork(t),
			Cache:          cachetest.New(t, t.TempDir(), sourcecache.New),
			RegistryConfig: path,
		},
	}
	repo := ociRepo("shared", func(s *sourcev1.OCIRepositorySpec) { s.URL = "oci://registry.invalid/app" })
	for range 2 {
		_, err := op.fetchSource(t.Context(), repo, sourcecache.Policy{})
		response, ok := errors.AsType[*fetcherr.ResponseError](err)
		if !ok || response.StatusCode != http.StatusServiceUnavailable ||
			strings.Contains(err.Error(), "credential helper") ||
			requests.Load() != 1 {
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
		assert.FatalIf(
			t,
			!errors.Is(err, auth.ErrBasicCredentialNotFound) || !errors.Is(err, manifest.ErrFlux),
			"shared error lost chain or remedies: %v",
			err,
		)
		for _, text := range []string{"no login entry for registry.invalid", "Docker config", "Helm's registry file", "credential helpers are not run", "plain auths entry"} {
			assert.ErrorIf(
				t,
				strings.Contains(err.Error(), text),
				"shared error retained obsolete advice %q: %v",
				text,
				err,
			)
		}
		for _, helper := range []string{"first", "second"} {
			assert.ErrorIf(
				t,
				strings.Contains(err.Error(), helper),
				"shared error named helper %q: %v",
				helper,
				err,
			)
		}
	}
	if requests.Load() != 2 {
		t.Fatalf("shared acquisition duplicated work: requests=%d", requests.Load())
	}
}
