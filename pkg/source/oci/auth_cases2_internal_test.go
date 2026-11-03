package oci

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetcherResolveConfigCredentialPrecedenceCase(
	t *testing.T, tt *struct {
		name     string
		registry string
		secret   string
		config   string
		user     string
		pass     string
	},
) {
	t.Helper()

	dockerDir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dockerDir)

	t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	testutil.WriteFile(
		t,
		dockerDir,
		"config.json",
		`{"auths":{"ghcr.io":{"username":"default","password":"default"}}}`,
	)
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
		selected, err = newTestNetwork(
			t,
		).RegistryCredentials(t.Context(), f.RegistryConfig, tt.registry)
		assert.NoError(t, err)
	}
	request, err := op.newRepoClient(t.Context(), repo.RegistryAccess())
	if err == nil && selected != nil {
		err = op.configure(repo.RegistryAccess(), &request, *selected)
	}
	assert.NoError(t, err)

	got, err := request.client.Client.(*auth.Client).Credential(t.Context(), request.client.Reference.Host())
	want := auth.Credential{Username: tt.user, Password: tt.pass}
	assert.FatalIf(t, err != nil || got != want, "credentials = %+v, %v; want %+v", got, err, want)
	assert.FatalIf(t, request.material == nil, "%s", "authenticated acquisition has anonymous identity")

	if tt.secret != "" {
		op.secrets = func(_, _ string) *manifest.Secret {
			config := fmt.Sprintf(
				`{"auths":{%q:{"username":"rotated","password":"rotated"}}}`,
				credentials.ServerAddressFromRegistry(tt.registry),
			)
			return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": config}}
		}
		captured, err := request.client.Client.(*auth.Client).Credential(
			t.Context(),
			request.client.Reference.Host(),
		)
		assert.FatalIf(t, err != nil || captured != want, "%s", "producer reread mutable credentials")

		rotated, err := op.newRepoClient(t.Context(), repo.RegistryAccess())
		assert.FatalIf(
			t,
			err != nil || cmp.Equal(request.material, rotated.material),
			"%s",
			"credential rotation reused identity",
		)
	}
}

func testFetcherMachineCredentialsCase(t *testing.T, tc *struct {
	name     string
	docker   string
	helm     string
	explicit string
	helper   string
	user     string
	fail     bool
	calls    int
},
) {
	t.Helper()
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
		op := &operation{
			Fetcher: &Fetcher{Network: newTestNetwork(t)},
			secrets: func(string, string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{dockerConfigJSONKey: tc.docker}}
			},
		}
		selected, err = op.resolveRegistryConfig(t.Context(), repo.RegistryAccess(), "ghcr.io")
	}
	assert.FatalIf(t, (err != nil) != tc.fail, "native lookup: %v", err)
	if !tc.fail {
		assert.Equal(
			t,
			*selected,
			auth.Credential{Username: tc.user, Password: tc.user},
		)
	}
	data, readErr := os.ReadFile(marker)
	if tc.calls == 0 {
		assert.FatalIf(
			t,
			!errors.Is(readErr, os.ErrNotExist),
			"unexpected helper invocation: %q %v",
			data,
			readErr,
		)
	} else {
		assert.FatalIf(t, readErr != nil || string(data) != "called", "helper calls: %q %v", data, readErr)
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
}

func testFetcherHelmConfigAvailabilityCase(t *testing.T, mode *string,
) {
	t.Helper()
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	home := t.TempDir()
	t.Setenv("HELM_CONFIG_HOME", home)
	t.Setenv("HELM_REGISTRY_CONFIG", "")
	path := filepath.Join(home, "registry", "config.json")
	wantUser := ""
	switch *mode {
	case "open failure":
		testutil.WriteFile(t, home, "registry", "regular file")
	case "read failure":
		assert.NoError(t, os.MkdirAll(path, 0o755))
	case "malformed":
		testutil.WriteFileAt(t, path, `{`)
	case "default", "override":
		wantUser = "default"
		testutil.WriteFileAt(
			t,
			path,
			`{"auths":{"https://index.docker.io/v1/":{"username":"default","password":"default"}}}`,
		)
		if (*mode) == "override" {
			wantUser = "override"
			path = filepath.Join(t.TempDir(), "config.json")
			t.Setenv("HELM_REGISTRY_CONFIG", path)
			testutil.WriteFileAt(
				t,
				path,
				`{"auths":{"https://index.docker.io/v1/":{"username":"override","password":"override"}}}`,
			)
		}
	}
	selected, err := newTestNetwork(t).RegistryCredentials(t.Context(), "", "registry-1.docker.io")
	if (*mode) == "malformed" || (*mode) == "open failure" ||
		(*mode) == "read failure" {
		assert.FatalIf(t, err == nil, "readable malformed Helm config = %v", err)
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, *selected, auth.Credential{Username: wantUser, Password: wantUser})
}

func testFetcherMachineCredentialsAcquisitionCase(
	t *testing.T, upstream **httptest.Server,
	name *string,
) {
	t.Helper()
	var accepted atomic.Int32
	var expectedPassword atomic.Value
	expectedPassword.Store("helm")
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherMachineCredentialsAcquisitionCaseCallback(
			w,
			r,
			testFetcherMachineCredentialsAcquisitionCaseCallbackInputs{
				upstream:         &upstream,
				name:             &name,
				accepted:         &accepted,
				expectedPassword: &expectedPassword,
			},
		)
	},
	))
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
	switch *name {
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
	if (*name) == "helper rotation" {
		bin := t.TempDir()
		t.Setenv("PATH", bin)
		t.Setenv("FLATE_HELPER_MARKER", marker)
		t.Setenv("FLATE_HELPER_SECRET", "helm")
		program := filepath.Join(bin, "docker-credential-fixture")
		testutil.WriteFileAt(
			t,
			program,
			"#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$FLATE_HELPER_MARKER.args\"\n/bin/cat > \"$FLATE_HELPER_MARKER.stdin\"\n/usr/bin/env > \"$FLATE_HELPER_MARKER.env\"\nprintf called >> \"$FLATE_HELPER_MARKER\"\nprintf '{\"Username\":\"helm\",\"Secret\":\"%s\"}' \"$FLATE_HELPER_SECRET\"\n",
		)
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
	if (*name) == "helper rotation" {
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			testFetcherMachineCredentialsAcquisitionCaseCallback2(
				w,
				r,
				testFetcherMachineCredentialsAcquisitionCaseCallback2Inputs{t: &t, host: &host, proxied: &proxied},
			)
		},
		))
		t.Cleanup(proxy.Close)
		proxyAddress = proxy.URL
		ca = testutil.SelfSignedCA(t)
		repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
		repo.CertSecretRef = repo.ProxySecretRef
		inputs.secrets = func(string, string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{
				"address":  proxyAddress,
				"username": "repository-proxy-user",
				"password": "repository-proxy-password",
				"ca.crt":   ca,
			}}
		}
	}
	got, err := f.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{MaxAge: time.Hour})
	if (*name) == "wrong Docker is authoritative" || (*name) == "explicit miss" ||
		(*name) == "missing helper" ||
		(*name) ==
			"unreadable Helm" {
		if err == nil || got != nil || accepted.Load() != 0 {
			t.Fatalf("unexpected fallback: artifact=%v error=%v accepted=%d", got, err, accepted.Load())
		}
		assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux), "machine failure lost domain: %v", err)
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, mustReadFile(t, got.FS, "app.yaml"), "kind: ConfigMap\n")
	assert.FatalIf(t, accepted.Load() == 0, "%s", "acquisition did not reach registry")

	if (*name) == "helper rotation" {
		before := accepted.Load()
		warm, err := f.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{MaxAge: time.Hour})
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			warm.Key != got.Key || accepted.Load() != before,
			"%s",
			"unchanged helper credential missed warm generation",
		)

		expectedPassword.Store("rotated")
		t.Setenv("FLATE_HELPER_SECRET", "rotated")
		rotated, err := f.Fetch(t.Context(), repo, inputs.readSet(repo), sourcecache.Policy{MaxAge: time.Hour})
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			rotated.Key == got.Key || accepted.Load() == before,
			"%s",
			"rotated helper credential reused identity",
		)

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
		assert.FatalIf(
			t,
			!strings.Contains(string(environment), "FLATE_HELPER_SECRET=rotated") || proxied.Load() == 0,
			"%s",
			"helper lost the machine environment or source proxy",
		)

		for _, canary := range []string{proxyAddress, "repository-proxy-user", "repository-proxy-password", ca, "-----BEGIN CERTIFICATE-----"} {
			assert.FatalIf(
				t,
				strings.Contains(string(args)+string(stdin)+string(environment), canary),
				"%s",
				"repository transport material reached registry helper inputs",
			)
		}
	}
}

func testFetcherMachineFailureCausesCase(t *testing.T, name *string,
) {
	t.Helper()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherMachineFailureCausesCaseCallback(
			w,
			r,
			testFetcherMachineFailureCausesCaseCallbackInputs{t: &t, name: &name},
		)
	},
	))
	t.Cleanup(srv.Close)
	path := filepath.Join(t.TempDir(), "config.json")
	config := `{}`
	if (*name) == "helper absence" {
		config = `{"credsStore":"fixture-helper"}`
	}
	if (*name) == "rejected credential" {
		config = fmt.Sprintf(
			`{"auths":{%q:{"username":"fixture-user","password":"private-password"}}}`,
			mustURL(t, srv.URL).Host,
		)
	}
	testutil.WriteFileAt(t, path, config)
	repo := ociRepo("advice", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://" + mustURL(t, srv.URL).Host + "/app"
		s.Insecure = true
	})
	op := &operation{
		Fetcher: &Fetcher{
			Network:        newTestNetwork(t),
			Cache:          cachetest.New(t, "", sourcecache.New),
			RegistryConfig: path,
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	policy := sourcecache.Policy{}
	if (*name) == "canceled" {
		policy.Admit = func(context.Context) (func(), error) {
			cancel()
			return func() {}, nil
		}
	}
	artifact, err := op.fetchSource(ctx, repo, policy)
	assert.FatalIf(
		t,
		err == nil || artifact != nil || !errors.Is(err, manifest.ErrFlux),
		"failure lost source classification: %v",
		err,
	)
	wantAdvice := false
	for _, text := range []string{"no login entry for " + mustURL(t, srv.URL).Host, "Docker config", "Helm's registry file", "credential helpers are not run", "--registry-config", "plain auths entry", "supplied Secret"} {
		assert.ErrorIf(t, strings.Contains(err.Error(), text) != wantAdvice, "advice %q: %v", text, err)
	}
	assert.ErrorIf(
		t,
		strings.Contains(err.Error(), "OCIRepository ns/advice"),
		"source identity repeated: %v",
		err,
	)
	assert.ErrorIf(
		t, (*name) ==
			"Basic absence" && !errors.Is(err, auth.ErrBasicCredentialNotFound), "Basic absence sentinel lost: %v",
		err,
	)
	if (*name) == "bearer absence" || (*name) == "rejected credential" ||
		(*name) == "forbidden" {
		response, ok := errors.AsType[*errcode.ErrorResponse](err)
		assert.ErrorIf(t, !ok || response.StatusCode == 0, "ORAS response chain lost: %v", err)
		{
			_, ok := errors.AsType[*fetcherr.ResponseError](err)
			assert.ErrorIf(t, !ok, "neutral response chain lost: %v", err)
		}
	}
	assert.ErrorIf(
		t, (*name) ==
			"missing content" && !errors.Is(err, errdef.ErrNotFound), "missing content sentinel lost: %v",
		err,
	)
	assert.ErrorIf(
		t,
		(*name) == "canceled" && !errors.Is(err, context.Canceled),
		"cancellation lost: %v",
		err,
	)
	for _, secret := range []string{"fixture-helper", "fixture-user", "private-password", base64.StdEncoding.EncodeToString([]byte("fixture-user:private-password"))} {
		assert.ErrorIf(t, strings.Contains(err.Error(), secret), "credential leaked in error")
	}
}

func testFetcherSecretCredentialsAcquisitionPolicyCase(
	t *testing.T, tt *struct {
		name        string
		cert        bool
		proxy       bool
		encoded     bool
		material    map[string]any
		wantMissing bool
		wantInput   bool
		canceled    bool
	},
) {
	t.Helper()
	var requests atomic.Int64
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	cache := cachetest.New(t, "", sourcecache.New)
	fInputs := testInputs{
		secrets: func(callbackArg1, name string) *manifest.Secret {
			return testFetcherSecretCredentialsAcquisitionPolicyCaseCallback(
				callbackArg1,
				name,
				testFetcherSecretCredentialsAcquisitionPolicyCaseCallbackInputs{tt: &tt},
			)
		},
	}
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
	if _, err := f.Fetch(
		ctx,
		repo,
		fInputs.readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	); err == nil || errors.Is(err, manifest.ErrMissingSecret) != tt.wantMissing ||
		errors.Is(err, context.Canceled) != tt.canceled {
		t.Fatalf(
			"Fetch error = %v, want missing=%t canceled=%t",
			err, tt.
				wantMissing, tt.
				canceled,
		)
	} else {
		assert.FatalIf(t, tt.wantInput &&
			!errors.Is(
				err,
				manifest.ErrInput,
			), "Fetch error = %v, want malformed material to be an input error", err)
	}
	{
		got := requests.Load()
		assert.FatalIf(t, got != 0, "registry requests = %d, want 0", got)
	}
}

type testFetcherResolveTLSSnapshotRotationCaseInputs struct {
	fail          *atomic.Bool
	requests      *atomic.Int32
	lookups       *atomic.Int32
	password      *atomic.Value
	srv           **httptest.Server
	proxyPassword *atomic.Value
	proxy         **httptest.Server
	repo          **manifest.OCIRepository
	root          *string
	ca            *string
	previous      *string
	wave          *string
}
type testFetcherMachineCredentialsAcquisitionCaseCallbackInputs struct {
	upstream         ***httptest.Server
	name             **string
	accepted         *atomic.Int32
	expectedPassword *atomic.Value
}

func testFetcherMachineCredentialsAcquisitionCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherMachineCredentialsAcquisitionCaseCallbackInputs,
) {
	if (*(*testCaseInput.name)) != "missing helper" && (*(*testCaseInput.name)) != "unreadable Helm" {
		user, password, ok := r.BasicAuth()
		want := "helm"
		if (*(*testCaseInput.name)) == "Docker wins" {
			want = "docker"
		}
		wantPassword := want
		if (*(*testCaseInput.name)) == "helper rotation" {
			wantPassword = testCaseInput.expectedPassword.Load().(string)
		}
		if !ok || user != want || password != wantPassword {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	testCaseInput.accepted.
		Add(1)
	(*(*testCaseInput.upstream)).
		Config.Handler.ServeHTTP(w, r)
}

type testFetcherMachineCredentialsAcquisitionCaseCallback2Inputs struct {
	t       **testing.T
	host    *string
	proxied *atomic.Int32
}

func testFetcherMachineCredentialsAcquisitionCaseCallback2(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherMachineCredentialsAcquisitionCaseCallback2Inputs,
) {
	if r.Method != http.MethodConnect || r.Host != (*testCaseInput.host) {
		http.Error(w, "unexpected proxy destination", http.StatusBadGateway)
		return
	}
	testCaseInput.proxied.
		Add(1)
	remote, err := (&net.Dialer{}).DialContext(r.Context(), "tcp", (*testCaseInput.host))
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	defer remote.Close()
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		return
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
	go func() { _, _ = io.Copy(remote, conn); _ = remote.Close() }()
	_, _ = io.Copy(conn, remote)
}

type testFetcherMachineFailureCausesCaseCallbackInputs struct {
	t    **testing.T
	name **string
}

func testFetcherMachineFailureCausesCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherMachineFailureCausesCaseCallbackInputs,
) {
	switch *(*testCaseInput.name) {
	case "Basic absence", "helper absence", "rejected credential":
		w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
		w.WriteHeader(http.StatusUnauthorized)
	case "bearer absence":
		if r.URL.Path != "/token" {
			w.Header().
				Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q,service="registry",scope="repository:app:pull"`, "https://"+r.Host+"/token"))
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
			(*testCaseInput.t).
				Error(err)
			return
		}
		_ = conn.Close()
	}
}

type testFetcherSecretCredentialsAcquisitionPolicyCaseCallbackInputs struct {
	tt **struct {
		name string
		cert bool

		proxy       bool
		encoded     bool
		material    map[string]any
		wantMissing bool
		wantInput   bool
		canceled    bool
	}
}

func testFetcherSecretCredentialsAcquisitionPolicyCaseCallback(
	_, name string,
	testCaseInput testFetcherSecretCredentialsAcquisitionPolicyCaseCallbackInputs,
) *manifest.Secret {
	if name == "creds" &&
		((*testCaseInput.tt).cert || (*testCaseInput.tt).proxy || (*testCaseInput.tt).canceled) {
		return &manifest.Secret{
			StringData: map[string]any{
				".dockerconfigjson": `{"auths":{"ghcr.io":{"username":"alice","password":"hunter2"}}}`,
			},
		}
	}
	if (*testCaseInput.tt).material != nil {
		if (*testCaseInput.tt).encoded {
			return &manifest.Secret{Data: (*testCaseInput.tt).material}
		}
		return &manifest.Secret{StringData: (*testCaseInput.tt).material}
	}
	return nil
}
