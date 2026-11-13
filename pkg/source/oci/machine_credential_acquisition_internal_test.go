package oci

import (
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

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"oras.land/oras-go/v2/registry/remote/auth"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

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
			secrets: func(string, string) store.ReferenceFacts {
				secretInput := &manifest.Secret{StringData: map[string]any{dockerConfigJSONKey: tc.docker}}

				return store.ReferenceFacts{Data: secretInput, Original: secretInput}
			},
		}
		selected, err = op.resolveRegistryConfig(t.Context(), new(repo.RegistryAccess()), "ghcr.io")
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
		inputs.secrets = func(string, string) store.ReferenceFacts {
			secretInput := &manifest.Secret{StringData: map[string]any{
				"address":  proxyAddress,
				"username": "repository-proxy-user",
				"password": "repository-proxy-password",
				"ca.crt":   ca,
			}}

			return store.ReferenceFacts{Data: secretInput, Original: secretInput}
		}
	}
	got, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		repo,
		inputs.readSet(repo),
		&sourcecache.Policy{MaxAge: time.Hour},
		nil,
	)
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
		warm, err := fetchtest.Fetch(
			t.Context(),
			f.Fetch,
			repo,
			inputs.readSet(repo),
			&sourcecache.Policy{MaxAge: time.Hour},
			nil,
		)
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			warm.Key != got.Key || accepted.Load() != before,
			"%s",
			"unchanged helper credential missed warm generation",
		)

		expectedPassword.Store("rotated")
		t.Setenv("FLATE_HELPER_SECRET", "rotated")
		rotated, err := fetchtest.Fetch(
			t.Context(),
			f.Fetch,
			repo,
			inputs.readSet(repo),
			&sourcecache.Policy{MaxAge: time.Hour},
			nil,
		)
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

type testFetcherInputsMachineCredentialsReuseCallbackInputs struct {
	upstream **httptest.Server
	password *atomic.Value
	lists    *atomic.Int32
	heads    *atomic.Int32
}

func testFetcherInputsMachineCredentialsReuseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherInputsMachineCredentialsReuseCallbackInputs,
) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != "fixture" || pass != testCaseInput.password.Load().(string) {
		w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/tags/list") {
		testCaseInput.lists.
			Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"fixture","tags":["1.0.0"]}`))
		return
	}
	if r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
		testCaseInput.heads.
			Add(1)
	}
	(*testCaseInput.upstream).
		Config.Handler.ServeHTTP(w, r)
}
