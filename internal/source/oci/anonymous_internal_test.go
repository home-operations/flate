package oci

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // Credential configuration and helper lookup use the process environment.
func TestFetcher_AnonymousPreparation(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, "{}")
	fixture := newFakeOCIArtifact(t, map[string]string{"public.yaml": "public"})
	upstream := startFakeRegistry(t, fixture.manifest, fixture.config, fixture.layer)
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	for _, authored := range []bool{false, true} {
		t.Run(strconv.FormatBool(authored), func(t *testing.T) {
			repo := &manifest.OCIRepository{
				Name:      "source",
				Namespace: "apps",
				URL:       "oci://" + mustURL(t, server.URL).Host + "/fixture",
				Insecure:  true,
			}
			if authored {
				repo.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
			}
			reads := (testInputs{}).readSet(repo)
			defer reads.Close()
			f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, "", cache.New), RegistryConfig: config}
			before := requests.Load()
			prepared, err := f.Fetch(t.Context(), repo, reads, &cache.Policy{})
			var missing *source.MissingSecretError
			if authored {
				var ok bool
				missing, ok = errors.AsType[*source.MissingSecretError](err)
				assert.FatalIf(t, !ok, "missing credentials: %v", err)
				prepared, err = missing.RetryMachine(t.Context(), reads)
			}
			assert.NoError(t, err)
			assert.Equal(t, prepared.LocalUnavailable, missing)
			assert.Equal(t, requests.Load(), before)
			artifact, err := prepared.Fetch(t.Context(), nil)
			assert.NoError(t, err)
			assert.Equal(t, mustReadFile(t, artifact.FS, "public.yaml"), "public")
			assert.Equal(t, requests.Load() > before, true)
		})
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_ProviderAnonymous(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	for _, private := range []bool{false, true} {
		t.Run(strconv.FormatBool(private), func(t *testing.T) {
			testFetcherProviderAnonymousCase(t, &private)
		},
		)
	}
}

func testFetcherProviderAnonymousCase(t *testing.T, private *bool,
) {
	t.Helper()
	artifact := newFakeOCIArtifact(t, map[string]string{"public.yaml": "public"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	var challenges, tokens, manifests atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherProviderAnonymousCaseCallback(
			w,
			r,
			testFetcherProviderAnonymousCaseCallbackInputs{
				t:          &t,
				private:    &private,
				upstream:   &upstream,
				challenges: &challenges,
				tokens:     &tokens,
				manifests:  &manifests,
			},
		)
	},
	))
	t.Cleanup(srv.Close)
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, "{}")
	repo := &manifest.OCIRepository{
		Name:      "public",
		Namespace: "apps",
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/fixture",
		Provider:  "aws",
		Insecure:  true,
	}
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, "", cache.New), RegistryConfig: config}
	got, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		repo,
		(testInputs{}).readSet(repo),
		&cache.Policy{Retry: cache.RetryConfig{Attempts: 3}},
		nil,
	)
	if challenges.Load() != 1 || tokens.Load() != 1 {
		t.Fatalf("challenge sequences=%d token exchanges=%d: %v", challenges.Load(), tokens.Load(), err)
	}
	if *private {
		assert.FatalIf(t, got != nil || !errors.Is(err, manifest.ErrMissingSecret) || manifests.Load() != 0 ||
			!strings.Contains(
				err.Error(),
				"machine credential acquisition failed",
			), "private acquisition=%v: %v", got, err)
		return
	}
	if err != nil || got == nil || manifests.Load() != 1 {
		t.Fatalf("public acquisition: manifests=%d: %v", manifests.Load(), err)
	}
	data, err := fs.ReadFile(got.FS, "public.yaml")
	assert.FatalIf(
		t,
		err != nil || string(data) != "public" || len(got.Warnings) != 0,
		"public content=%q: %v",
		data,
		err,
	)
}

type testFetcherProviderAnonymousCaseCallbackInputs struct {
	t          **testing.T
	private    **bool
	upstream   **httptest.Server
	challenges *atomic.Int32
	tokens     *atomic.Int32
	manifests  *atomic.Int32
}

func testFetcherProviderAnonymousCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherProviderAnonymousCaseCallbackInputs,
) {
	if r.Header.Get("Authorization") != "" && r.Header.Get("Authorization") != "Bearer public" {
		(*testCaseInput.t).
			Error("anonymous acquisition sent credentials")
	}
	if r.URL.Path == "/token" {
		testCaseInput.tokens.
			Add(1)
		if r.URL.Query().Get("account") != "" {
			(*testCaseInput.t).
				Error("anonymous token request had an account")
		}
		if *(*testCaseInput.private) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"token":"public"}`))
		return
	}
	if r.Header.Get("Authorization") == "" {
		testCaseInput.challenges.
			Add(1)
		w.Header().
			Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="https://%s/token",service="fixture",scope="repository:fixture:pull"`, r.Host))
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if strings.Contains(r.URL.Path, "/manifests/") && r.Method == http.MethodHead {
		testCaseInput.manifests.
			Add(1)
	}
	(*testCaseInput.upstream).
		Config.Handler.ServeHTTP(w, r)
}
