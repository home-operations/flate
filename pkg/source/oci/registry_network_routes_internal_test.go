package oci

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_NetworkRoutes(t *testing.T) {
	for _, route := range []string{"registry", "token", "redirect", "tags", "manifest", "layer", "copy", "fallback"} {
		t.Run(
			route,
			func(t *testing.T) {
				testFetcherNetworkRoutesCase(t, &route)
			},
		)
	}
}

func testFetcherNetworkRoutesCase(t *testing.T, route *string,
) {
	t.Helper()
	var network source.Network
	var bypass, requests atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		bypass.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(target.Close)
	testutil.StubDNS(t, func(context.Context, string) error { return os.ErrDeadlineExceeded })
	tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
	assert.NoError(t, err)

	req, _ := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		strings.Replace(target.URL, "127.0.0.1", "example.com", 1),
		nil,
	)
	_, err = (&http.Client{Transport: tr}).Do(req)
	first, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !first.Attempted, "seed: %v", err)
	cause := first.Err
	art := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap\n"})
	registry := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherNetworkRoutesCaseCallback(
			w,
			r,
			testFetcherNetworkRoutesCaseCallbackInputs{route: &route, requests: &requests, target: &target, art: &art},
		)
	},
	))
	t.Cleanup(registry.Close)
	registryURL := registry.URL
	if (*route) == "registry" || (*route) == "fallback" {
		registryURL = strings.Replace(target.URL, "127.0.0.1", "example.com", 1)
	}
	repo := &manifest.OCIRepository{
		Name:      "network",
		Namespace: "test",
		URL:       "oci://" + mustURL(t, registryURL).Host + "/test/chart",
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "v1"},
	}
	if (*route) == "tags" {
		repo.Reference = &sourcev1.OCIRepositoryRef{SemVer: ">=1"}
	}
	if (*route) == "copy" {
		repo.LayerSelector = &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}
	}
	fInputs := testInputs{}
	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: &network}
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}
	if (*route) == "fallback" {
		repo.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
		fInputs.secrets = func(_, _ string) *manifest.Secret { return nil }
		f.RegistryConfig = filepath.Join(t.TempDir(), "config.json")
		testutil.WriteFileAt(
			t,
			f.RegistryConfig,
			fmt.Sprintf(`{"auths":{%q:{"auth":"dXNlcjpwYXNz"}}}`, mustURL(t, registryURL).Host),
		)
		_, err = f.Fetch(
			t.Context(),
			repo,
			fInputs.readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval(), Retry: retry},
		)
		missing, innerOk := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !innerOk || missing.RetryAmbient == nil, "fallback: %v", err)
		_, err = missing.RetryAmbient(t.Context())
	} else {
		_, err = f.Fetch(
			t.Context(),
			repo,
			fInputs.readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval(), Retry: retry},
		)
	}
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	if !ok || terminal.Attempted || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux) ||
		bypass.Load() != 0 {
		t.Fatalf("route=%s error=%v terminal=%+v bypass=%d", (*route), err, terminal, bypass.Load())
	}
	want := int32(1)
	if (*route) == "layer" || (*route) == "copy" {
		want = 3
	}
	if (*route) == "manifest" {
		want = 2
	}
	if (*route) == "registry" || (*route) == "fallback" {
		want = 0
	}
	{
		got := requests.Load()
		assert.FatalIf(t, got != want, "registry requests=%d want=%d", got, want)
	}
}

type testFetcherNetworkRoutesCaseCallbackInputs struct {
	route    **string
	requests *atomic.Int32
	target   **httptest.Server
	art      *fakeOCIArtifact
}

func testFetcherNetworkRoutesCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherNetworkRoutesCaseCallbackInputs,
) {
	testCaseInput.requests.
		Add(1)
	switch {
	case (*(*testCaseInput.route)) == "token":
		w.Header().
			Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="registry"`, strings.Replace((*testCaseInput.target).URL, "127.0.0.1", "example.com", 1)))
		w.WriteHeader(http.StatusUnauthorized)
	case (*(*testCaseInput.route)) == "redirect", (*(*testCaseInput.route)) ==
		"tags" && strings.HasSuffix(r.URL.Path, "/tags/list"), (*(*testCaseInput.route)) ==
		"manifest" && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/manifests/"), strings.Contains(r.URL.Path, "/blobs/"):
		http.Redirect(
			w,
			r,
			strings.Replace((*testCaseInput.target).URL, "127.0.0.1", "example.com", 1),
			http.StatusTemporaryRedirect,
		)
	default:
		w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
		w.Header().Set("Docker-Content-Digest", testCaseInput.art.manifestDigest)
		w.Header().Set("Content-Length", strconv.Itoa(len(testCaseInput.art.manifest)))
		if r.Method != http.MethodHead {
			_, _ = w.Write(testCaseInput.art.manifest)
		}
	}
}
