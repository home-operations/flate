package oci

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetcherLayerDigestMismatchPublishesNothingCase(
	t *testing.T, backing *string,
	corruption *string,
) {
	t.Helper()

	honest := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	broken := honest
	payload := testutil.TarGz(
		t,
		map[string]string{"app.yaml": "version: v2\n"},
		0o644,
		gzip.DefaultCompression,
	)
	if (*corruption) != "digest" {
		if (*corruption) == "gzip header" {
			payload = []byte{0x1f, 0x8b}
		} else {
			payload = honest.layer[:len(honest.layer)-4]
		}
		broken.layerDigest = sha256Digest(payload)
		broken.manifest = mustManifestJSON(t,
			manifestBlob{data: honest.config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
			manifestBlob{data: payload, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
		)
		broken.manifestDigest = sha256Digest(broken.manifest)
	} else {
		assert.FatalIf(t, len(payload) != len(honest.layer), "%s", "fixture lengths differ")
	}

	var repaired atomic.Bool
	var layers atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherLayerDigestMismatchPublishesNothingCaseCallback(
			w,
			r,
			testFetcherLayerDigestMismatchPublishesNothingCaseCallbackInputs{
				honest:   &honest,
				broken:   &broken,
				payload:  &payload,
				repaired: &repaired,
				layers:   &layers,
			},
		)
	},
	))
	t.Cleanup(srv.Close)
	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = c.Close() })
	f := &Fetcher{Network: newTestNetwork(t), Cache: c}
	repo := &manifest.OCIRepository{
		Name:      "tampered",
		Namespace: "test",
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/chart",
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{
			Tag: "latest",
		},
		Interval: &metav1.Duration{Duration: time.Hour},
	}
	for attempt := range 3 {
		if attempt == 2 && (*backing) == "disk" {
			{
				closeErr2 := c.Close()
				assert.NoError(t, closeErr2)
			}

			c, err = sourcecache.New(t.Context(), root)
			assert.NoError(t, err)

			f.Cache = c
		}
		_, fetchErr := f.Fetch(
			t.Context(),
			repo,
			(testInputs{}).readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval()},
		)
		terminal, _ := errors.AsType[*sourcecache.TerminalError](fetchErr)
		if fetchErr == nil || terminal != nil && !terminal.Attempted || layers.Load() != int32(attempt+1) {
			t.Fatalf(
				"integrity failure suppressed: layers=%d terminal=%+v error=%v",
				layers.Load(),
				terminal,
				fetchErr,
			)
		}
		assert.FatalIf(
			t, (*corruption) !=
				"digest" && !errors.Is(fetchErr, io.ErrUnexpectedEOF), "lost gzip cause: %v",
			fetchErr,
		)
	}
	repaired.Store(true)
	art, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	assert.NoError(t, err)

	{
		got := mustReadFile(t, art.FS, "app.yaml")
		assert.FatalIf(t, got != "version: v1\n", "published %q", got)
	}
}

func testFetcherFailureSourceAndOperationCase(
	t *testing.T, layerOperation *string,
	tc *struct {
		stage    string
		status   int
		attempts int32
		replay   bool
	},
	stage *string,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	art := newFakeOCIArtifact(t, map[string]string{"file": "x"})
	var requests, failures atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherFailureSourceAndOperationCaseCallback(
			w,
			r,
			testFetcherFailureSourceAndOperationCaseCallbackInputs{
				tc:       &tc,
				stage:    &stage,
				art:      &art,
				requests: &requests,
				failures: &failures,
			},
		)
	},
	))
	t.Cleanup(srv.Close)
	repo := &manifest.OCIRepository{
		Name: "failure", Namespace: "test", URL: "oci://" + mustURL(t, srv.URL).Host + "/chart",
		Insecure: true, Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
		LayerSelector: &sourcev1.OCILayerSelector{Operation: (*layerOperation)},
	}
	if (*stage) == "list tags" {
		repo.Reference = &sourcev1.OCIRepositoryRef{SemVer: ">=1"}
	}
	root := t.TempDir()
	c, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	f := &Fetcher{Network: newTestNetwork(t), Cache: c}
	t.Cleanup(func() {
		if closeErr3 := f.Cache.Close(); closeErr3 != nil {
			t.Error(closeErr3)
		}
	})
	request, err := (&operation{Fetcher: f}).newRepoClient(ctx, repo.RegistryAccess())
	assert.NoError(t, err)

	var original error
	if strings.HasSuffix((*stage), "body") || (*stage) == "layer header" {
		response := &errcode.ErrorResponse{
			Method:     http.MethodGet,
			URL:        mustURL(t, srv.URL+"/body"),
			StatusCode: tc.status,
		}
		original = response
		if (*stage) == "neutral body" {
			original = &fetcherr.ResponseError{
				StatusCode: response.StatusCode,
				Op:         response.Method,
				Err:        response,
			}
		}
		if (*stage) == "canceled body" {
			original = context.Canceled
		}
		request.client.Client = &http.Client{
			Transport: registryRoundTripper(func(req *http.Request) (*http.Response, error) {
				return testFetcherFailureSourceAndOperationCaseCallback2(
					req,
					testFetcherFailureSourceAndOperationCaseCallback2Inputs{
						stage:    &stage,
						cancel:   &cancel,
						art:      &art,
						failures: &failures,
						srv:      &srv,
						original: &original,
					},
				)
			},
			),
		}
	}
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	_, err = (&operation{Fetcher: f}).fetchLayer(
		ctx,
		repo,
		request,
		sourcecache.Policy{MaxAge: repo.SourceInterval(), Retry: retry},
	)
	if failures.Load() != tc.attempts {
		t.Fatalf("producer failures: got %d, want %d: %v", failures.Load(), tc.attempts, err)
	}
	t.Logf("live producer failures=%d; total requests=%d", failures.Load(), requests.Load())
	prefix := (*stage)
	switch *stage {
	case "list tags":
		prefix = "semver: list tags:"
	case "manifest body":
		prefix = "fetch manifest:"
	case "layer header":
		prefix = "gzip:"
	case "layer body", "neutral body", "canceled body":
		prefix = ""
	}
	assert.FatalIf(t, err == nil || !strings.HasPrefix(err.Error(), prefix) || (*stage) !=
		"canceled body" && strings.Contains(err.Error(), "cache:"), "failure identity: %v", err)
	shouldReturn := testFetcherFailureSourceAndOperationCaseCompletion(
		t,
		&testFetcherFailureSourceAndOperationCaseCompletionInputs{
			ctx:      &ctx,
			err:      &err,
			tc:       &tc,
			stage:    &stage,
			original: &original,
			requests: &requests,
			c:        &c,
			f:        &f,
			root:     &root,
			repo:     &repo,
			request:  &request,
			retry:    &retry,
			failures: &failures,
		},
	)
	if shouldReturn {
		return
	}
}

func testFetcherFailureSourceAndOperationCaseCompletion(
	t *testing.T,
	captures *testFetcherFailureSourceAndOperationCaseCompletionInputs,
) bool {
	t.Helper()
	assert.FatalIf(t, !errors.Is((*captures.err), manifest.ErrFlux), "failure classification: %v", (*captures.err))
	neutral, ok := errors.AsType[*fetcherr.ResponseError]((*captures.err))
	if (*captures.tc).status == http.StatusNotFound {
		assert.FatalIf(
			t,
			ok || !errors.Is((*captures.err), errdef.ErrNotFound),
			"ORAS not-found mapping changed: %v", (*captures.err),
		)
		return true
	}
	if (*(*captures.stage)) == "canceled body" {
		assert.FatalIf(
			t,
			ok || !errors.Is((*captures.err), context.Canceled),
			"caller cancellation changed: %v",
			(*captures.err),
		)
		return true
	}
	response, typed := errors.AsType[*errcode.ErrorResponse]((*captures.err))
	method := http.MethodGet
	if (*(*captures.stage)) == "resolve" {
		method = http.MethodHead
	}
	assert.FatalIf(t, !ok || !typed || neutral.StatusCode != (*captures.tc).status || neutral.Op != method ||
		response.StatusCode != (*captures.tc).status ||
		response.Method != method ||
		neutral.Error() != neutral.Err.Error(), "lost response facts: neutral=%+v response=%+v error=%v", neutral, response, (*captures.err))
	if (*captures.original) == nil {
		assert.FatalIf(t, !errors.Is(response, neutral.Err), "original ORAS response changed: %v", (*captures.err))
	} else {
		assert.FatalIf(
			t,
			!errors.Is((*captures.err), (*captures.original)),
			"original body failure changed: %v",
			(*captures.err),
		)
	}
	assert.FatalIf(
		t, (*(*captures.stage)) ==
			"neutral body" && !errors.Is((*captures.original), neutral), "neutral body failure wrapped twice: %v", (*captures.err),
	)
	if !(*captures.tc).replay {
		return true
	}
	terminal, ok := errors.AsType[*sourcecache.TerminalError]((*captures.err))
	assert.FatalIf(t, !ok || !terminal.Attempted, "initial exhaustion: %+v, %v", terminal, (*captures.err))
	before := captures.requests.Load()
	{
		closeErr4 := (*captures.c).Close()
		assert.NoError(t, closeErr4)
	}
	(*captures.f).
		Cache, (*captures.err) = sourcecache.New(t.Context(), (*captures.root))
	assert.NoError(t, (*captures.err))
	assert.FatalIf(t, (*captures.f).Cache == (*captures.c), "%s", "reopen reused the closed owner")

	_, (*captures.err) = (&operation{Fetcher: (*captures.f)}).fetchLayer(
		(*captures.ctx),
		(*captures.repo),
		(*captures.request),
		sourcecache.Policy{MaxAge: (*captures.repo).SourceInterval(), Retry: (*captures.retry)},
	)
	if captures.requests.Load() != before || (*captures.failures).Load() != (*captures.tc).attempts {
		t.Fatalf(
			"replay fetched: requests=%d, want %d; failures=%d, want %d", captures.requests.
				Load(), before, captures.failures.
				Load(), (*captures.tc).
				attempts,
		)
	}
	terminal, ok = errors.AsType[*sourcecache.TerminalError]((*captures.err))
	neutral, neutralOK := errors.AsType[*fetcherr.ResponseError]((*captures.err))
	response, typed = errors.AsType[*errcode.ErrorResponse]((*captures.err))
	assert.FatalIf(
		t,
		!ok || terminal.Attempted || !errors.Is((*captures.err), manifest.ErrFlux) || !neutralOK || !typed ||
			neutral.StatusCode != (*captures.tc).status ||
			neutral.Op != method ||
			response.StatusCode != (*captures.tc).status ||
			response.Method != method,
		"replayed response: terminal=%+v neutral=%+v response=%+v error=%v",
		terminal,
		neutral,
		response, (*captures.err),
	)
	t.Logf(
		"attempts=%d; requests before/after replay=%d/%d; status=%d stage=%s", (*captures.tc).
			attempts, before, captures.requests.
			Load(), neutral.StatusCode,
		neutral.Op,
	)
	return false
}

type testFetcherLayerSelectionAndIgnoreFlavorCaseInputs struct {
	art    *fakeOCIArtifact
	second *fakeOCIArtifact
	prov   *manifestBlob
	tc     *struct {
		name     string
		layers   []manifestBlob
		selector *sourcev1.
				OCILayerSelector
		digest   bool
		wantCopy []byte
		wantErr  bool
	}
}

func testFetcherLayerSelectionAndIgnoreFlavorCase(
	t *testing.T,
	testCaseInput testFetcherLayerSelectionAndIgnoreFlavorCaseInputs,
) {
	t.Helper()

	data := mustManifestJSON(
		t,
		manifestBlob{
			data:      testCaseInput.art.config,
			mediaType: "application/vnd.cncf.flux.config.v1+json",
		}, testCaseInput.tc.
			layers...)
	srv := startFakeRegistry(
		t,
		data,
		testCaseInput.art.config,
		testCaseInput.art.layer,
		testCaseInput.second.layer,
		testCaseInput.prov.data,
	)
	repo := &manifest.OCIRepository{
		URL:           "oci://" + mustURL(t, srv.URL).Host + "/artifact",
		Insecure:      true,
		Reference:     &sourcev1.OCIRepositoryRef{Tag: "latest"},
		LayerSelector: testCaseInput.tc.selector,
	}
	if testCaseInput.tc.digest {
		repo.Reference = &sourcev1.OCIRepositoryRef{Digest: sha256Digest(data)}
	}
	root := t.TempDir()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, root, sourcecache.New)}
	var first *store.SourceArtifact
	for range 2 {
		got, err := f.Fetch(
			t.Context(),
			repo,
			(testInputs{}).readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval()},
		)
		if testCaseInput.tc.wantErr {
			assert.FatalIf(
				t,
				got != nil || !errors.Is(err, manifest.ErrFlux),
				"invalid selection: artifact=%v err=%v, want nil artifact and ErrFlux",
				got,
				err,
			)
			continue
		}
		assert.NoError(t, err)

		if testCaseInput.tc.wantCopy != nil {
			{
				data, err := fs.ReadFile(
					got.FS,
					source.OCICopiedLayerFilename,
				)
				assert.FatalIf(t, err != nil ||
					!bytes.Equal(
						data, testCaseInput.tc.wantCopy,
					), "copy changed selected layer: data=%q err=%v", data, err)
			}
		} else {
			for _, name := range []string{".git/config", "image.png"} {
				{
					_, err := fs.Stat(got.FS, name)
					assert.FatalIf(
						t,
						!errors.Is(err, fs.ErrNotExist),
						"default ignore retained %s: %v",
						name,
						err,
					)
				}
			}
			{
				got := mustReadFile(t, got.FS, "file.yaml")
				assert.FatalIf(t, got != "x", "content=%q", got)
			}
		}
		if first != nil {
			before, after := *first, *got
			before.FS, after.FS = nil, nil
			{
				diff := cmp.Diff(
					before,
					after,
					cmpopts.IgnoreFields(store.SourceArtifact{}, "material"),
				)
				assert.ErrorIf(t, diff != "", "acquired artifact mismatch (-want +got):\n%s", diff)
			}
		}
		first = got
	}
	if !testCaseInput.tc.digest {
		return
	}
	copyRepo := *repo
	copyRepo.LayerSelector = &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}
	copied, err := f.Fetch(
		t.Context(),
		&copyRepo,
		(testInputs{}).readSet(&copyRepo),
		sourcecache.Policy{MaxAge: copyRepo.SourceInterval()},
	)
	assert.NoError(t, err)

	{
		data, err := fs.ReadFile(
			copied.FS,
			source.OCICopiedLayerFilename,
		)
		assert.FatalIf(t, err != nil ||
			!bytes.Equal(data, testCaseInput.art.layer), "copy artifact changed layer bytes: %v", err)
	}
	if copied.Key == first.Key {
		t.Fatalf("different layer selectors reused cache key %q", first.Key)
	}
	{
		err := f.Cache.Close()
		assert.NoError(t, err)
	}

	f.Cache = cachetest.New(t, root, sourcecache.New)
	for range 2 {
		warm, err := f.Fetch(
			t.Context(),
			&copyRepo,
			(testInputs{}).readSet(&copyRepo),
			sourcecache.Policy{MaxAge: copyRepo.SourceInterval()},
		)
		assert.NoError(t, err)

		{
			data, err := fs.ReadFile(
				warm.FS,
				source.OCICopiedLayerFilename,
			)
			assert.FatalIf(t, err != nil ||
				!bytes.Equal(data, testCaseInput.art.layer), "warm copy changed layer bytes: %v", err)
		}
		before, after := *copied, *warm
		before.FS, after.FS = nil, nil
		{
			diff := cmp.Diff(
				before,
				after,
				cmpopts.IgnoreFields(store.SourceArtifact{}, "material"),
			)
			assert.ErrorIf(t, diff != "", "acquired artifact mismatch (-want +got):\n%s", diff)
		}
	}
}

type testFetcherLayerDigestMismatchPublishesNothingCaseCallbackInputs struct {
	honest   *fakeOCIArtifact
	broken   *fakeOCIArtifact
	payload  *[]byte
	repaired *atomic.Bool
	layers   *atomic.Int32
}

func testFetcherLayerDigestMismatchPublishesNothingCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherLayerDigestMismatchPublishesNothingCaseCallbackInputs,
) {
	current := (*testCaseInput.broken)
	if testCaseInput.repaired.Load() {
		current = (*testCaseInput.honest)
	}
	switch {
	case strings.Contains(r.URL.Path, "/manifests/"):
		w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
		w.Header().Set("Docker-Content-Digest", current.manifestDigest)
		_, _ = w.Write(current.manifest)
	case strings.HasSuffix(r.URL.Path, current.layerDigest):
		testCaseInput.layers.
			Add(1)
		if testCaseInput.repaired.Load() {
			_, _ = w.Write(testCaseInput.honest.layer)
		} else {
			_, _ = w.Write((*testCaseInput.payload))
		}
	case strings.HasSuffix(r.URL.Path, current.configDigest):
		_, _ = w.Write(current.config)
	}
}

type testFetcherFailureSourceAndOperationCaseCallbackInputs struct {
	tc **struct {
		stage    string
		status   int
		attempts int32
		replay   bool
	}
	stage    **string
	art      *fakeOCIArtifact
	requests *atomic.Int32
	failures *atomic.Int32
}

func testFetcherFailureSourceAndOperationCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherFailureSourceAndOperationCaseCallbackInputs,
) {
	testCaseInput.requests.
		Add(1)
	switch {
	case strings.HasSuffix(r.URL.Path, "/tags/list"):
		testCaseInput.failures.
			Add(1)
		http.Error(w, "registry failure", (*testCaseInput.tc).status)
	case strings.Contains(r.URL.Path, "/manifests/"):
		if (*(*testCaseInput.stage)) == "resolve" || (*(*testCaseInput.stage)) ==
			"fetch manifest" && strings.HasSuffix(r.URL.Path, testCaseInput.art.manifestDigest) &&
			r.Method == http.MethodGet {
			testCaseInput.failures.
				Add(1)
			http.Error(w, "registry failure", (*testCaseInput.tc).status)
			return
		}
		w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
		w.Header().Set("Docker-Content-Digest", testCaseInput.art.manifestDigest)
		_, _ = w.Write(testCaseInput.art.manifest)
	case strings.HasSuffix(r.URL.Path, testCaseInput.art.layerDigest):
		if (*(*testCaseInput.stage)) == "fetch layer" {
			testCaseInput.failures.
				Add(1)
			http.Error(w, "registry failure", (*testCaseInput.tc).status)
			return
		}
		_, _ = w.Write(testCaseInput.art.layer)
	}
}

type testFetcherFailureSourceAndOperationCaseCallback2Inputs struct {
	stage    **string
	cancel   *context.CancelFunc
	art      *fakeOCIArtifact
	failures *atomic.Int32
	srv      **httptest.Server
	original *error
}

func testFetcherFailureSourceAndOperationCaseCallback2(
	req *http.Request,
	testCaseInput testFetcherFailureSourceAndOperationCaseCallback2Inputs,
) (*http.Response, error) {
	resp, roundTripErr := (*testCaseInput.srv).Client().Transport.RoundTrip(req)
	if roundTripErr != nil || req.Method != http.MethodGet {
		return resp, roundTripErr
	}
	manifestBody := (*(*testCaseInput.stage)) == "manifest body" &&
		strings.Contains(req.URL.Path, "/manifests/")
	layerBody := (*(*testCaseInput.stage)) != "manifest body" &&
		strings.HasSuffix(req.URL.Path, testCaseInput.art.layerDigest)
	if manifestBody || layerBody {
		testCaseInput.failures.
			Add(1)
		_ = resp.Body.Close()
		var prefix []byte
		if layerBody && (*(*testCaseInput.stage)) != "layer header" {
			prefix = testCaseInput.art.layer[:10]
		}
		resp.Body = io.NopCloser(
			io.MultiReader(bytes.NewReader(prefix), iotest.ErrReader((*testCaseInput.original))),
		)
		if (*(*testCaseInput.stage)) == "canceled body" {
			(*testCaseInput.cancel)()
		}
	}
	return resp, nil
}

type testFetcherFailureSourceAndOperationCaseCompletionInputs struct {
	ctx *context.Context
	err *error
	tc  **struct {
		stage    string
		status   int
		attempts int32
		replay   bool
	}
	stage    **string
	original *error
	requests *atomic.Int32
	c        **sourcecache.Cache
	f        **Fetcher
	root     *string
	repo     **manifest.OCIRepository
	request  *registryAcquisition
	retry    *sourcecache.RetryConfig
	failures *atomic.Int32
}
