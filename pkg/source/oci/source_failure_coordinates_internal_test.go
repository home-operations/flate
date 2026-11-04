package oci

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_FailureSourceAndOperation(t *testing.T) {
	t.Parallel()

	for _, layerOperation := range []string{sourcev1.OCILayerCopy, sourcev1.OCILayerExtract} {
		for _, tc := range []struct {
			stage    string
			status   int
			attempts int32
			replay   bool
		}{
			{"list tags", 400, 1, false},
			{"resolve", 400, 1, false},
			{"fetch manifest", 400, 1, false},
			{"fetch layer", 400, 1, false},
			{"manifest body", 400, 1, false},
			{"layer header", 400, 1, false},
			{"layer body", 400, 1, false},
			{"neutral body", 400, 1, false},
			{"canceled body", 400, 1, false},
			{"list tags", 429, 3, true},
			{"resolve", 500, 3, true},
			{"fetch manifest", 599, 3, true},
			{"fetch layer", 600, 1, false},
			{"resolve", 404, 1, false},
		} {
			stage := tc.stage
			if stage == "layer header" && layerOperation == sourcev1.OCILayerCopy {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s/%d", layerOperation, stage, tc.status), func(t *testing.T) {
				t.Parallel()
				testFetcherFailureSourceAndOperationCase(
					t,

					&layerOperation,
					&tc,
					&stage,
				)
			},
			)
		}
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
	request, err := (&operation{Fetcher: f}).newRepoClient(ctx, new(repo.RegistryAccess()))
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
		repo, new(request), sourcecache.Policy{MaxAge: repo.SourceInterval(), Retry: retry},
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
		new((*captures.request)),
		sourcecache.Policy{MaxAge: (*captures.repo).SourceInterval(), Retry: (*captures.retry)},
	)
	if captures.requests.Load() != before || captures.failures.Load() != (*captures.tc).attempts {
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
