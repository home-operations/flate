package oci

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type pullTimeoutBody struct{ ctx context.Context } //nolint:containedctx // The test stores the producer context to assert deadline and cancellation propagation.

func (b pullTimeoutBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (pullTimeoutBody) Close() error { return nil }

func TestFetcher_PullTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		stage  string
		reopen bool
	}{{"tags", false}, {"manifest", false}, {"layer", true}} {
		t.Run(tc.stage, func(t *testing.T) {
			t.Parallel()
			testFetcherPullTimeoutCase(t, &tc)
		},
		)
	}
}

func testFetcherPullTimeoutCase(t *testing.T, tc *struct {
	stage  string
	reopen bool
},
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testFetcherPullTimeoutCaseCallback(t, testFetcherPullTimeoutCaseCallbackInputs{tc: &tc})
	},
	)
}

type testFetcherPullTimeoutCaseCallbackInputs struct {
	tc **struct {
		stage  string
		reopen bool
	}
}

func testFetcherPullTimeoutCaseCallback(t *testing.T, testCaseInput testFetcherPullTimeoutCaseCallbackInputs) {
	t.Helper()
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	root := t.TempDir()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, root, sourcecache.New)}
	r := &manifest.OCIRepository{
		URL:       "oci://registry.invalid/repository",
		Reference: &sourcev1.OCIRepositoryRef{SemVer: "*"},
		Timeout:   &metav1.Duration{Duration: 5 * time.Second},
	}
	request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), new(r.RegistryAccess()))
	assert.NoError(t, err)

	blocked := true
	start := time.Now()
	deadline := start.Add(5 * time.Second)
	calls := 0
	request.client.Client = &http.Client{
		Transport: registryRoundTripper(func(req *http.Request) (*http.Response, error) {
			calls++
			{
				got, ok := req.Context().Deadline()
				assert.ErrorIf(
					t,
					!ok || !got.Equal(deadline),
					"pull deadline=%v present=%t, want %v",
					got,
					ok,
					deadline,
				)
			}
			stage := "manifest"
			data := artifact.manifest
			header := http.Header{
				"Content-Type":          {ocispec.MediaTypeImageManifest},
				"Docker-Content-Digest": {artifact.manifestDigest},
			}
			switch {
			case strings.HasSuffix(req.URL.Path, "/tags/list"):
				stage = "tags"
				data = []byte(`{"name":"repository","tags":["v1.0.0"]}`)
				header.Set("Content-Type", "application/json")
			case strings.HasSuffix(req.URL.Path, artifact.layerDigest):
				stage = "layer"
				data = artifact.layer
				header.Set("Content-Type", "application/vnd.cncf.flux.content.v1.tar+gzip")
				header.Set("Docker-Content-Digest", artifact.layerDigest)
			}
			if stage == "tags" || stage == "layer" && !blocked {
				select {
				case <-time.After(3 * time.Second):
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			}
			body := io.NopCloser(bytes.NewReader(data))
			if req.Method == http.MethodHead {
				body = http.NoBody
			} else if blocked && stage == (*testCaseInput.tc).stage {
				body = pullTimeoutBody{req.Context()}
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        header,
				ContentLength: int64(len(data)),
				Body:          body,
				Request:       req,
			}, nil
		}),
	}
	testFetcherPullTimeoutCaseCallbackCompletion(
		t,
		&testFetcherPullTimeoutCaseCallbackCompletionInputs{
			err:           &err,
			f:             &f,
			r:             &r,
			request:       &request,
			start:         &start,
			testCaseInput: &testCaseInput,
			root:          &root,
			blocked:       &blocked,
			deadline:      &deadline,
			calls:         &calls,
		},
	)
}

func testFetcherPullTimeoutCaseCallbackCompletion(
	t *testing.T,
	captures *testFetcherPullTimeoutCaseCallbackCompletionInputs,
) {
	t.Helper()
	_, (*captures.err) = (&operation{Fetcher: (*captures.f)}).fetchLayer(
		t.Context(), (*captures.r), new((*captures.request)), sourcecache.Policy{MaxAge: time.Hour},
	)
	if !errors.Is((*captures.err), context.DeadlineExceeded) || time.Since((*captures.start)) != 5*time.Second {
		t.Fatalf(
			"%s timeout: elapsed=%v error=%v",
			(*captures.testCaseInput.tc).stage,
			time.Since((*captures.start)),
			(*captures.err),
		)
	}
	if (*captures.testCaseInput.tc).reopen {
		{
			closeErr5 := (*captures.f).Cache.Close()
			assert.NoError(t, closeErr5)
		}
		(*captures.f) = &Fetcher{
			Network: (*captures.f).Network,
			Cache:   cachetest.New(t, (*captures.root), sourcecache.New),
		}
	}
	(*captures.blocked) = false
	longer := *(*captures.r)
	longer.Timeout = &metav1.Duration{Duration: 10 * time.Second}
	(*captures.start) = time.Now()
	(*captures.deadline) = captures.start.Add(10 * time.Second)
	var acquired *store.SourceArtifact

	acquired, (*captures.err) = (&operation{Fetcher: (*captures.f)}).fetchLayer(
		t.Context(),
		&longer, new((*captures.request)), sourcecache.Policy{MaxAge: time.Hour},
	)
	if (*captures.err) != nil || time.Since((*captures.start)) != 6*time.Second {
		t.Fatalf("longer pull: elapsed=%v error=%v", time.Since((*captures.start)), (*captures.err))
	}
	{
		body, readFileErr2 := fs.ReadFile(acquired.FS, "app.yaml")
		assert.FatalIf(
			t,
			readFileErr2 != nil || string(body) != "version: v1\n",
			"pull content=%q error=%v",
			body,
			readFileErr2,
		)
	}
	{
		closeErr6 := (*captures.f).Cache.Close()
		assert.NoError(t, closeErr6)
	}
	(*captures.f) = &Fetcher{Network: (*captures.f).Network, Cache: cachetest.New(t, (*captures.root), sourcecache.New)}
	shorter := longer
	shorter.Timeout = &metav1.Duration{Duration: time.Nanosecond}
	before := (*captures.calls)
	var warm *store.SourceArtifact

	warm, (*captures.err) = (&operation{Fetcher: (*captures.f)}).fetchLayer(
		t.Context(),
		&shorter, new((*captures.request)), sourcecache.Policy{MaxAge: time.Hour},
	)
	assert.FatalIf(
		t, (*captures.err) !=
			nil || (*captures.calls) != before || warm.Key != acquired.Key, "timeout changed persistent content identity: calls=%d->%d error=%v",
		before, (*captures.calls), (*captures.err),
	)
}

type testFetcherPullTimeoutCaseCallbackCompletionInputs struct {
	err           *error
	f             **Fetcher
	r             **manifest.OCIRepository
	request       *registryAcquisition
	start         *time.Time
	testCaseInput *testFetcherPullTimeoutCaseCallbackInputs
	root          *string
	blocked       *bool
	deadline      *time.Time
	calls         *int
}
