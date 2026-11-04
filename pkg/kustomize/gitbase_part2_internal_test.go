package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_RemoteRetryPolicies(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse-%t", concurrency, reverse), func(t *testing.T) {
				t.Parallel()
				testRenderRemoteRetryPoliciesCase(
					t,
					&testRenderRemoteRetryPoliciesCaseInputs{concurrency: &concurrency, reverse: &reverse},
				)
			},
			)
		}
	}
}

//nolint:paralleltest // test replaces process-wide dependency configuration.
func TestPreflightGitBase_UpstreamSelection(t *testing.T) {
	for _, tc := range []struct {
		name, role, raw, body string
		status, http, git     int
	}{
		{"markerless repository", "resources", "https://github.com/org/repo/base", "", http.StatusNotFound, 1, 1},
		{"resource before directory", "resources", "https://github.com/org/repo//base?ref=v1", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: file}\n", http.StatusOK, 1, 0},
		{"component directory", "components", "https://github.com/org/repo/base", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: file}\n", http.StatusOK, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var httpCalls, gitCalls atomic.Int32
			stubHTTPTransport(t, remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
				httpCalls.Add(1)
				return &http.Response{
					StatusCode: tc.status,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(tc.body)),
					Request:    r,
				}, nil
			}))
			fsys := memFSWith(t, map[string]string{"kustomization.yaml": tc.role + ":\n  - " + tc.raw + "\n"})
			cache := newTestTreeCache(t, "")
			withFakeGitBase(
				t,
				cache,
				fakeWorktree(t, map[string]string{"base/kustomization.yaml": "resources: []\n"}),
				&gitCalls,
			)
			err := preflightRemoteResources(t.Context(), cache, fsys, ".")
			if err != nil || int(httpCalls.Load()) != tc.http || int(gitCalls.Load()) != tc.git {
				t.Errorf(
					"preflight: HTTP=%d Git=%d error=%v; want HTTP=%d Git=%d and success",
					httpCalls.Load(),
					gitCalls.Load(),
					err,
					tc.http,
					tc.git,
				)
			}
		})
	}
}

func TestPreflightGitBase_PolicyMounts(t *testing.T) {
	t.Parallel()
	c := newTestTreeCache(t, "")
	var requests []GitBaseRequest
	c.SetGitBaseFetcher(
		func(_ context.Context, r GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			requests = append(requests, r)
			return &store.SourceArtifact{
				Key:      "same",
				Content:  "same-content",
				Revision: "same",
				FS: memoryTree(
					t,
					map[string]string{
						"one/kustomization.yaml": "resources: []\n",
						"two/kustomization.yaml": "resources: []\n",
					},
				),
			}, nil
		},
	)
	view := memFSWith(t, map[string]string{"kustomization.yaml": `resources:
- file:///org/repo//one?timeout=1s&submodules=false
- file:///org/repo//two?timeout=1s&submodules=false
- file:///org/repo//one?timeout=2s&submodules=false
- file:///org/repo//one?timeout=2s&submodules=true
`})
	{
		err := preflightRemoteResources(t.Context(), c, view, ".")
		assert.NoError(t, err)
	}
	assert.FatalIf(
		t,
		len(requests) != 3 || len(view.bases) != 3,
		"policy acquisitions=%+v mounts=%+v",
		requests,
		view.bases,
	)
	for i, want := range []GitBaseRequest{{URL: "file:///org/repo", Timeout: time.Second}, {URL: "file:///org/repo", Timeout: 2 * time.Second}, {URL: "file:///org/repo", Timeout: 2 * time.Second, Submodules: true}} {
		if requests[i] != want {
			t.Fatalf("request %d=%+v, want %+v", i, requests[i], want)
		}
	}
}

func TestPreflightGitBase_ErrorPrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path string
		status     int
		timeout    bool
	}{
		{"malformed resource", "/org/repo/base", http.StatusOK, false},
		{"directory timeout", "/org/repo/base", http.StatusOK, true},
		{"HTTP failure", "/resource.yaml", http.StatusNotFound, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testPreflightGitBaseErrorPrecedenceCase(t, &tc)
		},
		)
	}
}

type testRenderRemoteRetryPoliciesCaseInputs struct {
	concurrency *int
	reverse     *bool
}

func testRenderRemoteRetryPoliciesCase(t *testing.T, testCaseInput *testRenderRemoteRetryPoliciesCaseInputs) {
	t.Helper()
	var httpCalls [4]atomic.Int32
	srv := httptest.NewServer(retryPolicyHandler(&httpCalls))
	defer srv.Close()
	c := newTestTreeCache(t, "")
	entered := make(chan int, (*testCaseInput.concurrency))
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	c.SetGitBaseFetcher(
		func(ctx context.Context, request GitBaseRequest, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			assert.ErrorIf(t, request.MaxAge != time.Hour, "remote interval=%v", request.MaxAge)
			select {
			case <-release:
			default:
				entered <- retry.Attempts
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			body := fmt.Sprintf("resources:\n  - %s/%d\n", srv.URL, retry.Attempts)
			return &store.SourceArtifact{
				Key:      "family",
				Content:  strconv.Itoa(retry.Attempts),
				Revision: strconv.Itoa(retry.Attempts),
				FS:       fstest.MapFS{"kustomization.yaml": {Data: []byte(body)}},
			}, nil
		},
	)
	type result struct {
		attempts int
		body     []map[string]any
		err      error
	}
	done := make(chan result, (*testCaseInput.concurrency))
	launch := func(i int) {
		go func() {
			attempts := 1
			if i%2 != 0 {
				attempts = 3
			}
			retry := sourcecache.RetryConfig{
				Attempts: attempts,
				MinWait:  time.Nanosecond,
				MaxWait:  time.Nanosecond,
			}
			input := &store.SourceArtifact{
				Key: "root",
				FS: fstest.MapFS{
					"kustomization.yaml": {
						Data: []byte("resources:\n  - https://example.invalid/owner/repo?ref=main\n"),
					},
				},
			}
			body, _, err := testutil.RenderMaps(
				Render(
					t.Context(),
					c,
					RenderRequest{
						Source: input,
						Spec:   &manifest.Kustomization{Interval: metav1.Duration{Duration: time.Hour}},
					},
					retry,
				),
			)
			done <- result{attempts, body, err}
		}()
	}
	first := 0
	if *testCaseInput.reverse {
		first = (*testCaseInput.concurrency) - 1
	}
	launch(first)
	select {
	case <-entered:
	case early := <-done:
		t.Fatalf("render before Git callback: %v", early.err)
	}
	testRenderRemoteRetryPoliciesCaseLoop(struct {
		testCaseInput **testRenderRemoteRetryPoliciesCaseInputs
		first         *int
		launch        *func(i int)
	}{testCaseInput: &testCaseInput, first: &first, launch: &launch})
	close(release)
	released = true
	for range *testCaseInput.concurrency {
		got := <-done
		if got.attempts == 1 {
			terminal, ok := errors.AsType[*sourcecache.TerminalError](got.err)
			assert.FatalIf(t, !ok || !terminal.Attempted, "one-attempt result=%v", got.err)
		} else if got.err != nil || !hasRenderField(got.body, "name", "retried") {
			t.Fatalf("three-attempt result=%s, %v", got.body, got.err)
		}
	}
	if httpCalls[3].Load() != 2 || httpCalls[1].Load() < 1 ||
		httpCalls[1].Load() > int32((*testCaseInput.concurrency)/2) {
		t.Fatalf("HTTP attempts=%d/%d", httpCalls[1].Load(), httpCalls[3].Load())
	}
}

func testPreflightGitBaseErrorPrecedenceCase(
	t *testing.T,
	testCaseInput *struct {
		name    string
		path    string
		status  int
		timeout bool
	},
) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(testCaseInput.status)
		_, _ = io.WriteString(w, "apiVersion: [")
	}))
	t.Cleanup(srv.Close)
	c := newTestTreeCache(t, "")
	var calls atomic.Int32
	c.SetGitBaseFetcher(
		func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			calls.Add(1)
			if testCaseInput.timeout {
				return nil, context.DeadlineExceeded
			}
			return nil, errors.New("directory failed")
		},
	)
	input := &store.SourceArtifact{
		FS: memoryTree(
			t,
			map[string]string{"kustomization.yaml": "resources: ['" + srv.URL + testCaseInput.path + "']\n"},
		),
	}
	_, _, err := testutil.RenderMaps(
		Render(t.Context(), c, RenderRequest{Source: input, Spec: demoSpec}, sourcecache.RetryConfig{}),
	)
	{
		assert.FatalIf(t, err == nil, "%s", "invalid input rendered")
	}
	switch {
	case testCaseInput.status != http.StatusOK:
		if calls.Load() != 0 || !strings.Contains(err.Error(), "HTTP 404") {
			t.Fatalf("HTTP error precedence: calls=%d error=%v", calls.Load(), err)
		}

	case testCaseInput.timeout:
		{
			assert.FatalIf(
				t,
				!errors.Is(err, context.DeadlineExceeded) ||
					!strings.Contains(err.Error(), "accumulation err="),
				"directory timeout lost file diagnostic: %v",
				err,
			)
		}

	case calls.Load() != 1 || strings.Contains(err.Error(), "directory failed") || !strings.Contains(err.Error(), "yaml"):
		t.Fatalf("malformed resource precedence: calls=%d error=%v", calls.Load(), err)
	}
}

func testRenderRemoteRetryPoliciesCaseLoop(loopInputs struct {
	testCaseInput **testRenderRemoteRetryPoliciesCaseInputs
	first         *int
	launch        *func(i int)
},
) {
	for i := range *(*loopInputs.testCaseInput).concurrency {
		if i != (*loopInputs.first) {
			(*loopInputs.launch)(i)
		}
	}
}

func retryPolicyHandler(httpCalls *[4]atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		attempts := 1
		if strings.HasSuffix(r.URL.Path, "/3") {
			attempts = 3
		}
		n := httpCalls[attempts].Add(1)
		if attempts == 1 || n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: retried}\n")
	}
}
