package kustomize

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

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
	assert.FatalIf(t, err == nil, "%s", "invalid input rendered")
	switch {
	case testCaseInput.status != http.StatusOK:
		if calls.Load() != 0 || !strings.Contains(err.Error(), "HTTP 404") {
			t.Fatalf("HTTP error precedence: calls=%d error=%v", calls.Load(), err)
		}

	case testCaseInput.timeout:
		assert.FatalIf(
			t,
			!errors.Is(err, context.DeadlineExceeded) ||
				!strings.Contains(err.Error(), "accumulation err="),
			"directory timeout lost file diagnostic: %v",
			err,
		)

	case calls.Load() != 1 || strings.Contains(err.Error(), "directory failed") || !strings.Contains(err.Error(), "yaml"):
		t.Fatalf("malformed resource precedence: calls=%d error=%v", calls.Load(), err)
	}
}
