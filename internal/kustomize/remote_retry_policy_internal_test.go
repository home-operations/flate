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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
