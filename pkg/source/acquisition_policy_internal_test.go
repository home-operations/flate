package source

import (
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testAcquirePolicyIsolationCase(t *testing.T, concurrency *int,
	reverse *bool,
	equal *bool,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquirePolicyIsolationCaseCallback(
			t,
			testAcquirePolicyIsolationCaseCallbackInputs{concurrency: &concurrency, reverse: &reverse, equal: &equal},
		)
	},
	)
}

type testAcquirePolicyIsolationCaseCallbackInputs struct {
	concurrency **int
	reverse     **bool
	equal       **bool
}

func testAcquirePolicyIsolationCaseCallback(t *testing.T, testCaseInput testAcquirePolicyIsolationCaseCallbackInputs) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	hits := 0
	c := newTestHTTPInputs(t, "")
	c.httpClient = &http.Client{
		Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
			hits++
			if hits == 1 {
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			}
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
			}, nil
		}),
	}
	done := make(chan error, (*(*testCaseInput.concurrency)))
	launch := func(i int) {
		go func() {
			retry := sourcecache.RetryConfig{
				Attempts: 3,
				MinWait:  time.Nanosecond,
				MaxWait:  time.Nanosecond,
			}
			if !(*(*testCaseInput.equal)) && i == 0 {
				retry.Attempts = 1
			}
			if *(*testCaseInput.equal) {
				switch i % 3 {
				case 0:
					retry.Jitter = math.NaN()
				case 1:
					retry.Jitter = math.Inf(1)
				}
			}
			_, err := c.Acquire(
				t.Context(),
				&HTTPRequest{URL: "https://example.invalid/resource", Retry: retry, HeldSlot: nil},
			)
			done <- err
		}()
	}
	testAcquirePolicyIsolationCaseCallbackCompletion(
		t,
		&testAcquirePolicyIsolationCaseCallbackCompletionInputs{
			testCaseInput: &testCaseInput,
			launch:        &launch,
			entered:       &entered,
			release:       &release,
			done:          &done,
			hits:          &hits,
		},
	)
}

func testAcquirePolicyIsolationCaseCallbackCompletion(
	t *testing.T,
	captures *testAcquirePolicyIsolationCaseCallbackCompletionInputs,
) {
	t.Helper()
	first := 0
	if *(*captures.testCaseInput.reverse) {
		first = (*(*captures.testCaseInput.concurrency)) - 1
	}
	(*captures.launch)(first)
	<-(*captures.entered)
	for i := range *(*captures.testCaseInput.concurrency) {
		if i != first {
			(*captures.launch)(i)
		}
	}
	synctest.Wait()
	close((*captures.release))
	for range *(*captures.testCaseInput.concurrency) {
		err := <-(*captures.done)
		terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
		assert.FatalIf(t, !ok || !terminal.Attempted, "terminal=%+v, error=%v", terminal, err)
	}
	want := 4
	if *(*captures.testCaseInput.equal) {
		want = 3
	}
	assert.FatalIf(t, (*captures.hits) != want, "requests=%d, want %d", (*captures.hits), want)
}

type testAcquirePolicyIsolationCaseCallbackCompletionInputs struct {
	testCaseInput *testAcquirePolicyIsolationCaseCallbackInputs
	launch        *func(i int)
	entered       *chan struct{}
	release       *chan struct{}
	done          *chan error
	hits          *int
}
