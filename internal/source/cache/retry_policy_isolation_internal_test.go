package cache

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func testAcquirePolicyIsolationCase(t *testing.T, concurrency *int,
	reverse *bool,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquirePolicyIsolationCaseCallback(
			t,
			testAcquirePolicyIsolationCaseCallbackInputs{concurrency: &concurrency, reverse: &reverse},
		)
	},
	)
}

type testAcquirePolicyIsolationCaseCallbackInputs struct {
	concurrency **int
	reverse     **bool
}

func testAcquirePolicyIsolationCaseCallback(t *testing.T, testCaseInput testAcquirePolicyIsolationCaseCallbackInputs) {
	t.Helper()
	c := cachetest.New(t, "", New)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, (*(*testCaseInput.concurrency)))
	calls := make([]int, (*(*testCaseInput.concurrency)))
	launch := func(i int) {
		go func() {
			attempts := 3
			if i == 0 {
				attempts = 1
			}
			_, _, _, err := c.Blob(
				t.Context(),
				BlobKey{Content: "shared"},
				&Policy{MaxAge: time.Hour, Retry: fastRetry(attempts)},
				func(context.Context, ProducerInput) (ProducerResult, error) {
					calls[i]++
					if (i == 0 && !(*(*testCaseInput.reverse)) || i == (*(*testCaseInput.concurrency))-1 && (*(*testCaseInput.reverse))) &&
						calls[i] == 1 {
						close(started)
						<-release
					}
					return ProducerResult{}, connReset()
				},
			)
			done <- err
		}()
	}
	first := 0
	if *(*testCaseInput.reverse) {
		first = (*(*testCaseInput.concurrency)) - 1
	}
	testAcquirePolicyIsolationCaseCallbackCompletion(
		t,
		&testAcquirePolicyIsolationCaseCallbackCompletionInputs{
			launch:        &launch,
			first:         &first,
			started:       &started,
			testCaseInput: &testCaseInput,
			c:             &c,
			release:       &release,
			done:          &done,
			calls:         &calls,
		},
	)
}

func testAcquirePolicyIsolationCaseCallbackCompletion(
	t *testing.T,
	captures *testAcquirePolicyIsolationCaseCallbackCompletionInputs,
) {
	t.Helper()
	(*captures.launch)((*captures.first))
	<-(*captures.started)
	for i := range *(*captures.testCaseInput.concurrency) {
		if i != (*captures.first) {
			(*captures.launch)(i)
		}
	}
	cancelCtx, cancel := context.WithCancel(t.Context())
	cancelled := make(chan error, 1)
	cancelCalls := 0
	go func() {
		_, _, _, err := (*captures.c).Blob(
			cancelCtx,
			BlobKey{Content: "shared"},
			&Policy{Retry: fastRetry(3)},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				cancelCalls++
				return ProducerResult{}, nil
			},
		)
		cancelled <- err
	}()
	synctest.Wait()
	cancel()
	{
		err := <-cancelled
		assert.FatalIf(
			t,
			!errors.Is(err, context.Canceled) || cancelCalls != 0,
			"canceled waiter: calls=%d, error=%v",
			cancelCalls,
			err,
		)
	}
	close((*captures.release))
	for range *(*captures.testCaseInput.concurrency) {
		if err := <-(*captures.done); !errors.Is(err, syscall.ECONNRESET) {
			t.Fatal(err)
		}
	}
	for i, got := range *captures.calls {
		want := 3
		if i == 0 {
			want = 1
		}
		assert.FatalIf(t, got != want, "caller %d: calls=%d, want %d", i, got, want)
	}
}

type testAcquirePolicyIsolationCaseCallbackCompletionInputs struct {
	launch        *func(i int)
	first         *int
	started       *chan struct{}
	testCaseInput *testAcquirePolicyIsolationCaseCallbackInputs
	c             **Cache
	release       *chan struct{}
	done          *chan error
	calls         *[]int
}
