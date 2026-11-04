package cache

import (
	"context"
	"errors"
	"io/fs"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func testAcquireConcurrentKeysAndCancellationCase(
	t *testing.T, callers *int,
) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		testAcquireConcurrentKeysAndCancellationCaseCallback(
			t,
			testAcquireConcurrentKeysAndCancellationCaseCallbackInputs{callers: &callers},
		)
	},
	)
}

type testAcquireConcurrentKeysAndCancellationCaseCallbackInputs struct {
	callers **int
}

func testAcquireConcurrentKeysAndCancellationCaseCallback(
	t *testing.T,
	testCaseInput testAcquireConcurrentKeysAndCancellationCaseCallbackInputs,
) {
	t.Helper()
	c := cachetest.New(t, t.TempDir(), New)
	key := BlobKey{Content: "due-canceled-winner"}
	var id string
	{
		_, _, _, err := c.Blob(
			t.Context(),
			key,
			Policy{},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				return ProducerResult{}, connReset()
			},
		)
		assert.FatalIf(t, err == nil, "%s", "want initial failure")
	}

	c.entries.Range(func(key, _ any) bool { id = key.(string); return false })
	time.Sleep(750 * time.Millisecond)
	unlock, err := c.locks.Acquire(t.Context(), id)
	assert.NoError(t, err)

	var calls atomic.Int32
	winnerCtx, cancelWinner := context.WithCancel(t.Context())
	defer cancelWinner()
	started := make(chan struct{})
	winnerFill := func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return ProducerResult{}, ctx.Err()
	}
	liveFill := func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
		calls.Add(1)
		return producer([]byte("complete"), &ocispec.Descriptor{})(ctx, w)
	}

	done := make(chan testtestAcquireConcurrentKeysAndCancellationCaseCallbackResult, (*(*testCaseInput.callers)))
	launch := func(ctx context.Context, fill func(context.Context, ProducerInput) (ProducerResult, error)) {
		go func() {
			r, _, _, err := c.Blob(ctx, key, Policy{Retry: fastRetry(5)}, fill)
			var data string
			if r != nil {
				b, readErr := fs.ReadFile(r, "content")
				data = string(b)
				err = errors.Join(err, readErr)
			}
			done <- testtestAcquireConcurrentKeysAndCancellationCaseCallbackResult{err: err, data: data}
		}()
	}
	launch(winnerCtx, winnerFill)
	synctest.Wait()
	unlock()
	<-started
	for range (*(*testCaseInput.callers)) - 1 {
		launch(t.Context(), liveFill)
	}
	synctest.Wait()
	cancelWinner()
	winnerCanceled, liveResults := 0, 0
	for range *(*testCaseInput.callers) {
		got := <-done
		if errors.Is(got.err, context.Canceled) {
			winnerCanceled++
			continue
		}
		if got.err != nil || got.data != "complete" {
			t.Fatalf("live result=%q, error=%v", got.data, got.err)
		}
		liveResults++
	}
	if winnerCanceled != 1 || liveResults != (*(*testCaseInput.callers))-1 || calls.Load() != 2 {
		t.Fatalf(
			"winner canceled=%d, live=%d, producer calls=%d",
			winnerCanceled,
			liveResults,
			calls.Load(),
		)
	}
}

type testtestAcquireConcurrentKeysAndCancellationCaseCallbackResult struct {
	err  error
	data string
}
