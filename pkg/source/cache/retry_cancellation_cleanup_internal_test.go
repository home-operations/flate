package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testAcquireRetryCancellationCase(t *testing.T, concurrency *int,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", New)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, (*concurrency))
		calls := make([]int, (*concurrency))
		for i := range *concurrency {
			go func() {
				_, _, _, err := c.Blob(
					ctx,
					BlobKey{Content: fmt.Sprintf("cancel-%d", i)},
					&Policy{Retry: RetryConfig{Attempts: 3, MinWait: time.Hour}},
					func(context.Context, ProducerInput) (ProducerResult, error) {
						calls[i]++
						return ProducerResult{}, connReset()
					},
				)
				done <- err
			}()
		}
		synctest.Wait()
		cancel()
		for range *concurrency {
			err := <-done
			terminal, ok := errors.AsType[*TerminalError](err)
			assert.ErrorIf(t, !ok || !terminal.Attempted || !errors.Is(err, context.Canceled) ||
				!errors.Is(err, syscall.ECONNRESET) ||
				!errors.Is(err, manifest.ErrFlux), "retry cancellation: %v", err)
		}
		for i := range *concurrency {
			_, _, _, err := c.Blob(
				ctx,
				BlobKey{Content: fmt.Sprintf("cancel-%d", i)},
				&Policy{Retry: fastRetry(3)},
				func(context.Context, ProducerInput) (ProducerResult, error) {
					calls[i]++
					return ProducerResult{}, nil
				},
			)
			if calls[i] != 1 || !errors.Is(err, context.Canceled) {
				t.Errorf("pre-cancel calls=%d, error=%v", calls[i], err)
			}
		}
	})
}

func testAcquireReaderFailureCancelsAttemptCase(
	t *testing.T, concurrency *int,
	backing *string,
) {
	t.Helper()

	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	synctest.Test(t, func(t *testing.T) {
		testAcquireReaderFailureCancelsAttemptCaseCallback(
			t,
			testAcquireReaderFailureCancelsAttemptCaseCallbackInputs{concurrency: &concurrency, c: &c},
		)
	},
	)
}

func testAcquireCleanupFailureStopsRetryCase(
	t *testing.T, concurrency *int,
	kind *string,
) {
	t.Helper()

	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, (*concurrency))
	calls := make([]int, (*concurrency))
	failure := connReset()
	for i := range *concurrency {
		c, err := New(t.Context(), t.TempDir())
		assert.NoError(t, err)

		t.Cleanup(func() {
			if err := c.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				t.Error(err)
			}
		})
		go func() {
			testAcquireCleanupFailureStopsRetryCaseCallback(
				testAcquireCleanupFailureStopsRetryCaseCallbackInputs{
					t:       &t,
					kind:    &kind,
					started: &started,
					release: &release,
					done:    &done,
					calls:   &calls,
					failure: &failure,
					i:       &i,
					c:       &c,
				},
			)
		}()
	}
	for range *concurrency {
		<-started
	}
	close(release)
	for range *concurrency {
		err := <-done
		cacheErr, ok := errors.AsType[*CacheError](err)
		if !ok || !errors.Is(err, failure) || !errors.Is(err, os.ErrClosed) ||
			!errors.Is(err, manifest.ErrFlux) {
			t.Errorf("cleanup error=%v", err)
			continue
		}
		joined, ok := cacheErr.Err.(interface{ Unwrap() []error })
		if !ok || len(joined.Unwrap()) < 2 || !errors.Is(joined.Unwrap()[0], failure) {
			t.Errorf("operation must precede cleanup: %v", err)
		} else {
			for _, cleanup := range joined.Unwrap()[1:] {
				assert.ErrorIf(t, errors.Is(cleanup, failure), "operation duplicated in cleanup: %v", err)
			}
		}
	}
	for i, count := range calls {
		assert.ErrorIf(t, count != 1, "producer %d retried after cleanup failure: %d", i, count)
	}
}

type testAcquireReaderFailureCancelsAttemptCaseCallbackInputs struct {
	concurrency **int
	c           **Cache
}

func testAcquireReaderFailureCancelsAttemptCaseCallback(
	t *testing.T,
	testCaseInput testAcquireReaderFailureCancelsAttemptCaseCallbackInputs,
) {
	t.Helper()
	canceled, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, (*(*testCaseInput.concurrency)))
	calls := make([]int, (*(*testCaseInput.concurrency)))
	for i := range *(*testCaseInput.concurrency) {
		go func() {
			_, _, _, err := (*testCaseInput.c).Acquire(
				t.Context(),
				ArchiveKey{Content: fmt.Sprintf("invalid-%d", i)},
				&Policy{Retry: fastRetry(3)},
				func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					calls[i]++
					_, _ = w.Write(bytes.Repeat([]byte("x"), 512))
					<-ctx.Done()
					canceled <- struct{}{}
					<-release
					return ProducerResult{}, errors.Join(ctx.Err(), connReset())
				},
			)
			done <- err
		}()
	}
	for range *(*testCaseInput.concurrency) {
		<-canceled
	}
	synctest.Wait()
	completed := 0
	select {
	case <-done:
		completed++
		t.Error("reader failure returned before producer completion")
	default:
	}
	close(release)
	for range (*(*testCaseInput.concurrency)) - completed {
		err := <-done
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.ErrorIf(t, !ok || !errors.Is(err, tar.ErrHeader), "reader error=%v", err)
		}
	}
	for i, count := range calls {
		assert.ErrorIf(t, count != 1, "producer %d: calls=%d", i, count)
	}
}

type testAcquireCleanupFailureStopsRetryCaseCallbackInputs struct {
	t       **testing.T
	kind    **string
	started *chan struct{}
	release *chan struct{}
	done    *chan error
	calls   *[]int
	failure *error
	i       *int
	c       **Cache
}

func testAcquireCleanupFailureStopsRetryCaseCallback(
	testCaseInput testAcquireCleanupFailureStopsRetryCaseCallbackInputs,
) {
	fail := func() error {
		(*testCaseInput.calls)[(*testCaseInput.i)]++
		(*testCaseInput.started) <- struct{}{}
		<-(*testCaseInput.release)
		return errors.Join((*testCaseInput.failure), (*testCaseInput.c).root.Close())
	}
	var err error
	switch *(*testCaseInput.kind) {
	case "git":
		_, _, err = (*testCaseInput.c).Git((*testCaseInput.t).
			Context(), GitKey{Content: "cleanup"},
			&Policy{Retry: fastRetry(3)},
			func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
				return plumbing.ZeroHash, fail()
			},
		)
	case "archive":
		_, _, _, err = (*testCaseInput.c).Acquire((*testCaseInput.t).
			Context(), ArchiveKey{Content: "cleanup"},
			&Policy{Retry: fastRetry(3)},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				return ProducerResult{}, fail()
			},
		)
	case "blob":
		_, _, _, err = (*testCaseInput.c).Blob((*testCaseInput.t).
			Context(), BlobKey{Content: "cleanup"},
			&Policy{Retry: fastRetry(3)},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				return ProducerResult{}, fail()
			},
		)
	}
	(*testCaseInput.done) <- err
}
