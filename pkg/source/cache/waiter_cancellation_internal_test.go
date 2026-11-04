package cache

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/task"
)

func testAcquireSharedWaiterCancellationCase(
	t *testing.T, workers *int,
	disk *bool,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireSharedWaiterCancellationCaseCallback(
			t,
			testAcquireSharedWaiterCancellationCaseCallbackInputs{workers: &workers, disk: &disk},
		)
	},
	)
}

type testAcquireSharedWaiterCancellationCaseCallbackInputs struct {
	workers **int
	disk    **bool
}

func testAcquireSharedWaiterCancellationCaseCallback(
	t *testing.T,
	testCaseInput testAcquireSharedWaiterCancellationCaseCallbackInputs,
) {
	t.Helper()
	root := ""
	if *(*testCaseInput.disk) {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	tasks := task.NewBounded((*(*testCaseInput.workers)))
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var calls atomic.Int64
	fill := func(ctx context.Context, input ProducerInput) (ProducerResult, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return producer([]byte("shared"), &ocispec.Descriptor{})(ctx, input)
	}
	key := BlobKey{Content: "shared-cancel"}
	launch := func(ctx context.Context, done chan<- error) {
		tasks.Go(ctx, "consumer", func(ctx context.Context) {
			reader, _, _, err := c.Blob(ctx, key, Policy{MaxAge: time.Hour}, fill)
			if reader != nil {
				data, readErr := fs.ReadFile(reader, "content")
				err = errors.Join(err, readErr)
				if string(data) != "shared" {
					err = fmt.Errorf("payload=%q, want shared", data)
				}
			}
			done <- err
		})
	}
	survivor, canceled := make(chan error, 1), make(chan error, 1)
	launch(t.Context(), survivor)
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	launch(ctx, canceled)
	synctest.Wait()
	cancel()
	{
		err := <-canceled
		assert.ErrorIf(t, !errors.Is(err, context.Canceled), "canceled waiter=%v", err)
	}
	unblock()
	assert.NoError(t, <-survivor)
	tasks.BlockTillDone()
	assert.Equal(t, calls.Load(), int64(1))
	assert.Equal(t, tasks.Failures(), int64(0))
	assert.NoError(t, c.Close())
	_, _, _, err := c.Blob(ctx, key, Policy{}, fill)
	if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("canceled run reacquired after close: calls=%d error=%v", calls.Load(), err)
	}
}
