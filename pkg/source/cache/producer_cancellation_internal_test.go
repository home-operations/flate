package cache

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testAcquireCancelStopsContextIgnoringProducerCase(
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
		testAcquireCancelStopsContextIgnoringProducerCaseCallback(
			t,
			testAcquireCancelStopsContextIgnoringProducerCaseCallbackInputs{
				concurrency: &concurrency,
				backing:     &backing,
				c:           &c,
			},
		)
	},
	)
}

type testAcquireCancelStopsContextIgnoringProducerCaseCallbackInputs struct {
	concurrency **int
	backing     **string
	c           **Cache
}

func testAcquireCancelStopsContextIgnoringProducerCaseCallback(
	t *testing.T,
	testCaseInput testAcquireCancelStopsContextIgnoringProducerCaseCallbackInputs,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, write := make(chan struct{}), make(chan struct{})
	done := make(chan error, (*(*testCaseInput.concurrency)))
	callbacks := make([]*archiveCancelContext, (*(*testCaseInput.concurrency)))
	for i := range *(*testCaseInput.concurrency) {
		callbacks[i] = &archiveCancelContext{
			Context: ctx,
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		go func() {
			_, _, _, err := (*testCaseInput.c).Acquire(
				callbacks[i],
				ArchiveKey{Content: fmt.Sprintf("source-ignore-context-%d", i)},
				Policy{MaxAge: 0},
				func(_ context.Context, w ProducerInput) (ProducerResult, error) {
					tw := tar.NewWriter(w)
					if err := tw.WriteHeader(
						&tar.Header{Name: "file", Size: 1 << 30, Mode: 0o600},
					); err != nil {
						return ProducerResult{}, err
					}
					started <- struct{}{}
					<-write
					var chunk [512]byte
					_, err := tw.Write(chunk[:])
					return ProducerResult{}, err
				},
			)
			done <- err
		}()
	}
	for range *(*testCaseInput.concurrency) {
		<-started
	}
	cancel()
	for _, callback := range callbacks {
		<-callback.entered
	}
	close(write)
	synctest.Wait()
	completed := 0
	select {
	case <-done:
		completed++
		t.Error("acquisition returned before both callback pipe closes")
	default:
	}
	for _, callback := range callbacks {
		close(callback.release)
	}
	for range (*(*testCaseInput.concurrency)) - completed {
		{
			err := <-done
			assert.ErrorIf(
				t,
				!errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux),
				"cancellation: %v",
				err,
			)
		}
	}
	if (*(*testCaseInput.backing)) == "disk" {
		for i := range *(*testCaseInput.concurrency) {
			{
				_, err := (*testCaseInput.c).store.Resolve(
					t.Context(),
					fmt.Sprintf("source-ignore-context-%d", i),
				)
				assert.ErrorIf(t, !errors.Is(
					err,
					errdef.ErrNotFound,
				), "interrupted archive published: %v", err)
			}
		}
	}
}
