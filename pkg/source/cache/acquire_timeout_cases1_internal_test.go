package cache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testAcquireAdmissionCase(t *testing.T, mode *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireAdmissionCaseCallback(t, testAcquireAdmissionCaseCallbackInputs{mode: &mode})
	},
	)
}

func testAcquireAdmissionFailure(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	admissions, productions := 0, 0
	cause := connReset()
	_, _, id, err := c.Blob(
		t.Context(),
		BlobKey{Content: "admission-failure"},
		Policy{Retry: fastRetry(3), Admit: func(context.Context) (func(), error) {
			admissions++
			return nil, cause
		}},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			productions++
			return ProducerResult{}, nil
		},
	)
	{
		_, ok := errors.AsType[*CacheError](
			err,
		)
		assert.FatalIf(
			t,
			!ok || !errors.Is(err, cause) || !errors.Is(err, syscall.ECONNRESET) || !errors.Is(err, manifest.ErrFlux) || id != "" || admissions != 1 ||
				productions != 0,
			"admission failure: admissions=%d productions=%d id=%q error=%v",
			admissions,
			productions,
			id,
			err,
		)
	}
	{
		_, ok := errors.AsType[*producerError](err)
		assert.FatalIf(t, ok, "%s", "admission failure classified as producer failure")
	}

	c.entries.Range(func(_, value any) bool {
		if value.(*entry).outcome != nil || value.(*entry).stored.Digest != "" {
			t.Error("admission failure persisted an outcome")
		}
		return true
	})
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	c = cachetest.New(t, root, New)
	{
		_, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "admission-failure"},
			Policy{},
			producer([]byte("complete"), &ocispec.Descriptor{}),
		)
		assert.FatalIf(t, err != nil, "admission failure blocked later production: %v", err)
	}
}

func testAcquireAdmissionStagingFailure(t *testing.T) {
	t.Helper()

	c, err := New(t.Context(), t.TempDir())
	assert.NoError(t, err)

	t.Cleanup(func() {
		if closeErr4 := c.Close(); closeErr4 != nil && !errors.Is(closeErr4, os.ErrClosed) {
			t.Error(closeErr4)
		}
	})
	releases, productions := 0, 0
	_, _, _, err = c.Blob(
		t.Context(),
		BlobKey{Content: "staging-admission"},
		Policy{Admit: func(context.Context) (func(), error) {
			{
				closeErr5 := c.root.Close()
				assert.NoError(t, closeErr5)
			}

			return func() { releases++ }, nil
		}},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			productions++
			return ProducerResult{}, nil
		},
	)
	assert.FatalIf(
		t,
		!errors.Is(err, os.ErrClosed) || releases != 1 || productions != 0,
		"staging failure: productions=%d releases=%d error=%v",
		productions,
		releases,
		err,
	)
}

func testAcquireAdmissionPersistedReplay(t *testing.T) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		admissions, releases := 0, 0
		policy := Policy{Admit: func(context.Context) (func(), error) {
			admissions++
			return func() { releases++ }, nil
		}}
		key := BlobKey{Content: "replay-admission"}
		_, _, _, err := c.Blob(
			t.Context(),
			key,
			policy,
			func(context.Context, ProducerInput) (ProducerResult, error) {
				return ProducerResult{}, connReset()
			},
		)
		assert.FatalIf(
			t,
			!errors.Is(err, syscall.ECONNRESET) || admissions != 1 || releases != 1,
			"live outcome: admissions=%d releases=%d error=%v",
			admissions,
			releases,
			err,
		)
		{
			closeErr6 := c.Close()
			assert.NoError(t, closeErr6)
		}

		c = cachetest.New(t, root, New)
		_, _, _, err = c.Blob(
			t.Context(),
			key,
			policy,
			func(context.Context, ProducerInput) (ProducerResult, error) {
				t.Error("persisted outcome invoked producer")
				return ProducerResult{}, nil
			},
		)
		assert.FatalIf(
			t,
			!errors.Is(err, syscall.ECONNRESET) || admissions != 1 || releases != 1,
			"outcome replay admitted: admissions=%d releases=%d error=%v",
			admissions,
			releases,
			err,
		)
	})
}

type testAcquireAdmissionCaseCallbackInputs struct {
	mode **string
}

func testAcquireAdmissionCaseCallback(t *testing.T, testCaseInput testAcquireAdmissionCaseCallbackInputs) {
	t.Helper()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if (*(*testCaseInput.mode)) == "deadline" {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, 2*time.Second)
		defer stop()
	}
	parentDeadline, parentHasDeadline := ctx.Deadline()
	entered, admit := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	admissions, releases, productions := 0, 0, 0
	timeout := time.Second
	policy := Policy{Timeout: &timeout, MaxAge: Forever, Admit: func(ctx context.Context) (func(), error) {
		admissions++
		{
			deadline, ok := ctx.Deadline()
			assert.ErrorIf(
				t,
				ok != parentHasDeadline || !deadline.Equal(parentDeadline),
				"admission inherited operation deadline: %v present=%t",
				deadline,
				ok,
			)
		}
		close(entered)
		select {
		case <-admit:
			return func() { releases++ }, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	key := BlobKey{Content: "admitted"}
	go func() {
		_, _, _, err := c.Blob(
			ctx,
			key,
			policy,
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				productions++
				{
					deadline, ok := ctx.Deadline()
					assert.ErrorIf(
						t,
						!ok || time.Until(deadline) != timeout,
						"production deadline=%v present=%t",
						deadline,
						ok,
					)
				}
				w.payload.discard = func() error {
					if releases != 0 {
						t.Error("admission released before successful cleanup")
					}
					return nil
				}
				return producer([]byte("complete"), &ocispec.Descriptor{})(ctx, w)
			},
		)
		done <- err
	}()
	<-entered
	synctest.Wait()
	time.Sleep(2 * time.Second)
	synctest.Wait()
	entries, err := os.ReadDir(filepath.Join(root, ociDir))
	assert.NoError(t, err)

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "acquire-") {
			t.Error("queued admission created staged storage")
		}
	}
	testAcquireAdmissionCaseCallbackCompletion(
		t,
		&testAcquireAdmissionCaseCallbackCompletionInputs{
			testCaseInput: &testCaseInput,
			done:          &done,
			admit:         &admit,
			cancel:        &cancel,
			err:           &err,
			productions:   &productions,
			releases:      &releases,
			timeout:       &timeout,
			c:             &c,
			key:           &key,
			policy:        &policy,
			admissions:    &admissions,
		},
	)
}

func testAcquireAdmissionCaseCallbackCompletion(
	t *testing.T,
	captures *testAcquireAdmissionCaseCallbackCompletionInputs,
) {
	t.Helper()
	switch *(*captures.testCaseInput.mode) {
	case "success":
		select {
		case innerErr3 := <-(*captures.done):
			t.Fatalf("operation budget expired in queue: %v", innerErr3)
		default:
		}
		close((*captures.admit))
	case "canceled":
		(*captures.cancel)()
	}
	(*captures.err) = <-(*captures.done)
	if (*(*captures.testCaseInput.mode)) == "success" {
		assert.FatalIf(
			t, (*captures.err) !=
				nil || (*captures.productions) != 1 || (*captures.releases) != 1, "admitted production: calls=%d releases=%d error=%v", (*captures.productions), (*captures.releases), (*captures.err),
		)
		(*captures.timeout) = 0
		{
			_, _, _, blobErr := (*captures.c).Blob(
				t.Context(), (*captures.key), (*captures.policy), producer(nil, &ocispec.Descriptor{}),
			)
			assert.FatalIf(t, blobErr != nil || (*captures.admissions) != 1 || (*captures.releases) !=
				1, "warm hit admitted: admissions=%d releases=%d error=%v", (*captures.admissions), (*captures.releases), blobErr)
		}
	} else {
		want := context.Canceled
		if (*(*captures.testCaseInput.mode)) == "deadline" {
			want = context.DeadlineExceeded
		}
		assert.FatalIf(
			t,
			!errors.Is((*captures.err), want) || (*captures.productions) != 0 || (*captures.releases) != 0,
			"queued cancellation: calls=%d releases=%d error=%v",
			(*captures.productions),
			(*captures.releases),
			(*captures.err),
		)
	}
}

type testAcquireAdmissionCaseCallbackCompletionInputs struct {
	testCaseInput *testAcquireAdmissionCaseCallbackInputs
	done          *chan error
	admit         *chan struct{}
	cancel        *context.CancelFunc
	err           *error
	productions   *int
	releases      *int
	timeout       *time.Duration
	c             **Cache
	key           *BlobKey
	policy        *Policy
	admissions    *int
}
