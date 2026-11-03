package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func testAcquireCancelAfterArchiveWorkersCase(
	t *testing.T, backing *string,
	result *string,
) {
	t.Helper()

	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	data := orderedArchive(t, "file", "same")
	want := ocispec.Descriptor{
		Digest:      digest.FromString("upstream"),
		Size:        123,
		Annotations: map[string]string{"flate.revision": "original"},
	}
	keys := []ArchiveKey{{Content: "completion-one"}, {Content: "completion-two"}}
	ids, pins := make([]string, len(keys)), make([]string, len(keys))
	previous := make([]struct {
		resolved time.Time
		stored   ocispec.Descriptor
	}, len(keys))
	for i, key := range keys {
		input, _, id, err := c.Acquire(t.Context(), key, Policy{}, producer(data, &want))
		assert.NoError(t, err)
		ids[i], pins[i] = id, AcquisitionIdentity(input)
		v, _ := c.entries.Load(id)
		e := v.(*entry)
		previous[i].resolved, previous[i].stored = e.resolved, e.stored
	}
	synctest.Test(t, func(t *testing.T) {
		testAcquireCancelAfterArchiveWorkersCaseCallback(
			t,
			testAcquireCancelAfterArchiveWorkersCaseCallbackInputs{
				result: &result,
				c:      &c,
				data:   &data,
				want:   &want,
				keys:   &keys,
			},
		)
	},
	)
	unexpected := func(context.Context, ProducerInput) (ProducerResult, error) {
		t.Error("retained generation called producer")
		return ProducerResult{}, errors.New("unexpected refetch")
	}
	for i, key := range keys {
		v, _ := c.entries.Load(ids[i])
		e := v.(*entry)
		if !e.resolved.Equal(previous[i].resolved) || e.outcome != nil ||
			cmp.Diff(previous[i].stored, e.stored) != "" {
			t.Errorf(
				"canceled attempt replaced original generation: resolved=%v, want %v",
				e.resolved,
				previous[i].resolved,
			)
		}
		if (*backing) == "disk" {
			stored, err := c.store.Resolve(t.Context(), ids[i])
			assert.NoError(t, err)
			if diff := cmp.Diff(previous[i].stored, stored); diff != "" {
				t.Error(diff)
			}
			payload, err := c.root.ReadFile(blobPath(stored.Digest))
			assert.FatalIf(
				t,
				err != nil || !bytes.Equal(payload, data),
				"retained archive payload: %v",
				err,
			)
		}
		input, desc, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, unexpected)
		assert.FatalIf(t, err != nil || id != ids[i] || AcquisitionIdentity(input) != pins[i] ||
			cmp.Diff(want, desc) != "", "retained acquisition: identity=%q, error=%v", id, err)
		{
			content, err := fs.ReadFile(input, "file")
			assert.FatalIf(
				t,
				err != nil || string(content) != "same",
				"retained content: %q, %v",
				content,
				err,
			)
		}
	}
	testAcquireCancelAfterArchiveWorkersCaseCompletion(
		t,
		&testAcquireCancelAfterArchiveWorkersCaseCompletionInputs{
			backing:    &backing,
			c:          &c,
			root:       &root,
			keys:       &keys,
			unexpected: &unexpected,
			ids:        &ids,
			pins:       &pins,
			previous:   &previous,
			want:       &want,
		},
	)
}

func testAcquireCancelAfterArchiveWorkersCaseCompletion(
	t *testing.T,
	captures *testAcquireCancelAfterArchiveWorkersCaseCompletionInputs,
) {
	t.Helper()
	if (*(*captures.backing)) == "disk" {
		assert.NoError(t, (*captures.c).Close())
		(*captures.c) = cachetest.New(t, (*captures.root), New)
	}
	for i, key := range *captures.keys {
		input, _, id, err := (*captures.c).Acquire(t.Context(), key, Policy{MaxAge: Forever}, (*captures.unexpected))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			id != (*captures.ids)[i] || AcquisitionIdentity(input) != (*captures.pins)[i],
			"%s",
			"reopened generation changed identity",
		)

		v, _ := (*captures.c).entries.Load(id)
		assert.FatalIf(
			t,
			!v.(*entry).resolved.Equal((*captures.previous)[i].resolved),
			"%s",
			"canceled attempt renewed resolution time",
		)

		input, _, id, err = (*captures.c).Acquire(
			t.Context(),
			key,
			Policy{},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				prior, previousErr := w.Previous(ctx)
				if previousErr != nil || prior == nil || cmp.Diff((*captures.want), *prior) != "" {
					t.Errorf("forced refresh lost previous descriptor: %+v, %v", prior, previousErr)
					return ProducerResult{}, manifest.ErrInput
				}
				return ProducerResult{Descriptor: *prior, Unchanged: true}, nil
			},
		)
		assert.FatalIf(
			t,
			err != nil || id != (*captures.ids)[i] || AcquisitionIdentity(input) != (*captures.pins)[i],
			"forced refresh: identity=%q, error=%v",
			id,
			err,
		)
		{
			content, err := fs.ReadFile(input, "file")
			assert.FatalIf(
				t,
				err != nil || string(content) != "same",
				"forced refresh content: %q, %v",
				content,
				err,
			)
		}
	}
}

func testAcquireUnchangedRefreshRenewsCase(t *testing.T, scenario *string,
) {
	t.Helper()

	c := cachetest.New(t, "", New)
	key := ArchiveKey{Content: (*scenario)}
	if (*scenario) != "missing previous" {
		_, _, _, err := c.Acquire(
			t.Context(),
			key,
			Policy{},
			producer(orderedArchive(t, "file", "same"), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)
	}
	input, _, id, err := c.Acquire(
		t.Context(),
		key,
		Policy{},
		func(callbackArg1 context.Context, w ProducerInput) (ProducerResult, error) {
			return testAcquireUnchangedRefreshRenewsCaseCallback(
				callbackArg1,
				w,
				testAcquireUnchangedRefreshRenewsCaseCallbackInputs{t: &t, scenario: &scenario},
			)
		},
	)
	want := manifest.ErrInput
	if (*scenario) == "producer error" {
		want = fs.ErrPermission
	}
	if input != nil || id != "" || !errors.Is(err, want) || !errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("invalid unchanged result: filesystem=%v identity=%q error=%v", input != nil, id, err)
	}
}

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

func testAcquireTrailingFailureKeepsCacheIdentityCase(
	t *testing.T, backing *string,
) {
	t.Helper()

	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	var payloadTar bytes.Buffer
	testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
	data := payloadTar.Bytes()
	trailingWriteFailed := false
	_, _, _, err := c.Acquire(
		t.Context(),
		ArchiveKey{Content: "source-trailing-failure"},
		Policy{MaxAge: 0},
		func(_ context.Context, w ProducerInput) (ProducerResult, error) {
			if _, err := w.Write(data); err != nil {
				return ProducerResult{}, err
			}
			trailing := make([]byte, 32<<10)
			for {
				if _, err := w.Write(trailing); err != nil {
					trailingWriteFailed = true
					return ProducerResult{}, err
				}
			}
		},
	)
	assert.FatalIf(t, !trailingWriteFailed, "%s", "trailing write did not fail")

	{
		_, ok := errors.AsType[*CacheError](err)
		assert.FatalIf(t, !ok, "%T %v, want *CacheError", err, err)
	}
}

type testAcquireCancelAfterArchiveWorkersCaseCallbackInputs struct {
	result **string
	c      **Cache
	data   *[]byte
	want   *ocispec.Descriptor
	keys   *[]ArchiveKey
}

func testAcquireCancelAfterArchiveWorkersCaseCallback(
	t *testing.T,
	testCaseInput testAcquireCancelAfterArchiveWorkersCaseCallbackInputs,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, produce := make(chan struct{}), make(chan struct{})
	done := make(chan struct{}, len((*testCaseInput.keys)))
	callbacks := make([]*archiveCompletionContext, len((*testCaseInput.keys)))
	for i, key := range *testCaseInput.keys {
		callback := &archiveCompletionContext{
			Context: ctx,
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		callbacks[i] = callback
		go func() {
			input, _, id, err := (*testCaseInput.c).Acquire(
				callback,
				key,
				Policy{},
				func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					prior, err := w.Previous(ctx)
					assert.ErrorIf(
						t,
						err != nil || prior == nil || cmp.Diff((*testCaseInput.want), *prior) != "",
						"previous descriptor: %+v, %v",
						prior,
						err,
					)
					child := ctx.Done()
					callback.child.Store(&child)
					started <- struct{}{}
					<-produce
					if (*(*testCaseInput.result)) == "fresh" {
						return producer((*testCaseInput.data), testCaseInput.want)(ctx, w)
					}
					return ProducerResult{Descriptor: (*testCaseInput.want), Unchanged: true}, nil
				},
			)
			if input != nil || id != "" || !errors.Is(err, context.Canceled) ||
				!errors.Is(err, manifest.ErrFlux) {
				var contents []byte
				if input != nil {
					contents, _ = fs.ReadFile(input, "file")
				}
				t.Errorf(
					"post-worker cancellation: filesystem=%v identity=%q contents=%q error=%v",
					input != nil,
					id,
					contents,
					err,
				)
			}
			done <- struct{}{}
		}()
	}
	for range *testCaseInput.keys {
		<-started
	}
	close(produce)
	synctest.Wait()
	for _, callback := range callbacks {
		select {
		case <-callback.entered:
		default:
			t.Error("post-worker cancellation checkpoint unreachable")
		}
	}
	cancel()
	for _, callback := range callbacks {
		close(callback.release)
	}
	for range *testCaseInput.keys {
		<-done
	}
}

type testAcquireUnchangedRefreshRenewsCaseCallbackInputs struct {
	t        **testing.T
	scenario **string
}

func testAcquireUnchangedRefreshRenewsCaseCallback(
	_ context.Context,
	w ProducerInput,
	testCaseInput testAcquireUnchangedRefreshRenewsCaseCallbackInputs,
) (ProducerResult, error) {
	if (*(*testCaseInput.scenario)) == "producer error" {
		return ProducerResult{Unchanged: true}, fs.ErrPermission
	}
	if (*(*testCaseInput.scenario)) == "wrote bytes" {
		_, err := w.Write(orderedArchive((*testCaseInput.t), "file", "new"))
		if err != nil {
			return ProducerResult{}, err
		}
	}
	return ProducerResult{Unchanged: true}, nil
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

type testAcquireCancelAfterArchiveWorkersCaseCompletionInputs struct {
	backing    **string
	c          **Cache
	root       *string
	keys       *[]ArchiveKey
	unexpected *func(context.Context, ProducerInput) (ProducerResult, error)
	ids        *[]string
	pins       *[]string
	previous   *[]struct {
		resolved time.Time
		stored   ocispec.Descriptor
	}
	want *ocispec.Descriptor
}
