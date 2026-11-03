package cache

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func testAcquireConcurrentKeysAndCancellationDueCase(
	t *testing.T,
	testCaseInput testAcquireConcurrentKeysAndCancellationDueCaseInputs,
) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		testAcquireConcurrentKeysAndCancellationDueCaseCallback(
			t,
			testAcquireConcurrentKeysAndCancellationDueCaseCallbackInputs{testCaseInput: &testCaseInput},
		)
	},
	)
}

type testDecodeRequiredMetadataCaseCaseInputs struct {
	family **struct {
		name     string
		required []string
		fields   map[string]string
	}
	base  *map[string]string
	check *struct {
		name string
		key  string

		value string
	}
}

func testDecodeRequiredMetadataCaseCase(t *testing.T, testCaseInput testDecodeRequiredMetadataCaseCaseInputs) {
	t.Helper()

	a := maps.Clone((*testCaseInput.base))
	if testCaseInput.check.key != "" {
		a[testCaseInput.check.key] = testCaseInput.check.value
	}
	stored := ocispec.Descriptor{Digest: digest.FromString("stored"), Annotations: a}
	if testCaseInput.check.name == "bad stored digest" {
		stored.Digest = "invalid"
	}
	_, _, valid := decode(&stored, (*testCaseInput.family).required)
	assert.FatalIf(t, valid != (testCaseInput.check.name == "valid"), "valid=%v", valid)
}

type testDecodeRequiredMetadataCaseCase2Inputs struct {
	family **struct {
		name     string
		required []string
		fields   map[string]string
	}
	field *string
}

func testDecodeRequiredMetadataCaseCase2(t *testing.T, testCaseInput testDecodeRequiredMetadataCaseCase2Inputs) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{
		Content:  struct{ URL string }{"https://example.test/" + (*testCaseInput.family).name},
		Required: (*testCaseInput.family).required,
	}
	upstream := ocispec.Descriptor{
		Digest:      digest.Digest((*testCaseInput.family).fields[prefix+"upstream-digest"]),
		Annotations: (*testCaseInput.family).fields,
	}
	_, _, id, err := c.Blob(
		t.Context(),
		key,
		Policy{MaxAge: Forever},
		producer([]byte("payload"), &upstream),
	)
	assert.NoError(t, err)

	stored, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	delete(stored.Annotations, prefix+(*testCaseInput.field))
	{
		tagErr := c.store.Tag(t.Context(), stored, id)
		assert.NoError(t, tagErr)
	}
	{
		closeErr4 := c.Close()
		assert.NoError(t, closeErr4)
	}

	c = cachetest.New(t, root, New)
	{
		r, _, failedID, blobErr3 := c.Blob(
			t.Context(),
			key,
			Policy{MaxAge: Forever},
			nil,
		)
		assert.FatalIf(t, r != nil || failedID != "" ||
			!errors.Is(
				blobErr3,
				errdef.ErrNotFound,
			), "invalid restored metadata hit: id=%q, %v", failedID, blobErr3)
	}
	calls := 0
	r, _, restoredID, err := c.Blob(
		t.Context(),
		key,
		Policy{MaxAge: Forever},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			calls++
			return producer([]byte("refilled"), &upstream)(ctx, w)
		},
	)
	assert.FatalIf(
		t,
		err != nil || calls != 1 || restoredID != id,
		"metadata refill: calls=%d id=%q, %v",
		calls,
		restoredID,
		err,
	)
	_, _ = fs.ReadFile(r, "content")
}

type testAcquireConcurrentKeysAndCancellationDueCaseCallbackInputs struct {
	testCaseInput *testAcquireConcurrentKeysAndCancellationDueCaseInputs
}

func testAcquireConcurrentKeysAndCancellationDueCaseCallback(
	t *testing.T,
	testCaseInput testAcquireConcurrentKeysAndCancellationDueCaseCallbackInputs,
) {
	t.Helper()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "due-wave"}
	held, _, id, err := c.Blob(
		t.Context(),
		key,
		Policy{},
		producer([]byte("held"), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	good, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	_, _, _, err = c.Blob(
		t.Context(),
		key,
		Policy{},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			return ProducerResult{}, connReset()
		},
	)
	if _, ok := errors.AsType[*TerminalError](err); !ok {
		t.Fatal(err)
	}
	{
		data, readFileErr4 := fs.ReadFile(held, "content")
		assert.FatalIf(
			t,
			readFileErr4 != nil || string(data) != "held",
			"held=%q, error=%v",
			data,
			readFileErr4,
		)
	}

	before, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	time.Sleep(750 * time.Millisecond)
	if *testCaseInput.testCaseInput.cold {
		{
			closeErr3 := c.Close()
			assert.NoError(t, closeErr3)
		}

		c = cachetest.New(t, root, New)
	}
	unlock, err := c.locks.Acquire(t.Context(), id)
	assert.NoError(t, err)

	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, (*testCaseInput.testCaseInput.callers))
	calls := 0
	var cause error
	switch *testCaseInput.testCaseInput.result {
	case "eligible", "publication":
		cause = connReset()
	case "unsupported":
		cause = timeoutError{}
	case "local":
		cause = &fs.PathError{Op: "write", Path: "sink", Err: fs.ErrPermission}
	}
	fill := func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
		calls++
		if calls == 1 {
			close(started)
		}
		<-release
		if cause != nil {
			return ProducerResult{}, cause
		}
		return producer([]byte("complete"), &ocispec.Descriptor{})(ctx, w)
	}
	launch := func(ctx context.Context, results chan error) {
		go func() {
			r, _, _, blobErr2 := c.Blob(ctx, key, Policy{Retry: fastRetry(5)}, fill)
			if r != nil {
				data, readErr := fs.ReadFile(r, "content")
				blobErr2 = errors.Join(blobErr2, readErr)
				assert.ErrorIf(t, string(data) != "complete", "shared payload=%q", data)
			}
			results <- blobErr2
		}()
	}
	for range *testCaseInput.testCaseInput.callers {
		launch(t.Context(), done)
	}
	synctest.Wait()
	if *testCaseInput.testCaseInput.cold {
		v, ok := c.entries.Load(id)
		assert.FatalIf(
			t,
			!ok || !v.(*entry).unloaded,
			"%s",
			"cold callers did not share an unloaded observation",
		)
	}
	unlock()
	<-started
	ctx, withCancelCancel := context.WithCancel(t.Context())
	canceled := make(chan error, 1)
	launch(ctx, canceled)
	synctest.Wait()
	withCancelCancel()
	{
		innerErr := <-canceled
		assert.FatalIf(t, !errors.Is(innerErr, context.Canceled), "canceled waiter=%v", innerErr)
	}
	time.Sleep(10 * time.Second)
	if (*testCaseInput.testCaseInput.result) == "publication" {
		{
			removeErr := c.root.Remove("oci/index.json")
			assert.NoError(t, removeErr)
		}
		{
			mkdirErr := c.root.Mkdir("oci/index.json", 0o750)
			assert.NoError(t, mkdirErr)
		}
	}
	testAcquireConcurrentKeysAndCancellationDueCaseCallbackCompletion(
		t,
		&testAcquireConcurrentKeysAndCancellationDueCaseCallbackCompletionInputs{
			release:       &release,
			testCaseInput: &testCaseInput,
			done:          &done,
			cause:         &cause,
			calls:         &calls,
			c:             &c,
			id:            &id,
			good:          &good,
			before:        &before,
			key:           &key,
		},
	)
}

func testAcquireConcurrentKeysAndCancellationDueCaseCallbackCompletion(
	t *testing.T,
	captures *testAcquireConcurrentKeysAndCancellationDueCaseCallbackCompletionInputs,
) {
	t.Helper()
	close((*captures.release))
	live := 0
	for range *captures.testCaseInput.testCaseInput.callers {
		innerErr2 := <-(*captures.done)
		assert.FatalIf(t, (*captures.cause) == nil && innerErr2 != nil || (*captures.cause) !=
			nil && !errors.Is(innerErr2, (*captures.cause)) && (*captures.testCaseInput.testCaseInput.result) !=
			"eligible", "shared error=%v, cause=%v", innerErr2, (*captures.cause))
		if (*captures.testCaseInput.testCaseInput.result) == "eligible" ||
			(*captures.testCaseInput.testCaseInput.result) == "publication" {
			assert.FatalIf(
				t,
				!errors.Is(innerErr2, syscall.ECONNRESET),
				"remote cause lost: %v",
				innerErr2,
			)
		}
		if (*captures.testCaseInput.testCaseInput.result) == "publication" {
			{
				_, ok := errors.AsType[*CacheError](innerErr2)
				assert.FatalIf(t, !ok, "publication failure lost: %v", innerErr2)
			}
		}
		terminal, ok := errors.AsType[*TerminalError](innerErr2)
		if (*captures.cause) != nil && (!ok || terminal.Attempted) {
			live++
		}
	}
	testAcquireConcurrentKeysAndCancellationDueCaseCallbackCompletionTail(t, captures, &live)
}

type testAcquireConcurrentKeysAndCancellationDueCaseCallbackCompletionInputs struct {
	release       *chan struct{}
	testCaseInput *testAcquireConcurrentKeysAndCancellationDueCaseCallbackInputs
	done          *chan error
	cause         *error
	calls         *int
	c             **Cache
	id            *string
	good          *ocispec.Descriptor
	before        *ocispec.Descriptor
	key           *BlobKey
}

func testAcquireConcurrentKeysAndCancellationDueCaseCallbackCompletionTail(
	t *testing.T,
	captures *testAcquireConcurrentKeysAndCancellationDueCaseCallbackCompletionInputs,
	live *int,
) {
	t.Helper()
	assert.FatalIf(
		t, (*captures.calls) !=
			1 || (*captures.cause) != nil && (*live) != 1, "due calls=%d, live results=%d", (*captures.calls), (*live),
	)
	after, err := (*captures.c).store.Resolve(t.Context(), (*captures.id))
	assert.NoError(t, err)

	if (*captures.testCaseInput.testCaseInput.result) == "eligible" {
		o, err := (*captures.c).readOutcome(t.Context(), &after)
		assert.FatalIf(t, err != nil || o == nil || o.record.Count != 2 ||
			!o.record.Completed.Equal(time.Now()) ||
			o.lastGood.Digest != captures.good.Digest, "due outcome=%+v, error=%v", o, err)
	} else {
		assert.FatalIf(
			t, (*captures.cause) !=
				nil && after.Digest != captures.before.Digest, "%s",
			"unsupported/local failure changed the durable outcome",
		)
	}

	if (*captures.testCaseInput.testCaseInput.result) != "eligible" {
		if (*captures.testCaseInput.testCaseInput.result) == "publication" {
			{
				err := (*captures.c).root.Remove("oci/index.json")
				assert.NoError(t, err)
			}
		}
		r, _, _, err := (*captures.c).Blob(
			t.Context(), (*captures.key), Policy{},
			producer([]byte("independent"), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)

		_, _ = fs.ReadFile(r, "content")
	}
}
