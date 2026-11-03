package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
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
					Policy{Retry: RetryConfig{Attempts: 3, MinWait: time.Hour}},
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
				Policy{Retry: fastRetry(3)},
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

func testGitRetryDiscardsGenerationCase(t *testing.T, disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	calls := 0
	g, _, err := c.Git(
		t.Context(),
		GitKey{Content: "partial-git", RecurseSubmodules: true},
		Policy{MaxAge: Forever, Retry: fastRetry(2)},
		func(callbackArg1 context.Context, callbackArg2 storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
			return testGitRetryDiscardsGenerationCaseCallback(
				callbackArg1,
				callbackArg2,
				worktree,
				testGitRetryDiscardsGenerationCaseCallbackInputs{t: &t, disk: &disk, c: &c, calls: &calls},
			)
		},
	)
	assert.FatalIf(t, err != nil || g == nil || calls != 2, "generation=%v, calls=%d, error=%v", g, calls, err)
	held, _, err := c.Git(
		t.Context(),
		GitKey{Content: "partial-git", RecurseSubmodules: true},
		Policy{MaxAge: Forever},
		func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
			calls++
			return plumbing.ZeroHash, nil
		},
	)
	assert.FatalIf(
		t,
		err != nil || held != g || calls != 2,
		"warm generation=%v, calls=%d, error=%v",
		held,
		calls,
		err,
	)
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

type testAcquireRetrySequencePersistenceCaseInputs struct {
	disk *bool
	tc   *struct {
		name  string
		cause error

		attempts int
		record   bool
	}
}

func testAcquireRetrySequencePersistenceCase(
	t *testing.T,
	testCaseInput testAcquireRetrySequencePersistenceCaseInputs,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireRetrySequencePersistenceCaseCallback(
			t,
			testAcquireRetrySequencePersistenceCaseCallbackInputs{testCaseInput: &testCaseInput},
		)
	},
	)
}

type testAcquireNestedTerminalDueCeilingCaseInputs struct {
	callers *int
	child   *string
}

func testAcquireNestedTerminalDueCeilingCase(
	t *testing.T,
	testCaseInput testAcquireNestedTerminalDueCeilingCaseInputs,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireNestedTerminalDueCeilingCaseCallback(
			t,
			testAcquireNestedTerminalDueCeilingCaseCallbackInputs{testCaseInput: &testCaseInput},
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
				Policy{MaxAge: time.Hour, Retry: fastRetry(attempts)},
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
			Policy{Retry: fastRetry(3)},
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

type testGitRetryDiscardsGenerationCaseCallbackInputs struct {
	t     **testing.T
	disk  **bool
	c     **Cache
	calls *int
}

func testGitRetryDiscardsGenerationCaseCallback(
	_ context.Context,
	_ storage.Storer,
	worktree billy.Filesystem,
	testCaseInput testGitRetryDiscardsGenerationCaseCallbackInputs,
) (plumbing.Hash, error) {
	(*testCaseInput.calls)++
	{
		_, err := worktree.Stat("partial")
		assert.FatalIf((*testCaseInput.t), !errors.Is(err, fs.ErrNotExist),
			"attempt inherited partial worktree: %v",
			err,
		)
	}
	if (*testCaseInput.calls) == 1 {
		f, err := worktree.Create("partial")
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if err := f.Close(); err != nil {
			return plumbing.ZeroHash, err
		}
		return plumbing.ZeroHash, connReset()
	}
	if *(*testCaseInput.disk) {
		entries, err := os.ReadDir(filepath.Dir(filepath.Join((*testCaseInput.c).root.Name(), gitObjectsPath(""))))
		assert.FatalIf((*testCaseInput.t), err != nil || len(entries) != 1,
			"failed generation remains: %v, %v",
			entries,
			err,
		)
	}
	return plumbing.NewHash(strings.Repeat("1", 40)), nil
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
				Policy{Retry: fastRetry(3)},
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
			Policy{Retry: fastRetry(3)},
			func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
				return plumbing.ZeroHash, fail()
			},
		)
	case "archive":
		_, _, _, err = (*testCaseInput.c).Acquire((*testCaseInput.t).
			Context(), ArchiveKey{Content: "cleanup"},
			Policy{Retry: fastRetry(3)},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				return ProducerResult{}, fail()
			},
		)
	case "blob":
		_, _, _, err = (*testCaseInput.c).Blob((*testCaseInput.t).
			Context(), BlobKey{Content: "cleanup"},
			Policy{Retry: fastRetry(3)},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				return ProducerResult{}, fail()
			},
		)
	}
	(*testCaseInput.done) <- err
}

type testAcquireRetrySequencePersistenceCaseCallbackInputs struct {
	testCaseInput *testAcquireRetrySequencePersistenceCaseInputs
}

func testAcquireRetrySequencePersistenceCaseCallback(
	t *testing.T,
	testCaseInput testAcquireRetrySequencePersistenceCaseCallbackInputs,
) {
	t.Helper()
	root := ""
	if *testCaseInput.testCaseInput.disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	calls := 0
	for i := range 2 {
		_, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "eligibility"},
			Policy{Retry: fastRetry(3)},
			func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				_, _ = io.WriteString(w, "partial")
				return ProducerResult{}, testCaseInput.testCaseInput.tc.cause
			},
		)
		want := testCaseInput.testCaseInput.tc.attempts * (i + 1)
		if (*testCaseInput.testCaseInput.disk) && testCaseInput.testCaseInput.tc.record {
			want = 1
		}
		assert.FatalIf(
			t,
			calls != want || err == nil || !errors.Is(err, manifest.ErrFlux),
			"calls=%d, want=%d, error=%v",
			calls,
			want,
			err,
		)
		if !(*testCaseInput.testCaseInput.disk) || !testCaseInput.testCaseInput.tc.record || i == 0 {
			assert.FatalIf(t, !errors.Is(err, testCaseInput.testCaseInput.tc.cause), "live cause lost: %v", err)
		}
	}
	if *testCaseInput.testCaseInput.disk {
		desc, err := c.store.Resolve(
			t.Context(),
			manifest.Fingerprint(
				struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"eligibility"}},
			),
		)
		assert.FatalIf(
			t, testCaseInput.testCaseInput.tc.
				record && (err != nil || desc.MediaType != ocispec.MediaTypeImageManifest) ||
				!testCaseInput.testCaseInput.tc.record &&
					!errors.Is(err, errdef.ErrNotFound), "eligibility descriptor=%+v, error=%v",
			desc,
			err,
		)
		files, err := os.ReadDir(filepath.Join(c.root.Name(), ociDir))
		assert.NoError(t, err)

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "acquire-") {
				t.Fatalf("failed sink remains: %s", file.Name())
			}
		}
	}
}

type testAcquireNestedTerminalDueCeilingCaseCallbackInputs struct {
	testCaseInput *testAcquireNestedTerminalDueCeilingCaseInputs
}

func testAcquireNestedTerminalDueCeilingCaseCallback(
	t *testing.T,
	testCaseInput testAcquireNestedTerminalDueCeilingCaseCallbackInputs,
) {
	t.Helper()
	c := cachetest.New(t, t.TempDir(), New)
	fail := func(context.Context, ProducerInput) (ProducerResult, error) {
		return ProducerResult{}, connReset()
	}
	_, _, outerID, err := c.Blob(
		t.Context(),
		BlobKey{Content: "outer"},
		Policy{},
		producer(nil, &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	for _, id := range []string{"outer", "inner"} {
		if id == "inner" && (*testCaseInput.testCaseInput.child) != "due" {
			continue
		}
		if _, _, _, blobErr := c.Blob(
			t.Context(),
			BlobKey{Content: id},
			Policy{},
			fail,
		); !errors.Is(
			blobErr,
			syscall.ECONNRESET,
		) {
			t.Fatal(blobErr)
		}
	}
	time.Sleep(750 * time.Millisecond)
	if (*testCaseInput.testCaseInput.child) == "fresh" {
		if _, _, _, blobErr2 := c.Blob(
			t.Context(),
			BlobKey{Content: "inner"},
			Policy{},
			fail,
		); !errors.Is(
			blobErr2,
			syscall.ECONNRESET,
		) {
			t.Fatal(blobErr2)
		}
	}
	before, err := c.store.Resolve(t.Context(), outerID)
	assert.NoError(t, err)

	unlock, err := c.locks.Acquire(t.Context(), outerID)
	assert.NoError(t, err)

	done := make(chan error, (*testCaseInput.testCaseInput.callers))
	outer, inner := 0, 0
	for range *testCaseInput.testCaseInput.callers {
		go func() {
			_, _, _, blobErr3 := c.Blob(
				t.Context(),
				BlobKey{Content: "outer"},
				Policy{Retry: fastRetry(5)},
				func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
					outer++
					_, _, _, blobErr4 := c.Blob(
						ctx,
						BlobKey{Content: "inner"},
						Policy{Retry: fastRetry(7)},
						func(context.Context, ProducerInput) (ProducerResult, error) {
							inner++
							return ProducerResult{}, connReset()
						},
					)
					return ProducerResult{}, blobErr4
				},
			)
			done <- blobErr3
		}()
	}
	synctest.Wait()
	unlock()
	for range *testCaseInput.testCaseInput.callers {
		innerErr := <-done
		terminal, ok := errors.AsType[*TerminalError](innerErr)
		assert.FatalIf(
			t,
			!ok || !errors.Is(innerErr, syscall.ECONNRESET) ||
				(*testCaseInput.testCaseInput.child) == "fresh" && terminal.Attempted,
			"nested error=%v",
			innerErr,
		)
	}
	wantInner := 1
	if (*testCaseInput.testCaseInput.child) == "fresh" {
		wantInner = 0
	}
	assert.FatalIf(t, outer != 1 || inner != wantInner, "producer calls=%d/%d", outer, inner)
	after, err := c.store.Resolve(t.Context(), outerID)
	assert.NoError(t, err)

	o, err := c.readOutcome(t.Context(), &after)
	wantCount := uint32(2)
	if (*testCaseInput.testCaseInput.child) == "fresh" {
		wantCount = 1
		assert.FatalIf(t, before.Digest != after.Digest, "%s", "cached child renewed outer outcome")
	}
	assert.FatalIf(
		t,
		err != nil || o == nil || o.record.Count != wantCount,
		"outer outcome=%+v, error=%v",
		o,
		err,
	)
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
