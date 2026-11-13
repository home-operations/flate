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
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func timeoutAcquisition(
	t *testing.T,
	kind string,
) func(context.Context, *Cache, *Policy, func(context.Context, ProducerInput) (ProducerResult, error)) (string, error) {
	t.Helper()
	if kind == "git" {
		src := t.TempDir()
		repo := testutil.GitInit(t, src)

		testutil.WriteFile(t, src, "file", "complete")
		testutil.GitCommitAll(
			t,
			repo,
			"seed",
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example.invalid"}},
		)
		return func(ctx context.Context, c *Cache, policyValue *Policy, produce func(context.Context, ProducerInput) (ProducerResult, error)) (string, error) {
			policy := *policyValue

			_, id, err := c.Git(
				ctx,
				GitKey{Content: "timed"},
				&policy,
				func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
					repo, err := git.CloneContext(
						ctx,
						storer,
						worktree,
						&git.CloneOptions{URL: "file://" + src, NoCheckout: true},
					)
					if err != nil {
						return plumbing.ZeroHash, err
					}
					ref, err := repo.Head()
					if err != nil {
						return plumbing.ZeroHash, err
					}
					_, err = produce(ctx, ProducerInput{Writer: io.Discard})
					return ref.Hash(), err
				},
			)
			return id, err
		}
	}
	return func(ctx context.Context, c *Cache, policyValue *Policy, produce func(context.Context, ProducerInput) (ProducerResult, error)) (string, error) {
		policy := *policyValue

		if kind == "archive" {
			_, _, id, err := c.Acquire(ctx, ArchiveKey{Content: "timed"}, &policy, produce)
			return id, err
		}
		_, _, id, err := c.Blob(ctx, BlobKey{Content: "timed"}, &policy, produce)
		return id, err
	}
}

func TestAcquire_OperationTimeout(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"archive", "blob", "git"} {
		for _, disk := range []bool{false, true} {
			for _, bound := range []string{"operation", "parent", "cancellation", "zero"} {
				t.Run(fmt.Sprintf("%s/disk=%t/%s", kind, disk, bound), func(t *testing.T) {
					t.Parallel()
					testAcquireOperationTimeoutCase(
						t,
						&kind, &disk, &bound,
					)
				},
				)
			}
		}
	}
}

func TestAcquire_OperationTimeoutAfterSuccess(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"archive", "blob", "git"} {
		for _, disk := range []bool{false, true} {
			results := []string{"fresh"}
			if kind != "git" {
				results = append(results, "unchanged")
			}
			for _, result := range results {
				t.Run(fmt.Sprintf("%s/disk=%t/%s", kind, disk, result), func(t *testing.T) {
					t.Parallel()
					testAcquireOperationTimeoutAfterSuccessCase(
						t,
						&kind, &disk, &result,
					)
				},
				)
			}
		}
	}
}

func TestAcquire_OperationTimeoutWaitAndRetry(t *testing.T) {
	t.Parallel()
	t.Run("waiters", func(t *testing.T) { t.Parallel(); testAcquireOperationTimeoutWaitAndRetryWaiters(t) })
	t.Run("retry", func(t *testing.T) { t.Parallel(); testAcquireOperationTimeoutWaitAndRetryRetry(t) })
}

func TestAcquire_OperationTimeoutPublication(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(strconv.FormatBool(disk), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				root := ""
				if disk {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				timeout := time.Second
				releases := 0
				_, _, id, err := c.Blob(
					t.Context(),
					BlobKey{Content: "publication", Validate: func(context.Context, fs.FS) (bool, error) {
						if releases != 0 {
							t.Error("admission released before validation/publication")
						}
						time.Sleep(timeout)
						synctest.Wait()
						return true, nil
					}},
					&Policy{Timeout: &timeout, Admit: func(context.Context) (func(), error) {
						return func() { releases++ }, nil
					}},
					producer([]byte("complete"), &ocispec.Descriptor{}),
				)
				assert.FatalIf(t, releases != 1 || id != "" || !errors.Is(err, context.DeadlineExceeded) ||
					!errors.Is(err, manifest.ErrFlux), "expired validation: id=%q error=%v", id, err)
			})
		})
	}
}

func TestAcquire_OperationTimeoutReaderFailure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", New)
		timeout := time.Second
		_, _, _, err := c.Acquire(
			t.Context(),
			ArchiveKey{Content: "invalid"},
			&Policy{Timeout: &timeout, Retry: fastRetry(3)},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				_, _ = w.Write(bytes.Repeat([]byte("x"), 512))
				<-ctx.Done()
				time.Sleep(2 * time.Second)
				return ProducerResult{}, nil
			},
		)
		{
			_, ok := errors.AsType[*CacheError](
				err,
			)
			assert.FatalIf(t, !ok || !errors.Is(err, tar.ErrHeader) ||
				!errors.Is(err, context.DeadlineExceeded), "reader failure at expiry: %v", err)
		}
	})
}

func TestAcquire_OperationTimeoutCleanupFailure(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"archive", "blob", "git"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			testAcquireOperationTimeoutCleanupFailureCase(
				t,
				&kind,
			)
		},
		)
	}
}

func TestAcquire_Admission(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"success", "canceled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testAcquireAdmissionCase(t, &mode)
		},
		)
	}
	t.Run("failure", func(t *testing.T) { t.Parallel(); testAcquireAdmissionFailure(t) })
	t.Run("staging failure", func(t *testing.T) { t.Parallel(); testAcquireAdmissionStagingFailure(t) })
	t.Run("persisted replay", func(t *testing.T) { t.Parallel(); testAcquireAdmissionPersistedReplay(t) })
}

func testAcquireOperationTimeoutCase(t *testing.T, kind *string,
	disk *bool,
	bound *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireOperationTimeoutCaseCallback(
			t,
			testAcquireOperationTimeoutCaseCallbackInputs{kind: &kind, disk: &disk, bound: &bound},
		)
	},
	)
}

func testAcquireOperationTimeoutAfterSuccessCase(
	t *testing.T, kind *string,
	disk *bool,
	result *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		acquire := timeoutAcquisition(t, (*kind))
		root := ""
		if *disk {
			root = t.TempDir()
		}
		c := cachetest.New(t, root, New)
		data := orderedArchive(t, "file", "complete")
		id, err := acquire(t.Context(), c, &Policy{}, producer(data, &ocispec.Descriptor{}))
		assert.NoError(t, err)

		value, _ := c.entries.Load(id)
		previous := value.(*entry)
		timeout := time.Second
		failedID, err := acquire(
			t.Context(),
			c,
			&Policy{Timeout: &timeout},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				var output ProducerResult
				if (*result) == "unchanged" {
					prior, previousErr := w.Previous(ctx)
					assert.FatalIf(
						t,
						previousErr != nil || prior == nil,
						"previous generation: %v, %v",
						prior,
						previousErr,
					)
					output = ProducerResult{Descriptor: *prior, Unchanged: true}
				} else {
					var innerErr error
					output, innerErr = producer(data, &ocispec.Descriptor{})(ctx, w)
					if innerErr != nil {
						return output, innerErr
					}
				}
				<-ctx.Done()
				return output, nil
			},
		)
		assert.FatalIf(t, failedID != "" || !errors.Is(err, context.DeadlineExceeded) ||
			!errors.Is(err, manifest.ErrFlux), "expired success: id=%q error=%v", failedID, err)
		value, _ = c.entries.Load(id)
		{
			e := value.(*entry)
			assert.FatalIf(
				t,
				e.fs != nil || e.git != nil || !e.resolved.IsZero(),
				"%s",
				"expired success published a memory generation",
			)
		}

		if *disk {
			stored, err := c.store.Resolve(t.Context(), id)
			assert.FatalIf(t, err != nil ||
				stored.Annotations[prefix+"resolved-at"] != previous.stored.Annotations[prefix+"resolved-at"], "expired success changed the durable generation: %v", err)
		}
	})
}

func testAcquireOperationTimeoutWaitAndRetryWaiters(t *testing.T) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", New)
		entered, release := make(chan struct{}), make(chan struct{})
		done := make(chan error, 2)
		key := BlobKey{Content: "wait"}
		admissions, releases := 0, 0
		admit := func(context.Context) (func(), error) {
			admissions++
			return func() { releases++ }, nil
		}
		go func() {
			_, _, _, err := c.Blob(
				t.Context(),
				key,
				&Policy{MaxAge: Forever, Admit: admit},
				func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					close(entered)
					<-release
					return producer([]byte("complete"), &ocispec.Descriptor{})(ctx, w)
				},
			)
			done <- err
		}()
		<-entered
		go func() {
			timeout := time.Second
			_, _, _, err := c.Blob(
				t.Context(),
				key,
				&Policy{Timeout: &timeout, MaxAge: Forever, Admit: admit},
				func(context.Context, ProducerInput) (ProducerResult, error) {
					t.Error("waiter did not reuse the completed generation")
					return ProducerResult{}, nil
				},
			)
			done <- err
		}()
		synctest.Wait()
		time.Sleep(6 * time.Second)
		close(release)
		for range 2 {
			{
				err := <-done
				assert.NoError(t, err)
			}
		}
		assert.FatalIf(
			t,
			admissions != 1 || releases != 1,
			"coalesced hit admitted: admissions=%d releases=%d",
			admissions,
			releases,
		)
	})
}

func testAcquireOperationTimeoutWaitAndRetryRetry(t *testing.T) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", New)
		timeout := 5 * time.Second
		calls, admissions, releases := 0, 0, 0
		start := time.Now()
		_, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "retry"},
			&Policy{
				Timeout: &timeout,
				Retry:   RetryConfig{Attempts: 2, MinWait: time.Second},
				Admit: func(context.Context) (func(), error) {
					if admissions != releases {
						t.Error("retry admitted before previous release")
					}
					admissions++
					return func() { releases++ }, nil
				},
			},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				{
					deadline, ok := ctx.Deadline()
					assert.ErrorIf(
						t,
						!ok || time.Until(deadline) != timeout,
						"attempt %d: deadline=%v present=%t",
						calls,
						deadline,
						ok,
					)
				}
				time.Sleep(2 * time.Second)
				if calls == 1 {
					return ProducerResult{}, connReset()
				}
				return producer([]byte("complete"), &ocispec.Descriptor{})(ctx, w)
			},
		)
		if err != nil || calls != 2 || admissions != 2 || releases != 2 || time.Since(start) != 5*time.Second {
			t.Fatalf("attempt budgets: calls=%d elapsed=%v error=%v", calls, time.Since(start), err)
		}
	})
}

func testAcquireOperationTimeoutCleanupFailureCase(
	t *testing.T, kind *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		acquire := timeoutAcquisition(t, (*kind))
		c, err := New(t.Context(), t.TempDir())
		assert.NoError(t, err)

		t.Cleanup(func() {
			if closeErr3 := c.Close(); closeErr3 != nil && !errors.Is(closeErr3, os.ErrClosed) {
				t.Error(closeErr3)
			}
		})
		timeout := time.Second
		releases := 0
		_, err = acquire(
			t.Context(),
			c,
			&Policy{Timeout: &timeout, Admit: func(context.Context) (func(), error) {
				return func() { releases++ }, nil
			}},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				output, innerErr2 := producer(orderedArchive(t, "file", "complete"), &ocispec.Descriptor{})(ctx, w)
				if innerErr2 != nil {
					return output, innerErr2
				}
				time.Sleep(timeout)
				synctest.Wait()
				if closeErr4 := c.root.Close(); closeErr4 != nil {
					t.Error(closeErr4)
				}
				return output, nil
			},
		)
		{
			_, ok := errors.AsType[*CacheError](
				err,
			)
			assert.FatalIf(
				t,
				releases != 1 || !ok || !errors.Is(err, os.ErrClosed) || !errors.Is(err, context.DeadlineExceeded) ||
					!errors.Is(err, manifest.ErrFlux),
				"cleanup failure at expiry: %v",
				err,
			)
		}
	})
}

type testAcquireOperationTimeoutCaseCallbackInputs struct {
	kind  **string
	disk  **bool
	bound **string
}

func testAcquireOperationTimeoutCaseCallback(
	t *testing.T,
	testCaseInput testAcquireOperationTimeoutCaseCallbackInputs,
) {
	t.Helper()
	acquire := timeoutAcquisition(t, (*(*testCaseInput.kind)))
	root := ""
	if *(*testCaseInput.disk) {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	timeout, elapsed := 5*time.Second, 5*time.Second
	cause := context.DeadlineExceeded
	switch *(*testCaseInput.bound) {
	case "parent":
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		elapsed = 2 * time.Second
	case "cancellation":
		go func() { time.Sleep(time.Second); cancel() }()
		elapsed, cause = time.Second, context.Canceled
	case "zero":
		timeout, elapsed = 0, 0
	}
	start := time.Now()
	called := false
	id, err := acquire(
		ctx,
		c,
		&Policy{Timeout: &timeout},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			called = true
			deadline, ok := ctx.Deadline()
			want := start.Add(timeout)
			if (*(*testCaseInput.bound)) == "parent" {
				want = start.Add(elapsed)
			}
			assert.ErrorIf(
				t,
				!ok || !deadline.Equal(want),
				"producer deadline=%v present=%t, want %v",
				deadline,
				ok,
				want,
			)
			if (*(*testCaseInput.kind)) != "archive" {
				_, _ = io.WriteString(w, "partial")
			}
			<-ctx.Done()
			return ProducerResult{}, ctx.Err()
		},
	)
	if id != "" || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux) ||
		time.Since(start) != elapsed ||
		called != ((*(*testCaseInput.bound)) != "zero") {
		t.Fatalf(
			"expired acquisition: id=%q elapsed=%v called=%t error=%v",
			id,
			time.Since(start),
			called,
			err,
		)
	}
	if *(*testCaseInput.disk) {
		{
			closeErr := c.Close()
			assert.NoError(t, closeErr)
		}

		c = cachetest.New(t, root, New)
	}
	data := orderedArchive(t, "file", "complete")
	timeout = 10 * time.Second
	id, err = acquire(
		t.Context(),
		c,
		&Policy{Timeout: &timeout, MaxAge: Forever},
		producer(data, &ocispec.Descriptor{}),
	)
	assert.FatalIf(t, err != nil || id == "", "timeout recovery: id=%q error=%v", id, err)
	if *(*testCaseInput.disk) {
		{
			closeErr2 := c.Close()
			assert.NoError(t, closeErr2)
		}

		c = cachetest.New(t, root, New)
	}
	timeout = 0
	warm, err := acquire(
		t.Context(),
		c,
		&Policy{Timeout: &timeout, MaxAge: Forever},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			t.Error("warm generation invoked production with a zero timeout")
			return ProducerResult{}, errors.New("unexpected production")
		},
	)
	assert.FatalIf(
		t,
		err != nil || warm != id,
		"warm timeout policy: id=%q want=%q error=%v",
		warm,
		id,
		err,
	)
}
