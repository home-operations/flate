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
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func timeoutAcquisition(t *testing.T, kind string) func(context.Context, *Cache, Policy, func(context.Context, ProducerInput) (ProducerResult, error)) (string, error) {
	t.Helper()
	if kind == "git" {
		src := t.TempDir()
		repo, err := git.PlainInit(src, false)
		if err != nil {
			t.Fatal(err)
		}
		testutil.WriteFile(t, src, "file", "complete")
		testutil.GitCommitAll(t, repo, "seed", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example.invalid"}})
		return func(ctx context.Context, c *Cache, policy Policy, produce func(context.Context, ProducerInput) (ProducerResult, error)) (string, error) {
			_, id, err := c.Git(ctx, GitKey{Content: "timed"}, policy, func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
				repo, err := git.CloneContext(ctx, storer, worktree, &git.CloneOptions{URL: "file://" + src, NoCheckout: true})
				if err != nil {
					return plumbing.ZeroHash, err
				}
				ref, err := repo.Head()
				if err != nil {
					return plumbing.ZeroHash, err
				}
				_, err = produce(ctx, ProducerInput{Writer: io.Discard})
				return ref.Hash(), err
			})
			return id, err
		}
	}
	return func(ctx context.Context, c *Cache, policy Policy, produce func(context.Context, ProducerInput) (ProducerResult, error)) (string, error) {
		if kind == "archive" {
			_, _, id, err := c.Acquire(ctx, ArchiveKey{Content: "timed"}, policy, produce)
			return id, err
		}
		_, _, id, err := c.Blob(ctx, BlobKey{Content: "timed"}, policy, produce)
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
					synctest.Test(t, func(t *testing.T) {
						acquire := timeoutAcquisition(t, kind)
						root := ""
						if disk {
							root = t.TempDir()
						}
						c := cachetest.New(t, root, New)
						ctx, cancel := context.WithCancel(t.Context())
						defer cancel()
						timeout, elapsed := 5*time.Second, 5*time.Second
						cause := context.DeadlineExceeded
						switch bound {
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
						id, err := acquire(ctx, c, Policy{Timeout: &timeout}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
							called = true
							deadline, ok := ctx.Deadline()
							want := start.Add(timeout)
							if bound == "parent" {
								want = start.Add(elapsed)
							}
							if !ok || !deadline.Equal(want) {
								t.Errorf("producer deadline=%v present=%t, want %v", deadline, ok, want)
							}
							if kind != "archive" {
								_, _ = io.WriteString(w, "partial")
							}
							<-ctx.Done()
							return ProducerResult{}, ctx.Err()
						})
						if id != "" || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux) || time.Since(start) != elapsed || called != (bound != "zero") {
							t.Fatalf("expired acquisition: id=%q elapsed=%v called=%t error=%v", id, time.Since(start), called, err)
						}
						if disk {
							if err := c.Close(); err != nil {
								t.Fatal(err)
							}
							c = cachetest.New(t, root, New)
						}
						data := orderedArchive(t, "file", "complete")
						timeout = 10 * time.Second
						id, err = acquire(t.Context(), c, Policy{Timeout: &timeout, MaxAge: Forever}, producer(data, ocispec.Descriptor{}))
						if err != nil || id == "" {
							t.Fatalf("timeout recovery: id=%q error=%v", id, err)
						}
						if disk {
							if err := c.Close(); err != nil {
								t.Fatal(err)
							}
							c = cachetest.New(t, root, New)
						}
						timeout = 0
						warm, err := acquire(t.Context(), c, Policy{Timeout: &timeout, MaxAge: Forever}, func(context.Context, ProducerInput) (ProducerResult, error) {
							t.Error("warm generation invoked production with a zero timeout")
							return ProducerResult{}, errors.New("unexpected production")
						})
						if err != nil || warm != id {
							t.Fatalf("warm timeout policy: id=%q want=%q error=%v", warm, id, err)
						}
					})
				})
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
					synctest.Test(t, func(t *testing.T) {
						acquire := timeoutAcquisition(t, kind)
						root := ""
						if disk {
							root = t.TempDir()
						}
						c := cachetest.New(t, root, New)
						data := orderedArchive(t, "file", "complete")
						id, err := acquire(t.Context(), c, Policy{}, producer(data, ocispec.Descriptor{}))
						if err != nil {
							t.Fatal(err)
						}
						value, _ := c.entries.Load(id)
						previous := value.(*entry)
						timeout := time.Second
						failedID, err := acquire(t.Context(), c, Policy{Timeout: &timeout}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
							var output ProducerResult
							if result == "unchanged" {
								prior, err := w.Previous(ctx)
								if err != nil || prior == nil {
									t.Fatalf("previous generation: %v, %v", prior, err)
								}
								output = ProducerResult{Descriptor: *prior, Unchanged: true}
							} else {
								var err error
								output, err = producer(data, ocispec.Descriptor{})(ctx, w)
								if err != nil {
									return output, err
								}
							}
							<-ctx.Done()
							return output, nil
						})
						if failedID != "" || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, manifest.ErrFlux) {
							t.Fatalf("expired success: id=%q error=%v", failedID, err)
						}
						value, _ = c.entries.Load(id)
						if e := value.(*entry); e.fs != nil || e.git != nil || !e.resolved.IsZero() {
							t.Fatal("expired success published a memory generation")
						}
						if disk {
							stored, err := c.store.Resolve(t.Context(), id)
							if err != nil || stored.Annotations[prefix+"resolved-at"] != previous.stored.Annotations[prefix+"resolved-at"] {
								t.Fatalf("expired success changed the durable generation: %v", err)
							}
						}
					})
				})
			}
		}
	}
}

func TestAcquire_OperationTimeoutWaitAndRetry(t *testing.T) {
	t.Parallel()
	t.Run("waiters", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			c := cachetest.New(t, "", New)
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 2)
			key := BlobKey{Content: "wait"}
			go func() {
				_, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					close(entered)
					<-release
					return producer([]byte("complete"), ocispec.Descriptor{})(ctx, w)
				})
				done <- err
			}()
			<-entered
			go func() {
				timeout := time.Second
				_, _, _, err := c.Blob(t.Context(), key, Policy{Timeout: &timeout, MaxAge: Forever}, func(context.Context, ProducerInput) (ProducerResult, error) {
					t.Error("waiter did not reuse the completed generation")
					return ProducerResult{}, nil
				})
				done <- err
			}()
			synctest.Wait()
			time.Sleep(6 * time.Second)
			close(release)
			for range 2 {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
		})
	})
	t.Run("retry", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			c := cachetest.New(t, "", New)
			timeout := 5 * time.Second
			calls := 0
			start := time.Now()
			_, _, _, err := c.Blob(t.Context(), BlobKey{Content: "retry"}, Policy{Timeout: &timeout, Retry: RetryConfig{Attempts: 2, MinWait: time.Second}}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) != timeout {
					t.Errorf("attempt %d: deadline=%v present=%t", calls, deadline, ok)
				}
				time.Sleep(2 * time.Second)
				if calls == 1 {
					return ProducerResult{}, connReset()
				}
				return producer([]byte("complete"), ocispec.Descriptor{})(ctx, w)
			})
			if err != nil || calls != 2 || time.Since(start) != 5*time.Second {
				t.Fatalf("attempt budgets: calls=%d elapsed=%v error=%v", calls, time.Since(start), err)
			}
		})
	})
}

func TestAcquire_OperationTimeoutPublication(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprint(disk), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				root := ""
				if disk {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				timeout := time.Second
				_, _, id, err := c.Blob(t.Context(), BlobKey{Content: "publication", Validate: func(context.Context, fs.FS) (bool, error) {
					time.Sleep(timeout)
					synctest.Wait()
					return true, nil
				}}, Policy{Timeout: &timeout}, producer([]byte("complete"), ocispec.Descriptor{}))
				if id != "" || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("expired validation: id=%q error=%v", id, err)
				}
			})
		})
	}
}

func TestAcquire_OperationTimeoutReaderFailure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", New)
		timeout := time.Second
		_, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "invalid"}, Policy{Timeout: &timeout, Retry: fastRetry(3)}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			_, _ = w.Write(bytes.Repeat([]byte("x"), 512))
			<-ctx.Done()
			time.Sleep(2 * time.Second)
			return ProducerResult{}, nil
		})
		if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, tar.ErrHeader) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("reader failure at expiry: %v", err)
		}
	})
}

func TestAcquire_OperationTimeoutCleanupFailure(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"archive", "blob", "git"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				acquire := timeoutAcquisition(t, kind)
				c, err := New(t.Context(), t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := c.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
						t.Error(err)
					}
				})
				timeout := time.Second
				_, err = acquire(t.Context(), c, Policy{Timeout: &timeout}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					output, err := producer(orderedArchive(t, "file", "complete"), ocispec.Descriptor{})(ctx, w)
					if err != nil {
						return output, err
					}
					time.Sleep(timeout)
					synctest.Wait()
					if err := c.root.Close(); err != nil {
						t.Error(err)
					}
					return output, nil
				})
				if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, os.ErrClosed) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("cleanup failure at expiry: %v", err)
				}
			})
		})
	}
}
