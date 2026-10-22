package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/gofrs/flock"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
)

func orderedArchive(t *testing.T, files ...string) []byte {
	t.Helper()
	var data bytes.Buffer
	w := tar.NewWriter(&data)
	for i := 0; i < len(files); i += 2 {
		if err := w.WriteHeader(&tar.Header{Name: files[i], Size: int64(len(files[i+1])), Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(w, files[i+1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func producer(data []byte, descriptor ocispec.Descriptor) func(context.Context, ProducerInput) (ProducerResult, error) {
	return func(_ context.Context, w ProducerInput) (ProducerResult, error) {
		_, err := w.Write(data)
		return ProducerResult{Descriptor: descriptor}, err
	}
}

func TestAcquire_GenerationsAndFreshness(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[disk], func(t *testing.T) {
			t.Parallel()
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			if !disk && c.root != nil {
				t.Fatal("memory cache opened a disk root")
			}
			key := ArchiveKey{Content: "source-test-generations"}
			want := ocispec.Descriptor{Digest: digest.FromString("upstream"), Size: 123, MediaType: "upstream/type", Annotations: map[string]string{"flate.revision": "v1", "other": "preserved"}}
			var initialArchive bytes.Buffer
			testutil.WriteTar(t, &initialArchive, map[string]string{"file": "v1"}, 0o644)
			first, got, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, producer(initialArchive.Bytes(), want))
			if err != nil {
				t.Fatal(err)
			}
			firstPin := AcquisitionIdentity(first)
			if firstPin == "" {
				t.Fatal("acquired archive has no identity")
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
			unexpected := func(context.Context, ProducerInput) (ProducerResult, error) {
				t.Error("fresh hit called producer")
				return ProducerResult{Descriptor: want}, errors.New("unexpected producer")
			}
			if disk {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				c = cachetest.New(t, root, New)
			}
			warm, got, warmID, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, unexpected)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
			if id == "" || warmID != id {
				t.Fatalf("acquisition identities: first=%q warm=%q", id, warmID)
			}
			invalid := struct{ Value any }{make(chan int)}
			if _, _, invalidID, err := c.Acquire(t.Context(), ArchiveKey{Content: invalid}, Policy{MaxAge: time.Hour}, unexpected); invalidID != "" || !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("invalid archive projection: id=%q, %v", invalidID, err)
			} else if _, ok := errors.AsType[*CacheError](err); !ok {
				t.Fatalf("invalid projection error: %v", err)
			}
			if r, _, invalidID, err := c.Blob(t.Context(), BlobKey{Content: invalid}, Policy{MaxAge: time.Hour}, unexpected); r != nil || invalidID != "" || !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("invalid blob projection: id=%q, %v", invalidID, err)
			}
			if g, invalidID, err := c.Git(t.Context(), GitKey{Content: invalid}, Policy{MaxAge: time.Hour}, nil); g != nil || invalidID != "" || !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("invalid Git projection: id=%q, %v", invalidID, err)
			}
			entries := 0
			c.entries.Range(func(_, _ any) bool { entries++; return true })
			if entries != 1 {
				t.Fatalf("invalid projection installed an entry: %d", entries)
			}
			if data, err := fs.ReadFile(warm, "file"); err != nil || string(data) != "v1" {
				t.Fatalf("warm: %q, %v", data, err)
			}
			e, _ := c.entries.Load(id)
			at := e.(*entry).resolved
			if disk {
				p := filepath.Join(c.root.Name(), blobPath(e.(*entry).stored.Digest))
				recent := time.Now()
				if err := os.Chtimes(p, recent, recent); err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(p)
				if err != nil {
					t.Fatal(err)
				}
				e.(*entry).touched = info.ModTime()
				var warm sync.WaitGroup
				for range 8 {
					warm.Go(func() {
						if _, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, unexpected); err != nil {
							t.Error(err)
						}
					})
				}
				warm.Wait()
				unchanged, err := os.Stat(p)
				if err != nil || !unchanged.ModTime().Equal(info.ModTime()) {
					t.Fatalf("warm hit rewrote retention timestamp: %v, %v", unchanged, err)
				}
				stale := time.Now().Add(-24 * time.Hour)
				if err := os.Chtimes(p, stale, stale); err != nil {
					t.Fatal(err)
				}
				e.(*entry).touched = stale
				if _, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, unexpected); err != nil {
					t.Fatal(err)
				}
				info, err = os.Stat(p)
				if err != nil || !info.ModTime().After(stale) {
					t.Fatalf("access time: %v, %v", info, err)
				}
				if !at.Equal(e.(*entry).resolved) {
					t.Fatal("access extended resolution TTL")
				}
			}
			var replacementArchive bytes.Buffer
			testutil.WriteTar(t, &replacementArchive, map[string]string{"file": "v2"}, 0o644)
			replacement, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: 0}, producer(replacementArchive.Bytes(), want))
			if err != nil {
				t.Fatal(err)
			}
			if AcquisitionIdentity(replacement) == firstPin || AcquisitionIdentity(warm) != firstPin {
				t.Fatal("replacement aliased or mutated held generation identity")
			}
			if data, err := fs.ReadFile(warm, "file"); err != nil || string(data) != "v1" {
				t.Fatalf("held generation: %q, %v", data, err)
			}
			failure := errors.New("verification failed")
			if _, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: 0}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				var partialArchive bytes.Buffer
				testutil.WriteTar(t, &partialArchive, map[string]string{"file": "partial"}, 0o644)
				_, _ = w.Write(partialArchive.Bytes())
				return ProducerResult{Descriptor: want}, failure
			}); !errors.Is(err, failure) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failure: %v", err)
			}
			calls := 0
			if _, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				var recoveredArchive bytes.Buffer
				testutil.WriteTar(t, &recoveredArchive, map[string]string{"file": "v3"}, 0o644)
				return producer(recoveredArchive.Bytes(), want)(ctx, w)
			}); err != nil || calls != 1 {
				t.Fatalf("failed fill reused previous alias: calls=%d, %v", calls, err)
			}
			if disk {
				t.Run("corrupt-reopen", func(t *testing.T) {
					t.Parallel()
					e, _ := c.entries.Load(id)
					path := filepath.Join(root, blobPath(e.(*entry).stored.Digest))
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					if err := os.Chmod(path, 0o600); err != nil {
						t.Fatal(err)
					}
					testutil.WriteFileAt(t, path, "truncated")
					c = cachetest.New(t, root, New)
					for attempt := range 3 {
						_, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, unexpected)
						if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, io.ErrUnexpectedEOF) {
							t.Fatalf("corrupt reload %d: %v", attempt, err)
						}
					}
				})
			}
		})
	}
}

func TestAcquire_ArchivePolicy(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		for _, defaults := range []bool{false, true} {
			t.Run(map[bool]string{false: "memory", true: "disk"}[disk]+map[bool]string{false: "/bucket", true: "/defaults"}[defaults], func(t *testing.T) {
				t.Parallel()
				root := ""
				if disk {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				data := orderedArchive(t, "dir/drop", "discard", "dir/keep", "first", "dir/.sourceignore", "!keep\n", ".sourceignore", "dir/*\nempty/*\n", "dir/keep", "last", "empty/file", "discard", "image.png", "image", ".git/config", "git")
				input, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-test-policy", WithDefaults: defaults}, Policy{MaxAge: time.Hour}, producer(data, ocispec.Descriptor{}))
				if err != nil {
					t.Fatal(err)
				}
				if got, err := fs.ReadFile(input, "dir/keep"); err != nil || string(got) != "last" {
					t.Fatalf("reincluded duplicate: %q, %v", got, err)
				}
				pin := AcquisitionIdentity(input)
				if pin == "" {
					t.Fatal("archive policy view has no acquisition identity")
				}
				for _, policy := range []ArchiveKey{
					{Content: "source-test-policy", WithDefaults: !defaults},
					{Content: "source-test-policy", WithDefaults: defaults, Ignore: "image.png\n"},
				} {
					other, _, _, err := c.Acquire(t.Context(), policy, Policy{MaxAge: 0}, producer(data, ocispec.Descriptor{}))
					if err != nil {
						t.Fatal(err)
					}
					if AcquisitionIdentity(other) == pin || AcquisitionIdentity(input) != pin {
						t.Fatal("archive policy identity collision")
					}
				}
				for _, name := range []string{"dir/drop", "empty"} {
					if _, err := fs.Stat(input, name); !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("ignored %s: %v", name, err)
					}
				}
				for _, name := range []string{".git/config"} {
					_, err := fs.Stat(input, name)
					if defaults != errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("defaults=%v %s: %v", defaults, name, err)
					}
				}
			})
		}
	}
}

func TestAcquire_ArchiveFailuresAndLinks(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[disk], func(t *testing.T) {
			t.Parallel()
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			for _, data := range [][]byte{orderedArchive(t, "../escape", "x"), orderedArchive(t, "file", "x", "file/child", "y"), orderedArchive(t, "dir/file", "x", "dir", "y"), []byte("truncated")} {
				if _, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-test-invalid"}, Policy{MaxAge: 0}, producer(data, ocispec.Descriptor{})); err == nil {
					t.Fatal("accepted invalid archive")
				}
			}
			for _, defaults := range []bool{false, true} {
				t.Run(fmt.Sprint(defaults), func(t *testing.T) {
					t.Parallel()
					var data bytes.Buffer
					w := tar.NewWriter(&data)
					for _, h := range []*tar.Header{
						{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "../absent"},
						{Name: "hardlink", Typeflag: tar.TypeLink, Linkname: "file"},
						{Name: "dir/link", Typeflag: tar.TypeSymlink, Linkname: "../../absent"},
						{Name: "file", Typeflag: tar.TypeReg, Size: 7},
						{Name: "ignored", Typeflag: tar.TypeReg, Size: 7},
					} {
						assert.NoError(t, w.WriteHeader(h))
						if h.Typeflag == tar.TypeReg {
							_, err := io.WriteString(w, "content")
							assert.NoError(t, err)
						}
					}
					assert.NoError(t, w.Close())
					key := ArchiveKey{Content: fmt.Sprintf("source-test-link-%t", defaults), Ignore: "ignored\n", WithDefaults: defaults}
					input, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, producer(data.Bytes(), ocispec.Descriptor{}))
					assert.NoError(t, err)
					readers := []fs.FS{input}
					if disk {
						fresh := cachetest.New(t, root, New)
						replayed, _, _, err := fresh.Acquire(t.Context(), key, Policy{MaxAge: Forever}, func(context.Context, ProducerInput) (ProducerResult, error) {
							t.Error("fresh owner refetched cached archive")
							return ProducerResult{}, errors.New("unexpected fetch")
						})
						assert.NoError(t, err)
						readers = append(readers, replayed)
					}
					for _, reader := range readers {
						entries, err := fs.ReadDir(reader, ".")
						assert.NoError(t, err)
						if len(entries) != 1 || entries[0].Name() != "file" {
							t.Fatalf("archive entries = %v", entries)
						}
						for _, name := range []string{"link", "link/child", "hardlink", "hardlink/child", "dir/link", "dir/link/nested/child"} {
							data, err := fs.ReadFile(reader, name)
							assert.Equal(t, len(data), 0)
							assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
							_, err = fs.Stat(reader, name)
							assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
						}
						for _, name := range []string{"absent", "ignored"} {
							_, err := fs.ReadFile(reader, name)
							assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
						}
						data, err := fs.ReadFile(reader, "file")
						assert.NoError(t, err)
						assert.Equal(t, string(data), "content")
					}
				})
			}
		})
	}
}

func TestAcquire_ExpandedCapCountsIgnoredDuplicates(t *testing.T) {
	old := maxExtractedBytes
	maxExtractedBytes = 2*512 + 5
	t.Cleanup(func() { maxExtractedBytes = old })
	for _, root := range []string{"", t.TempDir()} {
		c := cachetest.New(t, root, New)
		if _, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-test-cap", Ignore: "file"}, Policy{MaxAge: 0}, producer(orderedArchive(t, "file", "abc", "file", "def"), ocispec.Descriptor{})); err == nil {
			t.Fatal("ignored duplicate bypassed expanded cap")
		}
	}
}

func TestAcquire_ConcurrentKeysAndCancellation(t *testing.T) {
	t.Run("due", func(t *testing.T) {
		for _, callers := range []int{2, 4} {
			for _, cold := range []bool{false, true} {
				for _, result := range []string{"success", "eligible", "unsupported", "local", "publication"} {
					t.Run(fmt.Sprintf("%d/cold-%t/%s", callers, cold, result), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							root := t.TempDir()
							c := cachetest.New(t, root, New)
							key := BlobKey{Content: "due-wave"}
							held, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("held"), ocispec.Descriptor{}))
							if err != nil {
								t.Fatal(err)
							}
							good, err := c.store.Resolve(t.Context(), id)
							if err != nil {
								t.Fatal(err)
							}
							_, _, _, err = c.Blob(t.Context(), key, Policy{}, func(context.Context, ProducerInput) (ProducerResult, error) {
								return ProducerResult{}, connReset()
							})
							if _, ok := errors.AsType[*TerminalError](err); !ok {
								t.Fatal(err)
							}
							if data, err := io.ReadAll(held); err != nil || string(data) != "held" {
								t.Fatalf("held=%q, error=%v", data, err)
							}
							if err := held.Close(); err != nil {
								t.Fatal(err)
							}
							before, err := c.store.Resolve(t.Context(), id)
							if err != nil {
								t.Fatal(err)
							}
							time.Sleep(750 * time.Millisecond)
							if cold {
								if err := c.Close(); err != nil {
									t.Fatal(err)
								}
								c = cachetest.New(t, root, New)
							}
							unlock, err := c.locks.Acquire(t.Context(), id)
							if err != nil {
								t.Fatal(err)
							}
							started, release := make(chan struct{}), make(chan struct{})
							done := make(chan error, callers)
							calls := 0
							var cause error
							switch result {
							case "eligible", "publication":
								cause = connReset()
							case "unsupported":
								cause = timeoutErr{}
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
								return producer([]byte("complete"), ocispec.Descriptor{})(ctx, w)
							}
							launch := func(ctx context.Context, results chan error) {
								go func() {
									r, _, _, err := c.Blob(ctx, key, Policy{Retry: fastRetry(5)}, fill)
									if r != nil {
										data, readErr := io.ReadAll(r)
										err = errors.Join(err, readErr, r.Close())
										if string(data) != "complete" {
											t.Errorf("shared payload=%q", data)
										}
									}
									results <- err
								}()
							}
							for range callers {
								launch(t.Context(), done)
							}
							synctest.Wait()
							if cold {
								v, ok := c.entries.Load(id)
								if !ok || !v.(*entry).unloaded {
									t.Fatal("cold callers did not share an unloaded observation")
								}
							}
							unlock()
							<-started
							ctx, cancel := context.WithCancel(t.Context())
							canceled := make(chan error, 1)
							launch(ctx, canceled)
							synctest.Wait()
							cancel()
							if err := <-canceled; !errors.Is(err, context.Canceled) {
								t.Fatalf("canceled waiter=%v", err)
							}
							time.Sleep(10 * time.Second)
							if result == "publication" {
								if err := c.root.Remove("oci/index.json"); err != nil {
									t.Fatal(err)
								}
								if err := c.root.Mkdir("oci/index.json", 0o750); err != nil {
									t.Fatal(err)
								}
							}
							close(release)
							live := 0
							for range callers {
								err := <-done
								if cause == nil && err != nil || cause != nil && !errors.Is(err, cause) && result != "eligible" {
									t.Fatalf("shared error=%v, cause=%v", err, cause)
								}
								if result == "eligible" || result == "publication" {
									if !errors.Is(err, syscall.ECONNRESET) {
										t.Fatalf("remote cause lost: %v", err)
									}
								}
								if result == "publication" {
									if _, ok := errors.AsType[*CacheError](err); !ok {
										t.Fatalf("publication failure lost: %v", err)
									}
								}
								terminal, ok := errors.AsType[*TerminalError](err)
								if cause != nil && (!ok || terminal.Attempted) {
									live++
								}
							}
							if calls != 1 || cause != nil && live != 1 {
								t.Fatalf("due calls=%d, live results=%d", calls, live)
							}
							after, err := c.store.Resolve(t.Context(), id)
							if err != nil {
								t.Fatal(err)
							}
							if result == "eligible" {
								o, err := c.readOutcome(t.Context(), after)
								if err != nil || o == nil || o.record.Count != 2 || !o.record.Completed.Equal(time.Now()) || o.lastGood.Digest != good.Digest {
									t.Fatalf("due outcome=%+v, error=%v", o, err)
								}
							} else if cause != nil && after.Digest != before.Digest {
								t.Fatal("unsupported/local failure changed the durable outcome")
							}
							if result != "eligible" {
								if result == "publication" {
									if err := c.root.Remove("oci/index.json"); err != nil {
										t.Fatal(err)
									}
								}
								r, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("independent"), ocispec.Descriptor{}))
								if err != nil {
									t.Fatal(err)
								}
								data, readErr := io.ReadAll(r)
								if err := errors.Join(readErr, r.Close()); err != nil || string(data) != "independent" {
									t.Fatalf("independent result=%q, error=%v", data, err)
								}
							}
						})
					})
				}
			}
		}
	})

	for _, callers := range []int{2, 4} {
		t.Run(fmt.Sprintf("due/canceled-winner/%d", callers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := cachetest.New(t, t.TempDir(), New)
				key := BlobKey{Content: "due-canceled-winner"}
				var id string
				if _, _, _, err := c.Blob(t.Context(), key, Policy{}, func(context.Context, ProducerInput) (ProducerResult, error) {
					return ProducerResult{}, connReset()
				}); err == nil {
					t.Fatal("want initial failure")
				}
				c.entries.Range(func(key, _ any) bool { id = key.(string); return false })
				time.Sleep(750 * time.Millisecond)
				unlock, err := c.locks.Acquire(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
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
					return producer([]byte("complete"), ocispec.Descriptor{})(ctx, w)
				}
				type result struct {
					err  error
					data string
				}
				done := make(chan result, callers)
				launch := func(ctx context.Context, fill func(context.Context, ProducerInput) (ProducerResult, error)) {
					go func() {
						r, _, _, err := c.Blob(ctx, key, Policy{Retry: fastRetry(5)}, fill)
						var data string
						if r != nil {
							b, readErr := io.ReadAll(r)
							data = string(b)
							err = errors.Join(err, readErr, r.Close())
						}
						done <- result{err: err, data: data}
					}()
				}
				launch(winnerCtx, winnerFill)
				synctest.Wait()
				unlock()
				<-started
				for range callers - 1 {
					launch(t.Context(), liveFill)
				}
				synctest.Wait()
				cancelWinner()
				winnerCanceled, liveResults := 0, 0
				for range callers {
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
				if winnerCanceled != 1 || liveResults != callers-1 || calls.Load() != 2 {
					t.Fatalf("winner canceled=%d, live=%d, producer calls=%d", winnerCanceled, liveResults, calls.Load())
				}
			})
		})
	}

	for _, concurrency := range []int{2, 4} {
		for _, root := range []string{"", t.TempDir()} {
			c := cachetest.New(t, root, New)
			content := struct{ URL string }{"https://example.test/content"}
			started, release := make(chan struct{}), make(chan struct{})
			ids := make(chan string, concurrency)
			var calls atomic.Int32
			fill := func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-release
				var payloadTar bytes.Buffer
				testutil.WriteTar(t, &payloadTar, map[string]string{"file": "data"}, 0o644)
				return producer(payloadTar.Bytes(), ocispec.Descriptor{Annotations: map[string]string{"flate.revision": "v1"}})(ctx, w)
			}
			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Go(func() {
					key := ArchiveKey{Content: content}
					if i%2 == 0 {
						key.Content = map[string]string{"URL": content.URL}
						key.Required = []string{"revision"}
					}
					_, _, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, fill)
					if err != nil {
						t.Error(err)
					}
					ids <- id
				})
			}
			<-started
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, _, id, err := c.Acquire(ctx, ArchiveKey{Content: content}, Policy{MaxAge: time.Hour}, fill); id != "" || !errors.Is(err, context.Canceled) {
				t.Errorf("wait: id=%q, %v", id, err)
			}
			crossCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			r, _, blobID, err := c.Blob(crossCtx, BlobKey{Content: content}, Policy{MaxAge: time.Hour}, producer([]byte("blob"), ocispec.Descriptor{}))
			if err != nil {
				close(release)
				wg.Wait()
				t.Fatal(err)
			}
			data, readErr := io.ReadAll(r)
			if err := errors.Join(readErr, r.Close()); err != nil || string(data) != "blob" {
				t.Fatalf("cross-product blob: %q, %v", data, err)
			}
			g, gitID, err := c.Git(crossCtx, GitKey{Content: content}, Policy{MaxAge: time.Hour}, func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
				return plumbing.ZeroHash, nil
			})
			if err != nil || g == nil || blobID == "" || gitID == "" || blobID == gitID {
				close(release)
				wg.Wait()
				t.Fatalf("cross-product Git: blob=%q git=%q, %v", blobID, gitID, err)
			}
			close(release)
			wg.Wait()
			close(ids)
			var archiveID string
			for id := range ids {
				if id == "" || id == blobID || id == gitID || archiveID != "" && id != archiveID {
					t.Fatalf("cross-product identities: archive=%q other=%q blob=%q git=%q", archiveID, id, blobID, gitID)
				}
				archiveID = id
			}
			if calls.Load() != 1 {
				t.Fatalf("concurrency=%d calls=%d", concurrency, calls.Load())
			}
		}
	}
}

func TestNew_SharedOwnershipAndCanceledLock(t *testing.T) {
	t.Parallel()
	t.Run("unused-owner", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		assertBlobRoots(t, c)
		assert.NoError(t, c.Close())
		for _, algorithm := range blobAlgorithms {
			if _, err := os.Stat(filepath.Join(root, blobDir(), algorithm.String())); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("unused owner created %s root: %v", algorithm, err)
			}
		}
	})
	t.Run("partial-initialization", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		sha256Key := BlobKey{Content: "partial-sha256"}
		key := BlobKey{Content: "partial-sha384"}
		payload := []byte("payload")
		var id string
		for _, key := range []BlobKey{sha256Key, key} {
			r, _, storedID, err := c.Blob(t.Context(), key, Policy{}, producer(payload, ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}
			assert.NoError(t, r.Close())
			id = storedID
		}
		stored, err := c.store.Resolve(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		stored.Digest = digest.SHA384.FromBytes(payload)
		assert.NoError(t, c.store.Push(t.Context(), stored, bytes.NewReader(payload)))
		assert.NoError(t, c.store.Tag(t.Context(), stored, id))
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		r, _, _, err := c.Blob(t.Context(), sha256Key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		assert.NoError(t, r.Close())
		parent := c.blobRoots[0].Root
		if parent == nil {
			t.Fatal("partial initialization did not open the first root")
		}
		collision := filepath.Join(blobDir(), digest.SHA384.String())
		assert.NoError(t, c.root.Rename(collision, collision+"-held"))
		testutil.WriteFile(t, root, collision, "not a directory")
		r, _, _, firstErr := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if r != nil || !errors.Is(firstErr, manifest.ErrFlux) || c.blobRoots[1].err == nil || !errors.Is(firstErr, c.blobRoots[1].err) {
			t.Fatalf("partial initialization: reader=%v, error=%v", r, firstErr)
		}
		assert.NoError(t, c.root.Remove(collision))
		assert.NoError(t, c.root.Rename(collision+"-held", collision))
		if r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); r != nil || !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, c.blobRoots[1].err) {
			t.Fatalf("repaired filesystem changed initialization result: reader=%v, error=%v", r, err)
		}
		r, _, _, err = c.Blob(t.Context(), sha256Key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Diff(t, data, payload)
		assert.NoError(t, r.Close())
		assertBlobRoots(t, c, digest.SHA256)
		assert.NoError(t, c.Close())
		if _, err := parent.Stat("."); !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("last Close retained partial blob parent: %v", err)
		}
		lock := flock.New(filepath.Join(root, ociLock))
		acquired, err := lock.TryLock()
		assert.NoError(t, lock.Close())
		if err != nil || !acquired {
			t.Fatalf("last Close retained partial owner lock: acquired=%v, err=%v", acquired, err)
		}
		c = cachetest.New(t, root, New)
		r, _, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err = io.ReadAll(r)
		assert.NoError(t, err)
		assert.Diff(t, data, payload)
		assert.NoError(t, r.Close())
		assertBlobRoots(t, c, digest.SHA384)
		for i := range c.blobRoots {
			parent := &c.blobRoots[i]
			if parent.Root == nil {
				continue
			}
			if _, err := parent.Stat("."); err != nil {
				t.Fatalf("reopened parent: %v", err)
			}
		}
	})
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	shared := cachetest.New(t, alias, New)
	if shared != c {
		t.Fatal("canonical directory has multiple owners")
	}
	reader, _, _, err := c.Blob(t.Context(), BlobKey{Content: "blob-held"}, Policy{MaxAge: Forever}, producer([]byte("held"), ocispec.Descriptor{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	disk := c.root
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.Stat("."); err != nil {
		t.Fatalf("first Close closed shared root: %v", err)
	}
	assertBlobRoots(t, c, digest.SHA256)
	for i := range c.blobRoots {
		parent := &c.blobRoots[i]
		if parent.Root == nil {
			continue
		}
		if _, err := parent.Stat("."); err != nil {
			t.Fatalf("first Close closed shared blob parent: %v", err)
		}
	}
	if data, err := io.ReadAll(reader); err != nil || string(data) != "held" {
		t.Fatalf("reader after first Close: %q, %v", data, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	lock := flock.New(filepath.Join(root, ociLock))
	t.Cleanup(func() { _ = lock.Close() })
	if acquired, err := lock.TryLock(); err != nil || acquired {
		t.Fatalf("first Close released shared lock: acquired=%v, err=%v", acquired, err)
	}
	if r, _, _, err := shared.Blob(t.Context(), BlobKey{Content: "blob-shared"}, Policy{MaxAge: time.Hour}, producer([]byte("data"), ocispec.Descriptor{})); err != nil {
		t.Fatal(err)
	} else {
		_ = r.Close()
	}
	if err := shared.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.Stat("."); err == nil {
		t.Fatal("last Close retained root")
	}
	assertBlobRoots(t, c, digest.SHA256)
	for i := range c.blobRoots {
		parent := &c.blobRoots[i]
		if parent.Root == nil {
			continue
		}
		if _, err := parent.Stat("."); !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("last Close retained blob parent: %v", err)
		}
	}
	if acquired, err := lock.TryLock(); err != nil || !acquired {
		t.Fatalf("last Close retained lock: acquired=%v, err=%v", acquired, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	t.Cleanup(cancel)
	if _, err := New(ctx, root); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock wait: %v", err)
	}
}

func TestAcquire_TagSaveFailureDoesNotBecomeHit(t *testing.T) {
	t.Parallel()
	t.Run("outcome", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, t.TempDir(), New)
		index := filepath.Join(c.root.Name(), indexPath())
		if err := os.Remove(index); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(index, 0o750); err != nil {
			t.Fatal(err)
		}
		calls := 0
		for range 2 {
			_, _, _, err := c.Blob(t.Context(), BlobKey{Content: "outcome-save"}, Policy{MaxAge: Forever}, func(context.Context, ProducerInput) (ProducerResult, error) {
				calls++
				return ProducerResult{}, connReset()
			})
			if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, syscall.ECONNRESET) {
				t.Fatalf("publication cause=%v", err)
			}
			v, ok := c.entries.Load(manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"outcome-save"}}))
			if !ok || v.(*entry).outcome != nil {
				t.Fatal("failed publication installed a durable hit")
			}
		}
		if calls != 2 {
			t.Fatalf("uncommitted failure reused: calls=%d", calls)
		}
	})

	c := cachetest.New(t, t.TempDir(), New)
	index := filepath.Join(c.root.Name(), indexPath())
	if err := os.Remove(index); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(index, 0o750); err != nil {
		t.Fatal(err)
	}
	calls := 0
	fill := func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
		calls++
		var payloadTar bytes.Buffer
		testutil.WriteTar(t, &payloadTar, map[string]string{"file": "data"}, 0o644)
		return producer(payloadTar.Bytes(), ocispec.Descriptor{})(ctx, w)
	}
	for range 2 {
		if _, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-test-persist"}, Policy{MaxAge: time.Hour}, fill); err == nil {
			t.Fatal("tag save failure accepted")
		}
	}
	if calls != 2 {
		t.Fatalf("failed publication served an in-memory hit: %d", calls)
	}
}

func TestAcquire_SparseReplayMatchesMemory(t *testing.T) {
	t.Parallel()
	root, err := exec.CommandContext(t.Context(), "go", "env", "GOROOT").Output()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(root)), "src/archive/tar/testdata/sparse-formats.tar"))
	if err != nil {
		t.Fatal(err)
	}
	var inputs []fs.FS
	for _, root := range []string{"", t.TempDir()} {
		c := cachetest.New(t, root, New)
		input, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-test-sparse"}, Policy{MaxAge: time.Hour}, producer(data, ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, input)
	}
	for _, name := range []string{"sparse-posix-0.0", "sparse-posix-0.1", "sparse-posix-1.0"} {
		want, err := fs.ReadFile(inputs[0], name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := fs.ReadFile(inputs[1], name)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("sparse replay %s mismatch", name)
		}
	}
	if err := fstest.TestFS(inputs[1], "sparse-posix-0.0", "sparse-posix-0.1", "sparse-posix-1.0"); err != nil {
		t.Fatal(err)
	}
}

func TestDecode_RequiredMetadata(t *testing.T) {
	t.Parallel()
	for _, family := range []struct {
		name     string
		required []string
		fields   map[string]string
	}{
		{"oci", []string{"revision", "upstream-digest"}, map[string]string{"flate.revision": "v1", "flate.upstream-digest": digest.FromString("upstream").String()}},
		{"bucket", []string{"revision", "object-count"}, map[string]string{"flate.revision": "v1", "flate.object-count": "0"}},
		{"chart", []string{"chart-version", "chart-url"}, map[string]string{"flate.chart-version": "1.0.0", "flate.chart-url": "https://example.test/chart.tgz"}},
	} {
		t.Run(family.name, func(t *testing.T) {
			t.Parallel()
			base := map[string]string{
				"flate.resolved-at":         time.Now().UTC().Format(time.RFC3339Nano),
				"flate.upstream-size":       "12",
				"flate.upstream-media-type": "test/type",
			}
			maps.Copy(base, family.fields)
			checks := []struct{ name, key, value string }{
				{"valid", "", ""},
				{"bad stored digest", "", ""},
				{"bad resolved time", "flate.resolved-at", "invalid"},
				{"bad upstream size", "flate.upstream-size", "-1"},
				{"bad upstream digest", "flate.upstream-digest", "invalid"},
			}
			for _, field := range family.required {
				checks = append(checks, struct{ name, key, value string }{"missing " + field, prefix + field, ""})
			}
			if family.name == "bucket" {
				checks = append(checks, struct{ name, key, value string }{"negative count", "flate.object-count", "-1"},
					struct{ name, key, value string }{"malformed count", "flate.object-count", "invalid"})
			}
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					t.Parallel()
					a := maps.Clone(base)
					if check.key != "" {
						a[check.key] = check.value
					}
					stored := ocispec.Descriptor{Digest: digest.FromString("stored"), Annotations: a}
					if check.name == "bad stored digest" {
						stored.Digest = "invalid"
					}
					_, _, valid := decode(stored, family.required)
					if valid != (check.name == "valid") {
						t.Fatalf("valid=%v", valid)
					}
				})
			}
			for _, field := range family.required {
				t.Run("disk missing "+field, func(t *testing.T) {
					t.Parallel()
					root := t.TempDir()
					c := cachetest.New(t, root, New)
					key := BlobKey{Content: struct{ URL string }{"https://example.test/" + family.name}, Required: family.required}
					upstream := ocispec.Descriptor{Digest: digest.Digest(family.fields[prefix+"upstream-digest"]), Annotations: family.fields}
					r, _, id, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, producer([]byte("payload"), upstream))
					if err != nil {
						t.Fatal(err)
					}
					if err := r.Close(); err != nil {
						t.Fatal(err)
					}
					stored, err := c.store.Resolve(t.Context(), id)
					if err != nil {
						t.Fatal(err)
					}
					delete(stored.Annotations, prefix+field)
					if err := c.store.Tag(t.Context(), stored, id); err != nil {
						t.Fatal(err)
					}
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					c = cachetest.New(t, root, New)
					if r, _, failedID, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); r != nil || failedID != "" || !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("invalid restored metadata hit: id=%q, %v", failedID, err)
					}
					calls := 0
					r, _, restoredID, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
						calls++
						return producer([]byte("refilled"), upstream)(ctx, w)
					})
					if err != nil || calls != 1 || restoredID != id {
						t.Fatalf("metadata refill: calls=%d id=%q, %v", calls, restoredID, err)
					}
					data, readErr := io.ReadAll(r)
					if err := errors.Join(readErr, r.Close()); err != nil || string(data) != "refilled" {
						t.Fatalf("refilled payload=%q, %v", data, err)
					}
				})
			}
		})
	}
}

func TestBlob_SourcePrefixIsOpaque(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			key := BlobKey{Content: "source-blob"}
			reader, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, producer([]byte("blob bytes"), ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}
			input := FileFS("chart.tgz", reader)
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if err := fstest.TestFS(input, "chart.tgz"); err != nil {
				t.Fatal(err)
			}
			got, err := fs.ReadFile(input, "chart.tgz")
			if err != nil || string(got) != "blob bytes" {
				t.Fatalf("content=%q, err=%v", got, err)
			}
		})
	}
}

func TestAcquire_TrailingDataIsBounded(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			var written int
			_, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-trailing"}, Policy{MaxAge: 0}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				var payloadTar bytes.Buffer
				testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
				if _, err := w.Write(payloadTar.Bytes()); err != nil {
					return ProducerResult{}, err
				}
				var err error
				written, err = w.Write(make([]byte, 2<<20))
				return ProducerResult{}, err
			})
			if err == nil {
				t.Fatal("accepted trailing data beyond the limit")
			}
			if written > 1<<20+1 {
				t.Fatalf("consumed %d trailing bytes beyond the 1 MiB limit", written)
			}
		})
	}
}

func TestAcquire_CancellationKeepsContextError(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started := make(chan struct{})
			go func() { <-started; cancel() }()
			_, _, _, err := c.Acquire(ctx, ArchiveKey{Content: "source-cancel"}, Policy{MaxAge: 0}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				var payloadTar bytes.Buffer
				testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
				if _, err := w.Write(payloadTar.Bytes()[:512]); err != nil {
					return ProducerResult{}, err
				}
				close(started)
				<-ctx.Done()
				time.Sleep(time.Millisecond)
				return ProducerResult{}, ctx.Err()
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation identity: %v", err)
			}
			key := ArchiveKey{Content: "independent-child"}
			_, _, id, err := c.Acquire(t.Context(), key, Policy{}, producer(orderedArchive(t, "file", "same"), ocispec.Descriptor{}))
			assert.NoError(t, err)
			_, _, _, err = c.Acquire(t.Context(), key, Policy{}, func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
				child, cancel := context.WithCancel(ctx)
				cancel()
				return ProducerResult{}, child.Err()
			})
			if !errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux) || t.Context().Err() != nil {
				t.Fatalf("independently canceled child: %v", err)
			}
			v, _ := c.entries.Load(id)
			if e := v.(*entry); e.fs != nil || !e.resolved.IsZero() {
				t.Fatal("independently canceled child retained a successful entry")
			}
		})
	}
	t.Run("completed blob", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, "", New)
		key := BlobKey{Content: "source-completed-before-cancel"}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r, _, _, err := c.Blob(ctx, key, Policy{MaxAge: 0}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			desc, err := producer([]byte("completed"), ocispec.Descriptor{})(ctx, w)
			cancel()
			return desc, err
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		r, _, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		if closeErr := r.Close(); err != nil || closeErr != nil || string(data) != "completed" {
			t.Fatalf("completed fill: output=%q, read=%v, close=%v", data, err, closeErr)
		}
	})
}

type archiveCompletionContext struct {
	context.Context
	child   atomic.Pointer[<-chan struct{}]
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *archiveCompletionContext) Err() error {
	if child := c.child.Load(); child != nil && c.Context.Err() == nil {
		select {
		case <-*child:
			// errgroup cancels its child only after both successful workers have joined.
			c.once.Do(func() { close(c.entered); <-c.release })
		default:
		}
	}
	return c.Context.Err()
}

func TestAcquire_CancelAfterArchiveWorkers(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		for _, result := range []string{"unchanged", "fresh"} {
			t.Run(backing+"/"+result, func(t *testing.T) {
				t.Parallel()
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				data := orderedArchive(t, "file", "same")
				want := ocispec.Descriptor{Digest: digest.FromString("upstream"), Size: 123, Annotations: map[string]string{"flate.revision": "original"}}
				keys := []ArchiveKey{{Content: "completion-one"}, {Content: "completion-two"}}
				ids, pins := make([]string, len(keys)), make([]string, len(keys))
				previous := make([]entry, len(keys))
				for i, key := range keys {
					input, _, id, err := c.Acquire(t.Context(), key, Policy{}, producer(data, want))
					assert.NoError(t, err)
					ids[i], pins[i] = id, AcquisitionIdentity(input)
					v, _ := c.entries.Load(id)
					previous[i] = *v.(*entry)
				}
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					started, produce := make(chan struct{}), make(chan struct{})
					done := make(chan struct{}, len(keys))
					callbacks := make([]*archiveCompletionContext, len(keys))
					for i, key := range keys {
						callback := &archiveCompletionContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
						callbacks[i] = callback
						go func() {
							input, _, id, err := c.Acquire(callback, key, Policy{}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
								prior, err := w.Previous(ctx)
								if err != nil || prior == nil || cmp.Diff(want, *prior) != "" {
									t.Errorf("previous descriptor: %+v, %v", prior, err)
								}
								child := ctx.Done()
								callback.child.Store(&child)
								started <- struct{}{}
								<-produce
								if result == "fresh" {
									return producer(data, want)(ctx, w)
								}
								return ProducerResult{Descriptor: want, Unchanged: true}, nil
							})
							if input != nil || id != "" || !errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux) {
								var contents []byte
								if input != nil {
									contents, _ = fs.ReadFile(input, "file")
								}
								t.Errorf("post-worker cancellation: filesystem=%v identity=%q contents=%q error=%v", input != nil, id, contents, err)
							}
							done <- struct{}{}
						}()
					}
					for range keys {
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
					for range keys {
						<-done
					}
				})
				unexpected := func(context.Context, ProducerInput) (ProducerResult, error) {
					t.Error("retained generation called producer")
					return ProducerResult{}, errors.New("unexpected refetch")
				}
				for i, key := range keys {
					v, _ := c.entries.Load(ids[i])
					e := v.(*entry)
					if !e.resolved.Equal(previous[i].resolved) || e.outcome != nil || cmp.Diff(previous[i].stored, e.stored) != "" {
						t.Errorf("canceled attempt replaced original generation: resolved=%v, want %v", e.resolved, previous[i].resolved)
					}
					if backing == "disk" {
						stored, err := c.store.Resolve(t.Context(), ids[i])
						assert.NoError(t, err)
						if diff := cmp.Diff(previous[i].stored, stored); diff != "" {
							t.Error(diff)
						}
						payload, err := c.root.ReadFile(blobPath(stored.Digest))
						if err != nil || !bytes.Equal(payload, data) {
							t.Fatalf("retained archive payload: %v", err)
						}
					}
					input, desc, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, unexpected)
					if err != nil || id != ids[i] || AcquisitionIdentity(input) != pins[i] || cmp.Diff(want, desc) != "" {
						t.Fatalf("retained acquisition: identity=%q, error=%v", id, err)
					}
					if content, err := fs.ReadFile(input, "file"); err != nil || string(content) != "same" {
						t.Fatalf("retained content: %q, %v", content, err)
					}
				}
				if backing == "disk" {
					assert.NoError(t, c.Close())
					c = cachetest.New(t, root, New)
				}
				for i, key := range keys {
					input, _, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, unexpected)
					assert.NoError(t, err)
					if id != ids[i] || AcquisitionIdentity(input) != pins[i] {
						t.Fatal("reopened generation changed identity")
					}
					v, _ := c.entries.Load(id)
					if !v.(*entry).resolved.Equal(previous[i].resolved) {
						t.Fatal("canceled attempt renewed resolution time")
					}
					input, _, id, err = c.Acquire(t.Context(), key, Policy{}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
						prior, err := w.Previous(ctx)
						if err != nil || prior == nil || cmp.Diff(want, *prior) != "" {
							t.Errorf("forced refresh lost previous descriptor: %+v, %v", prior, err)
							return ProducerResult{}, manifest.ErrInput
						}
						return ProducerResult{Descriptor: *prior, Unchanged: true}, nil
					})
					if err != nil || id != ids[i] || AcquisitionIdentity(input) != pins[i] {
						t.Fatalf("forced refresh: identity=%q, error=%v", id, err)
					}
					if content, err := fs.ReadFile(input, "file"); err != nil || string(content) != "same" {
						t.Fatalf("forced refresh content: %q, %v", content, err)
					}
				}
			})
		}
	}
}

func TestAcquire_UnchangedRefreshRenews(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	key := ArchiveKey{Content: "source-unchanged"}
	var first time.Time
	for range 2 {
		_, _, id, err := c.Acquire(t.Context(), key, Policy{}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			previous, err := w.Previous(ctx)
			if err != nil {
				return ProducerResult{}, err
			}
			if previous != nil {
				return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
			}
			return producer(orderedArchive(t, "file", "same"), ocispec.Descriptor{})(ctx, w)
		})
		if err != nil {
			t.Fatal(err)
		}
		desc, err := c.store.Resolve(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		_, resolved, valid := decode(desc, key.Required)
		if !valid || !resolved.After(first) {
			t.Fatalf("resolution time not renewed: %v after %v", resolved, first)
		}
		first = resolved
	}
	for _, scenario := range []string{"missing previous", "wrote bytes", "producer error"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, "", New)
			key := ArchiveKey{Content: scenario}
			if scenario != "missing previous" {
				_, _, _, err := c.Acquire(t.Context(), key, Policy{}, producer(orderedArchive(t, "file", "same"), ocispec.Descriptor{}))
				assert.NoError(t, err)
			}
			input, _, id, err := c.Acquire(t.Context(), key, Policy{}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				if scenario == "producer error" {
					return ProducerResult{Unchanged: true}, fs.ErrPermission
				}
				if scenario == "wrote bytes" {
					_, err := w.Write(orderedArchive(t, "file", "new"))
					if err != nil {
						return ProducerResult{}, err
					}
				}
				return ProducerResult{Unchanged: true}, nil
			})
			want := manifest.ErrInput
			if scenario == "producer error" {
				want = fs.ErrPermission
			}
			if input != nil || id != "" || !errors.Is(err, want) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("invalid unchanged result: filesystem=%v identity=%q error=%v", input != nil, id, err)
			}
		})
	}
}

type archiveCancelContext struct {
	context.Context
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (c *archiveCancelContext) Err() error {
	err := c.Context.Err()
	// The callback's second Err call pauses it between the two pipe closes.
	if err != nil && c.calls.Add(1) == 2 {
		close(c.entered)
		<-c.release
	}
	return err
}

func TestAcquire_CancelStopsContextIgnoringProducer(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, backing := range []string{"memory", "disk"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, backing), func(t *testing.T) {
				t.Parallel()
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					started, write := make(chan struct{}), make(chan struct{})
					done := make(chan error, concurrency)
					callbacks := make([]*archiveCancelContext, concurrency)
					for i := range concurrency {
						callbacks[i] = &archiveCancelContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
						go func() {
							_, _, _, err := c.Acquire(callbacks[i], ArchiveKey{Content: fmt.Sprintf("source-ignore-context-%d", i)}, Policy{MaxAge: 0}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
								tw := tar.NewWriter(w)
								if err := tw.WriteHeader(&tar.Header{Name: "file", Size: 1 << 30, Mode: 0o600}); err != nil {
									return ProducerResult{}, err
								}
								started <- struct{}{}
								<-write
								var chunk [512]byte
								_, err := tw.Write(chunk[:])
								return ProducerResult{}, err
							})
							done <- err
						}()
					}
					for range concurrency {
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
					for range concurrency - completed {
						if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux) {
							t.Errorf("cancellation: %v", err)
						}
					}
					if backing == "disk" {
						for i := range concurrency {
							if _, err := c.store.Resolve(t.Context(), fmt.Sprintf("source-ignore-context-%d", i)); !errors.Is(err, errdef.ErrNotFound) {
								t.Errorf("interrupted archive published: %v", err)
							}
						}
					}
				})
			})
		}
	}
}

func TestNew_UnreadableIndexRebuilds(t *testing.T) {
	saved := slog.Default()
	t.Cleanup(func() { slog.SetDefault(saved) })
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := ArchiveKey{Content: "source-index-recovery"}
	var payloadTar bytes.Buffer
	testutil.WriteTar(t, &payloadTar, map[string]string{"file": "same"}, 0o644)
	data := payloadTar.Bytes()
	if _, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, producer(data, ocispec.Descriptor{})); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(root, indexPath())
	if err := os.WriteFile(index, []byte(`{"schemaVersion":`), 0o600); err != nil {
		t.Fatal(err)
	}
	c = cachetest.New(t, root, New)
	if got := logs.String(); strings.Count(got, "level=DEBUG msg=\"cache: rebuilding unreadable store index\"") != 1 || strings.Contains(got, "level=WARN") {
		t.Fatalf("index recovery log: %s", got)
	}
	calls := 0
	input, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
		calls++
		return producer(data, ocispec.Descriptor{})(ctx, w)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := fs.ReadFile(input, "file"); err != nil || string(got) != "same" || calls != 1 {
		t.Fatalf("recovery: content=%q calls=%d err=%v", got, calls, err)
	}
}

func TestNew_InvalidLayoutPreservesIndex(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(root, indexPath())
	want, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ociDir, "oci-layout"), []byte(`{"imageLayoutVersion":`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(t.Context(), root); err == nil {
		t.Fatal("invalid layout accepted")
	}
	if got, err := os.ReadFile(index); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("unrelated layout error changed index: %q %v", got, err)
	}
}

// The trailing allowance is exactly maxTrailingBytes; the producer returns right after its last write,
// so only the consumer's own check can reject the extra byte.
func TestAcquire_TrailingBoundary(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		for _, extra := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/%d", backing, extra), func(t *testing.T) {
				t.Parallel()
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				_, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-trailing-boundary"}, Policy{MaxAge: 0}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
					var payloadTar bytes.Buffer
					testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
					if _, err := w.Write(payloadTar.Bytes()); err != nil {
						return ProducerResult{}, err
					}
					_, err := w.Write(make([]byte, maxTrailingBytes+extra))
					return ProducerResult{}, err
				})
				if (err != nil) != (extra > 0) {
					t.Fatalf("extra=%d: err=%v", extra, err)
				}
			})
		}
	}
}

func TestAcquire_TrailingFailureKeepsCacheIdentity(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			var payloadTar bytes.Buffer
			testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
			data := payloadTar.Bytes()
			trailingWriteFailed := false
			_, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-trailing-failure"}, Policy{MaxAge: 0}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
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
			})
			if !trailingWriteFailed {
				t.Fatal("trailing write did not fail")
			}
			if _, ok := errors.AsType[*CacheError](err); !ok {
				t.Fatalf("%T %v, want *CacheError", err, err)
			}
		})
	}
}

func TestGit_RestartAndInvalidation(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"hit", "ref-mismatch", "missing-objects", "touch-failure", "outcome"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			src, root := t.TempDir(), t.TempDir()
			repo := testutil.GitInit(t, src)
			testutil.WriteFileAt(t, filepath.Join(src, "file"), "committed")
			testutil.GitCommitAll(t, repo, "seed", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example.invalid"}})
			calls := 0
			produce := func(_ context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
				calls++
				r, err := git.CloneContext(t.Context(), storer, worktree, &git.CloneOptions{URL: "file://" + src, NoCheckout: true})
				if err != nil {
					return plumbing.ZeroHash, err
				}
				head, err := r.Head()
				if err != nil {
					return plumbing.ZeroHash, err
				}
				return head.Hash(), nil
			}
			c := cachetest.New(t, root, New)
			key := GitKey{Content: "source-git-restart", Ref: "HEAD"}
			first, id, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, produce)
			if err != nil {
				t.Fatal(err)
			}
			archiveKey := ArchiveKey{Content: key.Content}
			var payloadTar bytes.Buffer
			testutil.WriteTar(t, &payloadTar, map[string]string{"file": "archive"}, 0o644)
			_, _, archiveID, err := c.Acquire(t.Context(), archiveKey, Policy{MaxAge: Forever}, producer(payloadTar.Bytes(), ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}
			blobKey := BlobKey{Content: key.Content}
			r, _, blobID, err := c.Blob(t.Context(), blobKey, Policy{MaxAge: Forever}, producer([]byte("blob"), ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if id == "" || archiveID == id || blobID == id || archiveID == blobID {
				t.Fatalf("product identities: git=%q archive=%q blob=%q", id, archiveID, blobID)
			}
			v, _ := c.entries.Load(id)
			e := v.(*entry)
			blob := filepath.Join(c.root.Name(), blobPath(e.stored.Digest))
			stale := time.Now().Add(-time.Hour)
			if err := os.Chtimes(blob, stale, stale); err != nil {
				t.Fatal(err)
			}
			if scenario == "outcome" {
				o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, e.stored)
				for n := uint32(1); n <= 4; n++ {
					o.record.Count = n
					desc, err := c.publishOutcome(t.Context(), id, o, connReset())
					if err != nil {
						t.Fatal(err)
					}
					o, err = c.readOutcome(t.Context(), desc)
					if err != nil || o == nil || o.lastGood.Digest != e.stored.Digest || o.lastGood.Annotations[prefix+"git-generation"] != first.id {
						t.Fatalf("direct last-good anchor: %+v, %v", o, err)
					}
				}
			}
			if scenario == "missing-objects" {
				if err := os.RemoveAll(filepath.Join(filepath.Join(c.root.Name(), gitObjectsPath(first.id)), "objects")); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if scenario == "ref-mismatch" {
				key.Ref = "tag:other"
			}
			if err := os.RemoveAll(src); err != nil {
				t.Fatal(err)
			}
			c = cachetest.New(t, root, New)
			unexpected := func(context.Context, ProducerInput) (ProducerResult, error) {
				t.Error("restored product called producer")
				return ProducerResult{}, errors.New("unexpected producer")
			}
			input, _, restoredID, err := c.Acquire(t.Context(), archiveKey, Policy{MaxAge: Forever}, unexpected)
			if err != nil || restoredID != archiveID {
				t.Fatalf("restored archive: id=%q, %v", restoredID, err)
			}
			if data, err := fs.ReadFile(input, "file"); err != nil || string(data) != "archive" {
				t.Fatalf("restored archive payload: %q, %v", data, err)
			}
			r, _, restoredID, err = c.Blob(t.Context(), blobKey, Policy{MaxAge: Forever}, nil)
			if err != nil || restoredID != blobID {
				t.Fatalf("restored blob: id=%q, %v", restoredID, err)
			}
			data, readErr := io.ReadAll(r)
			if err := errors.Join(readErr, r.Close()); err != nil || string(data) != "blob" {
				t.Fatalf("restored blob payload: %q, %v", data, err)
			}
			if scenario == "outcome" {
				desc, err := c.store.Resolve(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
				o, err := c.readOutcome(t.Context(), desc)
				if err != nil || o == nil || o.record.Count != 4 || o.lastGood.Digest != e.stored.Digest || calls != 1 {
					t.Fatalf("restored outcome: %+v, %v", o, err)
				}
				input, _, err := gittree.New(t.Context(), first.Backing, first.Commit, gittree.ViewOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if data, err := fs.ReadFile(input, "file"); err != nil || string(data) != "committed" {
					t.Fatalf("held Git content: %q, %v", data, err)
				}
				return
			}
			second, _, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, produce)
			if scenario == "ref-mismatch" || scenario == "missing-objects" {
				if err == nil || calls != 2 {
					t.Fatalf("invalid restore: calls=%d, %v", calls, err)
				}
				return
			}
			if err != nil || calls != 1 {
				t.Fatalf("restart hit: calls=%d, %v", calls, err)
			}
			input, _, err = gittree.New(t.Context(), second.Backing, second.Commit, gittree.ViewOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if data, err := fs.ReadFile(input, "file"); err != nil || string(data) != "committed" {
				t.Fatalf("restored content: %q, %v", data, err)
			}
			v, _ = c.entries.Load(id)
			if !e.resolved.Equal(v.(*entry).resolved) {
				t.Fatal("restart hit extended freshness")
			}
			info, err := os.Stat(blob)
			if err != nil || !info.ModTime().After(stale) {
				t.Fatalf("anchor touch: %v, %v", info, err)
			}
			if scenario == "touch-failure" {
				if err := os.Remove(blob); err != nil {
					t.Fatal(err)
				}
				v.(*entry).touched = stale
				if _, _, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, produce); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("touch failure: %v", err)
				}
				if err := os.WriteFile(blob, []byte(first.id), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, _, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, produce); err == nil || calls != 2 {
					t.Fatalf("poisoned hit reused: calls=%d, %v", calls, err)
				}
			}
		})
	}

	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("recursive-held-%t", disk), func(t *testing.T) {
			t.Parallel()
			src := t.TempDir()
			repo := testutil.GitInit(t, src)
			testutil.WriteFile(t, src, "nested/file", "first")
			testutil.GitCommitAll(t, repo, "first", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			key := GitKey{Content: "recursive-held", RecurseSubmodules: true}
			produce := func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
				repo, err := git.CloneContext(ctx, storer, worktree, &git.CloneOptions{URL: "file://" + src})
				if err != nil {
					return plumbing.ZeroHash, err
				}
				head, err := repo.Head()
				if err != nil {
					return plumbing.ZeroHash, err
				}
				return head.Hash(), nil
			}
			admit := func(generation *GitGeneration) (*gittree.FS, string) {
				t.Helper()
				input, pin, err := gittree.New(t.Context(), generation.Backing, generation.Commit, gittree.ViewOptions{Worktree: generation.Worktree, Content: key.Content})
				if err != nil {
					t.Fatal(err)
				}
				return input, pin
			}
			first, _, err := c.Git(t.Context(), key, Policy{MaxAge: 0}, produce)
			if err != nil {
				t.Fatal(err)
			}
			held, pin := admit(first)
			latestPin := pin
			for _, changed := range []bool{false, true} {
				if changed {
					testutil.WriteFile(t, src, "nested/file", "second")
					testutil.GitCommitAll(t, repo, "second", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
				}
				next, _, err := c.Git(t.Context(), key, Policy{MaxAge: 0}, produce)
				if err != nil {
					t.Fatal(err)
				}
				_, nextPin := admit(next)
				latestPin = nextPin
				if (nextPin != pin) != changed {
					t.Fatal("Git identity differs from acquired key/commit")
				}
				if data, err := fs.ReadFile(held, "nested/file"); err != nil || string(data) != "first" {
					t.Fatalf("held recursive generation changed: %q, %v", data, err)
				}
				_, retained, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: held})
				if err != nil || retained != pin {
					t.Fatalf("held recursive identity changed: %q, %v", retained, err)
				}
			}
			if disk {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				c = cachetest.New(t, root, New)
				restored, _, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, nil)
				if err != nil {
					t.Fatal(err)
				}
				input, pin := admit(restored)
				if pin == "" || pin != latestPin {
					t.Fatal("recursive restored identity lost")
				}
				if data, err := fs.ReadFile(input, "nested/file"); err != nil || string(data) != "second" {
					t.Fatalf("recursive restoration: %q, %v", data, err)
				}
			}
		})
	}
}

func TestGit_FailedAttemptRemovesDirectories(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"producer", "cancelled", "publication"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("incomplete clone")
			g, _, err := c.Git(ctx, GitKey{Content: "source-git-failed", RecurseSubmodules: true}, Policy{MaxAge: 0}, func(_ context.Context, _ storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
				file, err := worktree.Create("partial")
				if err != nil {
					return plumbing.ZeroHash, err
				}
				if err := file.Close(); err != nil {
					return plumbing.ZeroHash, err
				}
				if scenario == "publication" {
					index := filepath.Join(c.root.Name(), indexPath())
					if err := os.Rename(index, index+"-saved"); err != nil {
						return plumbing.ZeroHash, err
					}
					return plumbing.ZeroHash, os.Mkdir(index, 0o750)
				}
				if scenario == "cancelled" {
					cancel()
					return plumbing.ZeroHash, nil
				}
				return plumbing.ZeroHash, failure
			})
			if g != nil || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failed Git acquisition: generation=%v, error=%v", g, err)
			}
			want := failure
			if scenario == "cancelled" {
				want = context.Canceled
			}
			if scenario == "publication" {
				if _, ok := errors.AsType[*CacheError](err); !ok {
					t.Fatalf("publication error = %v", err)
				}
			} else if !errors.Is(err, want) {
				t.Fatalf("Git error = %v, want %v", err, want)
			}
			entries, err := os.ReadDir(filepath.Dir(filepath.Join(c.root.Name(), gitObjectsPath(""))))
			if err != nil || len(entries) != 0 {
				t.Fatalf("failed generation directories = %v, %v", entries, err)
			}
			c.entries.Range(func(_, v any) bool {
				if !v.(*entry).resolved.IsZero() {
					t.Error("failed Git acquisition installed a successful entry")
				}
				return true
			})
			if scenario == "publication" {
				index := filepath.Join(c.root.Name(), indexPath())
				if err := os.Remove(index); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(index+"-saved", index); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := cachetest.New(t, c.root.Name(), New)
			if err := reopened.store.Tags(t.Context(), "", func(tags []string) error {
				if len(tags) != 0 {
					t.Fatalf("failed generation published durable tags: %v", tags)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCache_RootedWritesRejectEscape(t *testing.T) {
	t.Parallel()
	for _, sink := range []string{"staging", "last-use", "last-use-parent", "git-generation", "staging-cleanup-parent", "staging-cleanup-final"} {
		t.Run(sink, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			outside := t.TempDir()
			testutil.WriteFile(t, outside, "marker", "untouched")
			key := BlobKey{Content: "rooted-write"}
			if sink == "git-generation" {
				key.Content = "source-git-rooted"
			}
			link, target := filepath.Join(c.root.Name(), ociDir), outside
			hit := strings.HasPrefix(sink, "last-use")
			cleanup := strings.HasPrefix(sink, "staging-cleanup")
			if hit {
				r, _, id, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, producer([]byte("content"), ocispec.Descriptor{}))
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				e, _ := c.entries.Load(id)
				payload := blobPath(e.(*entry).stored.Digest)
				link = filepath.Join(c.root.Name(), payload)
				target = filepath.Join(outside, "marker")
				if sink == "last-use-parent" {
					link = filepath.Dir(link)
					target = outside
					testutil.WriteFile(t, outside, filepath.Base(payload), "outside payload")
				}
			} else if sink == "git-generation" {
				link = filepath.Join(filepath.Join(c.root.Name(), ociDir), "git")
				if err := os.Mkdir(link, 0o750); err != nil {
					t.Fatal(err)
				}
			}
			if !cleanup {
				if err := os.Rename(link, link+"-saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
			}
			before := sweepSnapshot(t, outside)
			failure := errors.New("producer failed after staging replacement")
			called := false
			var err error
			if sink == "git-generation" {
				var g *GitGeneration
				g, _, err = c.Git(t.Context(), GitKey{Content: key.Content}, Policy{MaxAge: 0}, func(context.Context,

					storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
					called = true
					return plumbing.ZeroHash, nil
				})
				if g != nil {
					t.Fatal("escape returned a Git generation")
				}
			} else {
				var r io.ReadCloser
				r, _, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, func(context.Context, ProducerInput) (ProducerResult, error) {
					called = true
					if !cleanup {
						return ProducerResult{}, nil
					}
					entries, err := fs.ReadDir(c.root.FS(), "oci")
					if err != nil {
						t.Fatal(err)
					}
					var staging []string
					for _, entry := range entries {
						if strings.HasPrefix(entry.Name(), "acquire-") {
							staging = append(staging, entry.Name())
						}
					}
					if len(staging) != 1 {
						t.Fatalf("exclusive staging files = %v", staging)
					}
					name := filepath.Join("oci", staging[0])
					info, err := c.root.Stat(name)
					if err != nil || info.Mode().Perm() != 0o600 {
						t.Fatalf("staging permissions: %v, %v", info, err)
					}
					testutil.WriteFile(t, outside, staging[0], "outside staging")
					if sink == "staging-cleanup-final" {
						link = filepath.Join(c.root.Name(), name)
						target = filepath.Join(outside, staging[0])
					}
					if err := os.Rename(link, link+"-saved"); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(target, link); err != nil {
						t.Fatal(err)
					}
					before = sweepSnapshot(t, outside)
					return ProducerResult{}, failure
				})
				if r != nil {
					_ = r.Close()
					t.Fatal("escape returned a blob reader")
				}
			}
			if cleanup {
				if !called || !errors.Is(err, failure) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("staging cleanup: producer=%v, error=%v", called, err)
				}
			} else if _, ok := errors.AsType[*CacheError](err); !ok || called || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("escape: producer=%v, error=%v", called, err)
			}
			if diff := cmp.Diff(before, sweepSnapshot(t, outside)); diff != "" {
				t.Fatalf("outside changed (-want +got):\n%s", diff)
			}
			c.entries.Range(func(_, v any) bool {
				if !v.(*entry).resolved.IsZero() {
					t.Error("failed acquisition installed a successful entry")
				}
				return true
			})
			if sink == "staging-cleanup-final" {
				if _, err := os.Lstat(link); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("staging cleanup retained final link: %v", err)
				}
			} else if err := os.Remove(link); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(link+"-saved", link); err != nil {
				t.Fatal(err)
			}
			if err := c.store.Tags(t.Context(), "", func(tags []string) error {
				if len(tags) > 1 || hit != (len(tags) == 1) {
					t.Fatalf("publication after failure: %v", tags)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNew_IndexRecoveryRejectsEscapingParent(t *testing.T) {
	t.Parallel()
	for _, component := range []string{"parent", "final"} {
		t.Run(component, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			outside := cachetest.New(t, t.TempDir(), New)
			if err := errors.Join(c.Close(), outside.Close()); err != nil {
				t.Fatal(err)
			}
			index := filepath.Join(outside.root.Name(), indexPath())
			testutil.WriteFile(t, filepath.Join(outside.root.Name(), ociDir), ocispec.ImageIndexFile, `{"schemaVersion":`)
			link, target := filepath.Join(c.root.Name(), ociDir), filepath.Join(outside.root.Name(), ociDir)
			if component == "final" {
				link = filepath.Join(link, ocispec.ImageIndexFile)
				target = index
			}
			if err := os.Rename(link, link+"-saved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
			before := sweepSnapshot(t, outside.root.Name())
			recovered, err := New(t.Context(), c.root.Name())
			if component == "parent" {
				if _, ok := errors.AsType[*CacheError](err); !ok || recovered != nil || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("parent recovery: cache=%v, error=%v", recovered, err)
				}
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(link+"-saved", link); err != nil {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := recovered.Close(); err != nil {
					t.Fatal(err)
				}
				info, err := os.Lstat(link)
				if err != nil || !info.Mode().IsRegular() {
					t.Fatalf("rebuilt index: %v, %v", info, err)
				}
				if err := os.Remove(link + "-saved"); err != nil {
					t.Fatal(err)
				}
			}
			if diff := cmp.Diff(before, sweepSnapshot(t, outside.root.Name())); diff != "" {
				t.Fatalf("outside index changed (-want +got):\n%s", diff)
			}
			reopened := cachetest.New(t, c.root.Name(), New)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGit_FailedCleanupRejectsEscapingParent(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"producer", "publication"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			outside := t.TempDir()
			key := GitKey{Content: "source-git-escaping-cleanup", RecurseSubmodules: true}
			parent := filepath.Dir(filepath.Join(c.root.Name(), gitObjectsPath("")))
			index := filepath.Join(c.root.Name(), indexPath())
			failure := errors.New("producer failed after parent replacement")
			var before map[string]string
			g, _, err := c.Git(t.Context(), key, Policy{MaxAge: 0},
				func(_ context.Context, _ storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
					id := filepath.Base(filepath.Dir(worktree.Root()))
					testutil.WriteFile(t, outside, filepath.Join(id, "marker"), "untouched")
					before = sweepSnapshot(t, outside)
					if err := os.Rename(parent, parent+"-saved"); err != nil {
						return plumbing.ZeroHash, err
					}
					if err := os.Symlink(outside, parent); err != nil {
						return plumbing.ZeroHash, err
					}
					if phase == "publication" {
						if err := os.Rename(index, index+"-saved"); err != nil {
							return plumbing.ZeroHash, err
						}
						return plumbing.ZeroHash, os.Mkdir(index, 0o750)
					}
					return plumbing.ZeroHash, failure
				})
			if g != nil || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failed Git acquisition: generation=%v, error=%v", g, err)
			}
			if phase == "publication" {
				if _, ok := errors.AsType[*CacheError](err); !ok {
					t.Fatalf("publication error = %v", err)
				}
			} else if !errors.Is(err, failure) {
				t.Fatalf("producer error = %v", err)
			}
			if diff := cmp.Diff(before, sweepSnapshot(t, outside)); diff != "" {
				t.Fatalf("outside changed (-want +got):\n%s", diff)
			}
			c.entries.Range(func(_, v any) bool {
				if !v.(*entry).resolved.IsZero() {
					t.Error("failed Git acquisition installed a successful entry")
				}
				return true
			})
			if err := os.Remove(parent); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(parent+"-saved", parent); err != nil {
				t.Fatal(err)
			}
			if phase == "publication" {
				if err := os.Remove(index); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(index+"-saved", index); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := cachetest.New(t, c.root.Name(), New)
			if err := reopened.store.Tags(t.Context(), "", func(tags []string) error {
				if len(tags) != 0 {
					t.Fatalf("failed generation published durable tags: %v", tags)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
