package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
)

func TestBlob_LookupAndReplacement(t *testing.T) {
	t.Parallel()
	t.Run("concurrent-first-read", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		keys := []BlobKey{{Content: "first"}, {Content: "second"}}
		upstream := ocispec.Descriptor{Digest: digest.FromString("upstream"), Size: 123, MediaType: "upstream/type", Annotations: map[string]string{"revision": "v1"}}
		for _, key := range keys {
			r, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte(key.Content.(string)), upstream))
			if err != nil {
				t.Fatal(err)
			}
			assert.NoError(t, r.Close())
		}
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		start := make(chan struct{})
		var ready sync.WaitGroup
		var joined sync.WaitGroup
		ready.Add(len(keys))
		for _, key := range keys {
			joined.Go(func() {
				ready.Done()
				<-start
				r, desc, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
				if err != nil {
					t.Error(err)
					return
				}
				assert.Diff(t, desc, upstream)
				data, err := io.ReadAll(r)
				closeErr := r.Close()
				if err != nil || closeErr != nil || string(data) != key.Content.(string) {
					t.Errorf("first read=%q, read=%v, close=%v", data, err, closeErr)
				}
			})
		}
		ready.Wait()
		close(start)
		joined.Wait()
		assert.NoError(t, c.Close())
		for _, parent := range c.blobRoots {
			if _, err := parent.Stat("."); !errors.Is(err, fs.ErrClosed) {
				t.Fatalf("last Close retained blob parent: %v", err)
			}
		}
	})
	t.Run("recreated-parent", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, t.TempDir(), New)
		key := BlobKey{Content: "recreated-parent"}
		r, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("first"), ocispec.Descriptor{}))
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
		assert.NoError(t, os.RemoveAll(filepath.Join(c.root.Name(), blobDir())))
		r, _, _, err = c.Blob(t.Context(), key, Policy{}, producer([]byte("second"), ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, string(data), "second")
		view := FileFS("payload", r)
		assert.NoError(t, r.Close())
		data, err = fs.ReadFile(view, "payload")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "second")
	})
	for _, algorithm := range []digest.Algorithm{digest.SHA256, digest.SHA384, digest.SHA512} {
		t.Run(algorithm.String(), func(t *testing.T) {
			t.Parallel()
			if !algorithm.Available() {
				t.Skip("digest algorithm is not linked")
			}
			root := t.TempDir()
			c := cachetest.New(t, root, New)
			key := BlobKey{Content: "indexed-digest"}
			payload := []byte("independent readers")
			upstream := ocispec.Descriptor{Digest: digest.FromString("upstream"), Size: 123, MediaType: "upstream/type", Annotations: map[string]string{"revision": "v1"}}
			r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer(payload, upstream))
			if err != nil {
				t.Fatal(err)
			}
			assert.NoError(t, r.Close())
			stored, err := c.store.Resolve(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			stored.Digest = algorithm.FromBytes(payload)
			if err := c.store.Push(t.Context(), stored, bytes.NewReader(payload)); err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
				t.Fatal(err)
			}
			assert.NoError(t, c.store.Tag(t.Context(), stored, id))
			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
			var view fs.FS
			for range 2 {
				r, desc, restoredID, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
				if err != nil {
					t.Fatal(err)
				}
				assert.Diff(t, desc, upstream)
				assert.Equal(t, restoredID, id)
				view = FileFS("payload", r)
				assert.Equal(t, AcquisitionIdentity(view), manifest.Fingerprint([2]string{stored.Digest.String(), "payload"}))
				assert.NoError(t, r.Close())
			}
			var readers [2]fs.File
			for i := range readers {
				reader, err := view.Open("payload")
				if err != nil {
					t.Fatal(err)
				}
				readers[i] = reader
				t.Cleanup(func() { _ = reader.Close() })
				first := make([]byte, 1)
				if _, err := io.ReadFull(reader, first); err != nil || first[0] != payload[0] {
					t.Fatalf("independent first byte=%q: %v", first, err)
				}
			}
			start := make(chan struct{})
			var joined sync.WaitGroup
			for _, reader := range readers {
				joined.Go(func() {
					<-start
					data, err := io.ReadAll(reader)
					closeErr := reader.Close()
					if err != nil || closeErr != nil || !bytes.Equal(data, payload[1:]) {
						t.Errorf("held reader=%q, read=%v, close=%v", data, err, closeErr)
					}
				})
			}
			close(start)
			joined.Wait()
			parent := filepath.Dir(blobPath(stored.Digest))
			assert.NoError(t, c.root.Rename(parent, parent+"-held"))
			assert.NoError(t, c.root.Mkdir(parent, 0o750))
			if reader, err := view.Open("payload"); reader != nil || !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("held view ignored the replacement directory: reader=%v, error=%v", reader, err)
			}
			assert.NoError(t, c.root.Remove(parent))
			assert.NoError(t, os.Symlink(t.TempDir(), filepath.Join(root, parent)))
			if r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); r != nil || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("coalesced acquisition accepted a replaced parent: reader=%v, error=%v", r, err)
			}
			if reader, err := view.Open("payload"); reader != nil || err == nil {
				t.Fatalf("held view accepted a replaced parent: reader=%v, error=%v", reader, err)
			}
		})
	}
	t.Run("outcomes", func(t *testing.T) {
		t.Parallel()
		for _, maxAge := range []time.Duration{0, time.Hour, Forever, -2} {
			t.Run(maxAge.String(), func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					root := t.TempDir()
					c := cachetest.New(t, root, New)
					key := BlobKey{Content: "recorded"}
					calls := 0
					fail := func(context.Context, io.Writer) (ocispec.Descriptor, error) {
						calls++
						return ocispec.Descriptor{}, connReset()
					}
					policy := Policy{MaxAge: maxAge, Retry: fastRetry(3)}
					r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("original"), ocispec.Descriptor{}))
					if err != nil {
						t.Fatal(err)
					}
					if err := r.Close(); err != nil {
						t.Fatal(err)
					}
					lastGood, err := c.store.Resolve(t.Context(), id)
					if err != nil {
						t.Fatal(err)
					}
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					c = cachetest.New(t, root, New)
					_, _, _, err = c.Blob(t.Context(), key, Policy{Retry: fastRetry(3)}, fail)
					terminal, ok := errors.AsType[*TerminalError](err)
					if !ok || !terminal.Attempted || calls != 3 {
						t.Fatalf("initial calls=%d, error=%v", calls, err)
					}
					desc, err := c.store.Resolve(t.Context(), id)
					if err != nil || desc.MediaType != ocispec.MediaTypeImageManifest {
						t.Fatalf("same-tag outcome=%+v, error=%v", desc, err)
					}
					initial, err := c.readOutcome(t.Context(), desc)
					if err != nil || initial == nil || initial.record.Count != 1 || initial.lastGood.Digest != lastGood.Digest {
						t.Fatalf("initial outcome=%+v, error=%v", initial, err)
					}
					for range 2 {
						if err := c.Close(); err != nil {
							t.Fatal(err)
						}
						c = cachetest.New(t, root, New)
						for _, produce := range []func(context.Context, io.Writer) (ocispec.Descriptor, error){nil, fail} {
							_, _, _, err := c.Blob(t.Context(), key, policy, produce)
							terminal, ok := errors.AsType[*TerminalError](err)
							if !ok || terminal.Attempted || !errors.Is(err, syscall.ECONNRESET) || !errors.Is(err, manifest.ErrFlux) || calls != 3 {
								t.Fatalf("fresh calls=%d, error=%v", calls, err)
							}
						}
					}
					time.Sleep(750*time.Millisecond - time.Nanosecond)
					_, _, _, err = c.Blob(t.Context(), key, policy, fail)
					if terminal, ok := errors.AsType[*TerminalError](err); !ok || terminal.Attempted || calls != 3 {
						t.Fatalf("before deadline calls=%d, error=%v", calls, err)
					}
					unchanged, err := c.readOutcome(t.Context(), desc)
					if err != nil || unchanged == nil || unchanged.record.Count != initial.record.Count || !unchanged.record.Completed.Equal(initial.record.Completed) {
						t.Fatalf("read renewed outcome: %+v, error=%v", unchanged, err)
					}
					time.Sleep(time.Nanosecond)
					if _, _, _, err := c.Blob(t.Context(), key, policy, nil); !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("due lookup: %v", err)
					}
					_, _, _, err = c.Blob(t.Context(), key, policy, fail)
					if terminal, ok := errors.AsType[*TerminalError](err); !ok || !terminal.Attempted || calls != 4 {
						t.Fatalf("due calls=%d, error=%v", calls, err)
					}
					nextDesc, err := c.store.Resolve(t.Context(), id)
					if err != nil {
						t.Fatal(err)
					}
					next, err := c.readOutcome(t.Context(), nextDesc)
					if err != nil || next == nil || next.record.Count != 2 || !next.record.Completed.Equal(time.Now()) {
						t.Fatalf("second outcome=%+v, error=%v", next, err)
					}
					time.Sleep(1500 * time.Millisecond)
					r, _, _, err = c.Blob(t.Context(), key, policy, producer([]byte("recovered"), ocispec.Descriptor{}))
					if err != nil {
						t.Fatal(err)
					}
					if err := r.Close(); err != nil {
						t.Fatal(err)
					}
					good, err := c.store.Resolve(t.Context(), id)
					if err != nil || good.MediaType != "application/octet-stream" {
						t.Fatalf("recovery descriptor=%+v, error=%v", good, err)
					}
					_, _, _, err = c.Blob(t.Context(), key, Policy{}, fail)
					if !errors.Is(err, syscall.ECONNRESET) || calls != 5 {
						t.Fatalf("new sequence calls=%d, error=%v", calls, err)
					}
					resetDesc, err := c.store.Resolve(t.Context(), id)
					if err != nil {
						t.Fatal(err)
					}
					reset, err := c.readOutcome(t.Context(), resetDesc)
					if err != nil || reset == nil || reset.record.Count != 1 || reset.lastGood.Digest != good.Digest {
						t.Fatalf("reset outcome=%+v, error=%v", reset, err)
					}
				})
			})
		}
	})

	identities := make(map[string]string)
	memory := cachetest.New(t, "", New)
	for _, payload := range []string{"first", ""} {
		r, _, _, err := memory.Blob(t.Context(), BlobKey{Content: "blob-completed"}, Policy{}, producer([]byte(payload), ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}
		identities[payload] = AcquisitionIdentity(FileFS("chart.tgz", r))
		assert.NoError(t, r.Close())
	}
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			key := BlobKey{Content: "blob-completed"}
			for range 2 {
				r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
				if r != nil || !errors.Is(err, errdef.ErrNotFound) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("lookup miss: reader=%v, error=%v", r, err)
				}
			}
			c.entries.Range(func(_, _ any) bool { t.Error("lookup miss installed an entry"); return true })
			var held fs.FS
			var heldPin, heldPayload string
			for _, payload := range []string{"first", ""} {
				r, _, id, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, producer([]byte(payload), ocispec.Descriptor{}))
				if err != nil {
					t.Fatal(err)
				}
				view := FileFS("chart.tgz", r)
				pin := AcquisitionIdentity(view)
				assert.Equal(t, pin, identities[payload])
				if pin == "" || pin == AcquisitionIdentity(FileFS("other.tgz", r)) || AcquisitionIdentity(struct{ fs.FS }{view}) != "" {
					t.Fatal("blob identity lost generation, filename or confinement")
				}
				if held != nil {
					data, err := fs.ReadFile(held, "chart.tgz")
					if err != nil || string(data) != heldPayload || AcquisitionIdentity(held) != heldPin || heldPin == pin {
						t.Fatalf("held generation changed: %q, %v", data, err)
					}
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				if r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, nil); !errors.Is(err, errdef.ErrNotFound) || r != nil {
					t.Fatalf("stale lookup: reader=%v, error=%v", r, err)
				}
				if backing == "disk" {
					e, _ := c.entries.Load(id)
					raw, err := os.ReadFile(filepath.Join(c.root.Name(), blobPath(e.(*entry).stored.Digest)))
					if err != nil || string(raw) != payload {
						t.Fatalf("plain completed payload=%q: %v", raw, err)
					}
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					c = cachetest.New(t, root, New)
					if r, _, _, err := c.Blob(t.Context(), key, Policy{}, nil); r != nil || !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("cold zero-age lookup: reader=%v, error=%v", r, err)
					}
				}
				first, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
				if err != nil {
					t.Fatal(err)
				}
				restored := FileFS("chart.tgz", first)
				if AcquisitionIdentity(restored) != pin {
					t.Fatal("blob restoration changed identity")
				}
				held, heldPin, heldPayload = restored, pin, payload
				var data []byte
				if backing == "memory" {
					data, err = fs.ReadFile(view, "chart.tgz")
				}
				// The copied view retains its generation after the original cursor closes.
				if backing == "memory" && (err != nil || string(data) != payload) {
					t.Fatalf("closed cursor invalidated view: %q, %v", data, err)
				}
				// The same key must remain usable while a returned reader is retained.
				var wg sync.WaitGroup
				wg.Go(func() {
					other, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
					if err != nil {
						t.Error(err)
						return
					}
					got, err := io.ReadAll(other)
					closeErr := other.Close()
					if err != nil || closeErr != nil || string(got) != payload {
						t.Errorf("independent reader=%q, read=%v, close=%v", got, err, closeErr)
					}
				})
				wg.Wait()
				got, err := io.ReadAll(first)
				closeErr := first.Close()
				if err != nil || closeErr != nil || string(got) != payload {
					t.Fatalf("held reader=%q, read=%v, close=%v", got, err, closeErr)
				}
			}
		})
	}
}

func TestBlob_PersistOnlyLifetime(t *testing.T) {
	original := processIdentity
	t.Cleanup(func() { processIdentity = original })
	for _, identity := range []string{"", "identified"} {
		t.Run("identity="+identity, func(t *testing.T) {
			processIdentity = identity
			for _, backing := range []string{"memory", "disk"} {
				t.Run(backing, func(t *testing.T) {
					root := ""
					if backing == "disk" {
						root = t.TempDir()
					}
					c := cachetest.New(t, root, New)
					key := BlobKey{Content: "render-test-v1-payload", PersistOnly: true}
					for _, content := range []any{"", struct{ Key string }{"render-test-v1-payload"}} {
						if r, _, id, err := c.Blob(t.Context(), BlobKey{Content: content, PersistOnly: true}, Policy{MaxAge: Forever}, nil); r != nil || id != "" || !errors.Is(err, manifest.ErrInput) {
							t.Fatalf("invalid render identity: id=%q, %v", id, err)
						}
					}
					if r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); r != nil || !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("cold lookup: %v", err)
					}
					persisted := backing == "disk" && identity != ""
					calls := 0
					r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
						calls++
						_, err := io.WriteString(w, "completed")
						return ocispec.Descriptor{}, err
					})
					if err != nil {
						t.Fatal(err)
					}
					want, wantCalls := "", 0
					if persisted {
						want, wantCalls = "completed", 1
					}
					data, err := io.ReadAll(r)
					if err != nil || string(data) != want || calls != wantCalls {
						t.Fatalf("publication=%q, calls=%d, error=%v", data, calls, err)
					}
					if err := r.Close(); err != nil {
						t.Fatal(err)
					}
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					c = cachetest.New(t, root, New)
					r, _, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
					if persisted {
						if err != nil {
							t.Fatal(err)
						}
						data, readErr := io.ReadAll(r)
						if err := errors.Join(readErr, r.Close()); err != nil || string(data) != "completed" {
							t.Fatalf("reopened frame=%q, error=%v", data, err)
						}
						return
					}
					for range 2 {
						r, _, _, err := c.Blob(t.Context(), key, Policy{Retry: fastRetry(3)}, func(context.Context, io.Writer) (ocispec.Descriptor, error) {
							t.Fatal("disabled PersistOnly ran failing producer")
							return ocispec.Descriptor{}, connReset()
						})
						if err != nil {
							t.Fatal(err)
						}
						if err := r.Close(); err != nil {
							t.Fatal(err)
						}
					}
					ctx, cancel := context.WithCancel(t.Context())
					cancel()
					for _, produce := range []func(context.Context, io.Writer) (ocispec.Descriptor, error){nil, producer(nil, ocispec.Descriptor{})} {
						if r, _, _, err := c.Blob(ctx, key, Policy{MaxAge: Forever}, produce); r != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux) {
							t.Fatalf("disabled cancellation: %v", err)
						}
					}
					if r != nil || !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("retained transient output: %v", err)
					}
					c.entries.Range(func(_, _ any) bool { t.Error("transient output installed an owner entry"); return true })
					if backing == "disk" {
						tags := []string{}
						if err := c.store.Tags(t.Context(), "", func(page []string) error { tags = append(tags, page...); return nil }); err != nil {
							t.Fatal(err)
						}
						if len(tags) != 0 {
							t.Fatalf("transient output published tags: %v", tags)
						}
						if err := fs.WalkDir(os.DirFS(filepath.Join(c.root.Name(), ociDir)), ".", func(path string, entry fs.DirEntry, err error) error {
							if err != nil {
								return err
							}
							if !entry.IsDir() && path != "oci-layout" && path != "index.json" {
								t.Errorf("transient output wrote %s", path)
							}
							return nil
						}); err != nil {
							t.Fatal(err)
						}
					}
					source, _, _, err := c.Blob(t.Context(), BlobKey{Content: "blob-ordinary"}, Policy{MaxAge: Forever}, producer([]byte("source"), ocispec.Descriptor{}))
					if err != nil {
						t.Fatal(err)
					}
					if err := source.Close(); err != nil {
						t.Fatal(err)
					}
					source, _, _, err = c.Blob(t.Context(), BlobKey{Content: "blob-ordinary"}, Policy{MaxAge: Forever}, nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := source.Close(); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestBlob_FailuresRemainErrors(t *testing.T) {
	original := processIdentity
	processIdentity = "identified"
	t.Cleanup(func() { processIdentity = original })
	for _, root := range []string{"", t.TempDir()} {
		c := cachetest.New(t, root, New)
		for _, persistOnly := range []bool{false, true} {
			if persistOnly && root == "" {
				continue
			}
			key := BlobKey{Content: "blob-error", PersistOnly: persistOnly}
			failure := errors.New("failed completed producer")
			r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
				_, _ = io.WriteString(w, "partial")
				return ocispec.Descriptor{}, failure
			})
			if r != nil || !errors.Is(err, failure) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("producer error: %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if r, _, _, err := c.Blob(ctx, key, Policy{MaxAge: Forever}, nil); r != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("lookup cancellation: %v", err)
			}
		}
	}
	c := cachetest.New(t, t.TempDir(), New)
	key := BlobKey{Content: "blob-missing-payload"}
	r, _, id, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, producer([]byte("payload"), ocispec.Descriptor{}))
	if err != nil {
		t.Fatal(err)
	}
	handle := r.(*blobHandle)
	if err := handle.ReadCloser.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("backing Close error was lost: %v", err)
	}
	if handle.ReadCloser != nil || handle.input.open != nil {
		t.Fatal("failed Close retained the backing reader")
	}
	e, _ := c.entries.Load(id)
	if err := os.Remove(filepath.Join(c.root.Name(), blobPath(e.(*entry).stored.Digest))); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); r != nil || !errors.Is(err, fs.ErrNotExist) || errors.Is(err, errdef.ErrNotFound) || !errors.Is(err, manifest.ErrFlux) {
			t.Fatalf("missing payload became miss: %v", err)
		}
	}
}

func TestBlob_ConcurrentPayloads(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	const workers = 4
	start := make(chan struct{})
	var ready, joined sync.WaitGroup
	ready.Add(workers)
	for i := range workers {
		joined.Go(func() {
			ready.Done()
			<-start
			payload := bytes.Repeat([]byte{byte(i)}, 2<<20)
			key := BlobKey{Content: string(rune('a' + i))}
			r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, producer(payload, ocispec.Descriptor{}))
			if err != nil {
				t.Error(err)
				return
			}
			if err := r.Close(); err != nil {
				t.Error(err)
				return
			}
			r, _, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(r)
			closeErr := r.Close()
			if err != nil || closeErr != nil || !bytes.Equal(got, payload) {
				t.Errorf("concurrent payload %d differs: read=%v, close=%v", i, err, closeErr)
			}
		})
	}
	ready.Wait()
	close(start)
	joined.Wait()
}
