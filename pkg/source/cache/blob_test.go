package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
)

func assertBlobRoots(t *testing.T, c *Cache, algorithms ...digest.Algorithm) {
	t.Helper()
	for i, algorithm := range blobAlgorithms {
		if (c.blobRoots[i].Root != nil) != slices.Contains(algorithms, algorithm) {
			t.Fatalf("opened %s root=%v, want algorithms %v", algorithm, c.blobRoots[i].Root != nil, algorithms)
		}
	}
}

func TestBlob_LookupAndReplacement(t *testing.T) {
	t.Parallel()
	t.Run("sha256-only", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		checkUnused := func() {
			t.Helper()
			for _, algorithm := range []digest.Algorithm{digest.SHA384, digest.SHA512} {
				if _, err := c.root.Stat(filepath.Join(blobDir(), algorithm.String())); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("SHA256-only owner created unused %s directory: %v", algorithm, err)
				}
				if c.blobRoots[slices.Index(blobAlgorithms[:], algorithm)].Root != nil {
					t.Fatalf("SHA256-only owner opened unused %s root", algorithm)
				}
			}
		}
		checkUnused()
		assertBlobRoots(t, c)
		key := BlobKey{Content: "sha256-only"}
		payload := []byte("SHA256 payload")
		_, _, id, err := c.Blob(t.Context(), key, Policy{}, producer(payload, ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}

		checkUnused()
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		checkUnused()
		assertBlobRoots(t, c)
		r, _, restoredID, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := fs.ReadFile(r, "content")
		assert.NoError(t, err)
		assert.Diff(t, data, payload)
		assert.Equal(t, restoredID, id)
		assert.Equal(t, AcquisitionIdentity(r), digest.FromBytes(payload).String())
		view := NameBlob("unconventional.payload", r)
		assert.Equal(t, AcquisitionIdentity(view), manifest.Fingerprint([2]string{digest.FromBytes(payload).String(), "unconventional.payload"}))
		assert.Equal(t, FileName(view), "unconventional.payload")
		assert.Equal(t, FileName(struct{ fs.FS }{view}), "")
		assert.Equal(t, FileName(fstest.MapFS{"chart.tgz": {Data: payload}}), "")
		contents, err := fs.ReadFile(view, FileName(view))
		assert.NoError(t, err)
		assert.Diff(t, contents, payload)
		var treeData bytes.Buffer
		testutil.WriteTar(t, &treeData, map[string]string{"demo/Chart.yaml": "metadata"}, 0o644)
		tree, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "filename-tree"}, Policy{}, producer(treeData.Bytes(), ocispec.Descriptor{}))
		assert.NoError(t, err)
		assert.Equal(t, FileName(tree), "")

		checkUnused()
		if c.blobRoots[0].Root == nil {
			t.Fatal("SHA256 read did not open its root")
		}
	})
	t.Run("unused-invalid-path", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, filepath.Join(blobDir(), digest.SHA384.String()), "not a directory")
		c := cachetest.New(t, root, New)
		key := BlobKey{Content: "unused-invalid-path"}
		_, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("payload"), ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}

		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := fs.ReadFile(r, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "payload")

		assertBlobRoots(t, c, digest.SHA256)
	})
	t.Run("root-reuse", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		keys := []BlobKey{{Content: "reuse-first"}, {Content: "reuse-second"}}
		for _, key := range keys {
			_, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte(key.Content.(string)), ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}
		}
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		var retained *os.Root
		for _, key := range keys {
			r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
			if err != nil {
				t.Fatal(err)
			}
			data, err := fs.ReadFile(r, "content")
			assert.NoError(t, err)
			assert.Equal(t, string(data), key.Content.(string))

			assertBlobRoots(t, c, digest.SHA256)
			if retained != nil && c.blobRoots[0].Root != retained {
				t.Fatal("distinct warm keys reopened the SHA256 root")
			}
			retained = c.blobRoots[0].Root
		}
		assert.NoError(t, c.Close())
		if _, err := retained.Stat("."); !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("last Close retained reused blob root: %v", err)
		}
	})
	t.Run("concurrent-first-read", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		keys := []BlobKey{{Content: "first"}, {Content: "second"}}
		upstream := ocispec.Descriptor{Digest: digest.FromString("upstream"), Size: 123, MediaType: "upstream/type", Annotations: map[string]string{"revision": "v1"}}
		for _, key := range keys {
			_, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte(key.Content.(string)), upstream))
			if err != nil {
				t.Fatal(err)
			}
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
				data, err := fs.ReadFile(r, "content")
				if err != nil || string(data) != key.Content.(string) {
					t.Errorf("first read=%q, read=%v", data, err)
				}
			})
		}
		ready.Wait()
		close(start)
		joined.Wait()
		assertBlobRoots(t, c, digest.SHA256)
		assert.NoError(t, c.Close())
		for i := range c.blobRoots {
			parent := &c.blobRoots[i]
			if parent.Root == nil {
				continue
			}
			if _, err := parent.Stat("."); !errors.Is(err, fs.ErrClosed) {
				t.Fatalf("last Close retained blob parent: %v", err)
			}
		}
	})
	t.Run("mixed-algorithm-first-read", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		for _, algorithm := range blobAlgorithms {
			key := BlobKey{Content: algorithm.String()}
			payload := []byte(algorithm.String())
			_, _, id, err := c.Blob(t.Context(), key, Policy{}, producer(payload, ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}

			stored, err := c.store.Resolve(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			stored.Digest = algorithm.FromBytes(payload)
			if err := c.store.Push(t.Context(), stored, bytes.NewReader(payload)); err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
				t.Fatal(err)
			}
			assert.NoError(t, c.store.Tag(t.Context(), stored, id))
		}
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		assertBlobRoots(t, c)
		start := make(chan struct{})
		var ready, joined sync.WaitGroup
		ready.Add(len(blobAlgorithms))
		for _, algorithm := range blobAlgorithms {
			joined.Go(func() {
				ready.Done()
				<-start
				r, _, _, err := c.Blob(t.Context(), BlobKey{Content: algorithm.String()}, Policy{MaxAge: Forever}, nil)
				if err != nil {
					t.Error(err)
					return
				}
				data, err := fs.ReadFile(r, "content")
				if err != nil || string(data) != algorithm.String() {
					t.Errorf("%s first read=%q, read=%v", algorithm, data, err)
				}
			})
		}
		ready.Wait()
		close(start)
		joined.Wait()
		assertBlobRoots(t, c, blobAlgorithms[:]...)
		assert.NoError(t, c.Close())
		for i := range c.blobRoots {
			if _, err := c.blobRoots[i].Stat("."); !errors.Is(err, fs.ErrClosed) {
				t.Fatalf("last Close retained %s root: %v", blobAlgorithms[i], err)
			}
		}
	})
	t.Run("recreated-parent", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, t.TempDir(), New)
		key := BlobKey{Content: "recreated-parent"}
		_, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("first"), ocispec.Descriptor{}))
		assert.NoError(t, err)

		assert.NoError(t, os.RemoveAll(filepath.Join(c.root.Name(), blobDir())))
		r, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("second"), ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}
		data, err := fs.ReadFile(r, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "second")
		view := NameBlob("payload", r)

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
			_, _, id, err := c.Blob(t.Context(), key, Policy{}, producer(payload, upstream))
			if err != nil {
				t.Fatal(err)
			}

			stored, err := c.store.Resolve(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			stored.Digest = algorithm.FromBytes(payload)
			if err := c.store.Push(t.Context(), stored, bytes.NewReader(payload)); err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
				t.Fatal(err)
			}
			assert.NoError(t, c.store.Tag(t.Context(), stored, id))
			warmKey := BlobKey{Content: "independent-sha256"}
			_, _, _, err = c.Blob(t.Context(), warmKey, Policy{}, producer([]byte("warm SHA256"), ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}

			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
			for range 2 {
				r, _, _, err := c.Blob(t.Context(), warmKey, Policy{MaxAge: Forever}, nil)
				if err != nil {
					t.Fatal(err)
				}
				data, err := fs.ReadFile(r, "content")
				assert.NoError(t, err)
				assert.Equal(t, string(data), "warm SHA256")
			}
			assertBlobRoots(t, c, digest.SHA256)
			var view fs.FS
			for range 2 {
				r, desc, restoredID, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
				if err != nil {
					t.Fatal(err)
				}
				assert.Diff(t, desc, upstream)
				assert.Equal(t, restoredID, id)
				view = NameBlob("payload", r)
				assert.Equal(t, AcquisitionIdentity(view), manifest.Fingerprint([2]string{stored.Digest.String(), "payload"}))
			}
			assertBlobRoots(t, c, digest.SHA256, algorithm)
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
			generation, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
			if err == nil {
				_, err = fs.ReadFile(generation, "content")
			}
			if !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("coalesced generation accepted a replaced parent: %v", err)
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
					fail := func(context.Context, ProducerInput) (ProducerResult, error) {
						calls++
						return ProducerResult{}, connReset()
					}
					policy := Policy{MaxAge: maxAge, Retry: fastRetry(3)}
					_, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("original"), ocispec.Descriptor{}))
					if err != nil {
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
						for _, produce := range []func(context.Context, ProducerInput) (ProducerResult, error){nil, fail} {
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
					_, _, _, err = c.Blob(t.Context(), key, policy, producer([]byte("recovered"), ocispec.Descriptor{}))
					if err != nil {
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
		identities[payload] = AcquisitionIdentity(NameBlob("chart.tgz", r))
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
				view := NameBlob("chart.tgz", r)
				assert.Equal(t, FileName(view), "chart.tgz")
				readerPin := AcquisitionIdentity(r)
				assert.Equal(t, readerPin, digest.FromString(payload).String())
				assert.Equal(t, AcquisitionIdentity(struct{ fs.FS }{r}), "")
				pin := AcquisitionIdentity(view)
				assert.Equal(t, pin, identities[payload])
				if pin == "" || pin == AcquisitionIdentity(NameBlob("other.tgz", r)) || AcquisitionIdentity(struct{ fs.FS }{view}) != "" {
					t.Fatal("blob identity lost generation, filename or confinement")
				}
				if held != nil {
					data, err := fs.ReadFile(held, "chart.tgz")
					if err != nil || string(data) != heldPayload || AcquisitionIdentity(held) != heldPin || heldPin == pin {
						t.Fatalf("held generation changed: %q, %v", data, err)
					}
				}

				stream, err := r.Open("content")
				assert.NoError(t, err)
				assert.NoError(t, stream.Close())
				assert.Equal(t, AcquisitionIdentity(r), readerPin)
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
				restored := NameBlob("chart.tgz", first)
				assert.Equal(t, AcquisitionIdentity(first), readerPin)
				if AcquisitionIdentity(restored) != pin {
					t.Fatal("blob restoration changed identity")
				}
				held, heldPin, heldPayload = restored, pin, payload
				var data []byte
				if backing == "memory" {
					data, err = fs.ReadFile(view, "chart.tgz")
				}
				if backing == "memory" && (err != nil || string(data) != payload) {
					t.Fatalf("closed cursor invalidated view: %q, %v", data, err)
				}
				var wg sync.WaitGroup
				wg.Go(func() {
					other, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
					if err != nil {
						t.Error(err)
						return
					}
					got, err := fs.ReadFile(other, "content")
					if err != nil || string(got) != payload {
						t.Errorf("independent reader=%q, read=%v", got, err)
					}
				})
				wg.Wait()
				got, err := fs.ReadFile(first, "content")
				if err != nil || string(got) != payload {
					t.Fatalf("held reader=%q, read=%v", got, err)
				}
			}
		})
	}
}

func TestBlob_RenderLifetime(t *testing.T) {
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
					persisted := backing == "disk" && identity != ""
					assert.Equal(t, c.CanPersistRenders(), persisted)
					for _, invalid := range []BlobKey{
						{Content: "render", Render: RenderMode(255)},
						{Content: 1, Render: RenderAccepted},
						{Content: "", Render: RenderAccepted},
					} {
						_, _, _, err := c.Blob(t.Context(), invalid, Policy{MaxAge: Forever}, nil)
						if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, manifest.ErrInput) {
							t.Fatalf("invalid render key %+v: %v", invalid, err)
						}
					}
					_, _, enabled, err := c.RenderMetadata(t.Context(), "", "")
					if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, manifest.ErrInput) || enabled {
						t.Fatalf("invalid metadata content: enabled=%t, %v", enabled, err)
					}
					canceled, cancel := context.WithCancel(t.Context())
					cancel()
					_, _, _, err = c.Blob(canceled, BlobKey{Content: "render", Render: RenderAccepted}, Policy{MaxAge: Forever}, nil)
					if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, context.Canceled) {
						t.Fatalf("canceled render lookup: %v", err)
					}
					_, _, enabled, err = c.RenderMetadata(canceled, "render", "")
					if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, context.Canceled) || enabled != persisted {
						t.Fatalf("canceled metadata lookup: enabled=%t, %v", enabled, err)
					}
					assert.Equal(t, c.CanPersistRenders(), persisted)
					raw := BlobKey{Content: "raw-source"}
					_, _, _, err = c.Blob(t.Context(), raw, Policy{MaxAge: Forever}, producer([]byte("source"), ocispec.Descriptor{}))
					assert.NoError(t, err)

					diskFiles := func() map[string]string {
						files := map[string]string{}
						if root != "" {
							assert.NoError(t, fs.WalkDir(os.DirFS(root), ".", func(name string, d fs.DirEntry, err error) error {
								if err != nil || d.IsDir() {
									return err
								}
								data, err := os.ReadFile(filepath.Join(root, name))
								files[name] = string(data)
								return err
							}))
						}
						return files
					}
					before := diskFiles()
					key := BlobKey{Content: "render-test-v1-payload", Render: RenderPending}
					if backing == "disk" && identity != "" {
						key.Render = RenderAccepted
					}
					for _, content := range []any{"", struct{ Key string }{"render-test-v1-payload"}} {
						if r, _, id, err := c.Blob(t.Context(), BlobKey{Content: content, Render: RenderPending}, Policy{MaxAge: Forever}, nil); r != nil || id != "" || !errors.Is(err, manifest.ErrInput) {
							t.Fatalf("invalid render identity: id=%q, %v", id, err)
						}
					}
					if r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); r != nil || !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("cold lookup: %v", err)
					}
					calls := 0
					produce := func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
						calls++
						previous, err := w.Previous(ctx)
						assert.NoError(t, err)
						if calls > 1 {
							if previous == nil {
								t.Fatal("memory generation lost previous payload")
							}
							return ProducerResult{Unchanged: true}, nil
						}
						_, err = io.WriteString(w, "completed")
						return ProducerResult{}, err
					}
					for _, age := range []time.Duration{Forever, Forever, 0} {
						r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: age}, produce)
						assert.NoError(t, err)
						data, _ := fs.ReadFile(r, "content")

						assert.Equal(t, string(data), "completed")
					}
					assert.Equal(t, calls, 2)
					if !persisted {
						assert.Diff(t, diskFiles(), before)
					}
					assert.NoError(t, c.Close())
					c = cachetest.New(t, root, New)
					r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
					if persisted {
						assert.NoError(t, err)
						data, _ := fs.ReadFile(r, "content")

						assert.Equal(t, string(data), "completed")
					} else if r != nil || !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("reopened transient render: %v", err)
					}
					r, _, _, err = c.Blob(t.Context(), raw, Policy{MaxAge: Forever}, nil)
					if root != "" {
						assert.NoError(t, err)
						data, _ := fs.ReadFile(r, "content")

						assert.Equal(t, string(data), "source")
					} else if !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("reopened memory source: %v", err)
					}
				})
			}
		})
	}
}

func TestBlob_RenderAcquisition(t *testing.T) {
	original := processIdentity
	processIdentity = "identified"
	t.Cleanup(func() { processIdentity = original })
	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		accepted := BlobKey{Content: "render-acquisition", Render: RenderAccepted}
		generation, _, _, err := c.Blob(t.Context(), accepted, Policy{}, producer([]byte("accepted"), ocispec.Descriptor{}))
		assert.NoError(t, err)
		var current atomic.Value
		current.Store("accepted")
		pending := BlobKey{Content: accepted.Content, Render: RenderPending, Validate: func(_ context.Context, input fs.FS) (bool, error) {
			data, err := fs.ReadFile(input, "content")
			return string(data) == current.Load().(string), err
		}}
		hit, _, _, err := c.Blob(t.Context(), pending, Policy{MaxAge: Forever}, nil)
		assert.NoError(t, err)
		data, err := fs.ReadFile(hit, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "accepted")
		current.Store("pending")
		entered, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int64
		var wg sync.WaitGroup
		run := func() {
			input, _, _, err := c.Blob(t.Context(), pending, Policy{MaxAge: Forever}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				calls.Add(1)
				close(entered)
				<-release
				_, err := io.WriteString(w, "pending")
				return ProducerResult{}, err
			})
			assert.NoError(t, err)
			data, err := fs.ReadFile(input, "content")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "pending")
		}
		wg.Go(run)
		<-entered
		wg.Go(run)
		synctest.Wait()
		close(release)
		wg.Wait()
		assert.Equal(t, calls.Load(), int64(1))
		data, err = fs.ReadFile(generation, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "accepted")
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		if input, _, _, err := c.Blob(t.Context(), pending, Policy{MaxAge: Forever}, nil); input != nil || !errors.Is(err, errdef.ErrNotFound) {
			t.Fatalf("unaccepted work survived owner close: %v", err)
		}
	})
}

func TestBlob_RenderValidation(t *testing.T) {
	original := processIdentity
	t.Cleanup(func() { processIdentity = original })
	for _, backing := range []string{"memory", "unidentified disk", "identified disk", "identified disk due"} {
		t.Run(backing, func(t *testing.T) {
			processIdentity = ""
			root := ""
			if backing != "memory" {
				root = t.TempDir()
			}
			if strings.HasPrefix(backing, "identified disk") {
				processIdentity = "identified"
			}
			c := cachetest.New(t, root, New)
			mode := RenderPending
			if processIdentity != "" && root != "" {
				mode = RenderAccepted
			}
			synctest.Test(t, func(t *testing.T) {
				if backing == "identified disk due" {
					_, _, _, err := c.Blob(t.Context(), BlobKey{Content: "shared-render", Render: mode}, Policy{MaxAge: Forever}, func(context.Context, ProducerInput) (ProducerResult, error) { return ProducerResult{}, connReset() })
					if !errors.Is(err, syscall.ECONNRESET) {
						t.Fatalf("initial failure: %v", err)
					}
					time.Sleep(time.Second)
				}
				var calls, validations atomic.Int64
				var current atomic.Value
				current.Store("old")
				entered, release := make(chan struct{}), make(chan struct{})
				run := func(view func() string, pause bool) {
					key := BlobKey{Content: "shared-render", Render: mode, Validate: func(_ context.Context, r fs.FS) (bool, error) {
						validations.Add(1)
						data, err := fs.ReadFile(r, "content")
						return string(data) == view(), err
					}}
					r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
						calls.Add(1)
						if pause {
							close(entered)
							<-release
						}
						_, err := io.WriteString(w, view())
						return ProducerResult{}, err
					})
					assert.NoError(t, err)
					data, _ := fs.ReadFile(r, "content")

					assert.Equal(t, string(data), view())
				}
				var wg sync.WaitGroup
				wg.Go(func() { run(func() string { return "old" }, true) })
				<-entered
				for range 2 {
					wg.Go(func() { run(func() string { return current.Load().(string) }, false) })
				}
				synctest.Wait()
				current.Store("new")
				close(release)
				wg.Wait()
				assert.Equal(t, calls.Load(), int64(2))
				wantValidations := int64(4)
				if backing == "identified disk due" {
					wantValidations = 5
				}
				assert.Equal(t, validations.Load(), wantValidations)
				run(func() string { return "new" }, false)
				assert.Equal(t, calls.Load(), int64(2))
				failure := errors.New("caller validation failed")
				key := BlobKey{Content: "shared-render", Render: mode, Validate: func(context.Context, fs.FS) (bool, error) { return false, failure }}
				r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
				if r != nil || !errors.Is(err, failure) {
					t.Fatalf("validator failure: %v", err)
				}
				key.Validate = func(context.Context, fs.FS) (bool, error) { return false, nil }
				r, _, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
				if r != nil || !errors.Is(err, errdef.ErrNotFound) {
					t.Fatalf("invalid lookup: %v", err)
				}
			})
		})
	}
}

func TestBlob_NonReusableGeneration(t *testing.T) {
	original := processIdentity
	processIdentity = "identified"
	t.Cleanup(func() { processIdentity = original })
	for _, backing := range []string{"memory", "disk", "disk due"} {
		t.Run(backing, func(t *testing.T) {
			root := ""
			if backing != "memory" {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			synctest.Test(t, func(t *testing.T) {
				key := BlobKey{Content: "changing-input", Render: RenderPending}
				if root != "" {
					key.Render = RenderAccepted
				}
				if backing == "disk due" {
					_, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, func(context.Context, ProducerInput) (ProducerResult, error) { return ProducerResult{}, connReset() })
					if !errors.Is(err, syscall.ECONNRESET) {
						t.Fatalf("initial failure: %v", err)
					}
					time.Sleep(time.Second)
				}
				var reads atomic.Int64
				readInput := func() string { return fmt.Sprint(reads.Add(1)) }
				entered, release := make(chan struct{}), make(chan struct{})
				var generations [2]fs.FS
				var payloads [2]string
				run := func(i int) {
					calls, validations := 0, 0
					request := key
					request.Validate = func(_ context.Context, generation fs.FS) (bool, error) {
						validations++
						data, err := fs.ReadFile(generation, "content")
						return string(data) == readInput(), err
					}
					generation, _, _, err := c.Blob(t.Context(), request, Policy{MaxAge: Forever}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
						calls++
						if calls > 1 {
							return ProducerResult{}, fmt.Errorf("produced more than once in call %d", i)
						}
						payloads[i] = readInput()
						if i == 0 {
							close(entered)
							<-release
						}
						_, err := io.WriteString(w, payloads[i])
						return ProducerResult{}, err
					})
					if err != nil || calls != 1 || validations != 1 {
						t.Errorf("call %d: productions=%d validations=%d error=%v", i, calls, validations, err)
						return
					}
					generations[i] = generation
				}
				var wg sync.WaitGroup
				wg.Go(func() { run(0) })
				<-entered
				wg.Go(func() { run(1) })
				synctest.Wait()
				close(release)
				wg.Wait()
				for i, generation := range generations {
					if generation == nil {
						continue
					}
					data, err := fs.ReadFile(generation, "content")
					if err != nil || string(data) != payloads[i] {
						t.Errorf("call %d: payload=%q want=%q error=%v", i, data, payloads[i], err)
					}
				}
				if generation, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); generation != nil || !errors.Is(err, errdef.ErrNotFound) {
					t.Fatalf("reused rejected generation: %v", err)
				}
				if root != "" {
					assert.NoError(t, c.Close())
					c = cachetest.New(t, root, New)
					if generation, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); generation != nil || !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("persisted rejected generation: %v", err)
					}
				}
			})
		})
	}
}

func TestBlob_FailuresRemainErrors(t *testing.T) {
	original := processIdentity
	processIdentity = "identified"
	t.Cleanup(func() { processIdentity = original })
	for _, root := range []string{"", t.TempDir()} {
		c := cachetest.New(t, root, New)
		modes := []RenderMode{0, RenderPending}
		if root != "" {
			modes = append(modes, RenderAccepted)
		}
		for _, render := range modes {
			key := BlobKey{Content: "blob-error", Render: render}
			failure := errors.New("failed completed producer")
			r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				_, _ = io.WriteString(w, "partial")
				return ProducerResult{}, failure
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
	stream, err := r.Open("content")
	assert.NoError(t, err)
	assert.NoError(t, stream.Close())
	if err := stream.Close(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("backing Close error was lost: %v", err)
	}
	input := fileFS{name: "content", size: 7, open: func() (io.ReadCloser, error) { return stream, nil }}
	_, readErr := fs.ReadFile(input, "content")
	joined, ok := readErr.(interface{ Unwrap() []error })
	if !ok || len(joined.Unwrap()) != 2 {
		t.Fatalf("read and close failures were not joined: %v", readErr)
	}
	for i, op := range []string{"read", "close"} {
		pathErr, ok := errors.AsType[*fs.PathError](joined.Unwrap()[i])
		if !ok || pathErr.Op != op || !errors.Is(pathErr, fs.ErrClosed) {
			t.Fatalf("%s failure was lost: %v", op, joined.Unwrap()[i])
		}
	}
	e, _ := c.entries.Load(id)
	if err := os.Remove(filepath.Join(c.root.Name(), blobPath(e.(*entry).stored.Digest))); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		generation, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if err == nil {
			_, err = fs.ReadFile(generation, "content")
		}
		if !errors.Is(err, fs.ErrNotExist) || errors.Is(err, errdef.ErrNotFound) || !errors.Is(err, manifest.ErrFlux) {
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
			_, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, producer(payload, ocispec.Descriptor{}))
			if err != nil {
				t.Error(err)
				return
			}

			r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			got, err := fs.ReadFile(r, "content")
			if err != nil || !bytes.Equal(got, payload) {
				t.Errorf("concurrent payload %d differs: read=%v", i, err)
			}
		})
	}
	ready.Wait()
	close(start)
	joined.Wait()
}

func TestBlob_Revalidation(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				key := BlobKey{Content: "revalidated"}
				original := ocispec.Descriptor{Size: 7, Annotations: map[string]string{"validator": "opaque"}}
				r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("payload"), original))
				if err != nil {
					t.Fatal(err)
				}
				view := NameBlob("content", r)
				identity := AcquisitionIdentity(view)

				loaded, _ := c.entries.Load(id)
				first := loaded.(*entry)
				resolved := first.resolved
				var before os.FileInfo
				if root != "" {
					before, err = c.root.Stat(blobPath(first.stored.Digest))
					assert.NoError(t, err)
					assert.NoError(t, c.Close())
					c = cachetest.New(t, root, New)
				}
				time.Sleep(time.Second)
				r, desc, gotID, err := c.Blob(t.Context(), key, Policy{}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					previous, err := w.Previous(ctx)
					if err != nil || previous == nil || previous.Annotations["validator"] != "opaque" {
						t.Fatalf("previous=%+v, error=%v", previous, err)
					}
					previous.Annotations["validator"] = "mutated"
					independent, err := w.Previous(ctx)
					if err != nil || independent.Annotations["validator"] != "opaque" {
						t.Fatalf("metadata alias: %+v, %v", independent, err)
					}
					previous.Annotations = map[string]string{"validator": "updated"}
					return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if gotID != id || AcquisitionIdentity(NameBlob("content", r)) != identity || desc.Annotations["validator"] != "updated" {
					t.Fatalf("unchanged id=%q, descriptor=%+v", gotID, desc)
				}
				body, readErr := fs.ReadFile(r, "content")
				assert.NoError(t, readErr)

				assert.Equal(t, string(body), "payload")
				loaded, _ = c.entries.Load(id)
				next := loaded.(*entry)
				if !next.resolved.After(resolved) {
					t.Fatal("validation did not renew resolution time")
				}
				if root != "" {
					after, err := c.root.Stat(blobPath(next.stored.Digest))
					assert.NoError(t, err)
					if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
						t.Fatal("unchanged validation rewrote payload")
					}
					assert.NoError(t, c.Close())
					c = cachetest.New(t, root, New)
				}
				_, desc, _, err = c.Blob(t.Context(), key, Policy{MaxAge: time.Second}, nil)
				if err != nil {
					t.Fatal(err)
				}

				if desc.Annotations["validator"] != "updated" {
					t.Fatalf("validation metadata not persisted: %+v", desc)
				}
				loaded, _ = c.entries.Load(id)
				if !loaded.(*entry).resolved.Equal(next.resolved) {
					t.Fatal("resolution time not persisted")
				}
			})
		})
	}
	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, "", New)
		r, _, _, err := c.Blob(t.Context(), BlobKey{Content: "absent"}, Policy{}, func(context.Context, ProducerInput) (ProducerResult, error) {
			return ProducerResult{Unchanged: true}, nil
		})
		if r != nil || !errors.Is(err, manifest.ErrInput) {
			t.Fatalf("unchanged without payload: %v", err)
		}
	})
	t.Run("unchanged-parent-replacement", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		key := BlobKey{Content: "unchanged-parent-replacement"}
		upstream := ocispec.Descriptor{Digest: digest.FromString("upstream"), Size: 7, Annotations: map[string]string{"validator": "opaque"}}
		r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("payload"), upstream))
		assert.NoError(t, err)
		identity := AcquisitionIdentity(NameBlob("content", r))

		stored, err := c.store.Resolve(t.Context(), id)
		assert.NoError(t, err)
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		r, desc, gotID, err := c.Blob(t.Context(), key, Policy{}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			previous, err := w.Previous(ctx)
			if err != nil || previous == nil {
				t.Fatalf("previous=%+v, error=%v", previous, err)
			}
			return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
		})
		assert.NoError(t, err)
		assert.Diff(t, desc, upstream)
		assert.Equal(t, gotID, id)
		view := NameBlob("content", r)
		assert.Equal(t, AcquisitionIdentity(view), identity)
		body, readErr := fs.ReadFile(r, "content")
		assert.NoError(t, readErr)

		assert.Equal(t, string(body), "payload")
		current, err := c.store.Resolve(t.Context(), id)
		assert.NoError(t, err)
		assert.Equal(t, current.Digest, stored.Digest)
		parent := filepath.Dir(blobPath(stored.Digest))
		assert.NoError(t, c.root.Rename(parent, parent+"-held"))
		assert.NoError(t, c.root.Mkdir(parent, 0o750))
		if body, err := fs.ReadFile(view, "content"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("unchanged view ignored replacement: body=%q, error=%v", body, err)
		}
		assert.NoError(t, c.root.Remove(parent))
		outside := t.TempDir()
		testutil.WriteFile(t, outside, stored.Digest.Encoded(), "outside")
		assert.NoError(t, os.Symlink(outside, filepath.Join(root, parent)))
		if body, err := fs.ReadFile(view, "content"); err == nil {
			t.Fatalf("unchanged view accepted escaping parent: body=%q", body)
		}
	})
	for _, invalid := range []string{"missing bytes", "corrupted-size"} {
		t.Run(invalid, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			c := cachetest.New(t, root, New)
			key := BlobKey{Content: "missing"}
			_, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("old"), ocispec.Descriptor{}))
			assert.NoError(t, err)

			e, _ := c.entries.Load(id)
			path := blobPath(e.(*entry).stored.Digest)
			assert.NoError(t, c.root.Remove(path))
			if invalid == "corrupted-size" {
				testutil.WriteFile(t, root, path, "truncated")
			}
			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
			r, _, _, err := c.Blob(t.Context(), key, Policy{}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				previous, err := w.Previous(ctx)
				if err != nil || previous != nil {
					t.Fatalf("invalid previous=%+v, error=%v", previous, err)
				}
				_, err = io.WriteString(w, "old")
				return ProducerResult{}, err
			})
			assert.NoError(t, err)
			body, readErr := fs.ReadFile(r, "content")
			assert.NoError(t, readErr)

			assert.Equal(t, string(body), "old")
			info, err := c.root.Stat(path)
			assert.NoError(t, err)
			assert.Equal(t, info.Mode().Perm(), fs.FileMode(0o444))
			entries, err := os.ReadDir(filepath.Join(root, ociDir))
			assert.NoError(t, err)
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), "acquire-") {
					t.Errorf("successful repair left acquisition name %q", entry.Name())
				}
			}
		})
	}
}

func TestBlob_RepairFailureCleanup(t *testing.T) {
	t.Parallel()
	for _, failure := range []bool{false, true} {
		name := "cleanup-success"
		if failure {
			name = "cleanup-failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			c, err := New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := c.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Error(err)
				}
			})
			payload := []byte("repair payload")
			path := blobPath(digest.FromBytes(payload))
			assert.NoError(t, c.root.MkdirAll(path, 0o750))
			before, err := c.root.Stat(path)
			assert.NoError(t, err)
			if before.Size() == int64(len(payload)) {
				t.Fatal("destination directory must have a different size from the payload")
			}
			var names []string
			r, _, id, err := c.Blob(t.Context(), BlobKey{Content: "repair-failure"}, Policy{}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				w.payload.discard = func() error {
					entries, err := os.ReadDir(filepath.Join(root, ociDir))
					if err != nil {
						t.Fatal(err)
					}
					for _, entry := range entries {
						if strings.HasPrefix(entry.Name(), "acquire-") {
							names = append(names, filepath.Join(ociDir, entry.Name()))
						}
					}
					if len(names) != 2 {
						t.Fatalf("acquisition names before cleanup=%v, want two hard links", names)
					}
					first, err := c.root.Stat(names[0])
					assert.NoError(t, err)
					second, err := c.root.Stat(names[1])
					assert.NoError(t, err)
					if !os.SameFile(first, second) || first.Size() != int64(len(payload)) {
						t.Fatal("acquisition names must hard-link the staged payload")
					}
					if failure {
						return c.root.Close()
					}
					return nil
				}
				_, err := w.Write(payload)
				return ProducerResult{}, err
			})
			rename, ok := errors.AsType[*os.LinkError](err)
			if !ok || rename.Op != "renameat" || rename.New != path || !slices.Contains(names, rename.Old) {
				t.Fatalf("missing repair rename error: %v", err)
			}
			if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, manifest.ErrFlux) || r != nil || id != "" {
				t.Fatalf("failed repair reader=%v, identity=%q, error=%v", r, id, err)
			}
			if failure {
				removals := make(map[string]int)
				pending := []error{err}
				for len(pending) != 0 {
					cause := pending[0]
					pending = pending[1:]
					switch cause := cause.(type) { //nolint:errorlint // count each concrete removal node once while walking the joined tree
					case *os.PathError:
						if cause.Op == "removeat" {
							if !errors.Is(cause, os.ErrClosed) {
								t.Errorf("removal lost closed-root cause: %v", cause)
							}
							removals[cause.Path]++
						}
					case interface{ Unwrap() []error }:
						pending = append(pending, cause.Unwrap()...)
					case interface{ Unwrap() error }:
						pending = append(pending, cause.Unwrap())
					}
				}
				for _, name := range names {
					if removals[name] != 1 {
						t.Errorf("removal errors for %q=%d, want one: %v", name, removals[name], err)
					}
				}
				return
			}
			for _, name := range names {
				if _, err := c.root.Stat(name); !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("repair left acquisition name %q: %v", name, err)
				}
			}
			after, err := c.root.Stat(path)
			assert.NoError(t, err)
			if !after.IsDir() || !os.SameFile(before, after) {
				t.Fatal("failed repair replaced the destination directory")
			}
		})
	}
}

func TestBlob_RevalidationFailures(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		for _, failure := range []string{"producer", "canceled", "partial", "publication", "outcome"} {
			if !disk && failure == "publication" {
				continue
			}
			t.Run(fmt.Sprintf("disk=%t/%s", disk, failure), func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					root := ""
					if disk {
						root = t.TempDir()
					}
					c := cachetest.New(t, root, New)
					key := BlobKey{Content: "failed-validation"}
					r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("payload"), ocispec.Descriptor{Annotations: map[string]string{"validator": "original"}}))
					assert.NoError(t, err)
					view := NameBlob("content", r)

					var index []byte
					r, _, _, err = c.Blob(t.Context(), key, Policy{}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
						previous, err := w.Previous(ctx)
						if err != nil || previous == nil {
							t.Fatalf("previous=%+v, error=%v", previous, err)
						}
						switch failure {
						case "producer":
							return ProducerResult{Descriptor: *previous, Unchanged: true}, fs.ErrPermission
						case "outcome":
							return ProducerResult{}, connReset()
						case "canceled":
							canceled, cancel := context.WithCancel(ctx)
							cancel()
							_, err = w.Previous(canceled)
							return ProducerResult{Descriptor: *previous, Unchanged: true}, err
						case "partial":
							_, _ = io.WriteString(w, "partial")
						case "publication":
							index, err = c.root.ReadFile("oci/index.json")
							assert.NoError(t, err)
							assert.NoError(t, c.root.Remove("oci/index.json"))
							assert.NoError(t, c.root.Mkdir("oci/index.json", 0o750))
						}
						return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
					})
					if err == nil || r != nil || !errors.Is(err, manifest.ErrFlux) {
						t.Fatalf("failed validation reader=%v, error=%v", r, err)
					}
					loaded, _ := c.entries.Load(id)
					if !loaded.(*entry).resolved.IsZero() {
						t.Fatal("failed validation granted freshness")
					}
					body, err := fs.ReadFile(view, "content")
					if err != nil || string(body) != "payload" {
						t.Fatalf("held payload=%q, error=%v", body, err)
					}
					if failure == "publication" {
						assert.NoError(t, c.root.Remove("oci/index.json"))
						assert.NoError(t, c.root.WriteFile("oci/index.json", index, 0o600))
					}
					if failure == "outcome" && disk {
						outcome := loaded.(*entry).outcome
						if outcome == nil || outcome.lastGood.Digest == "" {
							t.Fatalf("last-good outcome=%+v", outcome)
						}
						if r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); r != nil || !errors.Is(err, syscall.ECONNRESET) {
							t.Fatalf("failure replay=%v", err)
						}
						time.Sleep(time.Second)
					}
					r, _, _, err = c.Blob(t.Context(), key, Policy{}, producer([]byte("complete"), ocispec.Descriptor{}))
					assert.NoError(t, err)
					body, err = fs.ReadFile(r, "content")
					assert.NoError(t, err)

					assert.Equal(t, string(body), "complete")
				})
			})
		}
	}
}

func TestRenderMetadata_CurrentGenerationWithoutPayloadOpen(t *testing.T) {
	t.Parallel()
	if reason := RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, t.TempDir(), New)
		key := BlobKey{Content: "render-metadata", Render: RenderAccepted}
		_, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("first"), ocispec.Descriptor{Annotations: map[string]string{"generation": "first"}}))
		assert.NoError(t, err)

		previous, _ := c.entries.Load(id)
		e := previous.(*entry)
		stamp := time.Now()
		assert.NoError(t, c.root.Chtimes(blobPath(e.stored.Digest), stamp, stamp))
		input := e.fs.(fileFS)
		input.open = func() (io.ReadCloser, error) {
			t.Error("metadata lookup opened the payload")
			return nil, errors.New("unexpected payload open")
		}
		e.fs = input
		descriptor, _, enabled, err := c.RenderMetadata(t.Context(), "render-metadata", id)
		assert.NoError(t, err)
		assert.Equal(t, enabled, true)
		assert.Equal(t, descriptor.Annotations["generation"], "first")
		started, publish := make(chan struct{}), make(chan struct{})
		finished := make(chan error, 1)
		go func() {
			_, _, _, err := c.Blob(t.Context(), key, Policy{}, func(_ context.Context, input ProducerInput) (ProducerResult, error) {
				close(started)
				<-publish
				_, err := input.Write([]byte("second"))
				return ProducerResult{Annotations: map[string]string{"generation": "second"}}, err
			})
			finished <- err
		}()
		<-started
		descriptor, _, enabled, err = c.RenderMetadata(t.Context(), "render-metadata", id)
		close(publish)
		assert.NoError(t, err)
		assert.Equal(t, enabled, true)
		assert.Equal(t, descriptor.Annotations["generation"], "first")
		assert.NoError(t, <-finished)
		current, _ := c.entries.Load(id)
		head := current.(*entry)
		assert.NoError(t, c.root.Chtimes(blobPath(head.stored.Digest), stamp, stamp))
		descriptor, _, enabled, err = c.RenderMetadata(t.Context(), "render-metadata", id)
		assert.NoError(t, err)
		assert.Equal(t, enabled, true)
		assert.Equal(t, descriptor.Annotations["generation"], "second")
		assert.Equal(t, e.descriptor.Annotations["generation"], "first")
		before, err := c.root.Stat(blobPath(head.stored.Digest))
		assert.NoError(t, err)
		time.Sleep(2 * accessInterval)
		_, _, _, err = c.RenderMetadata(t.Context(), "render-metadata", id)
		assert.NoError(t, err)
		after, err := c.root.Stat(blobPath(head.stored.Digest))
		assert.NoError(t, err)
		if !after.ModTime().After(before.ModTime()) {
			t.Fatal("metadata access did not retain the active generation")
		}

	})
}

func TestRenderMetadata_CurrentDirectory(t *testing.T) {
	t.Parallel()
	if reason := RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, name := range []string{"removed payload", "nonregular payload", "removed parent", "replaced parent empty", "replaced parent present", "escaping parent", "canceled", "closed"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			key := BlobKey{Content: "current-directory", Render: RenderAccepted}
			payload := []byte("current-directory-payload")
			r, _, lookup, err := c.Blob(t.Context(), key, Policy{}, producer(payload, ocispec.Descriptor{}))
			assert.NoError(t, err)
			identity := AcquisitionIdentity(r)

			stored, _ := c.entries.Load(lookup)
			path := blobPath(stored.(*entry).stored.Digest)
			parent := filepath.Dir(path)
			ctx := t.Context()
			switch name {
			case "removed payload":
				assert.NoError(t, c.root.Remove(path))
			case "nonregular payload":
				assert.NoError(t, c.root.Remove(path))
				assert.NoError(t, c.root.Mkdir(path, 0o750))
			case "removed parent":
				assert.NoError(t, c.root.Rename(parent, parent+"-old"))
			case "replaced parent empty", "replaced parent present":
				assert.NoError(t, c.root.Rename(parent, parent+"-old"))
				assert.NoError(t, c.root.Mkdir(parent, 0o750))
				if name == "replaced parent present" {
					assert.NoError(t, c.root.WriteFile(path, payload, 0o600))
				}
			case "escaping parent":
				assert.NoError(t, c.root.Rename(parent, parent+"-old"))
				assert.NoError(t, os.Symlink(t.TempDir(), filepath.Join(c.root.Name(), parent)))
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "closed":
				assert.NoError(t, c.Close())
			}
			_, current, enabled, err := c.RenderMetadata(ctx, "current-directory", lookup)
			if name == "replaced parent present" {
				assert.NoError(t, err)
				if !enabled || current == "" || current != identity {
					t.Fatal("replaced current directory lost the published generation")
				}
				r, _, _, err := c.Blob(ctx, key, Policy{MaxAge: Forever}, nil)
				assert.NoError(t, err)
				data, err := fs.ReadFile(r, "content")
				assert.NoError(t, err)
				assert.Equal(t, bytes.Equal(data, payload), true)
				return
			}
			if err == nil || current != "" {
				t.Fatal("invalid current directory authorized a metadata hit")
			}
			if _, ok := errors.AsType[*CacheError](err); !ok {
				t.Fatalf("metadata failure lost cache attribution: %v", err)
			}
			if name == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("metadata failure lost cancellation: %v", err)
			}
		})
	}
}
