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
	"syscall"
	"testing"
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
		r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer(payload, ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}
		assert.NoError(t, r.Close())
		checkUnused()
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		checkUnused()
		assertBlobRoots(t, c)
		r, _, restoredID, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Diff(t, data, payload)
		assert.Equal(t, restoredID, id)
		assert.Equal(t, AcquisitionIdentity(r), digest.FromBytes(payload).String())
		assert.Equal(t, AcquisitionIdentity(FileFS("payload", r)), manifest.Fingerprint([2]string{digest.FromBytes(payload).String(), "payload"}))
		assert.NoError(t, r.Close())
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
		r, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("payload"), ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}
		assert.NoError(t, r.Close())
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		r, _, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, string(data), "payload")
		assert.NoError(t, r.Close())
		assertBlobRoots(t, c, digest.SHA256)
	})
	t.Run("root-reuse", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		keys := []BlobKey{{Content: "reuse-first"}, {Content: "reuse-second"}}
		for _, key := range keys {
			r, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte(key.Content.(string)), ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}
			assert.NoError(t, r.Close())
		}
		assert.NoError(t, c.Close())
		c = cachetest.New(t, root, New)
		var retained *os.Root
		for _, key := range keys {
			r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(r)
			assert.NoError(t, err)
			assert.Equal(t, string(data), key.Content.(string))
			assert.NoError(t, r.Close())
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
			r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer(payload, ocispec.Descriptor{}))
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
				data, err := io.ReadAll(r)
				closeErr := r.Close()
				if err != nil || closeErr != nil || string(data) != algorithm.String() {
					t.Errorf("%s first read=%q, read=%v, close=%v", algorithm, data, err, closeErr)
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
			warmKey := BlobKey{Content: "independent-sha256"}
			r, _, _, err = c.Blob(t.Context(), warmKey, Policy{}, producer([]byte("warm SHA256"), ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}
			assert.NoError(t, r.Close())
			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
			for range 2 {
				r, _, _, err := c.Blob(t.Context(), warmKey, Policy{MaxAge: Forever}, nil)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(r)
				assert.NoError(t, err)
				assert.Equal(t, string(data), "warm SHA256")
				assert.NoError(t, r.Close())
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
				view = FileFS("payload", r)
				assert.Equal(t, AcquisitionIdentity(view), manifest.Fingerprint([2]string{stored.Digest.String(), "payload"}))
				assert.NoError(t, r.Close())
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
					fail := func(context.Context, ProducerInput) (ProducerResult, error) {
						calls++
						return ProducerResult{}, connReset()
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
				readerPin := AcquisitionIdentity(r)
				assert.Equal(t, readerPin, digest.FromString(payload).String())
				assert.Equal(t, AcquisitionIdentity(struct{ io.ReadCloser }{r}), "")
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
				assert.Equal(t, AcquisitionIdentity(r), "")
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
					r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
						calls++
						_, err := io.WriteString(w, "completed")
						return ProducerResult{}, err
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
					produce := func(context.Context, ProducerInput) (ProducerResult, error) {
						t.Fatal("disabled PersistOnly ran failing producer")
						return ProducerResult{}, connReset()
					}
					ctx := t.Context()
					allocs := testing.AllocsPerRun(100, func() {
						r, _, _, err := c.Blob(ctx, key, Policy{Retry: fastRetry(3)}, produce)
						if err != nil {
							t.Fatal(err)
						}
						if err := r.Close(); err != nil {
							t.Fatal(err)
						}
					})
					if allocs > 2 {
						t.Fatalf("disabled publication allocated %.0f times, want at most 2", allocs)
					}
					ctx, cancel := context.WithCancel(ctx)
					cancel()
					for _, produce := range []func(context.Context, ProducerInput) (ProducerResult, error){nil, producer(nil, ocispec.Descriptor{})} {
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
				view := FileFS("content", r)
				identity := AcquisitionIdentity(view)
				assert.NoError(t, r.Close())
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
				if gotID != id || AcquisitionIdentity(FileFS("content", r)) != identity || desc.Annotations["validator"] != "updated" {
					t.Fatalf("unchanged id=%q, descriptor=%+v", gotID, desc)
				}
				body, err := io.ReadAll(r)
				assert.NoError(t, errors.Join(err, r.Close()))
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
				r, desc, _, err = c.Blob(t.Context(), key, Policy{MaxAge: time.Second}, nil)
				if err != nil {
					t.Fatal(err)
				}
				assert.NoError(t, r.Close())
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
		identity := AcquisitionIdentity(FileFS("content", r))
		assert.NoError(t, r.Close())
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
		view := FileFS("content", r)
		assert.Equal(t, AcquisitionIdentity(view), identity)
		body, err := io.ReadAll(r)
		assert.NoError(t, errors.Join(err, r.Close()))
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
			r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("old"), ocispec.Descriptor{}))
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
			e, _ := c.entries.Load(id)
			path := blobPath(e.(*entry).stored.Digest)
			assert.NoError(t, c.root.Remove(path))
			if invalid == "corrupted-size" {
				testutil.WriteFile(t, root, path, "truncated")
			}
			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
			r, _, _, err = c.Blob(t.Context(), key, Policy{}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				previous, err := w.Previous(ctx)
				if err != nil || previous != nil {
					t.Fatalf("invalid previous=%+v, error=%v", previous, err)
				}
				_, err = io.WriteString(w, "old")
				return ProducerResult{}, err
			})
			assert.NoError(t, err)
			body, err := io.ReadAll(r)
			assert.NoError(t, errors.Join(err, r.Close()))
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
					view := FileFS("content", r)
					assert.NoError(t, r.Close())
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
					body, err = io.ReadAll(r)
					assert.NoError(t, errors.Join(err, r.Close()))
					assert.Equal(t, string(body), "complete")
				})
			})
		}
	}
}
