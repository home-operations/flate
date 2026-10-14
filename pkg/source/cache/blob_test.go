package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
)

func TestBlob_LookupAndReplacement(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := newCache(t, root)
			key := BlobKey{ID: "blob-completed"}
			for range 2 {
				r, _, err := c.Blob(t.Context(), key, Forever, nil)
				if r != nil || !errors.Is(err, errdef.ErrNotFound) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("lookup miss: reader=%v, error=%v", r, err)
				}
			}
			if _, ok := c.entries.Load(key.ID); ok {
				t.Fatal("lookup miss installed an entry")
			}
			for _, payload := range []string{"first", ""} {
				r, _, err := c.Blob(t.Context(), key, 0, producer([]byte(payload), ocispec.Descriptor{}))
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				if r, _, err := c.Blob(t.Context(), key, 0, nil); !errors.Is(err, errdef.ErrNotFound) || r != nil {
					t.Fatalf("stale lookup: reader=%v, error=%v", r, err)
				}
				if backing == "disk" {
					e, _ := c.entries.Load(key.ID)
					raw, err := os.ReadFile(c.layout.OCIBlob(e.(*entry).stored.Digest))
					if err != nil || string(raw) != payload {
						t.Fatalf("plain completed payload=%q: %v", raw, err)
					}
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					c = newCache(t, root)
				}
				first, _, err := c.Blob(t.Context(), key, Forever, nil)
				if err != nil {
					t.Fatal(err)
				}
				// The same key must remain usable while a returned reader is retained.
				var wg sync.WaitGroup
				wg.Go(func() {
					other, _, err := c.Blob(t.Context(), key, Forever, nil)
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
					c := newCache(t, root)
					key := BlobKey{ID: "render-test-v1-payload", PersistOnly: true}
					if r, _, err := c.Blob(t.Context(), key, Forever, nil); r != nil || !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("cold lookup: %v", err)
					}
					r, _, err := c.Blob(t.Context(), key, 0, producer([]byte("completed"), ocispec.Descriptor{}))
					if err != nil {
						t.Fatal(err)
					}
					input := FileFS("frame", r)
					handle := r.(*blobHandle)
					data, err := io.ReadAll(r)
					if err != nil || string(data) != "completed" {
						t.Fatalf("transient output=%q, %v", data, err)
					}
					if err := r.Close(); err != nil {
						t.Fatal(err)
					}
					if handle.ReadCloser != nil || handle.input.open != nil {
						t.Fatal("closed reader retains payload references")
					}
					if data, err := fs.ReadFile(input, "frame"); err != nil || string(data) != "completed" {
						t.Fatalf("independently captured FileFS=%q, %v", data, err)
					}
					persisted := backing == "disk" && processIdentity != ""
					r, _, err = c.Blob(t.Context(), key, Forever, nil)
					if persisted {
						if err != nil {
							t.Fatal(err)
						}
						if err := r.Close(); err != nil {
							t.Fatal(err)
						}
						return
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
						if err := fs.WalkDir(os.DirFS(c.layout.OCI()), ".", func(path string, entry fs.DirEntry, err error) error {
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
					source, _, err := c.Blob(t.Context(), BlobKey{ID: "blob-ordinary"}, Forever, producer([]byte("source"), ocispec.Descriptor{}))
					if err != nil {
						t.Fatal(err)
					}
					if err := source.Close(); err != nil {
						t.Fatal(err)
					}
					source, _, err = c.Blob(t.Context(), BlobKey{ID: "blob-ordinary"}, Forever, nil)
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
	for _, root := range []string{"", t.TempDir()} {
		c := newCache(t, root)
		for _, persistOnly := range []bool{false, true} {
			key := BlobKey{ID: "blob-error", PersistOnly: persistOnly}
			failure := errors.New("failed completed producer")
			r, _, err := c.Blob(t.Context(), key, 0, func(w io.Writer) (ocispec.Descriptor, error) {
				_, _ = io.WriteString(w, "partial")
				return ocispec.Descriptor{}, failure
			})
			if r != nil || !errors.Is(err, failure) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("producer error: %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if r, _, err := c.Blob(ctx, key, Forever, nil); r != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("lookup cancellation: %v", err)
			}
		}
	}
	c := newCache(t, t.TempDir())
	key := BlobKey{ID: "blob-missing-payload"}
	r, _, err := c.Blob(t.Context(), key, 0, producer([]byte("payload"), ocispec.Descriptor{}))
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
	e, _ := c.entries.Load(key.ID)
	if err := os.Remove(c.layout.OCIBlob(e.(*entry).stored.Digest)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if r, _, err := c.Blob(t.Context(), key, Forever, nil); r != nil || !errors.Is(err, fs.ErrNotExist) || errors.Is(err, errdef.ErrNotFound) || !errors.Is(err, manifest.ErrFlux) {
			t.Fatalf("missing payload became miss: %v", err)
		}
	}
}

func TestBlob_ConcurrentPayloads(t *testing.T) {
	c := newCache(t, t.TempDir())
	const workers = 4
	start := make(chan struct{})
	var ready, joined sync.WaitGroup
	ready.Add(workers)
	for i := range workers {
		joined.Go(func() {
			ready.Done()
			<-start
			payload := bytes.Repeat([]byte{byte(i)}, 2<<20)
			key := BlobKey{ID: string(rune('a' + i))}
			r, _, err := c.Blob(t.Context(), key, 0, producer(payload, ocispec.Descriptor{}))
			if err != nil {
				t.Error(err)
				return
			}
			if err := r.Close(); err != nil {
				t.Error(err)
				return
			}
			r, _, err = c.Blob(t.Context(), key, Forever, nil)
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
