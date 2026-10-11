package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/gofrs/flock"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func newCache(t *testing.T, root string) *Cache {
	t.Helper()
	c, err := New(t.Context(), cacheroot.New(root))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}

func archive(t *testing.T, files ...string) []byte {
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

func producer(data []byte, descriptor ocispec.Descriptor) func(io.Writer) (ocispec.Descriptor, error) {
	return func(w io.Writer) (ocispec.Descriptor, error) { _, err := w.Write(data); return descriptor, err }
}

func TestAcquire_GenerationsAndFreshness(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[disk], func(t *testing.T) {
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := newCache(t, root)
			key := ArchiveKey{ID: "source-test-generations"}
			want := ocispec.Descriptor{Digest: digest.FromString("upstream"), Size: 123, MediaType: "upstream/type", Annotations: map[string]string{"flate.revision": "v1", "other": "preserved"}}
			old, got, err := c.Acquire(t.Context(), key, time.Hour, producer(archive(t, "file", "v1"), want))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
			unexpected := func(io.Writer) (ocispec.Descriptor, error) {
				t.Error("fresh hit called producer")
				return want, errors.New("unexpected producer")
			}
			if disk {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				c = newCache(t, root)
			}
			warm, got, err := c.Acquire(t.Context(), key, time.Hour, unexpected)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
			if data, err := fs.ReadFile(warm, "file"); err != nil || string(data) != "v1" {
				t.Fatalf("warm: %q, %v", data, err)
			}
			e, _ := c.entries.Load(key.ID)
			at := e.(*entry).resolved
			if disk {
				p := c.layout.OCIBlob(e.(*entry).stored.Digest)
				stale := time.Now().Add(-24 * time.Hour)
				if err := os.Chtimes(p, stale, stale); err != nil {
					t.Fatal(err)
				}
				if _, _, err := c.Acquire(t.Context(), key, time.Hour, unexpected); err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(p)
				if err != nil || !info.ModTime().After(stale) {
					t.Fatalf("access time: %v, %v", info, err)
				}
				if !at.Equal(e.(*entry).resolved) {
					t.Fatal("access extended resolution TTL")
				}
			}
			if _, _, err := c.Acquire(t.Context(), key, 0, producer(archive(t, "file", "v2"), want)); err != nil {
				t.Fatal(err)
			}
			if data, err := fs.ReadFile(old, "file"); err != nil || string(data) != "v1" {
				t.Fatalf("held generation: %q, %v", data, err)
			}
			failure := errors.New("verification failed")
			if _, _, err := c.Acquire(t.Context(), key, 0, func(w io.Writer) (ocispec.Descriptor, error) {
				_, _ = w.Write(archive(t, "file", "partial"))
				return want, failure
			}); !errors.Is(err, failure) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failure: %v", err)
			}
			calls := 0
			if _, _, err := c.Acquire(t.Context(), key, time.Hour, func(w io.Writer) (ocispec.Descriptor, error) {
				calls++
				return producer(archive(t, "file", "v3"), want)(w)
			}); err != nil || calls != 1 {
				t.Fatalf("failed fill reused previous alias: calls=%d, %v", calls, err)
			}
		})
	}
}

func TestAcquire_ArchivePolicy(t *testing.T) {
	for _, disk := range []bool{false, true} {
		for _, defaults := range []bool{false, true} {
			t.Run(map[bool]string{false: "memory", true: "disk"}[disk]+map[bool]string{false: "/bucket", true: "/defaults"}[defaults], func(t *testing.T) {
				root := ""
				if disk {
					root = t.TempDir()
				}
				c := newCache(t, root)
				data := archive(t, "dir/drop", "discard", "dir/keep", "first", "dir/.sourceignore", "!keep\n", ".sourceignore", "dir/*\nempty/*\n", "dir/keep", "last", "empty/file", "discard", "image.png", "image", ".git/config", "git")
				input, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-policy", WithDefaults: defaults}, time.Hour, producer(data, ocispec.Descriptor{}))
				if err != nil {
					t.Fatal(err)
				}
				if got, err := fs.ReadFile(input, "dir/keep"); err != nil || string(got) != "last" {
					t.Fatalf("reincluded duplicate: %q, %v", got, err)
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
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[disk], func(t *testing.T) {
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := newCache(t, root)
			for _, data := range [][]byte{archive(t, "../escape", "x"), archive(t, "file", "x", "file/child", "y"), archive(t, "dir/file", "x", "dir", "y"), []byte("truncated")} {
				if _, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-invalid"}, 0, producer(data, ocispec.Descriptor{})); err == nil {
					t.Fatal("accepted invalid archive")
				}
			}
			var data bytes.Buffer
			w := tar.NewWriter(&data)
			if err := w.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "target"}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			input, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-link"}, 0, producer(data.Bytes(), ocispec.Descriptor{}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fs.Stat(input, "link"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("link: %v", err)
			}
		})
	}
}

func TestAcquire_ExpandedCapCountsIgnoredDuplicates(t *testing.T) {
	old := maxExtractedBytes
	maxExtractedBytes = 5
	t.Cleanup(func() { maxExtractedBytes = old })
	for _, root := range []string{"", t.TempDir()} {
		c := newCache(t, root)
		if _, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-cap", Ignore: "file"}, 0, producer(archive(t, "file", "abc", "file", "def"), ocispec.Descriptor{})); err == nil {
			t.Fatal("ignored duplicate bypassed expanded cap")
		}
	}
}

func TestAcquire_ConcurrentKeysAndCancellation(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		for _, root := range []string{"", t.TempDir()} {
			c := newCache(t, root)
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			fill := func(w io.Writer) (ocispec.Descriptor, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-release
				return producer(archive(t, "file", "data"), ocispec.Descriptor{})(w)
			}
			var wg sync.WaitGroup
			for range concurrency {
				wg.Go(func() {
					if _, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-concurrent"}, time.Hour, fill); err != nil {
						t.Error(err)
					}
				})
			}
			<-started
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, _, err := c.Acquire(ctx, ArchiveKey{ID: "source-test-concurrent"}, time.Hour, fill); !errors.Is(err, context.Canceled) {
				t.Errorf("wait: %v", err)
			}
			if r, _, err := c.Blob(t.Context(), BlobKey{ID: "blob-unrelated"}, time.Hour, producer([]byte("blob"), ocispec.Descriptor{})); err != nil {
				t.Error(err)
			} else {
				_ = r.Close()
			}
			close(release)
			wg.Wait()
			if calls.Load() != 1 {
				t.Fatalf("concurrency=%d calls=%d", concurrency, calls.Load())
			}
		}
	}
}

func TestNew_SharedOwnershipAndCanceledLock(t *testing.T) {
	root := t.TempDir()
	c := newCache(t, root)
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	shared := newCache(t, alias)
	if shared != c {
		t.Fatal("canonical directory has multiple owners")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if r, _, err := shared.Blob(t.Context(), BlobKey{ID: "blob-shared"}, time.Hour, producer([]byte("data"), ocispec.Descriptor{})); err != nil {
		t.Fatal(err)
	} else {
		_ = r.Close()
	}
	if err := shared.Close(); err != nil {
		t.Fatal(err)
	}
	lock := flock.New(cacheroot.New(root).OCILock())
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := New(ctx, cacheroot.New(root)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock wait: %v", err)
	}
}

func TestAcquire_TagSaveFailureDoesNotBecomeHit(t *testing.T) {
	c := newCache(t, t.TempDir())
	index := filepath.Join(c.layout.OCI(), "index.json")
	if err := os.Remove(index); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(index, 0o750); err != nil {
		t.Fatal(err)
	}
	calls := 0
	fill := func(w io.Writer) (ocispec.Descriptor, error) {
		calls++
		return producer(archive(t, "file", "data"), ocispec.Descriptor{})(w)
	}
	for range 2 {
		if _, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-persist"}, time.Hour, fill); err == nil {
			t.Fatal("tag save failure accepted")
		}
	}
	if calls != 2 {
		t.Fatalf("failed publication served an in-memory hit: %d", calls)
	}
}

func TestAcquire_SparseReplayMatchesMemory(t *testing.T) {
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
		c := newCache(t, root)
		input, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-sparse"}, time.Hour, producer(data, ocispec.Descriptor{}))
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
