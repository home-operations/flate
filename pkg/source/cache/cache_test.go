package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/gofrs/flock"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
)

func newCache(t *testing.T, root string) *Cache {
	t.Helper()
	c, err := New(t.Context(), root)
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

func producer(data []byte, descriptor ocispec.Descriptor) func(context.Context, io.Writer) (ocispec.Descriptor, error) {
	return func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
		_, err := w.Write(data)
		return descriptor, err
	}
}

func TestAcquire_GenerationsAndFreshness(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[disk], func(t *testing.T) {
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := newCache(t, root)
			if !disk && c.root != nil {
				t.Fatal("memory cache opened a disk root")
			}
			key := ArchiveKey{ID: "source-test-generations"}
			want := ocispec.Descriptor{Digest: digest.FromString("upstream"), Size: 123, MediaType: "upstream/type", Annotations: map[string]string{"flate.revision": "v1", "other": "preserved"}}
			first, got, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, producer(archive(t, "file", "v1"), want))
			firstPin := AcquisitionIdentity(first)
			if firstPin == "" {
				t.Fatal("acquired archive has no identity")
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
			unexpected := func(context.Context, io.Writer) (ocispec.Descriptor, error) {
				t.Error("fresh hit called producer")
				return want, errors.New("unexpected producer")
			}
			if disk {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				c = newCache(t, root)
			}
			warm, got, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, unexpected)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatal(diff)
			}
			if AcquisitionIdentity(warm) != firstPin {
				t.Fatal("restoration changed held archive identity")
			}
			if data, err := fs.ReadFile(warm, "file"); err != nil || string(data) != "v1" {
				t.Fatalf("warm: %q, %v", data, err)
			}
			e, _ := c.entries.Load(key.ID)
			at := e.(*entry).resolved
			if disk {
				p := filepath.Join(c.layout.Root, blobPath(e.(*entry).stored.Digest))
				stale := time.Now().Add(-24 * time.Hour)
				if err := os.Chtimes(p, stale, stale); err != nil {
					t.Fatal(err)
				}
				if _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, unexpected); err != nil {
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
			replacement, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: 0}, producer(archive(t, "file", "v2"), want))
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
			if _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
				_, _ = w.Write(archive(t, "file", "partial"))
				return want, failure
			}); !errors.Is(err, failure) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failure: %v", err)
			}
			calls := 0
			if _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
				calls++
				return producer(archive(t, "file", "v3"), want)(ctx, w)
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
				input, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-policy", WithDefaults: defaults}, Policy{MaxAge: time.Hour}, producer(data, ocispec.Descriptor{}))
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
					{ID: "source-test-policy", WithDefaults: !defaults},
					{ID: "source-test-policy", WithDefaults: defaults, Ignore: "image.png\n"},
				} {
					other, _, err := c.Acquire(t.Context(), policy, Policy{MaxAge: 0}, producer(data, ocispec.Descriptor{}))
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
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[disk], func(t *testing.T) {
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := newCache(t, root)
			for _, data := range [][]byte{archive(t, "../escape", "x"), archive(t, "file", "x", "file/child", "y"), archive(t, "dir/file", "x", "dir", "y"), []byte("truncated")} {
				if _, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-invalid"}, Policy{MaxAge: 0}, producer(data, ocispec.Descriptor{})); err == nil {
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
			input, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-link"}, Policy{MaxAge: 0}, producer(data.Bytes(), ocispec.Descriptor{}))
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
	maxExtractedBytes = 2*512 + 5
	t.Cleanup(func() { maxExtractedBytes = old })
	for _, root := range []string{"", t.TempDir()} {
		c := newCache(t, root)
		if _, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-cap", Ignore: "file"}, Policy{MaxAge: 0}, producer(archive(t, "file", "abc", "file", "def"), ocispec.Descriptor{})); err == nil {
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
			fill := func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-release
				return producer(archive(t, "file", "data"), ocispec.Descriptor{})(ctx, w)
			}
			var wg sync.WaitGroup
			for range concurrency {
				wg.Go(func() {
					if _, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-concurrent"}, Policy{MaxAge: time.Hour}, fill); err != nil {
						t.Error(err)
					}
				})
			}
			<-started
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, _, err := c.Acquire(ctx, ArchiveKey{ID: "source-test-concurrent"}, Policy{MaxAge: time.Hour}, fill); !errors.Is(err, context.Canceled) {
				t.Errorf("wait: %v", err)
			}
			if r, _, err := c.Blob(t.Context(), BlobKey{ID: "blob-unrelated"}, Policy{MaxAge: time.Hour}, producer([]byte("blob"), ocispec.Descriptor{})); err != nil {
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
	reader, _, err := c.Blob(t.Context(), BlobKey{ID: "blob-held"}, Policy{MaxAge: Forever}, producer([]byte("held"), ocispec.Descriptor{}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	disk := c.root
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.Stat("."); err != nil {
		t.Fatalf("first Close closed shared root: %v", err)
	}
	if data, err := io.ReadAll(reader); err != nil || string(data) != "held" {
		t.Fatalf("reader after first Close: %q, %v", data, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	lock := flock.New(layout{Root: root}.OCILock())
	t.Cleanup(func() { _ = lock.Close() })
	if acquired, err := lock.TryLock(); err != nil || acquired {
		t.Fatalf("first Close released shared lock: acquired=%v, err=%v", acquired, err)
	}
	if r, _, err := shared.Blob(t.Context(), BlobKey{ID: "blob-shared"}, Policy{MaxAge: time.Hour}, producer([]byte("data"), ocispec.Descriptor{})); err != nil {
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
	if acquired, err := lock.TryLock(); err != nil || !acquired {
		t.Fatalf("last Close retained lock: acquired=%v, err=%v", acquired, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := New(ctx, root); !errors.Is(err, context.DeadlineExceeded) {
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
	fill := func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
		calls++
		return producer(archive(t, "file", "data"), ocispec.Descriptor{})(ctx, w)
	}
	for range 2 {
		if _, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-persist"}, Policy{MaxAge: time.Hour}, fill); err == nil {
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
		input, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-test-sparse"}, Policy{MaxAge: time.Hour}, producer(data, ocispec.Descriptor{}))
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
	for _, family := range []struct {
		id     string
		fields map[string]string
	}{
		{"source-oci-test", map[string]string{"flate.revision": "v1", "flate.upstream-digest": digest.FromString("upstream").String()}},
		{"blob-oci-test", map[string]string{"flate.revision": "v1", "flate.upstream-digest": digest.FromString("upstream").String()}},
		{"source-bucket-test", map[string]string{"flate.revision": "v1", "flate.object-count": "0"}},
		{"blob-chart-test", map[string]string{"flate.chart-version": "1.0.0", "flate.chart-url": "https://example.test/chart.tgz"}},
	} {
		t.Run(family.id, func(t *testing.T) {
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
			for k := range family.fields {
				checks = append(checks, struct{ name, key, value string }{"missing " + k, k, ""})
			}
			if strings.HasPrefix(family.id, "source-bucket-") {
				checks = append(checks, struct{ name, key, value string }{"negative count", "flate.object-count", "-1"},
					struct{ name, key, value string }{"malformed count", "flate.object-count", "invalid"})
			}
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					a := make(map[string]string, len(base))
					maps.Copy(a, base)
					if check.key != "" {
						a[check.key] = check.value
					}
					stored := ocispec.Descriptor{Digest: digest.FromString("stored"), Annotations: a}
					if check.name == "bad stored digest" {
						stored.Digest = "invalid"
					}
					_, _, valid := decode(stored, family.id)
					if valid != (check.name == "valid") {
						t.Fatalf("valid=%v", valid)
					}
				})
			}
		})
	}
}

func TestBlob_SourcePrefixIsOpaque(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := newCache(t, root)
			key := BlobKey{ID: "source-blob"}
			reader, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, producer([]byte("blob bytes"), ocispec.Descriptor{}))
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
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := newCache(t, root)
			var written int
			_, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-trailing"}, Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
				if _, err := w.Write(archive(t, "file", "x")); err != nil {
					return ocispec.Descriptor{}, err
				}
				var err error
				written, err = w.Write(make([]byte, 2<<20))
				return ocispec.Descriptor{}, err
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
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := newCache(t, root)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started := make(chan struct{})
			go func() { <-started; cancel() }()
			_, _, err := c.Acquire(ctx, ArchiveKey{ID: "source-cancel"}, Policy{MaxAge: 0}, func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
				if _, err := w.Write(archive(t, "file", "x")[:512]); err != nil {
					return ocispec.Descriptor{}, err
				}
				close(started)
				<-ctx.Done()
				time.Sleep(time.Millisecond)
				return ocispec.Descriptor{}, ctx.Err()
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation identity: %v", err)
			}
		})
	}
	t.Run("completed blob", func(t *testing.T) {
		c := newCache(t, "")
		key := BlobKey{ID: "source-completed-before-cancel"}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r, _, err := c.Blob(ctx, key, Policy{MaxAge: 0}, func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
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
		r, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		if closeErr := r.Close(); err != nil || closeErr != nil || string(data) != "completed" {
			t.Fatalf("completed fill: output=%q, read=%v, close=%v", data, err, closeErr)
		}
	})
}

func TestAcquire_UnchangedRefreshRenews(t *testing.T) {
	c := newCache(t, t.TempDir())
	key := ArchiveKey{ID: "source-unchanged"}
	var first time.Time
	for range 2 {
		if _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: 0}, producer(archive(t, "file", "same"), ocispec.Descriptor{})); err != nil {
			t.Fatal(err)
		}
		desc, err := c.store.Resolve(t.Context(), key.ID)
		if err != nil {
			t.Fatal(err)
		}
		_, resolved, valid := decode(desc, key.ID)
		if !valid || !resolved.After(first) {
			t.Fatalf("resolution time not renewed: %v after %v", resolved, first)
		}
		first = resolved
	}
}

func TestAcquire_CancelStopsContextIgnoringProducer(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := newCache(t, root)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started, halt := make(chan struct{}), make(chan struct{})
			go func() { <-started; cancel() }()
			done := make(chan error, 1)
			go func() {
				_, _, err := c.Acquire(ctx, ArchiveKey{ID: "source-ignore-context"}, Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
					tw := tar.NewWriter(w)
					if err := tw.WriteHeader(&tar.Header{Name: "file", Size: 1 << 30, Mode: 0o600}); err != nil {
						return ocispec.Descriptor{}, err
					}
					close(started)
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					var chunk [512]byte
					for {
						select {
						case <-halt:
							return ocispec.Descriptor{}, errors.New("producer stopped")
						case <-ticker.C:
							if _, err := tw.Write(chunk[:]); err != nil {
								return ocispec.Descriptor{}, err
							}
						}
					}
				})
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation: %v", err)
				}
			case <-time.After(5 * time.Second):
				close(halt)
				<-done
				t.Fatal("canceled acquisition did not stop the producer")
			}
		})
	}
}

func TestNew_UnreadableIndexRebuilds(t *testing.T) {
	root := t.TempDir()
	c := newCache(t, root)
	key := ArchiveKey{ID: "source-index-recovery"}
	data := archive(t, "file", "same")
	if _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, producer(data, ocispec.Descriptor{})); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(layout{Root: root}.OCI(), "index.json")
	if err := os.WriteFile(index, []byte(`{"schemaVersion":`), 0o600); err != nil {
		t.Fatal(err)
	}
	c = newCache(t, root)
	calls := 0
	input, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
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
	root := t.TempDir()
	c := newCache(t, root)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(layout{Root: root}.OCI(), "index.json")
	want, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(layout{Root: root}.OCI(), "oci-layout"), []byte(`{"imageLayoutVersion":`), 0o600); err != nil {
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
	for _, backing := range []string{"memory", "disk"} {
		for _, extra := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/%d", backing, extra), func(t *testing.T) {
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c := newCache(t, root)
				_, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-trailing-boundary"}, Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
					if _, err := w.Write(archive(t, "file", "x")); err != nil {
						return ocispec.Descriptor{}, err
					}
					_, err := w.Write(make([]byte, maxTrailingBytes+extra))
					return ocispec.Descriptor{}, err
				})
				if (err != nil) != (extra > 0) {
					t.Fatalf("extra=%d: err=%v", extra, err)
				}
			})
		}
	}
}

// The consumer rejecting trailing data while the producer is still writing must surface as the cache's
// own error on every run, never as the producer's write failure. The race is a fraction of a percent per call.
func TestAcquire_TrailingFailureKeepsCacheIdentity(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := newCache(t, root)
			for i := range 2000 {
				_, _, err := c.Acquire(t.Context(), ArchiveKey{ID: fmt.Sprintf("source-trailing-failure-%d", i)}, Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
					if _, err := w.Write(archive(t, "file", "x")); err != nil {
						return ocispec.Descriptor{}, err
					}
					_, err := w.Write(make([]byte, 2<<20))
					return ocispec.Descriptor{}, err
				})
				if _, ok := errors.AsType[*CacheError](err); !ok {
					t.Fatalf("run %d: %T %v, want *CacheError", i, err, err)
				}
			}
		})
	}
}

func TestGit_RestartAndInvalidation(t *testing.T) {
	for _, scenario := range []string{"hit", "ref-mismatch", "missing-objects", "touch-failure"} {
		t.Run(scenario, func(t *testing.T) {
			src, root := t.TempDir(), t.TempDir()
			repo, err := git.PlainInit(src, false)
			if err != nil {
				t.Fatal(err)
			}
			testutil.WriteFileAt(t, filepath.Join(src, "file"), "committed")
			wt, err := repo.Worktree()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := wt.Add("."); err != nil {
				t.Fatal(err)
			}
			if _, err := wt.Commit("seed", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example.invalid"}}); err != nil {
				t.Fatal(err)
			}
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
			c := newCache(t, root)
			key := GitKey{ID: "source-git-restart", Ref: "HEAD"}
			first, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, produce)
			if err != nil {
				t.Fatal(err)
			}
			v, _ := c.entries.Load(key.ID)
			e := v.(*entry)
			blob := filepath.Join(c.layout.Root, blobPath(e.stored.Digest))
			stale := time.Now().Add(-time.Hour)
			if err := os.Chtimes(blob, stale, stale); err != nil {
				t.Fatal(err)
			}
			if scenario == "missing-objects" {
				if err := os.RemoveAll(filepath.Join(c.layout.GitObjects(first.id), "objects")); err != nil {
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
			c = newCache(t, root)
			second, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, produce)
			if scenario == "ref-mismatch" || scenario == "missing-objects" {
				if err == nil || calls != 2 {
					t.Fatalf("invalid restore: calls=%d, %v", calls, err)
				}
				return
			}
			if err != nil || calls != 1 {
				t.Fatalf("restart hit: calls=%d, %v", calls, err)
			}
			input, pin, err := gittree.New(t.Context(), second.Backing, second.Commit, gittree.ViewOptions{Identity: key.ID + "@" + second.Commit.String()})
			if pin != key.ID+"@"+second.Commit.String() {
				t.Fatal("restored Git identity lost")
			}
			if err != nil {
				t.Fatal(err)
			}
			if data, err := fs.ReadFile(input, "file"); err != nil || string(data) != "committed" {
				t.Fatalf("restored content: %q, %v", data, err)
			}
			v, _ = c.entries.Load(key.ID)
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
				if _, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, produce); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("touch failure: %v", err)
				}
				if err := os.WriteFile(blob, []byte(first.id), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, produce); err == nil || calls != 2 {
					t.Fatalf("poisoned hit reused: calls=%d, %v", calls, err)
				}
			}
		})
	}

	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("recursive-held-%t", disk), func(t *testing.T) {
			src := t.TempDir()
			repo, err := git.PlainInit(src, false)
			if err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, src, "nested/file", "first")
			wt, err := repo.Worktree()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := wt.Add("."); err != nil {
				t.Fatal(err)
			}
			if _, err := wt.Commit("first", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}}); err != nil {
				t.Fatal(err)
			}
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := newCache(t, root)
			key := GitKey{ID: "recursive-held", RecurseSubmodules: true}
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
				input, pin, err := gittree.New(t.Context(), generation.Backing, generation.Commit, gittree.ViewOptions{Worktree: generation.Worktree, Identity: key.ID + "@" + generation.Commit.String()})
				if err != nil {
					t.Fatal(err)
				}
				return input, pin
			}
			first, err := c.Git(t.Context(), key, Policy{MaxAge: 0}, produce)
			if err != nil {
				t.Fatal(err)
			}
			held, pin := admit(first)
			for _, changed := range []bool{false, true} {
				if changed {
					testutil.WriteFile(t, src, "nested/file", "second")
					if _, err := wt.Add("."); err != nil {
						t.Fatal(err)
					}
					if _, err := wt.Commit("second", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}}); err != nil {
						t.Fatal(err)
					}
				}
				next, err := c.Git(t.Context(), key, Policy{MaxAge: 0}, produce)
				if err != nil {
					t.Fatal(err)
				}
				_, nextPin := admit(next)
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
				c = newCache(t, root)
				restored, err := c.Git(t.Context(), key, Policy{MaxAge: Forever}, nil)
				if err != nil {
					t.Fatal(err)
				}
				input, pin := admit(restored)
				if pin != key.ID+"@"+restored.Commit.String() {
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
	for _, scenario := range []string{"producer", "cancelled", "publication"} {
		t.Run(scenario, func(t *testing.T) {
			c := newCache(t, t.TempDir())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("incomplete clone")
			g, err := c.Git(ctx, GitKey{ID: "source-git-failed", RecurseSubmodules: true}, Policy{MaxAge: 0}, func(_ context.Context, _ storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
				file, err := worktree.Create("partial")
				if err != nil {
					return plumbing.ZeroHash, err
				}
				if err := file.Close(); err != nil {
					return plumbing.ZeroHash, err
				}
				if scenario == "publication" {
					index := filepath.Join(c.layout.OCI(), ocispec.ImageIndexFile)
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
			entries, err := os.ReadDir(filepath.Dir(c.layout.GitObjects("")))
			if err != nil || len(entries) != 0 {
				t.Fatalf("failed generation directories = %v, %v", entries, err)
			}
			if v, ok := c.entries.Load("source-git-failed"); ok && !v.(*entry).resolved.IsZero() {
				t.Fatal("failed Git acquisition installed a successful entry")
			}
			if scenario == "publication" {
				index := filepath.Join(c.layout.OCI(), ocispec.ImageIndexFile)
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
			reopened := newCache(t, c.layout.Root)
			if _, err := reopened.store.Resolve(t.Context(), "source-git-failed"); !errors.Is(err, errdef.ErrNotFound) {
				t.Fatalf("failed generation published a durable tag: %v", err)
			}
		})
	}
}

func TestCache_RootedWritesRejectEscape(t *testing.T) {
	for _, sink := range []string{"staging", "last-use", "last-use-parent", "git-generation", "staging-cleanup-parent", "staging-cleanup-final"} {
		t.Run(sink, func(t *testing.T) {
			c := newCache(t, t.TempDir())
			outside := t.TempDir()
			testutil.WriteFile(t, outside, "marker", "untouched")
			key := BlobKey{ID: "rooted-write"}
			if sink == "git-generation" {
				key.ID = "source-git-rooted"
			}
			link, target := c.layout.OCI(), outside
			hit := strings.HasPrefix(sink, "last-use")
			cleanup := strings.HasPrefix(sink, "staging-cleanup")
			if hit {
				r, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, producer([]byte("content"), ocispec.Descriptor{}))
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				e, _ := c.entries.Load(key.ID)
				payload := blobPath(e.(*entry).stored.Digest)
				link = filepath.Join(c.layout.Root, payload)
				target = filepath.Join(outside, "marker")
				if sink == "last-use-parent" {
					link = filepath.Dir(link)
					target = outside
					testutil.WriteFile(t, outside, filepath.Base(payload), "outside payload")
				}
			} else if sink == "git-generation" {
				link = filepath.Join(c.layout.OCI(), "git")
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
				g, err = c.Git(t.Context(), GitKey{ID: key.ID}, Policy{MaxAge: 0}, func(context.Context,

					storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
					called = true
					return plumbing.ZeroHash, nil
				})
				if g != nil {
					t.Fatal("escape returned a Git generation")
				}
			} else {
				var r io.ReadCloser
				r, _, err = c.Blob(t.Context(), key, Policy{MaxAge: Forever}, func(context.Context, io.Writer) (ocispec.Descriptor, error) {
					called = true
					if !cleanup {
						return ocispec.Descriptor{}, nil
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
						link = filepath.Join(c.layout.Root, name)
						target = filepath.Join(outside, staging[0])
					}
					if err := os.Rename(link, link+"-saved"); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(target, link); err != nil {
						t.Fatal(err)
					}
					before = sweepSnapshot(t, outside)
					return ocispec.Descriptor{}, failure
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
			if v, ok := c.entries.Load(key.ID); ok && !v.(*entry).resolved.IsZero() {
				t.Fatal("failed acquisition installed a successful entry")
			}
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
			_, err = c.store.Resolve(t.Context(), key.ID)
			if hit && err != nil || !hit && !errors.Is(err, errdef.ErrNotFound) {
				t.Fatalf("publication after failure: %v", err)
			}
		})
	}
}

func TestNew_IndexRecoveryRejectsEscapingParent(t *testing.T) {
	for _, component := range []string{"parent", "final"} {
		t.Run(component, func(t *testing.T) {
			c := newCache(t, t.TempDir())
			outside := newCache(t, t.TempDir())
			if err := errors.Join(c.Close(), outside.Close()); err != nil {
				t.Fatal(err)
			}
			index := filepath.Join(outside.layout.OCI(), ocispec.ImageIndexFile)
			testutil.WriteFile(t, outside.layout.OCI(), ocispec.ImageIndexFile, `{"schemaVersion":`)
			link, target := c.layout.OCI(), outside.layout.OCI()
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
			before := sweepSnapshot(t, outside.layout.Root)
			recovered, err := New(t.Context(), c.layout.Root)
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
			if diff := cmp.Diff(before, sweepSnapshot(t, outside.layout.Root)); diff != "" {
				t.Fatalf("outside index changed (-want +got):\n%s", diff)
			}
			reopened := newCache(t, c.layout.Root)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGit_FailedCleanupRejectsEscapingParent(t *testing.T) {
	for _, phase := range []string{"producer", "publication"} {
		t.Run(phase, func(t *testing.T) {
			c := newCache(t, t.TempDir())
			outside := t.TempDir()
			key := GitKey{ID: "source-git-escaping-cleanup", RecurseSubmodules: true}
			parent := filepath.Dir(c.layout.GitObjects(""))
			index := filepath.Join(c.layout.OCI(), ocispec.ImageIndexFile)
			failure := errors.New("producer failed after parent replacement")
			var before map[string]string
			g, err := c.Git(t.Context(), key, Policy{MaxAge: 0},
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
			if v, ok := c.entries.Load(key.ID); ok && !v.(*entry).resolved.IsZero() {
				t.Fatal("failed Git acquisition installed a successful entry")
			}
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
			reopened := newCache(t, c.layout.Root)
			if _, err := reopened.store.Resolve(t.Context(), key.ID); !errors.Is(err, errdef.ErrNotFound) {
				t.Fatalf("failed generation published a durable tag: %v", err)
			}
		})
	}
}
