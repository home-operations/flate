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
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"
	"github.com/gofrs/flock"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func orderedArchive(t *testing.T, files ...string) []byte {
	t.Helper()
	var data bytes.Buffer
	w := tar.NewWriter(&data)
	for i := 0; i < len(files); i += 2 {
		{
			err := w.WriteHeader(&tar.Header{Name: files[i], Size: int64(len(files[i+1])), Mode: 0o644})
			assert.NoError(t, err)
		}
		{
			_, err := io.WriteString(w, files[i+1])
			assert.NoError(t, err)
		}
	}
	{
		err := w.Close()
		assert.NoError(t, err)
	}

	return data.Bytes()
}

func producer(
	data []byte,
	descriptorValue *ocispec.Descriptor,
) func(context.Context, ProducerInput) (ProducerResult, error) {
	descriptor := *descriptorValue
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
			testAcquireGenerationsAndFreshnessCase(t, &disk)
		},
		)
	}
}

func TestAcquire_ArchivePolicy(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		for _, defaults := range []bool{false, true} {
			t.Run(
				map[bool]string{false: "memory", true: "disk"}[disk]+map[bool]string{false: "/bucket", true: "/defaults"}[defaults],
				func(t *testing.T) {
					t.Parallel()
					testAcquireArchivePolicyCase(
						t,
						&disk, &defaults,
					)
				},
			)
		}
	}
}

func TestAcquire_ArchiveFailuresAndLinks(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[disk], func(t *testing.T) {
			t.Parallel()
			testAcquireArchiveFailuresAndLinksCase(t, &disk)
		},
		)
	}
}

func TestAcquire_ConcurrentKeysAndCancellation(t *testing.T) {
	t.Parallel()

	t.Run("due", func(t *testing.T) {
		t.Parallel()
		testAcquireConcurrentKeysAndCancellationDue(

			t)
	})

	for _, callers := range []int{2, 4} {
		t.Run(fmt.Sprintf("due/canceled-winner/%d", callers), func(t *testing.T) {
			t.Parallel()
			testAcquireConcurrentKeysAndCancellationCase(t, &callers)
		},
		)
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
				return producer(
					payloadTar.Bytes(), &ocispec.Descriptor{Annotations: map[string]string{"flate.revision": "v1"}},
				)(
					ctx,
					w,
				)
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
			{
				_, _, id, err := c.Acquire(
					ctx,
					ArchiveKey{Content: content},
					Policy{MaxAge: time.Hour},
					fill,
				)
				assert.ErrorIf(t, id != "" ||
					!errors.Is(err, context.Canceled), "wait: id=%q, %v", id, err)
			}
			crossCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			testAcquireSharedContentAfterCancellation(
				t,
				crossCtx,
				&sharedContentAfterCancellation{
					concurrency: &concurrency,
					c:           &c,
					content:     &content,
					release:     &release,
					ids:         &ids,
					calls:       &calls,
					wg:          &wg,
				},
			)
		}
	}
}

func TestNew_SharedOwnershipAndCanceledLock(t *testing.T) {
	t.Parallel()
	t.Run("unused-owner", func(t *testing.T) { t.Parallel(); testNewSharedOwnershipAndCanceledLockUnusedOwner(t) })
	t.Run(
		"partial-initialization",
		func(t *testing.T) { t.Parallel(); testNewSharedOwnershipAndCanceledLockPartialInitialization(t) },
	)
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	alias := filepath.Join(t.TempDir(), "alias")
	{
		err := os.Symlink(root, alias)
		assert.NoError(t, err)
	}

	shared := cachetest.New(t, alias, New)
	assert.FatalIf(t, shared != c, "%s", "canonical directory has multiple owners")

	reader, _, _, err := c.Blob(
		t.Context(),
		BlobKey{Content: "blob-held"},
		Policy{MaxAge: Forever},
		producer([]byte("held"), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	disk := c.root
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	{
		_, err := disk.Stat(".")
		assert.FatalIf(t, err != nil, "first Close closed shared root: %v", err)
	}
	assertBlobRoots(t, c, digest.SHA256)
	for i := range c.blobRoots {
		parent := &c.blobRoots[i]
		if parent.Root == nil {
			continue
		}
		{
			_, err := parent.Stat(".")
			assert.FatalIf(t, err != nil, "first Close closed shared blob parent: %v", err)
		}
	}
	{
		data, err := fs.ReadFile(reader, "content")
		assert.FatalIf(t, err != nil || string(data) != "held", "reader after first Close: %q, %v", data, err)
	}

	lock := flock.New(filepath.Join(root, ociLock))
	t.Cleanup(func() { _ = lock.Close() })
	{
		acquired, err := lock.TryLock()
		assert.FatalIf(
			t,
			err != nil || acquired,
			"first Close released shared lock: acquired=%v, err=%v",
			acquired,
			err,
		)
	}
	{
		_, _, _, err := shared.Blob(
			t.Context(),
			BlobKey{Content: "blob-shared"},
			Policy{MaxAge: time.Hour},
			producer([]byte("data"), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)
	}
	{
		err := shared.Close()
		assert.NoError(t, err)
	}
	{
		_, err := disk.Stat(".")
		assert.FatalIf(t, err == nil, "%s", "last Close retained root")
	}

	assertBlobRoots(t, c, digest.SHA256)
	for i := range c.blobRoots {
		parent := &c.blobRoots[i]
		if parent.Root == nil {
			continue
		}
		{
			_, err := parent.Stat(".")
			assert.FatalIf(t, !errors.Is(err, fs.ErrClosed), "last Close retained blob parent: %v", err)
		}
	}
	{
		acquired, err := lock.TryLock()
		assert.FatalIf(t, err != nil || !acquired, "last Close retained lock: acquired=%v, err=%v", acquired, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	t.Cleanup(cancel)
	{
		_, err := New(ctx, root)
		assert.FatalIf(t, !errors.Is(err, context.DeadlineExceeded), "lock wait: %v", err)
	}
}

func TestAcquire_TagSaveFailureDoesNotBecomeHit(t *testing.T) {
	t.Parallel()
	t.Run("outcome", func(t *testing.T) { t.Parallel(); testAcquireTagSaveFailureDoesNotBecomeHitOutcome(t) })

	c := cachetest.New(t, t.TempDir(), New)
	index := filepath.Join(c.root.Name(), indexPath())
	{
		err := os.Remove(index)
		assert.NoError(t, err)
	}
	{
		err := os.Mkdir(index, 0o750)
		assert.NoError(t, err)
	}

	calls := 0
	fill := func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
		calls++
		var payloadTar bytes.Buffer
		testutil.WriteTar(t, &payloadTar, map[string]string{"file": "data"}, 0o644)
		return producer(payloadTar.Bytes(), &ocispec.Descriptor{})(ctx, w)
	}
	for range 2 {
		{
			_, _, _, err := c.Acquire(
				t.Context(),
				ArchiveKey{Content: "source-test-persist"},
				Policy{MaxAge: time.Hour},
				fill,
			)
			assert.FatalIf(t, err == nil, "%s", "tag save failure accepted")
		}
	}
	assert.FatalIf(t, calls != 2, "failed publication served an in-memory hit: %d", calls)
}

func TestAcquire_SparseReplayMatchesMemory(t *testing.T) {
	t.Parallel()
	root, err := exec.CommandContext(t.Context(), "go", "env", "GOROOT").Output()
	assert.NoError(t, err)

	data, err := os.ReadFile(
		filepath.Join(strings.TrimSpace(string(root)), "src/archive/tar/testdata/sparse-formats.tar"),
	)
	assert.NoError(t, err)

	var inputs []fs.FS
	for _, root := range []string{"", t.TempDir()} {
		c := cachetest.New(t, root, New)
		input, _, _, err := c.Acquire(
			t.Context(),
			ArchiveKey{Content: "source-test-sparse"},
			Policy{MaxAge: time.Hour},
			producer(data, &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)

		inputs = append(inputs, input)
	}
	for _, name := range []string{"sparse-posix-0.0", "sparse-posix-0.1", "sparse-posix-1.0"} {
		want, err := fs.ReadFile(inputs[0], name)
		assert.NoError(t, err)

		got, err := fs.ReadFile(inputs[1], name)
		assert.NoError(t, err)

		assert.FatalIf(t, !bytes.Equal(got, want), "sparse replay %s mismatch", name)
	}
	{
		err := fstest.TestFS(inputs[1], "sparse-posix-0.0", "sparse-posix-0.1", "sparse-posix-1.0")
		assert.NoError(t, err)
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
			testDecodeRequiredMetadataCase(t, &family)
		},
		)
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
			reader, _, _, err := c.Blob(
				t.Context(),
				key,
				Policy{MaxAge: Forever},
				producer([]byte("blob bytes"), &ocispec.Descriptor{}),
			)
			assert.NoError(t, err)

			input := NameBlob("chart.tgz", reader)
			{
				testFSErr := fstest.TestFS(input, "chart.tgz")
				assert.NoError(t, testFSErr)
			}

			got, err := fs.ReadFile(input, "chart.tgz")
			assert.FatalIf(t, err != nil || string(got) != "blob bytes", "content=%q, err=%v", got, err)
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
			_, _, _, err := c.Acquire(
				t.Context(),
				ArchiveKey{Content: "source-trailing"},
				Policy{MaxAge: 0},
				func(_ context.Context, w ProducerInput) (ProducerResult, error) {
					var payloadTar bytes.Buffer
					testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
					if _, err := w.Write(payloadTar.Bytes()); err != nil {
						return ProducerResult{}, err
					}
					var err error
					written, err = w.Write(make([]byte, 2<<20))
					return ProducerResult{}, err
				},
			)
			assert.FatalIf(t, err == nil, "%s", "accepted trailing data beyond the limit")

			assert.FatalIf(t, written > 1<<20+1, "consumed %d trailing bytes beyond the 1 MiB limit", written)
		})
	}
}

func TestAcquire_CancellationKeepsContextError(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			testAcquireCancellationKeepsContextErrorCase(
				t,
				&backing,
			)
		},
		)
	}
	t.Run(
		"completed blob",
		func(t *testing.T) { t.Parallel(); testAcquireCancellationKeepsContextErrorCompletedBlob(t) },
	)
}

type archiveCompletionContext struct {
	context.Context //nolint:containedctx // The test stores the producer context to assert deadline and cancellation propagation.

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
				testAcquireCancelAfterArchiveWorkersCase(
					t,
					&backing, &result,
				)
			},
			)
		}
	}
}

func TestAcquire_UnchangedRefreshRenews(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	key := ArchiveKey{Content: "source-unchanged"}
	var first time.Time
	for range 2 {
		_, _, id, err := c.Acquire(
			t.Context(),
			key,
			Policy{},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				previous, err := w.Previous(ctx)
				if err != nil {
					return ProducerResult{}, err
				}
				if previous != nil {
					return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
				}
				return producer(orderedArchive(t, "file", "same"), &ocispec.Descriptor{})(ctx, w)
			},
		)
		assert.NoError(t, err)

		desc, err := c.store.Resolve(t.Context(), id)
		assert.NoError(t, err)

		_, resolved, valid := decode(&desc, key.Required)
		assert.FatalIf(t, !valid || !resolved.After(first), "resolution time not renewed: %v after %v", resolved, first)
		first = resolved
	}
	for _, scenario := range []string{"missing previous", "wrote bytes", "producer error"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testAcquireUnchangedRefreshRenewsCase(t, &scenario)
		},
		)
	}
}

type archiveCancelContext struct {
	context.Context //nolint:containedctx // The test stores the producer context to assert deadline and cancellation propagation.

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

func TestAcquire_SharedWaiterCancellation(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		for _, disk := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers=%d/disk=%t", workers, disk), func(t *testing.T) {
				t.Parallel()
				testAcquireSharedWaiterCancellationCase(
					t,
					&workers, &disk,
				)
			},
			)
		}
	}
}

func TestAcquire_CancelStopsContextIgnoringProducer(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 16} {
		for _, backing := range []string{"memory", "disk"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, backing), func(t *testing.T) {
				t.Parallel()
				testAcquireCancelStopsContextIgnoringProducerCase(
					t,

					&concurrency,
					&backing,
				)
			},
			)
		}
	}
}

type sharedContentAfterCancellation struct {
	concurrency *int
	c           **Cache
	content     *struct{ URL string }
	release     *chan struct{}
	ids         *chan string
	calls       *atomic.Int32
	wg          *sync.WaitGroup
}

func testAcquireSharedContentAfterCancellation(
	t *testing.T,
	crossCtx context.Context,
	scope *sharedContentAfterCancellation,
) {
	t.Helper()
	r, _, blobID, err := (*scope.c).Blob(
		crossCtx,
		BlobKey{Content: (*scope.content)},
		Policy{MaxAge: time.Hour},
		producer([]byte("blob"), &ocispec.Descriptor{}),
	)
	if err != nil {
		close((*scope.release))
		(*scope.wg).Wait()
		t.Fatal(err)
	}
	_, _ = fs.ReadFile(r, "content")

	g, gitID, err := (*scope.c).Git(
		crossCtx,
		GitKey{Content: (*scope.content)},
		Policy{MaxAge: time.Hour},
		func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
			return plumbing.ZeroHash, nil
		},
	)
	if err != nil || g == nil || blobID == "" || gitID == "" || blobID == gitID {
		close((*scope.release))
		(*scope.wg).Wait()
		t.Fatalf("cross-product Git: blob=%q git=%q, %v", blobID, gitID, err)
	}
	close((*scope.release))
	(*scope.wg).Wait()
	close((*scope.ids))
	var archiveID string
	for id := range *scope.ids {
		assert.FatalIf(
			t,
			id == "" || id == blobID || id == gitID || archiveID != "" && id != archiveID,
			"cross-product identities: archive=%q other=%q blob=%q git=%q",
			archiveID,
			id,
			blobID,
			gitID,
		)
		archiveID = id
	}
	if (*scope.calls).Load() != 1 {
		t.Fatalf("concurrency=%d calls=%d", (*scope.concurrency), (*scope.calls).Load())
	}
}
