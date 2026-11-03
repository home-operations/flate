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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The test changes the process logging destination.
//nolint:paralleltest // The test changes process logging state.
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
	{
		_, _, _, err := c.Acquire(
			t.Context(),
			key,
			Policy{MaxAge: time.Hour},
			producer(data, &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)
	}
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	index := filepath.Join(root, indexPath())
	{
		err := os.WriteFile(index, []byte(`{"schemaVersion":`), 0o600)
		assert.NoError(t, err)
	}

	c = cachetest.New(t, root, New)
	{
		got := logs.String()
		assert.FatalIf(t, strings.Count(got, "level=DEBUG msg=\"cache: rebuilding unreadable store index\"") != 1 ||
			strings.Contains(got, "level=WARN"), "index recovery log: %s", got)
	}
	calls := 0
	input, _, _, err := c.Acquire(
		t.Context(),
		key,
		Policy{MaxAge: time.Hour},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			calls++
			return producer(data, &ocispec.Descriptor{})(ctx, w)
		},
	)
	assert.NoError(t, err)

	{
		got, err := fs.ReadFile(input, "file")
		assert.FatalIf(
			t,
			err != nil || string(got) != "same" || calls != 1,
			"recovery: content=%q calls=%d err=%v",
			got,
			calls,
			err,
		)
	}
}

func TestNew_InvalidLayoutPreservesIndex(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	index := filepath.Join(root, indexPath())
	want, err := os.ReadFile(index)
	assert.NoError(t, err)
	{
		err := os.WriteFile(
			filepath.Join(root, ociDir, "oci-layout"),
			[]byte(`{"imageLayoutVersion":`),
			0o600,
		)
		assert.NoError(t, err)
	}
	{
		_, err := New(t.Context(), root)
		assert.FatalIf(t, err == nil, "%s", "invalid layout accepted")
	}

	{
		got, err := os.ReadFile(index)
		assert.FatalIf(
			t,
			err != nil || !bytes.Equal(got, want),
			"unrelated layout error changed index: %q %v",
			got,
			err,
		)
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
				_, _, _, err := c.Acquire(
					t.Context(),
					ArchiveKey{Content: "source-trailing-boundary"},
					Policy{MaxAge: 0},
					func(_ context.Context, w ProducerInput) (ProducerResult, error) {
						var payloadTar bytes.Buffer
						testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
						if _, err := w.Write(payloadTar.Bytes()); err != nil {
							return ProducerResult{}, err
						}
						_, err := w.Write(make([]byte, maxTrailingBytes+extra))
						return ProducerResult{}, err
					},
				)
				assert.FatalIf(t, (err != nil) != (extra > 0), "extra=%d: err=%v", extra, err)
			})
		}
	}
}

func TestAcquire_TrailingFailureKeepsCacheIdentity(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			testAcquireTrailingFailureKeepsCacheIdentityCase(
				t,
				&backing,
			)
		},
		)
	}
}

func TestGit_RestartAndInvalidation(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"hit", "ref-mismatch", "missing-objects", "touch-failure", "outcome"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testGitRestartAndInvalidationCase(t, &scenario)
		},
		)
	}

	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("recursive-held-%t", disk), func(t *testing.T) {
			t.Parallel()
			testGitRestartAndInvalidationCase2(t, &disk)
		},
		)
	}
}

func TestGit_FailedAttemptRemovesDirectories(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"producer", "cancelled", "publication"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testGitFailedAttemptRemovesDirectoriesCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func TestCache_RootedWritesRejectEscape(t *testing.T) {
	t.Parallel()
	for _, sink := range []string{"staging", "last-use", "last-use-parent", "git-generation", "staging-cleanup-parent", "staging-cleanup-final"} {
		t.Run(sink, func(t *testing.T) {
			t.Parallel()
			testCacheRootedWritesRejectEscapeCase(t, &sink)
		},
		)
	}
}

func TestNew_IndexRecoveryRejectsEscapingParent(t *testing.T) {
	t.Parallel()
	for _, component := range []string{"parent", "final"} {
		t.Run(component, func(t *testing.T) {
			t.Parallel()
			testNewIndexRecoveryRejectsEscapingParentCase(
				t,
				&component,
			)
		},
		)
	}
}

func TestGit_FailedCleanupRejectsEscapingParent(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"producer", "publication"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			testGitFailedCleanupRejectsEscapingParentCase(
				t,
				&phase,
			)
		},
		)
	}
}

func TestAcquire_GeneratedPolicy(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(strconv.FormatBool(disk), func(t *testing.T) {
			t.Parallel()
			testAcquireGeneratedPolicyCase(t, &disk)
		},
		)
	}
}

func TestAcquire_ExtractPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path string
		kind       byte
	}{
		{"symlink", "entry", tar.TypeSymlink},
		{"hardlink", "entry", tar.TypeLink},
		{"fifo", "entry", tar.TypeFifo},
		{"character_device", "entry", tar.TypeChar},
		{"block_device", "entry", tar.TypeBlock},
		{"traversal", "a/../entry", tar.TypeReg},
		{"absolute", "/entry", tar.TypeReg},
		{"drive", "C:/entry", tar.TypeReg},
		{"backslash", `a\entry`, tar.TypeReg},
	} {
		for _, disk := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disk=%v", tc.name, disk), func(t *testing.T) {
				t.Parallel()
				testAcquireExtractPolicyCase(t, &tc, &disk)
			},
			)
		}
	}
}

func TestAcquire_MemoryRetirement(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			testAcquireMemoryRetirementCase(t, &backing)
		},
		)
	}
}

func TestNew_SnapshotStartsAtOCIUse(t *testing.T) {
	t.Parallel()
	c, err := New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, c.Close()) }()
	assert.FatalIf(t, c.snapshot.Load() != nil, "%s", "unused cache retained an OCI snapshot")

	_, err = c.acquireOnce(t.Context(), &acquireKey{id: "missing", lookupOnly: true}, acquisitionCallbacks{})
	assert.FatalIf(t, !errors.Is(err, errdef.ErrNotFound), "lookup error = %v, want not found", err)
	assert.FatalIf(t, c.snapshot.Load() == nil, "%s", "OCI lookup did not prepare a reusable snapshot")

	assert.NoError(t, c.Close())
	owners.mu.Lock()
	_, published := owners.indexes[c.root.Name()]
	owners.mu.Unlock()
	assert.FatalIf(t, published, "%s", "read-only lookup published a snapshot")

	root := t.TempDir()
	c, err = New(t.Context(), root)
	assert.NoError(t, err)
	key := ArchiveKey{Content: "snapshot publication", Policy: "fixed"}
	fill := producer(orderedArchive(t, "file", "content"), &ocispec.Descriptor{})
	_, _, _, err = c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
	assert.NoError(t, err)
	snapshot := c.snapshot.Load()
	assert.NoError(t, c.Close())
	index := snapshot.index
	c, err = New(t.Context(), root)
	assert.NoError(t, err)
	assert.FatalIf(t, c.snapshot.Load() != snapshot, "%s", "unchanged index did not reuse the published snapshot")

	_, _, _, err = c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, nil)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	assert.FatalIf(t, &snapshot.index[0] != &index[0], "%s", "read-only run captured the snapshot files again")
}

func testAcquireGenerationsAndFreshnessCase(t *testing.T, disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	assert.FatalIf(t, !(*disk) && c.root != nil, "%s", "memory cache opened a disk root")

	key := ArchiveKey{Content: "source-test-generations"}
	want := ocispec.Descriptor{
		Digest:      digest.FromString("upstream"),
		Size:        123,
		MediaType:   "upstream/type",
		Annotations: map[string]string{"flate.revision": "v1", "other": "preserved"},
	}
	var initialArchive bytes.Buffer
	testutil.WriteTar(t, &initialArchive, map[string]string{"file": "v1"}, 0o644)
	first, got, id, err := c.Acquire(
		t.Context(),
		key,
		Policy{MaxAge: time.Hour},
		producer(initialArchive.Bytes(), &want),
	)
	assert.NoError(t, err)

	firstPin := AcquisitionIdentity(first)
	assert.FatalIf(t, firstPin == "", "%s", "acquired archive has no identity")

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatal(diff)
	}
	unexpected := func(context.Context, ProducerInput) (ProducerResult, error) {
		t.Error("fresh hit called producer")
		return ProducerResult{Descriptor: want}, errors.New("unexpected producer")
	}
	if *disk {
		{
			closeErr := c.Close()
			assert.NoError(t, closeErr)
		}

		c = cachetest.New(t, root, New)
	}
	warm, got, warmID, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, unexpected)
	assert.NoError(t, err)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatal(diff)
	}
	assert.FatalIf(t, id == "" || warmID != id, "acquisition identities: first=%q warm=%q", id, warmID)
	invalid := struct{ Value any }{make(chan int)}
	if _, _, acquireInvalidID, acquireErr := c.Acquire(
		t.Context(),
		ArchiveKey{Content: invalid},
		Policy{MaxAge: time.Hour},
		unexpected,
	); acquireInvalidID != "" ||
		!errors.Is(acquireErr, manifest.ErrInput) {
		t.Fatalf("invalid archive projection: id=%q, %v", acquireInvalidID, acquireErr)
	} else {
		_, ok := errors.AsType[*CacheError](
			acquireErr,
		)
		assert.FatalIf(t, !ok, "invalid projection error: %v", acquireErr)
	}
	{
		r, _, blobInvalidID, blobErr := c.Blob(
			t.Context(),
			BlobKey{Content: invalid},
			Policy{MaxAge: time.Hour},
			unexpected,
		)
		assert.FatalIf(t, r != nil || blobInvalidID != "" ||
			!errors.Is(blobErr, manifest.ErrInput), "invalid blob projection: id=%q, %v", blobInvalidID, blobErr)
	}
	{
		g, gitInvalidID, gitErr := c.Git(
			t.Context(),
			GitKey{Content: invalid},
			Policy{MaxAge: time.Hour},
			nil,
		)
		assert.FatalIf(t, g != nil || gitInvalidID != "" ||
			!errors.Is(gitErr, manifest.ErrInput), "invalid Git projection: id=%q, %v", gitInvalidID, gitErr)
	}
	entries := 0
	c.entries.Range(func(_, _ any) bool { entries++; return true })
	assert.FatalIf(t, entries != 1, "invalid projection installed an entry: %d", entries)
	{
		data, readFileErr := fs.ReadFile(warm, "file")
		assert.FatalIf(t, readFileErr != nil || string(data) != "v1", "warm: %q, %v", data, readFileErr)
	}
	testAcquireGenerationsAndFreshnessCaseCompletion(
		t,
		&testAcquireGenerationsAndFreshnessCaseCompletionInputs{
			c:          &c,
			id:         &id,
			disk:       &disk,
			key:        &key,
			unexpected: &unexpected,
			warm:       &warm,
			want:       &want,
			firstPin:   &firstPin,
			root:       &root,
		},
	)
}

func testAcquireGenerationsAndFreshnessCaseCompletion(
	t *testing.T,
	captures *testAcquireGenerationsAndFreshnessCaseCompletionInputs,
) {
	t.Helper()
	e, _ := (*captures.c).entries.Load((*captures.id))
	at := e.(*entry).resolved
	if *(*captures.disk) {
		p := filepath.Join((*captures.c).root.Name(), blobPath(e.(*entry).stored.Digest))
		recent := time.Now()
		{
			chtimesErr := os.Chtimes(p, recent, recent)
			assert.NoError(t, chtimesErr)
		}

		info, statErr := os.Stat(p)
		assert.NoError(t, statErr)

		e.(*entry).touched.Store(info.ModTime().UnixNano())
		var warm sync.WaitGroup
		for range 8 {
			warm.Go(func() {
				if _, _, _, acquireErr2 := (*captures.c).Acquire(
					t.Context(), (*captures.key), Policy{MaxAge: time.Hour}, (*captures.unexpected),
				); acquireErr2 != nil {
					t.Error(acquireErr2)
				}
			})
		}
		warm.Wait()
		unchanged, statErr := os.Stat(p)
		assert.FatalIf(
			t,
			statErr != nil || !unchanged.ModTime().Equal(info.ModTime()),
			"warm hit rewrote retention timestamp: %v, %v",
			unchanged,
			statErr,
		)
		stale := time.Now().Add(-24 * time.Hour)
		{
			chtimesErr2 := os.Chtimes(p, stale, stale)
			assert.NoError(t, chtimesErr2)
		}

		e.(*entry).touched.Store(stale.UnixNano())
		{
			_, _, _, acquireErr3 := (*captures.c).Acquire(
				t.Context(),
				(*captures.key),
				Policy{MaxAge: time.Hour},
				(*captures.unexpected),
			)
			assert.NoError(t, acquireErr3)
		}

		info, statErr = os.Stat(p)
		assert.FatalIf(t, statErr != nil || !info.ModTime().After(stale), "access time: %v, %v", info, statErr)
		assert.FatalIf(t, !at.Equal(e.(*entry).resolved), "%s", "access extended resolution TTL")
	}
	var replacementArchive bytes.Buffer
	testutil.WriteTar(t, &replacementArchive, map[string]string{"file": "v2"}, 0o644)
	openFirst, err := (*captures.warm).Open("file")
	assert.NoError(t, err)

	defer func() {
		if closeErr2 := openFirst.Close(); closeErr2 != nil {
			t.Error(closeErr2)
		}
	}()
	replacement, _, _, err := (*captures.c).Acquire(
		t.Context(), (*captures.key), Policy{MaxAge: 0},
		producer(replacementArchive.Bytes(), &(*captures.want)),
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		AcquisitionIdentity(replacement) == (*captures.firstPin) ||
			AcquisitionIdentity((*captures.warm)) != (*captures.firstPin),
		"%s",
		"replacement aliased or mutated held generation identity",
	)

	{
		data, err := fs.ReadFile((*captures.warm), "file")
		assert.FatalIf(t, err != nil || string(data) != "v1", "held generation: %q, %v", data, err)
	}
	{
		data, err := io.ReadAll(openFirst)
		assert.FatalIf(t, err != nil || string(data) != "v1", "open superseded generation: %q, %v", data, err)
	}
	{
		data, err := fs.ReadFile(replacement, "file")
		assert.FatalIf(t, err != nil || string(data) != "v2", "replacement generation: %q, %v", data, err)
	}
	failure := errors.New("verification failed")
	{
		_, _, _, err := (*captures.c).Acquire(
			t.Context(), (*captures.key), Policy{MaxAge: 0},
			func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				var partialArchive bytes.Buffer
				testutil.WriteTar(t, &partialArchive, map[string]string{"file": "partial"}, 0o644)
				_, _ = w.Write(partialArchive.Bytes())
				return ProducerResult{Descriptor: (*captures.want)}, failure
			},
		)
		assert.FatalIf(t, !errors.Is(err, failure) ||
			!errors.Is(err, manifest.ErrFlux), "failure: %v", err)
	}
	calls := 0
	{
		_, _, _, err := (*captures.c).Acquire(
			t.Context(), (*captures.key), Policy{MaxAge: time.Hour},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				var recoveredArchive bytes.Buffer
				testutil.WriteTar(t, &recoveredArchive, map[string]string{"file": "v3"}, 0o644)
				return producer(recoveredArchive.Bytes(), &(*captures.want))(ctx, w)
			},
		)
		assert.FatalIf(t, err != nil ||
			calls != 1, "failed fill reused previous alias: calls=%d, %v", calls, err)
	}
	if *(*captures.disk) {
		t.Run("corrupt-reopen", func(t *testing.T) {
			t.Parallel()
			testAcquireGenerationsAndFreshnessCaseCorruptReopen(

				t, testAcquireGenerationsAndFreshnessCaseCorruptReopenInputs{
					root: captures.root, c: captures.c,
					key: captures.key, id: captures.id, unexpected: captures.unexpected,
				})
		},
		)
	}
}

type testAcquireGenerationsAndFreshnessCaseCorruptReopenInputs struct {
	root       *string
	c          **Cache
	key        *ArchiveKey
	id         *string
	unexpected *func(context.Context, ProducerInput) (ProducerResult, error)
}
type testAcquireGenerationsAndFreshnessCaseCompletionInputs struct {
	c          **Cache
	id         *string
	disk       **bool
	key        *ArchiveKey
	unexpected *func(context.Context, ProducerInput) (ProducerResult, error)
	warm       *fs.FS
	want       *ocispec.Descriptor
	firstPin   *string
	root       *string
}
