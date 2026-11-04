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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

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
		&Policy{MaxAge: time.Hour},
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
	warm, got, warmID, err := c.Acquire(t.Context(), key, &Policy{MaxAge: time.Hour}, unexpected)
	assert.NoError(t, err)

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatal(diff)
	}
	assert.FatalIf(t, id == "" || warmID != id, "acquisition identities: first=%q warm=%q", id, warmID)
	invalid := struct{ Value any }{make(chan int)}
	if _, _, acquireInvalidID, acquireErr := c.Acquire(
		t.Context(),
		ArchiveKey{Content: invalid},
		&Policy{MaxAge: time.Hour},
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
			&Policy{MaxAge: time.Hour},
			unexpected,
		)
		assert.FatalIf(t, r != nil || blobInvalidID != "" ||
			!errors.Is(blobErr, manifest.ErrInput), "invalid blob projection: id=%q, %v", blobInvalidID, blobErr)
	}
	{
		g, gitInvalidID, gitErr := c.Git(
			t.Context(),
			GitKey{Content: invalid},
			&Policy{MaxAge: time.Hour},
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
					t.Context(), (*captures.key), &Policy{MaxAge: time.Hour}, (*captures.unexpected),
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
				&Policy{MaxAge: time.Hour},
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
		t.Context(), (*captures.key), &Policy{MaxAge: 0},
		producer(replacementArchive.Bytes(), captures.want),
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
			t.Context(), (*captures.key), &Policy{MaxAge: 0},
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
			t.Context(), (*captures.key), &Policy{MaxAge: time.Hour},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				var recoveredArchive bytes.Buffer
				testutil.WriteTar(t, &recoveredArchive, map[string]string{"file": "v3"}, 0o644)
				return producer(recoveredArchive.Bytes(), captures.want)(ctx, w)
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

func testAcquireGenerationsAndFreshnessCaseCorruptReopen(
	t *testing.T,
	testCaseInput testAcquireGenerationsAndFreshnessCaseCorruptReopenInputs,
) {
	t.Helper()

	e, _ := (*testCaseInput.c).entries.Load((*testCaseInput.id))
	path := filepath.Join((*testCaseInput.root), blobPath(e.(*entry).stored.Digest))
	{
		err := (*testCaseInput.c).Close()
		assert.NoError(t, err)
	}
	{
		err := os.Chmod(path, 0o600)
		assert.NoError(t, err)
	}

	testutil.WriteFileAt(t, path, "truncated")
	(*testCaseInput.c) = cachetest.New(t, (*testCaseInput.root), New)
	for attempt := range 3 {
		_, _, _, err := (*testCaseInput.c).Acquire(
			t.Context(),
			(*testCaseInput.key),
			&Policy{MaxAge: Forever},
			(*testCaseInput.unexpected),
		)
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.FatalIf(
				t,
				!ok || !errors.Is(err, io.ErrUnexpectedEOF),
				"corrupt reload %d: %v",
				attempt,
				err,
			)
		}
	}
}
