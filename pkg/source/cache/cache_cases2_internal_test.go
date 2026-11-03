package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gofrs/flock"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testAcquireArchivePolicyCase(t *testing.T, disk *bool,
	defaults *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	data := orderedArchive(
		t,
		"dir/drop",
		"discard",
		"dir/keep",
		"first",
		"dir/.sourceignore",
		"!keep\n",
		".sourceignore",
		"dir/*\nempty/*\n",
		"dir/keep",
		"last",
		"empty/file",
		"discard",
		"image.png",
		"image",
		".git/config",
		"git",
	)
	input, _, _, err := c.Acquire(
		t.Context(),
		ArchiveKey{Content: "source-test-policy", Policy: map[bool]string{true: "source"}[(*defaults)]},
		Policy{MaxAge: time.Hour},
		producer(data, &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	{
		got, err := fs.ReadFile(input, "dir/keep")
		assert.FatalIf(t, err != nil || string(got) != "last", "reincluded duplicate: %q, %v", got, err)
	}
	pin := AcquisitionIdentity(input)
	assert.FatalIf(t, pin == "", "%s", "archive policy view has no acquisition identity")

	for _, policy := range []ArchiveKey{
		{Content: "source-test-policy", Policy: map[bool]string{true: "source"}[!(*defaults)]},
		{Content: "source-test-policy", Policy: map[bool]string{true: "source"}[(*defaults)], Ignore: "image.png\n"},
	} {
		other, _, _, err := c.Acquire(
			t.Context(),
			policy,
			Policy{MaxAge: 0},
			producer(data, &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			AcquisitionIdentity(other) == pin || AcquisitionIdentity(input) != pin,
			"%s",
			"archive policy identity collision",
		)
	}
	for _, name := range []string{"dir/drop", "empty"} {
		{
			_, err := fs.Stat(input, name)
			assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "ignored %s: %v", name, err)
		}
	}
	for _, name := range []string{".git/config"} {
		_, err := fs.Stat(input, name)
		assert.FatalIf(
			t, (*defaults) !=
				errors.Is(err, fs.ErrNotExist), "defaults=%v %s: %v", (*defaults), name,
			err,
		)
	}
}

func testAcquireArchiveFailuresAndLinksCase(t *testing.T, disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	for _, data := range [][]byte{orderedArchive(t, "../escape", "x"), orderedArchive(t, "file", "x", "file/child", "y"), orderedArchive(t, "dir/file", "x", "dir", "y"), []byte("truncated")} {
		{
			_, _, _, err := c.Acquire(
				t.Context(),
				ArchiveKey{Content: "source-test-invalid"},
				Policy{MaxAge: 0},
				producer(data, &ocispec.Descriptor{}),
			)
			assert.FatalIf(t, err == nil, "%s", "accepted invalid archive")
		}
	}
	for _, defaults := range []bool{false, true} {
		t.Run(strconv.FormatBool(defaults), func(t *testing.T) {
			t.Parallel()
			testAcquireArchiveFailuresAndLinksCaseCase(
				t,
				testAcquireArchiveFailuresAndLinksCaseCaseInputs{disk: &disk, root: &root, c: &c, defaults: &defaults},
			)
		},
		)
	}
}

func testAcquireConcurrentKeysAndCancellationDue(t *testing.T) {
	t.Helper()
	for _, callers := range []int{2, 4} {
		for _, cold := range []bool{false, true} {
			for _, result := range []string{"success", "eligible", "unsupported", "local", "publication"} {
				t.Run(fmt.Sprintf("%d/cold-%t/%s", callers, cold, result), func(t *testing.T) {
					testAcquireConcurrentKeysAndCancellationDueCase(
						t,
						testAcquireConcurrentKeysAndCancellationDueCaseInputs{
							callers: &callers,
							cold:    &cold,
							result:  &result,
						},
					)
				},
				)
			}
		}
	}
}

func testAcquireConcurrentKeysAndCancellationCase(
	t *testing.T, callers *int,
) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		testAcquireConcurrentKeysAndCancellationCaseCallback(
			t,
			testAcquireConcurrentKeysAndCancellationCaseCallbackInputs{callers: &callers},
		)
	},
	)
}

func testNewSharedOwnershipAndCanceledLockUnusedOwner(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	assertBlobRoots(t, c)
	assert.NoError(t, c.Close())
	for _, algorithm := range blobAlgorithms {
		{
			_, err := os.Stat(filepath.Join(root, blobDir(), algorithm.String()))
			assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "unused owner created %s root: %v", algorithm, err)
		}
	}
}

func testNewSharedOwnershipAndCanceledLockPartialInitialization(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	newC := cachetest.New(t, root, New)
	sha256Key := BlobKey{Content: "partial-sha256"}
	key := BlobKey{Content: "partial-sha384"}
	payload := []byte("payload")
	var id string
	for _, key := range []BlobKey{sha256Key, key} {
		_, _, storedID, err := newC.Blob(t.Context(), key, Policy{}, producer(payload, &ocispec.Descriptor{}))
		assert.NoError(t, err)

		id = storedID
	}
	stored, err := newC.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	stored.Digest = digest.SHA384.FromBytes(payload)
	assert.NoError(t, newC.store.Push(t.Context(), stored, bytes.NewReader(payload)))
	assert.NoError(t, newC.store.Tag(t.Context(), stored, id))
	assert.NoError(t, newC.Close())
	newC = cachetest.New(t, root, New)
	_, _, _, err = newC.Blob(t.Context(), sha256Key, Policy{MaxAge: Forever}, nil)
	assert.NoError(t, err)

	parent := newC.blobRoots[0].Root
	assert.FatalIf(t, parent == nil, "%s", "partial initialization did not open the first root")

	collision := filepath.Join(blobDir(), digest.SHA384.String())
	assert.NoError(t, newC.root.Rename(collision, collision+"-held"))
	testutil.WriteFile(t, root, collision, "not a directory")
	r, _, _, firstErr := newC.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
	assert.FatalIf(t, r != nil || !errors.Is(firstErr, manifest.ErrFlux) || newC.blobRoots[1].err == nil ||
		!errors.Is(firstErr, newC.blobRoots[1].err), "partial initialization: reader=%v, error=%v", r, firstErr)
	assert.NoError(t, newC.root.Remove(collision))
	assert.NoError(t, newC.root.Rename(collision+"-held", collision))
	{
		blobR, _, _, blobErr := newC.Blob(
			t.Context(),
			key,
			Policy{MaxAge: Forever},
			nil,
		)
		assert.FatalIf(t, blobR != nil || !errors.Is(blobErr, manifest.ErrFlux) ||
			!errors.Is(
				blobErr,
				newC.blobRoots[1].err,
			), "repaired filesystem changed initialization result: reader=%v, error=%v", blobR, blobErr)
	}
	r, _, _, err = newC.Blob(t.Context(), sha256Key, Policy{MaxAge: Forever}, nil)
	assert.NoError(t, err)

	data, err := fs.ReadFile(r, "content")
	assert.NoError(t, err)
	assert.Diff(t, data, payload)

	assertBlobRoots(t, newC, digest.SHA256)
	assert.NoError(t, newC.Close())
	{
		_, statErr2 := parent.Stat(".")
		assert.FatalIf(t, !errors.Is(statErr2, fs.ErrClosed), "last Close retained partial blob parent: %v", statErr2)
	}
	lock := flock.New(filepath.Join(root, ociLock))
	acquired, err := lock.TryLock()
	assert.NoError(t, lock.Close())
	assert.FatalIf(
		t,
		err != nil || !acquired,
		"last Close retained partial owner lock: acquired=%v, err=%v",
		acquired,
		err,
	)
	newC = cachetest.New(t, root, New)
	r, _, _, err = newC.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
	assert.NoError(t, err)

	data, err = fs.ReadFile(r, "content")
	assert.NoError(t, err)
	assert.Diff(t, data, payload)

	assertBlobRoots(t, newC, digest.SHA384)
	for i := range newC.blobRoots {
		parent := &newC.blobRoots[i]
		if parent.Root == nil {
			continue
		}
		{
			_, err := parent.Stat(".")
			assert.FatalIf(t, err != nil, "reopened parent: %v", err)
		}
	}
}

func testAcquireTagSaveFailureDoesNotBecomeHitOutcome(t *testing.T) {
	t.Helper()

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
	for range 2 {
		_, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "outcome-save"},
			Policy{MaxAge: Forever},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				calls++
				return ProducerResult{}, connReset()
			},
		)
		{
			_, ok := errors.AsType[*CacheError](
				err,
			)
			assert.FatalIf(t, !ok || !errors.Is(err, manifest.ErrFlux) ||
				!errors.Is(err, syscall.ECONNRESET), "publication cause=%v", err)
		}
		v, ok := c.entries.Load(
			manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"outcome-save"}}),
		)
		assert.FatalIf(t, !ok || v.(*entry).outcome != nil, "%s", "failed publication installed a durable hit")
	}
	assert.FatalIf(t, calls != 2, "uncommitted failure reused: calls=%d", calls)
}

func testDecodeRequiredMetadataCase(t *testing.T, family *struct {
	name     string
	required []string
	fields   map[string]string
},
) {
	t.Helper()

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
			testDecodeRequiredMetadataCaseCase(
				t,
				testDecodeRequiredMetadataCaseCaseInputs{family: &family, base: &base, check: &check},
			)
		},
		)
	}
	for _, field := range family.required {
		t.Run("disk missing "+field, func(t *testing.T) {
			t.Parallel()
			testDecodeRequiredMetadataCaseCase2(
				t,
				testDecodeRequiredMetadataCaseCase2Inputs{family: &family, field: &field},
			)
		},
		)
	}
}

func testAcquireCancellationKeepsContextErrorCase(
	t *testing.T, backing *string,
) {
	t.Helper()

	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	go func() { <-started; cancel() }()
	_, _, _, err := c.Acquire(
		ctx,
		ArchiveKey{Content: "source-cancel"},
		Policy{MaxAge: 0},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			var payloadTar bytes.Buffer
			testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
			if _, err := w.Write(payloadTar.Bytes()[:512]); err != nil {
				return ProducerResult{}, err
			}
			close(started)
			<-ctx.Done()
			time.Sleep(time.Millisecond)
			return ProducerResult{}, ctx.Err()
		},
	)
	assert.FatalIf(t, !errors.Is(err, context.Canceled), "cancellation identity: %v", err)
	key := ArchiveKey{Content: "independent-child"}
	_, _, id, err := c.Acquire(
		t.Context(),
		key,
		Policy{},
		producer(orderedArchive(t, "file", "same"), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)
	_, _, _, err = c.Acquire(
		t.Context(),
		key,
		Policy{},
		func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
			child, cancel := context.WithCancel(ctx)
			cancel()
			return ProducerResult{}, child.Err()
		},
	)
	assert.FatalIf(
		t,
		!errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux) || t.Context().Err() != nil,
		"independently canceled child: %v",
		err,
	)
	v, _ := c.entries.Load(id)
	{
		e := v.(*entry)
		assert.FatalIf(
			t,
			e.fs != nil || !e.resolved.IsZero(),
			"%s",
			"independently canceled child retained a successful entry",
		)
	}
}

func testAcquireCancellationKeepsContextErrorCompletedBlob(t *testing.T) {
	t.Helper()

	c := cachetest.New(t, "", New)
	key := BlobKey{Content: "source-completed-before-cancel"}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, _, _, err := c.Blob(
		ctx,
		key,
		Policy{MaxAge: 0},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			desc, err := producer([]byte("completed"), &ocispec.Descriptor{})(ctx, w)
			cancel()
			return desc, err
		},
	)
	assert.NoError(t, err)

	r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
	assert.NoError(t, err)

	_, err = fs.ReadFile(r, "content")
	assert.NoError(t, err)
}

type testAcquireConcurrentKeysAndCancellationCaseCallbackInputs struct {
	callers **int
}

func testAcquireConcurrentKeysAndCancellationCaseCallback(
	t *testing.T,
	testCaseInput testAcquireConcurrentKeysAndCancellationCaseCallbackInputs,
) {
	t.Helper()
	c := cachetest.New(t, t.TempDir(), New)
	key := BlobKey{Content: "due-canceled-winner"}
	var id string
	{
		_, _, _, err := c.Blob(
			t.Context(),
			key,
			Policy{},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				return ProducerResult{}, connReset()
			},
		)
		assert.FatalIf(t, err == nil, "%s", "want initial failure")
	}

	c.entries.Range(func(key, _ any) bool { id = key.(string); return false })
	time.Sleep(750 * time.Millisecond)
	unlock, err := c.locks.Acquire(t.Context(), id)
	assert.NoError(t, err)

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
		return producer([]byte("complete"), &ocispec.Descriptor{})(ctx, w)
	}

	done := make(chan testtestAcquireConcurrentKeysAndCancellationCaseCallbackResult, (*(*testCaseInput.callers)))
	launch := func(ctx context.Context, fill func(context.Context, ProducerInput) (ProducerResult, error)) {
		go func() {
			r, _, _, err := c.Blob(ctx, key, Policy{Retry: fastRetry(5)}, fill)
			var data string
			if r != nil {
				b, readErr := fs.ReadFile(r, "content")
				data = string(b)
				err = errors.Join(err, readErr)
			}
			done <- testtestAcquireConcurrentKeysAndCancellationCaseCallbackResult{err: err, data: data}
		}()
	}
	launch(winnerCtx, winnerFill)
	synctest.Wait()
	unlock()
	<-started
	for range (*(*testCaseInput.callers)) - 1 {
		launch(t.Context(), liveFill)
	}
	synctest.Wait()
	cancelWinner()
	winnerCanceled, liveResults := 0, 0
	for range *(*testCaseInput.callers) {
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
	if winnerCanceled != 1 || liveResults != (*(*testCaseInput.callers))-1 || calls.Load() != 2 {
		t.Fatalf(
			"winner canceled=%d, live=%d, producer calls=%d",
			winnerCanceled,
			liveResults,
			calls.Load(),
		)
	}
}

type testtestAcquireConcurrentKeysAndCancellationCaseCallbackResult struct {
	err  error
	data string
}
