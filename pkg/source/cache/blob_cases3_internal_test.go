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
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testBlobRevalidationFailuresCase(t *testing.T, disk *bool,
	failure *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testBlobRevalidationFailuresCaseCallback(
			t,
			testBlobRevalidationFailuresCaseCallbackInputs{disk: &disk, failure: &failure},
		)
	},
	)
}

func testRenderMetadataCurrentDirectoryCase(t *testing.T, name *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	key := BlobKey{Content: "current-directory", Render: RenderAccepted}
	payload := []byte("current-directory-payload")
	r, _, lookup, err := c.Blob(t.Context(), key, Policy{}, producer(payload, &ocispec.Descriptor{}))
	assert.NoError(t, err)
	identity := AcquisitionIdentity(r)

	stored, _ := c.entries.Load(lookup)
	path := blobPath(stored.(*entry).stored.Digest)
	parent := filepath.Dir(path)
	ctx := t.Context()
	switch *name {
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
		if (*name) == "replaced parent present" {
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
	if (*name) == "replaced parent present" {
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			!enabled || current == "" || current != identity,
			"%s",
			"replaced current directory lost the published generation",
		)

		r, _, _, blobErr12 := c.Blob(ctx, key, Policy{MaxAge: Forever}, nil)
		assert.NoError(t, blobErr12)
		data, blobErr12 := fs.ReadFile(r, "content")
		assert.NoError(t, blobErr12)
		assert.Equal(t, bytes.Equal(data, payload), true)
		return
	}
	assert.FatalIf(t, err == nil || current != "", "%s", "invalid current directory authorized a metadata hit")

	{
		_, ok := errors.AsType[*CacheError](err)
		assert.FatalIf(t, !ok, "metadata failure lost cache attribution: %v", err)
	}
	assert.FatalIf(
		t, (*name) ==
			"canceled" && !errors.Is(err, context.Canceled), "metadata failure lost cancellation: %v",
		err,
	)
}

func testBlobAcquisitionWavesCase(t *testing.T, backing *string,
) {
	t.Helper()

	for _, failed := range []bool{false, true} {
		t.Run(strconv.FormatBool(failed), func(t *testing.T) {
			t.Parallel()
			testBlobAcquisitionWavesCaseCase(
				t,
				testBlobAcquisitionWavesCaseCaseInputs{backing: &backing, failed: &failed},
			)
		},
		)
	}
}

type testBlobRenderLifetimeCaseCaseInputs struct {
	identity **string
	backing  *string
}

func testBlobRenderLifetimeCaseCase(t *testing.T, testCaseInput testBlobRenderLifetimeCaseCaseInputs) {
	t.Helper()
	root := ""
	if (*testCaseInput.backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	persisted := (*testCaseInput.backing) == "disk" && (*(*testCaseInput.identity)) != ""
	assert.Equal(t, c.CanPersistRenders(), persisted)
	for _, invalid := range []BlobKey{
		{Content: "render", Render: RenderMode(255)},
		{Content: 1, Render: RenderAccepted},
		{Content: "", Render: RenderAccepted},
	} {
		_, _, _, err := c.Blob(t.Context(), invalid, Policy{MaxAge: Forever}, nil)
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.FatalIf(
				t,
				!ok || !errors.Is(err, manifest.ErrInput),
				"invalid render key %+v: %v",
				invalid,
				err,
			)
		}
	}
	_, _, enabled, err := c.RenderMetadata(t.Context(), "", "")
	{
		_, ok := errors.AsType[*CacheError](err)
		assert.FatalIf(
			t,
			!ok || !errors.Is(err, manifest.ErrInput) || enabled,
			"invalid metadata content: enabled=%t, %v",
			enabled,
			err,
		)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, _, err = c.Blob(
		canceled,
		BlobKey{Content: "render", Render: RenderAccepted},
		Policy{MaxAge: Forever},
		nil,
	)
	{
		_, ok := errors.AsType[*CacheError](err)
		assert.FatalIf(t, !ok || !errors.Is(err, context.Canceled), "canceled render lookup: %v", err)
	}
	_, _, enabled, err = c.RenderMetadata(canceled, "render", "")
	{
		_, ok := errors.AsType[*CacheError](
			err,
		)
		assert.FatalIf(t, !ok || !errors.Is(err, context.Canceled) ||
			enabled != persisted, "canceled metadata lookup: enabled=%t, %v", enabled, err)
	}
	assert.Equal(t, c.CanPersistRenders(), persisted)
	raw := BlobKey{Content: "raw-source"}
	_, _, _, err = c.Blob(
		t.Context(),
		raw,
		Policy{MaxAge: Forever},
		producer([]byte("source"), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	diskFiles := func() map[string]string {
		files := map[string]string{}
		if root != "" {
			assert.NoError(
				t,
				fs.WalkDir(os.DirFS(root), ".", func(name string, d fs.DirEntry, err error) error {
					if err != nil || d.IsDir() {
						return err
					}
					data, err := os.ReadFile(filepath.Join(root, name))
					files[name] = string(data)
					return err
				}),
			)
		}
		return files
	}
	before := diskFiles()
	key := BlobKey{Content: "render-test-v1-payload", Render: RenderPending}
	if (*testCaseInput.backing) == "disk" && (*(*testCaseInput.identity)) != "" {
		key.Render = RenderAccepted
	}
	testBlobRenderLifetimeCaseCaseCompletion(
		t,
		&testBlobRenderLifetimeCaseCaseCompletionInputs{
			c:         &c,
			key:       &key,
			persisted: &persisted,
			diskFiles: &diskFiles,
			before:    &before,
			root:      &root,
			raw:       &raw,
		},
	)
}

func testBlobRenderLifetimeCaseCaseCompletion(t *testing.T, captures *testBlobRenderLifetimeCaseCaseCompletionInputs) {
	t.Helper()
	for _, content := range []any{"", struct{ Key string }{"render-test-v1-payload"}} {
		{
			r, _, id, blobErr9 := (*captures.c).Blob(
				t.Context(),
				BlobKey{Content: content, Render: RenderPending},
				Policy{MaxAge: Forever},
				nil,
			)
			assert.FatalIf(t, r != nil || id != "" ||
				!errors.Is(blobErr9, manifest.ErrInput), "invalid render identity: id=%q, %v", id, blobErr9)
		}
	}
	{
		r, _, _, blobErr10 := (*captures.c).Blob(
			t.Context(), (*captures.key), Policy{MaxAge: Forever},
			nil,
		)
		assert.FatalIf(t, r != nil ||
			!errors.Is(blobErr10, errdef.ErrNotFound), "cold lookup: %v", blobErr10)
	}
	calls := 0
	produce := func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
		calls++
		previous, previousErr := w.Previous(ctx)
		assert.NoError(t, previousErr)
		if calls > 1 {
			assert.FatalIf(t, previous == nil, "%s", "memory generation lost previous payload")

			return ProducerResult{Unchanged: true}, nil
		}
		_, previousErr = io.WriteString(w, "completed")
		return ProducerResult{}, previousErr
	}
	for _, age := range []time.Duration{Forever, Forever, 0} {
		r, _, _, blobErr11 := (*captures.c).Blob(t.Context(), (*captures.key), Policy{MaxAge: age}, produce)
		assert.NoError(t, blobErr11)
		data, _ := fs.ReadFile(r, "content")

		assert.Equal(t, string(data), "completed")
	}
	assert.Equal(t, calls, 2)
	if !(*captures.persisted) {
		assert.Diff(t, (*captures.diskFiles)(), (*captures.before))
	}
	assert.NoError(t, (*captures.c).Close())
	(*captures.c) = cachetest.New(t, (*captures.root), New)
	r, _, _, err := (*captures.c).Blob(t.Context(), (*captures.key), Policy{MaxAge: Forever}, nil)
	if *captures.persisted {
		assert.NoError(t, err)
		data, _ := fs.ReadFile(r, "content")

		assert.Equal(t, string(data), "completed")
	} else {
		assert.FatalIf(
			t,
			r != nil || !errors.Is(err, errdef.ErrNotFound),
			"reopened transient render: %v",
			err,
		)
	}
	r, _, _, err = (*captures.c).Blob(t.Context(), (*captures.raw), Policy{MaxAge: Forever}, nil)
	if (*captures.root) != "" {
		assert.NoError(t, err)
		data, _ := fs.ReadFile(r, "content")

		assert.Equal(t, string(data), "source")
	} else {
		assert.FatalIf(t, !errors.Is(err, errdef.ErrNotFound), "reopened memory source: %v", err)
	}
}

type testBlobAcquisitionWavesCaseCaseInputs struct {
	backing **string
	failed  *bool
}

func testBlobAcquisitionWavesCaseCase(t *testing.T, testCaseInput testBlobAcquisitionWavesCaseCaseInputs) {
	t.Helper()

	root := ""
	if (*(*testCaseInput.backing)) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	synctest.Test(t, func(t *testing.T) {
		testBlobAcquisitionWavesCaseCaseCallback(
			t,
			testBlobAcquisitionWavesCaseCaseCallbackInputs{testCaseInput: &testCaseInput, c: &c},
		)
	},
	)
}

type testBlobRevalidationFailuresCaseCallbackInputs struct {
	disk    **bool
	failure **string
}

func testBlobRevalidationFailuresCaseCallback(
	t *testing.T,
	testCaseInput testBlobRevalidationFailuresCaseCallbackInputs,
) {
	t.Helper()
	root := ""
	if *(*testCaseInput.disk) {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "failed-validation"}
	r, _, id, err := c.Blob(
		t.Context(),
		key,
		Policy{},
		producer(
			[]byte("payload"), &ocispec.Descriptor{Annotations: map[string]string{"validator": "original"}},
		),
	)
	assert.NoError(t, err)
	view := NameBlob("content", r)

	var index []byte
	r, _, _, err = c.Blob(
		t.Context(),
		key,
		Policy{},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			previous, previousErr5 := w.Previous(ctx)
			assert.FatalIf(
				t,
				previousErr5 != nil || previous == nil,
				"previous=%+v, error=%v",
				previous,
				previousErr5,
			)
			switch *(*testCaseInput.failure) {
			case "producer":
				return ProducerResult{Descriptor: *previous, Unchanged: true}, fs.ErrPermission
			case "outcome":
				return ProducerResult{}, connReset()
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				_, previousErr5 = w.Previous(canceled)
				return ProducerResult{Descriptor: *previous, Unchanged: true}, previousErr5
			case "partial":
				_, _ = io.WriteString(w, "partial")
			case "publication":
				index, previousErr5 = c.root.ReadFile("oci/index.json")
				assert.NoError(t, previousErr5)
				assert.NoError(t, c.root.Remove("oci/index.json"))
				assert.NoError(t, c.root.Mkdir("oci/index.json", 0o750))
			}
			return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
		},
	)
	assert.FatalIf(
		t,
		err == nil || r != nil || !errors.Is(err, manifest.ErrFlux),
		"failed validation reader=%v, error=%v",
		r,
		err,
	)
	loaded, _ := c.entries.Load(id)
	assert.FatalIf(t, !loaded.(*entry).resolved.IsZero(), "%s", "failed validation granted freshness")

	body, err := fs.ReadFile(view, "content")
	assert.FatalIf(t, err != nil || string(body) != "payload", "held payload=%q, error=%v", body, err)
	if (*(*testCaseInput.failure)) == "publication" {
		assert.NoError(t, c.root.Remove("oci/index.json"))
		assert.NoError(t, c.root.WriteFile("oci/index.json", index, 0o600))
	}
	if (*(*testCaseInput.failure)) == "outcome" && (*(*testCaseInput.disk)) {
		outcome := loaded.(*entry).outcome
		assert.FatalIf(
			t,
			outcome == nil || outcome.lastGood.Digest == "",
			"last-good outcome=%+v",
			outcome,
		)
		{
			blobR, _, _, blobErr := c.Blob(
				t.Context(),
				key,
				Policy{MaxAge: Forever},
				nil,
			)
			assert.FatalIf(t, blobR != nil ||
				!errors.Is(blobErr, syscall.ECONNRESET), "failure replay=%v", blobErr)
		}
		time.Sleep(time.Second)
	}
	r, _, _, err = c.Blob(
		t.Context(),
		key,
		Policy{},
		producer([]byte("complete"), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)
	body, err = fs.ReadFile(r, "content")
	assert.NoError(t, err)

	assert.Equal(t, string(body), "complete")
}

type testBlobAcquisitionWavesCaseCaseCallbackInputs struct {
	testCaseInput *testBlobAcquisitionWavesCaseCaseInputs
	c             **Cache
}

func testBlobAcquisitionWavesCaseCaseCallback(
	t *testing.T,
	testCaseInput testBlobAcquisitionWavesCaseCaseCallbackInputs,
) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	cause := errors.New("acquisition failure")
	produce := func(_ context.Context, w ProducerInput) (ProducerResult, error) {
		call := calls.Add(1)
		if call == 1 {
			close(entered)
			<-release
		}
		if *testCaseInput.testCaseInput.failed {
			return ProducerResult{}, cause
		}
		_, err := fmt.Fprint(w, call)
		return ProducerResult{}, err
	}
	key := BlobKey{Content: "acquisition-wave"}
	var generations [2]fs.FS
	var failures [2]error
	var wg sync.WaitGroup
	wg.Go(
		func() {
			generations[0], _, _, failures[0] = (*testCaseInput.c).Blob(t.Context(), key, Policy{}, produce)
		},
	)
	<-entered
	wg.Go(
		func() {
			generations[1], _, _, failures[1] = (*testCaseInput.c).Blob(t.Context(), key, Policy{}, produce)
		},
	)
	synctest.Wait()
	close(release)
	wg.Wait()
	assert.Equal(t, calls.Load(), int64(1))
	for i, generation := range generations {
		if *testCaseInput.testCaseInput.failed {
			if generation != nil || !errors.Is(failures[i], cause) {
				t.Fatalf("caller %d: generation=%v error=%v", i, generation, failures[i])
			}
			continue
		}
		assert.NoError(t, failures[i])
		body, err := fs.ReadFile(generation, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(body), "1")
	}
	generation, _, _, err := (*testCaseInput.c).Blob(t.Context(), key, Policy{}, produce)
	assert.Equal(t, calls.Load(), int64(2))
	if *testCaseInput.testCaseInput.failed {
		assert.FatalIf(
			t,
			generation != nil || !errors.Is(err, cause),
			"later caller: generation=%v error=%v",
			generation,
			err,
		)
		return
	}
	assert.NoError(t, err)
	body, err := fs.ReadFile(generation, "content")
	assert.NoError(t, err)
	assert.Equal(t, string(body), "2")
	body, err = fs.ReadFile(generations[0], "content")
	assert.NoError(t, err)
	assert.Equal(t, string(body), "1")
}

type testBlobRenderLifetimeCaseCaseCompletionInputs struct {
	c         **Cache
	key       *BlobKey
	persisted *bool
	diskFiles *func() map[string]string
	before    *map[string]string
	root      *string
	raw       *BlobKey
}
