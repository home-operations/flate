package cache

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testBlobRenderLifetimeCase(t *testing.T, identity *string,
) {
	t.Helper()
	processIdentity = (*identity)
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			testBlobRenderLifetimeCaseCase(
				t,
				testBlobRenderLifetimeCaseCaseInputs{identity: &identity, backing: &backing},
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
	metadata, err := c.RenderMetadata(t.Context(), "", "")

	{
		_, ok := errors.AsType[*CacheError](err)
		assert.FatalIf(
			t,
			!ok || !errors.Is(err, manifest.ErrInput) || metadata.Enabled,
			"invalid metadata content: enabled=%t, %v",
			metadata.Enabled,
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
	metadata2, metadataErr := c.RenderMetadata(canceled, "render", "")
	metadata.Enabled, err = metadata2.Enabled, metadataErr
	{
		_, ok := errors.AsType[*CacheError](
			err,
		)
		assert.FatalIf(t, !ok || !errors.Is(err, context.Canceled) ||
			metadata.Enabled != persisted, "canceled metadata lookup: enabled=%t, %v", metadata.Enabled, err)
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

type testBlobRenderLifetimeCaseCaseCompletionInputs struct {
	c         **Cache
	key       *BlobKey
	persisted *bool
	diskFiles *func() map[string]string
	before    *map[string]string
	root      *string
	raw       *BlobKey
}
