package cache

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func testRenderMetadataCurrentDirectoryCase(t *testing.T, name *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	key := BlobKey{Content: "current-directory", Render: RenderAccepted}
	payload := []byte("current-directory-payload")
	r, _, lookup, err := c.Blob(t.Context(), key, &Policy{}, producer(payload, &ocispec.Descriptor{}))
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
	renderMetadataResult, err := c.RenderMetadata(ctx, "current-directory", "", nil)
	current, enabled := renderMetadataResult.Identity, renderMetadataResult.Enabled
	if (*name) == "replaced parent present" {
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			!enabled || current == "" || current != identity,
			"%s",
			"replaced current directory lost the published generation",
		)

		r, _, _, blobErr12 := c.Blob(ctx, key, &Policy{MaxAge: Forever}, nil)
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
