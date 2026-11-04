package cache

import (
	"errors"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
)

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
