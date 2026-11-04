package cache

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/gofrs/flock"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

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
