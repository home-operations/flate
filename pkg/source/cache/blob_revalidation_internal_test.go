package cache

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testBlobRevalidationCase(t *testing.T, backing *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		root := ""
		if (*backing) == "disk" {
			root = t.TempDir()
		}
		c := cachetest.New(t, root, New)
		key := BlobKey{Content: "revalidated"}
		original := ocispec.Descriptor{Size: 7, Annotations: map[string]string{"validator": "opaque"}}
		r, _, id, err := c.Blob(t.Context(), key, &Policy{}, producer([]byte("payload"), &original))
		assert.NoError(t, err)

		view := NameBlob("content", r)
		identity := AcquisitionIdentity(view)

		loaded, _ := c.entries.Load(id)
		first := loaded.(*entry)
		resolved := first.resolved
		var before os.FileInfo
		if root != "" {
			before, err = c.root.Stat(blobPath(first.stored.Digest))
			assert.NoError(t, err)
			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
		}
		time.Sleep(time.Second)
		r, desc, gotID, err := c.Blob(
			t.Context(),
			key,
			&Policy{},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				previous, previousErr2 := w.Previous(ctx)
				assert.FatalIf(
					t,
					previousErr2 != nil || previous == nil || previous.Annotations["validator"] != "opaque",
					"previous=%+v, error=%v",
					previous,
					previousErr2,
				)
				previous.Annotations["validator"] = "mutated"
				independent, previousErr2 := w.Previous(ctx)
				assert.FatalIf(
					t,
					previousErr2 != nil || independent.Annotations["validator"] != "opaque",
					"metadata alias: %+v, %v",
					independent,
					previousErr2,
				)
				previous.Annotations = map[string]string{"validator": "updated"}
				return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
			},
		)
		assert.NoError(t, err)

		assert.FatalIf(t, gotID != id || AcquisitionIdentity(NameBlob("content", r)) != identity ||
			desc.Annotations["validator"] != "updated", "unchanged id=%q, descriptor=%+v", gotID, desc)
		body, readErr := fs.ReadFile(r, "content")
		assert.NoError(t, readErr)

		assert.Equal(t, string(body), "payload")
		loaded, _ = c.entries.Load(id)
		next := loaded.(*entry)
		assert.FatalIf(t, !next.resolved.After(resolved), "%s", "validation did not renew resolution time")

		if root != "" {
			after, statErr := c.root.Stat(blobPath(next.stored.Digest))
			assert.NoError(t, statErr)
			assert.FatalIf(
				t,
				!os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()),
				"%s",
				"unchanged validation rewrote payload",
			)

			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
		}
		_, desc, _, err = c.Blob(t.Context(), key, &Policy{MaxAge: time.Second}, nil)
		assert.NoError(t, err)

		assert.FatalIf(
			t,
			desc.Annotations["validator"] != "updated",
			"validation metadata not persisted: %+v",
			desc,
		)
		loaded, _ = c.entries.Load(id)
		assert.FatalIf(t, !loaded.(*entry).resolved.Equal(next.resolved), "%s", "resolution time not persisted")
	})
}

func testBlobRevalidationAbsent(t *testing.T) {
	t.Helper()

	c := cachetest.New(t, "", New)
	r, _, _, err := c.Blob(
		t.Context(),
		BlobKey{Content: "absent"},
		&Policy{},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			return ProducerResult{Unchanged: true}, nil
		},
	)
	assert.FatalIf(t, r != nil || !errors.Is(err, manifest.ErrInput), "unchanged without payload: %v", err)
}

func testBlobRevalidationUnchangedParentReplacement(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "unchanged-parent-replacement"}
	upstream := ocispec.Descriptor{
		Digest:      digest.FromString("upstream"),
		Size:        7,
		Annotations: map[string]string{"validator": "opaque"},
	}
	r, _, id, err := c.Blob(t.Context(), key, &Policy{}, producer([]byte("payload"), &upstream))
	assert.NoError(t, err)
	identity := AcquisitionIdentity(NameBlob("content", r))

	stored, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	r, desc, gotID, err := c.Blob(
		t.Context(),
		key,
		&Policy{},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			previous, previousErr3 := w.Previous(ctx)
			assert.FatalIf(t, previousErr3 != nil || previous == nil, "previous=%+v, error=%v", previous, previousErr3)
			return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
		},
	)
	assert.NoError(t, err)
	assert.Diff(t, desc, upstream)
	assert.Equal(t, gotID, id)
	view := NameBlob("content", r)
	assert.Equal(t, AcquisitionIdentity(view), identity)
	body, readErr := fs.ReadFile(r, "content")
	assert.NoError(t, readErr)

	assert.Equal(t, string(body), "payload")
	current, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)
	assert.Equal(t, current.Digest, stored.Digest)
	parent := filepath.Dir(blobPath(stored.Digest))
	assert.NoError(t, c.root.Rename(parent, parent+"-held"))
	assert.NoError(t, c.root.Mkdir(parent, 0o750))
	{
		body, err := fs.ReadFile(view, "content")
		assert.FatalIf(
			t,
			!errors.Is(err, fs.ErrNotExist),
			"unchanged view ignored replacement: body=%q, error=%v",
			body,
			err,
		)
	}
	assert.NoError(t, c.root.Remove(parent))
	outside := t.TempDir()
	testutil.WriteFile(t, outside, stored.Digest.Encoded(), "outside")
	assert.NoError(t, os.Symlink(outside, filepath.Join(root, parent)))
	{
		body, err := fs.ReadFile(view, "content")
		assert.FatalIf(t, err == nil, "unchanged view accepted escaping parent: body=%q", body)
	}
}

func testBlobRevalidationCase2(t *testing.T, invalid *string,
) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "missing"}
	_, _, id, err := c.Blob(t.Context(), key, &Policy{}, producer([]byte("old"), &ocispec.Descriptor{}))
	assert.NoError(t, err)

	e, _ := c.entries.Load(id)
	path := blobPath(e.(*entry).stored.Digest)
	assert.NoError(t, c.root.Remove(path))
	if (*invalid) == "corrupted-size" {
		testutil.WriteFile(t, root, path, "truncated")
	}
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	r, _, _, err := c.Blob(
		t.Context(),
		key,
		&Policy{},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			previous, previousErr4 := w.Previous(ctx)
			assert.FatalIf(
				t,
				previousErr4 != nil || previous != nil,
				"invalid previous=%+v, error=%v",
				previous,
				previousErr4,
			)
			_, previousErr4 = io.WriteString(w, "old")
			return ProducerResult{}, previousErr4
		},
	)
	assert.NoError(t, err)
	body, readErr := fs.ReadFile(r, "content")
	assert.NoError(t, readErr)

	assert.Equal(t, string(body), "old")
	info, err := c.root.Stat(path)
	assert.NoError(t, err)
	assert.Equal(t, info.Mode().Perm(), fs.FileMode(0o444))
	entries, err := os.ReadDir(filepath.Join(root, ociDir))
	assert.NoError(t, err)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "acquire-") {
			t.Errorf("successful repair left acquisition name %q", entry.Name())
		}
	}
}

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
		&Policy{},
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
		&Policy{},
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
				&Policy{MaxAge: Forever},
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
		&Policy{},
		producer([]byte("complete"), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)
	body, err = fs.ReadFile(r, "content")
	assert.NoError(t, err)

	assert.Equal(t, string(body), "complete")
}
