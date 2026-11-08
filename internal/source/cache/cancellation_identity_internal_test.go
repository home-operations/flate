package cache

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

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
		&Policy{MaxAge: 0},
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
		&Policy{},
		producer(orderedArchive(t, "file", "same"), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)
	_, _, _, err = c.Acquire(
		t.Context(),
		key,
		&Policy{},
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
		&Policy{MaxAge: 0},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			desc, err := producer([]byte("completed"), &ocispec.Descriptor{})(ctx, w)
			cancel()
			return desc, err
		},
	)
	assert.NoError(t, err)

	r, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
	assert.NoError(t, err)

	_, err = fs.ReadFile(r, "content")
	assert.NoError(t, err)
}
