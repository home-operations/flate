package cache

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func TestBlob_EmptyRenderPreservesGeneration(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name string
		mode RenderMode
	}{{"pending", RenderPending}, {"accepted", RenderAccepted}} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			if scenario.mode == RenderAccepted && !c.CanPersistRenders() {
				t.Skip("requires a clean VCS-stamped build for positive persistence")
			}
			frame, err := NewFrame("test-render", "metadata", []byte("valid output"))
			assert.NoError(t, err)
			publication, err := frame.PreparePublication(t.Context(), FramePublicationInput{})
			assert.NoError(t, err)
			key := BlobKey{Content: "existing-render", Render: scenario.mode}
			prior, _, _, err := c.Blob(
				t.Context(),
				key,
				&Policy{},
				func(_ context.Context, out ProducerInput) (ProducerResult, error) {
					_, writeErr := publication.WriteTo(out)
					return ProducerResult{}, writeErr
				},
			)
			assert.NoError(t, err)
			before, err := fs.ReadFile(prior, "content")
			assert.NoError(t, err)
			generation, _, identity, err := c.Blob(
				t.Context(),
				key,
				&Policy{},
				func(context.Context, ProducerInput) (ProducerResult, error) { return ProducerResult{}, nil },
			)
			assert.NoError(t, err)
			assert.Equal(t, generation == nil, true)
			assert.Equal(t, identity, "")
			current, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
			assert.NoError(t, err)
			after, err := fs.ReadFile(current, "content")
			assert.NoError(t, err)
			assert.Diff(t, after, before)
			generation, _, _, err = c.Blob(
				t.Context(),
				key,
				&Policy{},
				func(ctx context.Context, out ProducerInput) (ProducerResult, error) {
					previous, readErr := out.Previous(ctx)
					assert.Equal(t, previous != nil, true)
					return ProducerResult{Unchanged: true}, readErr
				},
			)
			assert.NoError(t, err)
			after, err = fs.ReadFile(generation, "content")
			assert.NoError(t, err)
			assert.Diff(t, after, before)
			cancelKey := key
			cancelKey.Validate = func(ctx context.Context, _ fs.FS) (bool, error) {
				return ctx.Err() == nil, ctx.Err()
			}
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			_, _, _, err = c.Blob(
				ctx,
				cancelKey,
				&Policy{},
				func(context.Context, ProducerInput) (ProducerResult, error) {
					cancel()
					return ProducerResult{}, nil
				},
			)
			assert.Equal(t, errors.Is(err, context.Canceled), true)
			current, _, _, err = c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
			assert.NoError(t, err)
			after, err = fs.ReadFile(current, "content")
			assert.NoError(t, err)
			assert.Diff(t, after, before)
			key.Content = "unavailable-render"
			generation, _, identity, err = c.Blob(
				t.Context(),
				key,
				&Policy{},
				func(context.Context, ProducerInput) (ProducerResult, error) { return ProducerResult{}, nil },
			)
			assert.NoError(t, err)
			assert.Equal(t, generation == nil, true)
			assert.Equal(t, identity, "")
			_, _, _, err = c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
			assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
		})
	}
	t.Run("ordinary empty blob", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, "", New)
		generation, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "empty"},
			&Policy{},
			func(context.Context, ProducerInput) (ProducerResult, error) { return ProducerResult{}, nil },
		)
		assert.NoError(t, err)
		data, err := fs.ReadFile(generation, "content")
		assert.NoError(t, err)
		assert.Equal(t, len(data), 0)
	})
}
