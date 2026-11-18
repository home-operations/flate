package kustomize

import (
	"context"
	"testing"
	"testing/fstest"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPreparation_RenderGeneration(t *testing.T) {
	t.Parallel()
	cache := newTestTreeCache(t, "")
	var preparation Preparation
	t.Cleanup(func() { preparation = Preparation{} })
	var previous *byte
	for _, value := range []string{"first", "second"} {
		output := testRenderEncoding(t, []map[string]any{{"value": value}})
		wire, err := testEncodeFrame(
			t,
			&renderFrame{Key: "render", Output: output},
		)
		assert.NoError(t, err)
		generation, _, _, err := cache.owner.Blob(
			t.Context(),
			sourcecache.BlobKey{Content: "frame"},
			&sourcecache.Policy{},
			func(_ context.Context, out sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				_, writeErr := out.Write(wire)
				return sourcecache.ProducerResult{}, writeErr
			},
		)
		assert.NoError(t, err)
		first, valid, err := preparation.readFrame(t.Context(), generation)
		assert.NoError(t, err)
		assert.Equal(t, valid, true)
		second, valid, err := preparation.readFrame(t.Context(), generation)
		assert.NoError(t, err)
		assert.Equal(t, valid, true)
		assert.Equal(t, &first.Output[0] == &second.Output[0], true)
		assert.Equal(t, &first.Output[0] == previous, false)
		previous = &first.Output[0]
		left, err := manifest.DecodeRender(first.Output)
		assert.NoError(t, err)
		right, err := manifest.DecodeRender(second.Output)
		assert.NoError(t, err)
		left[0].Document["value"] = "changed"
		assert.Equal(t, right[0].Document["value"], any(value))
		anonymous := fstest.MapFS{"content": &fstest.MapFile{Data: wire}}
		independent, valid, err := preparation.readFrame(t.Context(), anonymous)
		assert.NoError(t, err)
		assert.Equal(t, valid, true)
		assert.Equal(t, &independent.Output[0] == &first.Output[0], false)
	}
	preparation.cache, preparation.complete = cache, true
	var next Preparation
	assert.Equal(t, preparation.Matches(cache, &RenderRequest{}), true)
	next, preparation = preparation, Preparation{}
	assert.Equal(t, preparation.decoded == nil, true)
	assert.Equal(t, &next.decoded.frame.Output[0] == previous, true)
	next = Preparation{}
	assert.Equal(t, next.decoded == nil, true)
}
