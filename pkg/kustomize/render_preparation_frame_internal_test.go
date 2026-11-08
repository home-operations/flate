package kustomize

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestPreparation_RenderGeneration(t *testing.T) {
	t.Parallel()
	cache := newTestTreeCache(t, "")
	var preparation Preparation
	t.Cleanup(preparation.Retire)
	var previous *byte
	for _, value := range []string{"first", "second"} {
		output := testRenderEncoding(t, []map[string]any{{"value": value}})
		wire, err := encodeFrame(&renderHeader{Key: "render"}, output)
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
		first, valid, err := preparation.readFrame(generation)
		assert.NoError(t, err)
		assert.Equal(t, valid, true)
		second, valid, err := preparation.readFrame(generation)
		assert.NoError(t, err)
		assert.Equal(t, valid, true)
		assert.Equal(t, &first.output[0] == &second.output[0], true)
		assert.Equal(t, &first.output[0] == previous, false)
		previous = &first.output[0]
		left, err := manifest.DecodeRender(first.output)
		assert.NoError(t, err)
		right, err := manifest.DecodeRender(second.output)
		assert.NoError(t, err)
		left[0].Document["value"] = "changed"
		assert.Equal(t, right[0].Document["value"], any(value))
		anonymous := fstest.MapFS{"content": &fstest.MapFile{Data: wire}}
		independent, valid, err := preparation.readFrame(anonymous)
		assert.NoError(t, err)
		assert.Equal(t, valid, true)
		assert.Equal(t, &independent.output[0] == &first.output[0], false)
	}
	preparation.cache, preparation.complete = cache, true
	var next Preparation
	assert.Equal(t, next.Take(&preparation, cache, &RenderRequest{}), true)
	assert.Equal(t, preparation.decoded == nil, true)
	assert.Equal(t, &next.decoded.frame.output[0] == previous, true)
	next.Retire()
	assert.Equal(t, next.decoded == nil, true)
}
