package cache

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderFrame_Rebind(t *testing.T) {
	t.Parallel()
	original := RenderFrame{Key: "render", Lookup: "runtime", Binding: "binding", Output: []byte("output")}
	stored, err := original.Encode("domain", "metadata")
	assert.NoError(t, err)
	original.Stored = stored
	current := original
	current.Rebind("binding")
	assert.Equal(t, current.Stored, stored)
	current.Rebind("new-binding")
	assert.Equal(t, current.Stored == nil, true)
	assert.Equal(t, original.Stored, stored)
	assert.Equal(t, original.Binding, "binding")
	assert.Equal(t, current.Binding, "new-binding")
	assert.Equal(t, current.Lookup, "runtime")
}

func TestRenderFrame_Encode(t *testing.T) {
	t.Parallel()
	frame := RenderFrame{Key: "render", Binding: "binding", Output: []byte("output")}
	stored, err := frame.Encode("domain", "metadata")
	assert.NoError(t, err)
	frame.Stored = stored
	again, err := frame.Encode("domain", make(chan int))
	assert.NoError(t, err)
	assert.Equal(t, again, stored)
	frame.Rebind("new-binding")
	again, err = frame.Encode("domain", "metadata")
	assert.NoError(t, err)
	assert.Equal(t, again == stored, false)
}

func TestOpenRenderFrame_Metadata(t *testing.T) {
	t.Parallel()
	for _, domain := range []string{"helm-output", "kustomize-render"} {
		t.Run(domain, func(t *testing.T) {
			t.Parallel()
			metadata := struct{ Value string }{Value: "engine"}
			frame := RenderFrame{Key: "render", Binding: "binding", Lookup: "runtime-only", Output: []byte("output")}
			stored, err := frame.Encode(domain, metadata)
			assert.NoError(t, err)
			generation := frameGeneration(frameBytes(t, stored, FramePublicationInput{}))
			var decoded struct{ Value string }
			opened, err := OpenRenderFrame(t.Context(), domain, generation, &decoded)
			assert.NoError(t, err)
			assert.Equal(t, decoded, metadata)
			assert.Equal(t, opened.Key, frame.Key)
			assert.Equal(t, opened.Binding, frame.Binding)
			assert.Equal(t, opened.Lookup, "")
			assert.Diff(t, opened.Output, frame.Output)
			assert.Equal(t, opened.Stored != nil, true)
			_, err = OpenRenderFrame(t.Context(), "foreign", generation, &decoded)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			var wrong int
			_, err = OpenRenderFrame(t.Context(), domain, generation, &wrong)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			_, err = OpenRenderFrame(t.Context(), domain, fstest.MapFS{}, &decoded)
			_, storage := errors.AsType[*Error](err)
			assert.Equal(t, storage, true)
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		})
	}
}
