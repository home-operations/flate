package cache

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

type compositionOpenFS struct {
	fs.FS

	open func(string) (fs.File, error)
}

func (f compositionOpenFS) Open(name string) (fs.File, error) { return f.open(name) }

func TestCompose_BaseCancellationDoesNotPublish(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", New)
	files := fstest.MapFS{"base": {Data: []byte("bytes"), Mode: 0o755}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	base := compositionOpenFS{FS: files, open: func(name string) (fs.File, error) {
		if name == "base" {
			cancel()
		}
		return files.Open(name)
	}}
	request := Composition{Base: CompositionInput{FS: base, Content: "generation"}, Policy: "source"}
	output, _, key, err := owner.Compose(ctx, request, &Policy{MaxAge: Forever})
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, output == nil, true)
	assert.Equal(t, key, "")
	request.Base.FS = files
	output, _, _, err = owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
	assert.NoError(t, err)
	assert.Diff(t, compositionFiles(t, output), map[string]compositionFile{"base": {Data: "bytes", Mode: 0o700}})
	assert.Equal(t, string(files["base"].Data), "bytes")
	assert.Equal(t, files["base"].Mode, fs.FileMode(0o755))
}
