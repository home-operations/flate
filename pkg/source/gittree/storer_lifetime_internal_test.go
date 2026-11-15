package gittree

import (
	"errors"
	"io"
	"io/fs"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"weak"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/home-operations/flate/internal/assert"
)

type blobReaderStorer struct {
	storer.EncodedObjectStorer

	object plumbing.EncodedObject
}

func (s blobReaderStorer) EncodedObject(plumbing.ObjectType, plumbing.Hash) (plumbing.EncodedObject, error) {
	return s.object, nil
}

type blobReaderObject struct {
	*plumbing.MemoryObject

	reader io.ReadCloser
	err    error
}

func (o blobReaderObject) Reader() (io.ReadCloser, error) { return o.reader, o.err }

func TestGitView_BlobErrors(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"lookup", "reader"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			var expected io.ReadCloser
			var storage storer.EncodedObjectStorer = memory.NewStorage()
			want, operation := plumbing.ErrObjectNotFound, "read blob "
			if failure == "reader" {
				expected = io.NopCloser(strings.NewReader("retained reader"))
				object := &plumbing.MemoryObject{}
				object.SetType(plumbing.BlobObject)
				storage = blobReaderStorer{object: blobReaderObject{
					MemoryObject: object, reader: expected, err: fs.ErrPermission,
				}}
				want, operation = fs.ErrPermission, "open blob reader "
			}
			view := &gitView{newStorer: func() storer.EncodedObjectStorer { return storage }}
			reader, err := view.openBlob(plumbing.ZeroHash)
			assert.Equal(t, reader, expected)
			assert.Equal(t, errors.Is(err, want), true)
			assert.Equal(t, strings.HasPrefix(err.Error(), operation), true)
			assert.Equal(t, view.storer.Load() != nil, true)
			if reader != nil {
				assert.NoError(t, reader.Close())
			}
		})
	}
}

func TestGitView_StorerLifetime(t *testing.T) {
	t.Parallel()
	var created atomic.Int32
	g := &gitView{newStorer: func() storer.EncodedObjectStorer {
		created.Add(1)
		return memory.NewStorage()
	}}
	first := g.borrowStorer()
	captured := make(chan weak.Pointer[memory.Storage])
	release, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		second := g.borrowStorer()
		defer g.releaseStorer(second)
		assert.Equal(t, first.storage == second.storage, false)
		value, ok := second.storage.(*memory.Storage)
		assert.Equal(t, ok, true)
		captured <- weak.Make(value)
		<-release
	}()
	surplus := <-captured
	assert.Equal(t, created.Load(), int32(2))
	g.releaseStorer(first)
	close(release)
	<-done
	runtime.GC()
	assert.Equal(t, surplus.Value() == nil, true)
	reused := g.borrowStorer()
	assert.Equal(t, reused.storage, first.storage)
	assert.Equal(t, created.Load(), int32(2))
}

func TestGitView_IdleStorerCollection(t *testing.T) {
	t.Parallel()
	var created atomic.Int32
	g := &gitView{newStorer: func() storer.EncodedObjectStorer {
		created.Add(1)
		return memory.NewStorage()
	}}
	idle := func() weak.Pointer[memory.Storage] {
		borrowed := g.borrowStorer()
		defer g.releaseStorer(borrowed)
		storage, ok := borrowed.storage.(*memory.Storage)
		assert.Equal(t, ok, true)
		return weak.Make(storage)
	}()
	runtime.GC()
	assert.Equal(t, idle.Value() == nil, true)
	assert.Equal(t, g.borrowStorer().storage != nil, true)
	assert.Equal(t, created.Load(), int32(2))
}
