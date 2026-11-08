package gittree

import (
	"runtime"
	"sync/atomic"
	"testing"
	"weak"

	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage/memory"

	"github.com/home-operations/flate/internal/assert"
)

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
