package change

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestDetect_NoOneSidedInputCancellation(t *testing.T) {
	t.Parallel()
	before, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
			"nested/input.yaml": {Data: []byte("before")},
		}},
	)
	assert.NoError(t, err)
	after, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
			"nested/input.yaml": {Data: []byte("after")},
		}},
	)
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := &observationCancelFS{FS: after, cancel: cancel}
	current, _, err := gittree.New(
		ctx,
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: input},
	)
	assert.NoError(t, err)
	changes, err := Detect(
		ctx,
		&store.SourceArtifact{FS: before},
		&store.SourceArtifact{FS: current},
		DetectConfig{Candidates: map[string]bool{"nested/input.yaml": false}},
	)
	assert.Equal(t, ctx.Err(), context.Canceled)
	assert.Equal(t, input.reads, 1)
	assert.FatalIf(
		t,
		changes != nil || !errors.Is(err, context.Canceled),
		"canceled two-sided selection published changes: changes=%v err=%v",
		changes,
		err,
	)
}

type observationCancelFS struct {
	*gittree.FS

	cancel context.CancelFunc
	reads  int
}

func (f *observationCancelFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := f.FS.ReadDir(name)
	f.reads++
	f.cancel()
	return entries, err
}
