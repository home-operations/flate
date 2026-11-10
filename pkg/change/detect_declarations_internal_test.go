package change

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestDetect_OneSidedInputOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		routing []string
		want    []string
		readErr error
	}{
		{name: "duplicates", routing: []string{".sourceignore", "input.yaml", "z.yaml"}, want: []string{"input.yaml", "z.yaml"}, readErr: io.EOF},
		{name: "first_error", routing: []string{".sourceignore", "z.yaml"}, want: []string{"input.yaml"}, readErr: fs.ErrPermission},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
					".sourceignore": {Data: []byte("*.yaml\n")}, "input.yaml": {}, "z.yaml": {},
				}},
			)
			assert.NoError(t, err)
			after, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
					".sourceignore": {}, "input.yaml": {}, "z.yaml": {},
				}},
			)
			assert.NoError(t, err)
			var reads []string
			input := &declarationFailureFS{FS: after, fail: func(name string) error {
				reads = append(reads, name)
				return tc.readErr
			}}
			current, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: input},
			)
			assert.NoError(t, err)
			candidates := make(map[string]bool, len(tc.routing))
			for _, name := range tc.routing {
				candidates[name] = false
			}
			changes, err := Detect(
				t.Context(),
				&store.SourceArtifact{FS: before},
				&store.SourceArtifact{FS: current},
				DetectConfig{Candidates: candidates},
			)
			assert.Diff(t, reads, tc.want)
			if errors.Is(tc.readErr, io.EOF) {
				assert.NoError(t, err)
				assert.Diff(t, changes.Paths(), []string{".sourceignore", "input.yaml", "z.yaml"})
			} else {
				assert.FatalIf(
					t,
					!errors.Is(err, tc.readErr) || changes != nil ||
						!strings.Contains(err.Error(), "read input input.yaml:"),
					"first input failure: changes=%v err=%v",
					changes,
					err,
				)
			}
		})
	}
}

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

type declarationFailureFS struct {
	*gittree.FS

	fail func(string) error
}

func (f *declarationFailureFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err != nil || !strings.HasSuffix(name, ".yaml") {
		return file, err
	}
	return &declarationFailureFile{File: file, fail: func() error { return f.fail(name) }}, nil
}

type declarationFailureFile struct {
	fs.File

	fail func() error
}

func (f *declarationFailureFile) Read([]byte) (int, error) { return 0, f.fail() }
