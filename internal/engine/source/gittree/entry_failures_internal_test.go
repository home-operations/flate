package gittree

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func testNewEntryPolicyMissingOrdinaryBody(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	c, err := repo.CommitObject(commit)
	assert.NoError(t, err)
	tr, err := c.Tree()
	assert.NoError(t, err)
	hash := tr.Entries[0].Hash.String()
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	assert.NoError(t, backing.Remove("objects/"+hash[:2]+"/"+hash[2:]))
	for _, mode := range []ViewMode{Source, Direct, RemoteBase, ViewMode(255)} {
		input, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: mode})
		assert.NoError(t, err)
		entries, err := fs.ReadDir(input, ".")
		assert.NoError(t, err)
		assert.Equal(t, len(entries), 1)
		assert.Equal(t, entries[0].Name(), "file")
		_, err = fs.ReadFile(input, "file")
		assert.Equal(t, errors.Is(err, plumbing.ErrObjectNotFound), true)
	}
}

func testNewEntryPolicyErrorClassification(t *testing.T) {
	t.Helper()

	for _, want := range []error{fs.ErrPermission, fs.ErrInvalid} {
		_, _, err := New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{Mode: Source, Current: admissionErrorFS{err: want}},
		)
		assert.Equal(t, errors.Is(err, want), true)
		assert.Equal(t, strings.HasPrefix(err.Error(), "sourceignore load: "), errors.Is(want, fs.ErrPermission))

		current := &FS{view: &directView{admissionErrorFS{err: want}}, snapshot: &localPolicy{}}
		_, _, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: current})
		assert.Equal(t, errors.Is(err, want), true)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := New(
		ctx,
		nil,
		plumbing.ZeroHash,
		ViewOptions{Mode: Source, Current: admissionErrorFS{err: fs.ErrPermission}},
	)
	assert.Equal(t, err, context.Canceled)
	t.Run("size errors", func(t *testing.T) {
		t.Parallel()
		testNewEntryPolicySizeErrors(t)
	})
}

type statErrorFS struct {
	fs.FS

	err error
}

func (f statErrorFS) Stat(string) (fs.FileInfo, error)  { return nil, f.err }
func (f statErrorFS) Lstat(string) (fs.FileInfo, error) { return nil, f.err }

func TestGitView_MissingNodeBackingError(t *testing.T) {
	t.Parallel()

	want := &fs.PathError{Op: "backing-lstat", Path: "physical-file", Err: fs.ErrPermission}
	view := &gitView{mode: Source, current: statErrorFS{err: want}}
	_, err := view.Lstat("missing")
	assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
	got, ok := errors.AsType[*fs.PathError](err)
	assert.FatalIf(t, !ok, "backing lookup error = %v", err)
	assert.Equal(t, got, want)
	assert.Equal(t, got.Op, "backing-lstat")
	assert.Equal(t, got.Path, "physical-file")
	assert.Equal(t, got.Err, fs.ErrPermission)
}

func testNewEntryPolicySizeErrors(t *testing.T) {
	t.Helper()
	pathErr := &fs.PathError{Op: "current-size", Path: "physical-file", Err: fs.ErrPermission}
	for _, tt := range []struct {
		name    string
		current fs.FS
		want    error
	}{
		{name: "current filesystem", current: statErrorFS{err: pathErr}, want: fs.ErrPermission},
		{name: "missing stored blob", want: plumbing.ErrObjectNotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			view := &gitView{
				backing: memfs.New(), current: tt.current,
				nodes:     map[string]*gitNode{"file": {name: "file"}},
				newStorer: func() storer.EncodedObjectStorer { return memory.NewStorage() },
			}
			_, err := view.Lstat("file")
			assert.Equal(t, errors.Is(err, tt.want), true)
			assert.Equal(t, strings.HasPrefix(err.Error(), "size \"file\": "), true)
			if tt.current != nil {
				got, ok := errors.AsType[*fs.PathError](err)
				assert.FatalIf(t, !ok, "current size error = %v", err)
				assert.Equal(t, got, pathErr)
				assert.Equal(t, got.Op, "current-size")
				assert.Equal(t, got.Path, "physical-file")
				assert.Equal(t, got.Err, fs.ErrPermission)
				_, err = (&FS{view: view}).Stat("file")
				got, ok = errors.AsType[*fs.PathError](err)
				assert.FatalIf(t, !ok, "stat size error = %v", err)
				assert.Equal(t, got.Op, statOperation)
				assert.Equal(t, got.Path, pathErr.Path)
				assert.Equal(t, got.Err, pathErr.Err)
				assert.Equal(t, pathErr.Op, "current-size")
				assert.Equal(t, errors.Is(err, tt.want), true)
			}
		})
	}
}

func TestGitView_MissingCommit(t *testing.T) {
	t.Parallel()
	view := &gitView{newStorer: func() storer.EncodedObjectStorer { return memory.NewStorage() }}
	_, err := view.walk(t.Context(), plumbing.ZeroHash)
	assert.Equal(t, errors.Is(err, plumbing.ErrObjectNotFound), true)
	assert.Equal(t, strings.HasPrefix(err.Error(), "read commit "), true)
	assert.Equal(t, view.storer.Load() != nil, true)
}

func testNewEntryPolicyLocalPermissionDiagnostic(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "ignored/\n")
	testutil.WriteFile(t, root, "denied/file", "file")
	ignore, denied := filepath.Join(root, ".sourceignore"), filepath.Join(root, "denied")
	assert.NoError(t, os.Chmod(ignore, 0))
	assert.NoError(t, os.Chmod(denied, 0))
	t.Cleanup(func() {
		assert.NoError(t, os.Chmod(ignore, 0o644))
		assert.NoError(t, os.Chmod(denied, 0o755))
	})
	if _, err := os.ReadFile(ignore); !errors.Is(err, fs.ErrPermission) {
		t.Skipf("filesystem cannot enforce ignore-file permission fixture: %v", err)
	}
	if _, err := os.ReadDir(denied); !errors.Is(err, fs.ErrPermission) {
		t.Skipf("filesystem cannot enforce directory permission fixture: %v", err)
	}
	local, err := admitLocal(t, root)
	assert.NoError(t, err)
	_, _, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: local.FS})
	assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
	pathErr, ok := errors.AsType[*fs.PathError](err)
	assert.FatalIf(t, !ok || pathErr.Path != ignore, "permission diagnostic = %v; want %s", err, ignore)
}
