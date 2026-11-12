package gittree

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
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
