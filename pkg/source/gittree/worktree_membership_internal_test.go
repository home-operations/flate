package gittree

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func testNewEntryPolicyMutableMembershipAndLiveWorktree(t *testing.T) {
	t.Helper()

	for _, live := range []bool{false, true} {
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "committed", Mode: filemode.Regular})
		testutil.WriteFile(t, root, "old/file", "old")
		testutil.WriteFile(t, root, ".sourceignore", "ignored/\n")
		testutil.WriteFile(t, root, "ignored/file", "hidden")
		local, err := admitLocal(t, root)
		assert.NoError(t, err)
		opts := ViewOptions{Mode: Source, Current: local.FS}
		if live {
			opts.Current = nil
			opts.Worktree = osfs.New(root, osfs.WithBoundOS())
		}
		backing := repo.Storer.(*filesystem.Storage).Filesystem()
		if !live {
			backing, commit = nil, plumbing.ZeroHash
		}
		view, pin, err := New(t.Context(), backing, commit, opts)
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		_, missingErr := fs.Stat(view, "new/deep/file")
		assert.Equal(t, errors.Is(missingErr, fs.ErrNotExist), true)
		_, err = fs.ReadDir(view, "old")
		assert.NoError(t, err)
		assert.NoError(t, os.RemoveAll(filepath.Join(root, "old")))
		testutil.WriteFile(t, root, "new/deep/file", "new")
		fresh, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: view})
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		data, err := fs.ReadFile(fresh, "new/deep/file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "new")
		for _, name := range []string{"old/file", "ignored/file"} {
			_, err := fs.Stat(fresh, name)
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		}
	}
}

func testNewEntryPolicyCommittedTreeOverridesCurrent(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	currentCommit := craftedCommit(t, repo, object.TreeEntry{Name: "current", Mode: filemode.Regular})
	current, _, err := New(t.Context(), backing, currentCommit, ViewOptions{})
	assert.NoError(t, err)
	target := craftedCommit(t, repo, object.TreeEntry{Name: "target", Mode: filemode.Regular})
	view, _, err := New(
		t.Context(),
		backing,
		target,
		ViewOptions{Mode: Direct, Current: current, Changes: []string{"target"}},
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(view, "target")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "malicious")
	_, err = fs.Stat(view, "current")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
}
