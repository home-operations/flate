package gittree

import (
	"errors"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/home-operations/flate/internal/testutil"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
)

func mustInit(t *testing.T, dir string) *git.Repository {
	t.Helper()
	r, err := git.PlainInit(dir, false)
	assert.NoError(t, err)
	return r
}

func craftedCommit(t *testing.T, repo *git.Repository, entry object.TreeEntry) plumbing.Hash {
	t.Helper()
	blob := repo.Storer.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, err := blob.Writer()
	assert.NoError(t, err)
	if _, err := w.Write([]byte("malicious")); err != nil {
		t.Fatal(err)
	}
	assert.NoError(t, w.Close())
	entry.Hash, err = repo.Storer.SetEncodedObject(blob)
	assert.NoError(t, err)
	if entry.Mode == filemode.Dir {
		child := repo.Storer.NewEncodedObject()
		assert.NoError(t, (&object.Tree{}).Encode(child))
		entry.Hash, err = repo.Storer.SetEncodedObject(child)
		assert.NoError(t, err)
	}
	tree := &object.Tree{Entries: []object.TreeEntry{entry}}
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, tree.Encode(encoded))
	treeHash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	commit := &object.Commit{TreeHash: treeHash, Author: object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}}
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	hash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	return hash
}

func TestNew_EntryPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    filemode.FileMode
		invalid bool
	}{
		{"a/b", filemode.Regular, true}, {"module", filemode.Submodule, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := mustInit(t, t.TempDir())
			commit := craftedCommit(t, repo, object.TreeEntry{Name: tc.name, Mode: tc.mode})
			input, _, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{})
			if tc.invalid {
				if err == nil {
					t.Fatal("malformed entry accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			entries, err := fs.ReadDir(input, ".")
			if err != nil || len(entries) != 0 {
				t.Fatalf("gitlink included: %v, %v", entries, err)
			}
		})
	}

	t.Run("mutable membership and live worktree", func(t *testing.T) {
		for _, live := range []bool{false, true} {
			root := t.TempDir()
			repo := mustInit(t, root)
			commit := craftedCommit(t, repo, object.TreeEntry{Name: "committed", Mode: filemode.Regular})
			testutil.WriteFile(t, root, "old/file", "old")
			testutil.WriteFile(t, root, ".sourceignore", "ignored/\n")
			testutil.WriteFile(t, root, "ignored/file", "hidden")
			local, err := AdmitDirectory(t.Context(), root)
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
	})
	t.Run("committed tree overrides current", func(t *testing.T) {
		repo := mustInit(t, t.TempDir())
		backing := repo.Storer.(*filesystem.Storage).Filesystem()
		currentCommit := craftedCommit(t, repo, object.TreeEntry{Name: "current", Mode: filemode.Regular})
		current, _, err := New(t.Context(), backing, currentCommit, ViewOptions{})
		assert.NoError(t, err)
		target := craftedCommit(t, repo, object.TreeEntry{Name: "target", Mode: filemode.Regular})
		view, _, err := New(t.Context(), backing, target, ViewOptions{Mode: Direct, Current: current, Changes: []string{"target"}})
		assert.NoError(t, err)
		data, err := fs.ReadFile(view, "target")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "malicious")
		_, err = fs.Stat(view, "current")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	})
	t.Run("retained identity", func(t *testing.T) {
		repo := mustInit(t, t.TempDir())
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
		identity := "source-git-acquired@" + commit.String()
		input, pin, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{Mode: Direct, Identity: identity})
		assert.NoError(t, err)
		assert.Equal(t, pin, identity)
		held, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
		assert.NoError(t, err)
		assert.Equal(t, pin, identity)
		assert.Equal(t, held == input, true)
		_, pin, err = New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{Mode: Direct})
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
	})
}
