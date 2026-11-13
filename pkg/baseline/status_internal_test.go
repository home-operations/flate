package baseline

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/utils/merkletrie"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func TestStatus_MetadataObservation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "nested/deep/file", "before")
	testutil.WriteFile(t, root, "removed", "before")
	testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{
		Author: &object.Signature{Name: "test", Email: "test@example.invalid"},
	})
	original, err := repo.Worktree()
	assert.NoError(t, err)
	counted := &countedMetadata{Filesystem: original.Filesystem()}
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	location.Repository, err = git.Open(repo.Storer, counted)
	assert.NoError(t, err)
	s := selection{repo: repo}
	head, err := repo.Head()
	assert.NoError(t, err)
	commit, err := repo.CommitObject(head.Hash())
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	_, base, err := treeEntries(t.Context(), tree)
	assert.NoError(t, err)
	compare := func() map[string]git.StatusCode {
		t.Helper()
		directory, err := gittree.OpenDirectory(t.Context(), location)
		assert.NoError(t, err)
		defer func() { _ = directory.Close() }()
		worktree, err := directory.Worktree(directory.Artifact().FS.(*gittree.FS))
		assert.NoError(t, err)
		changes, err := s.worktreeChanges(t.Context(), worktree, base)
		assert.NoError(t, err)
		got := map[string]git.StatusCode{}
		for _, change := range changes {
			action, actionErr := change.Action()
			assert.NoError(t, actionErr)
			name := change.To.String()
			if name == "" {
				name = change.From.String()
			}
			got[name] = map[merkletrie.Action]git.StatusCode{merkletrie.Insert: git.Untracked, merkletrie.Delete: git.Deleted, merkletrie.Modify: git.Modified}[action]
		}
		status, err := original.Status()
		assert.NoError(t, err)
		want := map[string]git.StatusCode{}
		for name, item := range status {
			want[name] = item.Worktree
		}
		assert.Diff(t, got, want)
		return got
	}
	testutil.WriteFile(t, root, "nested/deep/file", "after the observation opens")
	testutil.WriteFile(t, root, "nested/deep/added", "new")
	assert.NoError(t, os.Remove(filepath.Join(root, "removed")))
	assert.NoError(t, os.Symlink("file", filepath.Join(root, "nested", "deep", "link")))
	got := compare()
	assert.Equal(t, counted.directories, 0)
	assert.Equal(t, got["nested/deep/file"], git.Modified)
	assert.Equal(t, got["nested/deep/added"], git.Untracked)
	assert.Equal(t, got["removed"], git.Deleted)
	testutil.WriteFile(t, root, "nested/deep/file", "before")
	assert.NoError(t, os.Remove(filepath.Join(root, "nested", "deep", "added")))
	compare()
}

func TestStatus_Cancellation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	repo := testutil.GitInit(t, dir)
	ctx, cancel := context.WithCancel(t.Context())
	location, err := gitrepo.Locate(ctx, dir, dir)
	assert.NoError(t, err)
	directory, err := gittree.OpenDirectory(ctx, location)
	assert.NoError(t, err)
	defer func() { _ = directory.Close() }()
	original, err := directory.Worktree(directory.Artifact().FS.(*gittree.FS))
	assert.NoError(t, err)
	cancel()
	_, err = original.Filesystem().ReadDir("")
	assert.FatalIf(t, !errors.Is(err, context.Canceled), "metadata read ignored cancellation: %v", err)
	_, err = (&selection{repo: repo}).worktreeChanges(ctx, original, nil)
	assert.FatalIf(t, !errors.Is(err, context.Canceled), "observation ignored cancellation: %v", err)
}

type countedMetadata struct {
	billy.Filesystem

	directories int
}

func (f *countedMetadata) ReadDir(name string) ([]fs.DirEntry, error) {
	f.directories++
	return f.Filesystem.ReadDir(name)
}

func TestSelection_IdenticalTreeMetadata(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	pin := initRepoWithFile(t, root, "nested/deep/file", "base")
	repo := openHelper(t, root)
	reads := &testutil.GitReads{}
	repo.Storer = &testutil.CountingStorer{Storer: repo.Storer, Reads: reads}
	input, _, err := AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
	assert.NoError(t, err)
	s := selection{
		repo:     repo,
		worktree: comparisonWorktree(t, input.FS.(*gittree.FS)),
		base:     &resolution{Repo: repo, Hash: pin},
		current:  input.FS.(*gittree.FS),
	}
	entries, comparison, err := s.baseEntries(t.Context())
	assert.NoError(t, err)
	reads.Reset()
	got, warnings, err := s.candidates(t.Context(), entries, comparison)
	assert.NoError(t, err)
	assert.Equal(t, len(got), 0)
	assert.Equal(t, len(warnings), 0)
	assert.Equal(t, reads.Metadata.Load(), int64(2))
	assert.Equal(t, reads.Bodies.Load(), int64(0))
}

func comparisonWorktree(t *testing.T, current *gittree.FS) *git.Worktree {
	t.Helper()
	directory, err := gittree.OpenDirectory(t.Context(), current.Location())
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, directory.Close()) })
	worktree, err := directory.Worktree(current)
	assert.NoError(t, err)
	return worktree
}
