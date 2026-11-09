package gittree

import (
	"errors"
	"io/fs"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func testFSContentFingerprintGitIgnoredNoise(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "nested/file", "tracked")
	testutil.WriteFile(t, root, ".gitignore", "*.noise\n")
	testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	first, err := admitLocal(t, root)
	assert.NoError(t, err)
	before, err := ContentFingerprint(t.Context(), first.FS)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "desktop.noise", "local")
	testutil.WriteFile(t, root, "nested/desktop.noise", "local")
	second, err := admitLocal(t, root)
	assert.NoError(t, err)
	after, err := ContentFingerprint(t.Context(), second.FS)
	assert.NoError(t, err)
	assert.Equal(t, before, after)
	assert.Equal(t, second.FS.(*FS).FileDigests()["desktop.noise"], "")
	assert.Equal(t, second.FS.(*FS).FileDigests()["nested/desktop.noise"], "")
}

func testFSGitContentEvidenceCommittedMembership(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "old.yaml", "historical")
	testutil.WriteFile(t, root, "excluded.yaml", "Flux excludes this tracked file")
	testutil.WriteFile(t, root, ".sourceignore", "excluded.yaml\n")
	commit := testutil.GitCommitAll(
		t,
		repo,
		"historical inputs",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
	)
	worktree, err := repo.Worktree()
	assert.NoError(t, err)
	_, err = worktree.Remove("old.yaml")
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "old.yaml", "local ignored bytes")
	testutil.WriteFile(t, root, ".gitignore", "*.yaml\n")
	testutil.WriteFile(t, root, ".sourceignore", "old.yaml\n")
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	var keys [2]string
	for i := range keys {
		local, localErr := admitLocal(t, root)
		assert.NoError(t, localErr)
		input, key, newErr := New(t.Context(), backing, commit, ViewOptions{
			Mode: Source, Current: local.FS, Changes: []string{"old.yaml", ".sourceignore"},
		})
		assert.NoError(t, newErr)
		data, readErr := input.ReadFile("old.yaml")
		assert.NoError(t, readErr)
		assert.Equal(t, string(data), "historical")
		_, readErr = input.ReadFile("excluded.yaml")
		assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
		keys[i], newErr = ContentFingerprint(t.Context(), input)
		assert.NoError(t, newErr)
		assert.Equal(t, keys[i], key)
		testutil.WriteFile(t, root, ".gitignore", "other.yaml\n")
		assert.NoError(t, worktree.AddWithOptions(&git.AddOptions{Path: "old.yaml", SkipStatus: true}))
	}
	assert.Equal(t, keys[0], keys[1])
	committed, _, err := New(t.Context(), backing, commit, ViewOptions{
		Mode:    Direct,
		Content: struct{ Revision string }{commit.String()}, Worktree: worktree.Filesystem,
	})
	assert.NoError(t, err)
	_, err = committed.ReadFile("old.yaml")
	assert.NoError(t, err)
	assert.Equal(t, committed.membership.ignored == nil, true)
	assert.Equal(t, committed.membership.captured, false)
}
