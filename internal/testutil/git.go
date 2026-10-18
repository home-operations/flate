package testutil

import (
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

// GitInit initializes a Git repository at dir.
func GitInit(tb testing.TB, dir string) *git.Repository {
	tb.Helper()
	repo, err := git.PlainInit(dir, false)
	if err != nil {
		tb.Fatal(err)
	}
	return repo
}

// GitCommitAll stages the entire worktree and commits it with the supplied options.
func GitCommitAll(tb testing.TB, repo *git.Repository, message string, options *git.CommitOptions) plumbing.Hash {
	tb.Helper()
	wt, err := repo.Worktree()
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := wt.Add("."); err != nil {
		tb.Fatal(err)
	}
	hash, err := wt.Commit(message, options)
	if err != nil {
		tb.Fatal(err)
	}
	return hash
}
