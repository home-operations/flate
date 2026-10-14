package gitrepo

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestLocate_RepositorySelection(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		ancestor, linked, remotes, broken bool
	}{
		{name: "empty marker below repository", ancestor: true},
		{name: "empty marker without repository"},
		{name: "linked gitfile", linked: true},
		{name: "sorted linked remotes", linked: true, remotes: true},
		{name: "selected open retains root", linked: true, broken: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			if tc.ancestor || tc.linked {
				_, err := git.PlainInit(parent, false)
				assert.Equal(t, err, nil)
			}
			root := filepath.Join(parent, "nested")
			wantRoot := ""
			if tc.linked {
				metadata := filepath.Join(parent, ".git", "worktrees", "linked")
				testutil.WriteFile(t, root, ".git", "gitdir: "+metadata+"\n")
				if !tc.broken {
					testutil.WriteFile(t, metadata, "commondir", "../..\n")
					testutil.WriteFile(t, metadata, "HEAD", "ref: refs/heads/master\n")
				}
				wantRoot = root
			} else {
				assert.Equal(t, os.MkdirAll(filepath.Join(root, ".git"), 0o750), nil)
				if tc.ancestor {
					wantRoot = parent
				}
			}
			if tc.remotes {
				testutil.WriteFile(t, parent, ".git/config", "[remote \"z\"]\nurl = ssh://host/z\nurl = https://host/a\n[remote \"a\"]\nurl = https://host/a\n")
			}
			got, repo, err := Locate(root)
			if (err != nil) != tc.broken || got != wantRoot || (repo != nil) != (wantRoot != "" && !tc.broken) {
				t.Fatalf("repository = %q, %v, %v; want root %q", got, repo, err, wantRoot)
			}
			if tc.remotes {
				urls, err := RemoteURLs(repo)
				want := []string{"https://host/a", "https://host/a", "ssh://host/z"}
				assert.Equal(t, err, nil)
				assert.Diff(t, urls, want)
			}
		})
	}
	t.Run("metadata error boundary", func(t *testing.T) {
		outer := t.TempDir()
		_, err := git.PlainInit(outer, false)
		assert.Equal(t, err, nil)
		root := filepath.Join(outer, "broken")
		assert.Equal(t, os.Mkdir(root, 0o750), nil)
		child := filepath.Join(root, "exported")
		assert.Equal(t, os.Mkdir(child, 0o750), nil)
		assert.Equal(t, os.Symlink(".git", filepath.Join(root, ".git")), nil)

		got, repo, err := Locate(child)
		assert.Equal(t, err, nil)
		assert.Equal(t, got, "")
		assert.Equal(t, repo == nil, true)

		got, repo, err = Locate(root)
		assert.Equal(t, err != nil, true)
		assert.Equal(t, got, "")
		assert.Equal(t, repo == nil, true)
	})
}
