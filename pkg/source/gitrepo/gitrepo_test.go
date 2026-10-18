package gitrepo

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestLocate_RepositorySelection(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			parent := t.TempDir()
			if tc.ancestor || tc.linked {
				testutil.GitInit(t, parent)
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
			location, err := Locate(t.Context(), root, "")
			assert.Equal(t, err, nil)
			got, repo, err := location.RepositoryRoot, location.Repository, location.LookupErr
			if (err != nil) != tc.broken || got != wantRoot || (repo != nil) != (wantRoot != "" && !tc.broken) {
				t.Fatalf("repository = %q, %v, %v; want root %q", got, repo, err, wantRoot)
			}
			if tc.remotes {
				urls, err := location.URLs, location.ConfigErr
				want := []string{"https://host/a", "https://host/a", "ssh://host/z"}
				assert.Equal(t, err, nil)
				assert.Diff(t, urls, want)
			}
		})
	}
	t.Run("metadata error boundary", func(t *testing.T) {
		t.Parallel()
		outer := t.TempDir()
		testutil.GitInit(t, outer)
		root := filepath.Join(outer, "broken")
		assert.Equal(t, os.Mkdir(root, 0o750), nil)
		child := filepath.Join(root, "exported")
		assert.Equal(t, os.Mkdir(child, 0o750), nil)
		assert.Equal(t, os.Symlink(".git", filepath.Join(root, ".git")), nil)

		location, err := Locate(t.Context(), child, "")
		assert.Equal(t, err, nil)
		got, repo, err := location.RepositoryRoot, location.Repository, location.LookupErr
		assert.Equal(t, err, nil)
		assert.Equal(t, got, "")
		assert.Equal(t, repo == nil, true)

		location, err = Locate(t.Context(), root, "")
		assert.Equal(t, err, nil)
		got, repo, err = location.RepositoryRoot, location.Repository, location.LookupErr
		assert.Equal(t, err != nil, true)
		assert.Equal(t, got, "")
		assert.Equal(t, repo == nil, true)
	})
	t.Run("retained coordinates and configuration", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.GitInit(t, root)
		testutil.WriteFile(t, root, ".git/config", "[remote \"origin\"]\nurl = https://host/original\n")
		content := filepath.Join(root, "content")
		assert.Equal(t, os.Mkdir(content, 0o750), nil)
		link := filepath.Join(t.TempDir(), "input")
		assert.Equal(t, os.Symlink(content, link), nil)
		location, err := Locate(t.Context(), link, link)
		assert.Equal(t, err, nil)
		assert.Equal(t, location.ScanPath, content)
		assert.Equal(t, location.ContentRoot, content)
		assert.Equal(t, location.RepositoryRoot, root)
		testutil.WriteFile(t, root, ".git/config", "[remote \"origin\"]\nurl = https://host/replaced\n")
		assert.Diff(t, location.URLs, []string{"https://host/original"})
		assert.Diff(t, location.Config.Remotes["origin"].URLs, []string{"https://host/original"})
		assert.Equal(t, os.Remove(filepath.Join(root, ".git/config")), nil)
		assert.Diff(t, location.Config.Remotes["origin"].URLs, []string{"https://host/original"})
	})

	t.Run("explicit root owns nested repositories", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.GitInit(t, root)
		testutil.WriteFile(t, root, ".git/config", "[remote \"origin\"]\nurl = https://host/outer\n")
		nested := filepath.Join(root, "nested")
		testutil.GitInit(t, nested)
		testutil.WriteFile(t, nested, ".git/config", "[remote \"origin\"]\nurl = https://host/inner\n")
		location, err := Locate(t.Context(), nested, root)
		assert.Equal(t, err, nil)
		assert.Equal(t, location.ScanPath, nested)
		assert.Equal(t, location.ContentRoot, root)
		assert.Equal(t, location.RepositoryRoot, root)
		assert.Diff(t, location.URLs, []string{"https://host/outer"})
	})

}
