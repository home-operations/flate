package gittree

import (
	"errors"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestAdmitDirectory_GitIgnoredUntrackedPaths(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "apps/app.yaml", "tracked")
	testutil.WriteFile(t, root, "apps/forced.dec.yaml", "tracked before its rule")
	testutil.GitCommitAll(t, repo, "init", &git.CommitOptions{
		Author: &object.Signature{Name: "test", Email: "test@example.invalid"},
	})
	testutil.WriteFile(t, root, ".gitignore", ".DS_Store\nlocal/\n*.dec.yaml\n")
	testutil.WriteFile(t, root, "apps/.gitignore", "notes.txt\n")
	testutil.WriteFile(t, root, "apps/new.yaml", "untracked")
	for _, name := range []string{".DS_Store", "apps/.DS_Store", "apps/notes.txt", "apps/x.dec.yaml", "local/secret.yaml"} {
		testutil.WriteFile(t, root, name, "ignored")
	}

	cases := []struct {
		name, dir     string
		files, hidden []string
	}{
		{
			name: "repository_root",
			dir:  root,
			files: []string{
				".gitignore", "apps/.gitignore", "apps/app.yaml", "apps/forced.dec.yaml", "apps/new.yaml",
			},
			hidden: []string{
				".DS_Store", "apps/.DS_Store", "apps/notes.txt", "apps/x.dec.yaml", "local", "local/secret.yaml",
			},
		},
		{
			name:   "content_root_below_repository",
			dir:    filepath.Join(root, "apps"),
			files:  []string{".gitignore", "app.yaml", "forced.dec.yaml", "new.yaml"},
			hidden: []string{".DS_Store", "notes.txt", "x.dec.yaml"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input, err := admitLocal(t, tc.dir)
			assert.NoError(t, err)
			var files []string
			assert.NoError(t, fs.WalkDir(input.FS, ".", func(name string, entry fs.DirEntry, err error) error {
				if err == nil && !entry.IsDir() {
					files = append(files, name)
				}
				return err
			}))
			if diff := cmp.Diff(tc.files, files); diff != "" {
				t.Errorf("files (-want +got):\n%s", diff)
			}
			for _, name := range tc.hidden {
				_, err := fs.Stat(input.FS, name)
				assert.ErrorIf(t, !errors.Is(err, fs.ErrNotExist), "Stat(%q) err = %v, want not exist", name, err)
			}
		})
	}
}
