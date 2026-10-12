package gittree

import (
	"io/fs"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

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
			input, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, ViewOptions{})
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
}
