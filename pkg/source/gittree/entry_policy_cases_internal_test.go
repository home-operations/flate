package gittree

import (
	"io/fs"
	"testing"

	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func testNewEntryPolicyCase(t *testing.T, tc *struct {
	name    string
	mode    filemode.FileMode
	invalid bool
},
) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: tc.name, Mode: tc.mode})
	input, identity, err := New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{},
	)
	if tc.invalid {
		assert.FatalIf(t, err == nil || identity != "", "%s", "malformed entry accepted or assigned an identity")

		return
	}
	assert.NoError(t, err)
	assert.FatalIf(t, identity == "", "%s", "committed view has no identity")

	entries, err := fs.ReadDir(input, ".")
	assert.FatalIf(t, err != nil || len(entries) != 0, "gitlink included: %v, %v", entries, err)
}
