package gittree

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

func testNewEntryPolicyRejectedLinks(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "regular", "content")
	testutil.WriteFile(t, root, "ignored", "ignored")
	testutil.WriteFile(t, root, ".sourceignore", "ignored\n")
	assert.NoError(t, os.Symlink("../absent", filepath.Join(root, "link")))
	commit := testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	local, err := admitLocal(t, root)
	assert.NoError(t, err)
	for _, mode := range []ViewMode{Direct, Source} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicyRejectedLinksCase(
				t,
				testNewEntryPolicyRejectedLinksCaseInputs{
					root:    &root,
					commit:  &commit,
					backing: &backing,
					local:   &local,
					mode:    &mode,
				},
			)
		},
		)
	}
}

func testNewEntryPolicyUnknownModeVisibility(t *testing.T) {
	t.Helper()

	current := fstest.MapFS{
		".rootfile": {Data: []byte("root")}, "target": {Data: []byte("target")},
		".hidden/file": {Data: []byte("hidden")}, "node_modules/file": {Data: []byte("modules")},
	}
	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: ViewMode(255), Current: current})
	assert.NoError(t, err)
	entries, err := fs.ReadDir(input, ".")
	assert.NoError(t, err)
	var names []string
	if len(entries) != 0 {
		names = make([]string, 0, len(entries))
	}
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	assert.Diff(t, names, []string{".rootfile", "target"})
}

func testNewEntryPolicyLinkErrors(t *testing.T) {
	t.Helper()

	for _, tc := range []struct {
		name    string
		missing bool
		want    error
	}{
		{"loop", false, tree.ErrLoop}, {"read", true, plumbing.ErrObjectNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := testutil.GitInit(t, t.TempDir())
			commit := craftedCommit(t, repo, object.TreeEntry{Name: "malicious", Mode: filemode.Symlink})
			backing := repo.Storer.(*filesystem.Storage).Filesystem()
			if tc.missing {
				c, err := repo.CommitObject(commit)
				assert.NoError(t, err)
				tr, err := c.Tree()
				assert.NoError(t, err)
				hash := tr.Entries[0].Hash.String()
				assert.NoError(t, backing.Remove("objects/"+hash[:2]+"/"+hash[2:]))
			}
			_, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: RemoteBase})
			assert.Equal(t, errors.Is(err, tc.want), true)
			for _, mode := range []ViewMode{Source, Direct} {
				input, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: mode})
				assert.NoError(t, err)
				entries, err := fs.ReadDir(input, ".")
				assert.NoError(t, err)
				assert.Equal(t, len(entries), 0)
			}
		})
	}
}

type testNewEntryPolicyRejectedLinksCaseInputs struct {
	root    *string
	commit  *plumbing.Hash
	backing *billy.Filesystem
	local   **store.SourceArtifact
	mode    *ViewMode
}

func testNewEntryPolicyRejectedLinksCase(t *testing.T, testCaseInput testNewEntryPolicyRejectedLinksCaseInputs) {
	t.Helper()

	committed, _, err := New(
		t.Context(),
		(*testCaseInput.backing),
		(*testCaseInput.commit),
		ViewOptions{Mode: (*testCaseInput.mode)},
	)
	assert.NoError(t, err)
	mutable, _, err := New(
		t.Context(), (*testCaseInput.backing), plumbing.ZeroHash,
		ViewOptions{Mode: (*testCaseInput.mode), Worktree: osfs.New((*testCaseInput.root))},
	)
	assert.NoError(t, err)
	mounted, _, err := operation(t, (*testCaseInput.local).FS, (*testCaseInput.mode))
	assert.NoError(t, err)
	for _, input := range []struct {
		name string
		fs   *FS
	}{{"committed", committed}, {"mutable", mutable}, {"mounted", mounted}} {
		refreshed, _, err := operation(t, input.fs, Source)
		assert.NoError(t, err)
		for _, view := range []struct {
			name string
			fs   *FS
		}{{"initial", input.fs}, {"refreshed", refreshed}} {
			reader := view.fs
			entries, err := fs.ReadDir(reader, ".")
			assert.NoError(t, err)
			for _, entry := range entries {
				assert.FatalIf(t, entry.Name() == "link", "%s", "listing exposes rejected link")
			}
			for _, name := range []string{"link", "link/child", "link/nested/child"} {
				t.Run(input.name+"/"+view.name+"/"+name, func(t *testing.T) {
					t.Parallel()
					testNewEntryPolicyRejectedLinksCaseCase(
						t,
						testNewEntryPolicyRejectedLinksCaseCaseInputs{reader: &reader, name: &name},
					)
				},
				)
			}
			_, err = fs.ReadFile(reader, "absent")
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			if reader == refreshed || (*testCaseInput.mode) == Source {
				_, err = fs.ReadFile(reader, "ignored")
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
			data, err := fs.ReadFile(reader, "regular")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "content")
		}
	}
}

type testNewEntryPolicyRejectedLinksCaseCaseInputs struct {
	reader **FS
	name   *string
}

func testNewEntryPolicyRejectedLinksCaseCase(
	t *testing.T,
	testCaseInput testNewEntryPolicyRejectedLinksCaseCaseInputs,
) {
	t.Helper()

	data, readFileErr2 := fs.ReadFile((*testCaseInput.reader), (*testCaseInput.name))
	assert.Equal(t, len(data), 0)
	assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
	_, readFileErr2 = fs.Stat((*testCaseInput.reader), (*testCaseInput.name))
	assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
	target, readFileErr2 := fs.ReadLink((*testCaseInput.reader), (*testCaseInput.name))
	assert.Equal(t, target, "")
	assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
}
