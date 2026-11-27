package gittree

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/tree"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
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
			for _, name := range []string{"link", "link/child"} {
				t.Run("mounted point lookup/"+name, func(t *testing.T) {
					t.Parallel()
					current := local.FS.(*FS).OperationView()
					reader := &FS{view: &gitView{mode: mode, current: current}}
					_, statErr := fs.Stat(reader, name)
					if mode == Direct {
						assert.FatalIf(t, !errors.Is(statErr, fs.ErrPermission), "Stat(%q) = %v", name, statErr)
						return
					}
					got, ok := errors.AsType[*fs.PathError](statErr)
					assert.FatalIf(t, !ok || !errors.Is(got, fs.ErrNotExist), "Stat(%q) = %v", name, statErr)
					assert.Equal(t, got.Path, name)
				})
			}
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
	assert.Diff(t, names, []string{".hidden", ".rootfile", "node_modules", "target"})
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

type rejectedLinkView struct {
	name string
	fs   *FS
	mode ViewMode
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
	current := (*testCaseInput.local).FS.(*FS).OperationView()
	_, err = fs.ReadDir(current, ".")
	assert.NoError(t, err)
	mounted, _, err := New(
		t.Context(), nil, plumbing.ZeroHash,
		ViewOptions{Mode: *testCaseInput.mode, Current: current, CurrentObserved: true},
	)
	assert.NoError(t, err)
	for _, input := range []struct {
		name string
		fs   *FS
	}{{"committed", committed}, {"mutable", mutable}, {"mounted", mounted}} {
		refreshed, _, err := operation(t, input.fs, Source)
		assert.NoError(t, err)
		isCommitted := input.name == "committed"
		if isCommitted && *testCaseInput.mode == Source {
			assert.Equal(t, refreshed == input.fs, true)
		}
		if input.name == "mutable" {
			assert.Equal(t, refreshed == input.fs, false)
		}
		for _, view := range []rejectedLinkView{{"initial", input.fs, *testCaseInput.mode}, {"refreshed", refreshed, Source}} {
			reader := view.fs
			entries, err := fs.ReadDir(reader, ".")
			assert.NoError(t, err)
			for _, entry := range entries {
				assert.FatalIf(t, entry.Name() == "link", "%s", "listing exposes rejected link")
			}
			for _, name := range []string{"link", "link/child", "link/nested/child"} {
				t.Run(input.name+"/"+view.name+"/"+name, func(t *testing.T) {
					t.Parallel()
					testNewEntryPolicyRejectedLinksCaseCase(t, view, name, isCommitted)
				},
				)
			}
			_, err = fs.ReadFile(reader, "absent")
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			if view.mode == Source {
				_, err = fs.ReadFile(reader, "ignored")
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
			data, err := fs.ReadFile(reader, "regular")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "content")
		}
	}
}

func testNewEntryPolicyRejectedLinksCaseCase(t *testing.T, view rejectedLinkView, name string, committed bool) {
	t.Helper()

	reader := view.fs
	data, readErr := fs.ReadFile(reader, name)
	assert.Equal(t, len(data), 0)
	_, statErr := fs.Stat(reader, name)
	target, linkErr := fs.ReadLink(reader, name)
	assert.Equal(t, target, "")

	if view.mode == Direct {
		for _, err := range []error{readErr, statErr, linkErr} {
			assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
			got, ok := errors.AsType[*fs.PathError](err)
			assert.FatalIf(t, !ok, "rejected link %q error = %v", name, err)
			if committed {
				assert.Equal(t, got.Path, name)
			} else {
				assert.Equal(t, filepath.Base(got.Path), "link")
			}
		}
		return
	}
	absent := "absent" + strings.TrimPrefix(name, "link")
	_, missingRead := fs.ReadFile(reader, absent)
	_, missingStat := fs.Stat(reader, absent)
	_, missingLink := fs.ReadLink(reader, absent)
	for _, tc := range []struct{ got, missing error }{
		{readErr, missingRead}, {statErr, missingStat}, {linkErr, missingLink},
	} {
		want, ok := errors.AsType[*fs.PathError](tc.missing)
		assert.FatalIf(t, !ok || !errors.Is(want, fs.ErrNotExist), "missing path error = %v", tc.missing)
		got, ok := errors.AsType[*fs.PathError](tc.got)
		assert.FatalIf(t, !ok, "omitted link error = %v", tc.got)
		assert.Equal(t, got.Op, want.Op)
		assert.Equal(t, got.Path, name)
		assert.Equal(t, got.Err, want.Err)
	}
}
