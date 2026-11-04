package gittree

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func testNewEntryPolicySourceReuse(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	input, pin, err := New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{Mode: Source},
	)
	assert.NoError(t, err)
	for _, tc := range []struct {
		name   string
		ignore string
		sparse []string
		reuse  bool
	}{
		{name: "empty", reuse: true},
		{name: "ignore", ignore: "file\n"},
		{name: "sparse", sparse: []string{"other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, key, err := New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				ViewOptions{Mode: Source, Current: input, Ignore: tc.ignore, Sparse: tc.sparse},
			)
			assert.NoError(t, err)
			assert.Equal(t, got == input, tc.reuse)
			assert.Equal(t, key == pin, tc.reuse)
			assert.Equal(t, key != "", true)
			_, err = fs.Stat(got, "file")
			assert.Equal(t, err == nil, tc.reuse)
		})
	}
}

func testNewEntryPolicyDirectArbitraryAndWrapperReuse(t *testing.T) {
	t.Helper()

	current := fstest.MapFS{
		"file":               {Data: []byte("current")},
		".git":               {Data: []byte("gitfile")},
		"nested/.git/config": {Data: []byte("config")},
	}
	input, key, err := New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		ViewOptions{Mode: Direct, Current: current, Ignore: "file", Sparse: []string{"other"}},
	)
	assert.NoError(t, err)
	assert.Equal(t, key, "")
	current["new"] = &fstest.MapFile{Data: []byte("new")}
	got, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
	assert.NoError(t, err)
	assert.Equal(t, got == input, true)
	assert.Equal(t, key, "")
	for _, name := range []string{"file", "new", ".git", "nested/.git/config"} {
		data, err := fs.ReadFile(got, name)
		assert.NoError(t, err)
		assert.Equal(t, string(data), string(current[name].Data))
	}
}

func testNewEntryPolicyDirectPinnedWorktree(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "local")
	input, key, err := New(
		t.Context(),
		nil,
		plumbing.NewHash(strings.Repeat("a", 40)),
		ViewOptions{Mode: Direct, Worktree: osfs.New(root, osfs.WithBoundOS())},
	)
	assert.NoError(t, err)
	assert.Equal(t, key, "")
	data, err := fs.ReadFile(input, "file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "local")
	testutil.WriteFile(t, root, "new/deep/file", "new")
	data, err = fs.ReadFile(input, "new/deep/file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "new")
	fresh, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
	assert.NoError(t, err)
	assert.Equal(t, fresh != input, true)
}

func testNewEntryPolicyRefreshBeforePinnedConstruction(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "new", Mode: filemode.Regular})
	testutil.WriteFile(t, root, "old", "old")
	local, err := admitLocal(t, root)
	assert.NoError(t, err)
	current, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: local.FS})
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "new", "current")
	input, key, err := New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{Mode: Direct, Current: current},
	)
	assert.NoError(t, err)
	assert.Equal(t, key != "", true)
	data, err := fs.ReadFile(input, "new")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "current")
}

func testNewEntryPolicyRefreshSnapshots(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	opts := ViewOptions{
		Mode:    Source,
		Current: fstest.MapFS{"file": {Data: []byte("dirty")}},
		Sparse:  []string{"file"},
		Changes: []string{"file"},
	}
	input, key, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, opts)
	assert.NoError(t, err)
	opts.Sparse[0], opts.Changes[0] = "other", "other"
	fresh, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
	assert.NoError(t, err)
	assert.Equal(t, pin, key)
	data, err := fs.ReadFile(fresh, "file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "malicious")
}
