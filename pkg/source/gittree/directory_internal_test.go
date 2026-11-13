package gittree

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/utils/merkletrie"
	merklefs "github.com/go-git/go-git/v6/utils/merkletrie/filesystem"
	merkleindex "github.com/go-git/go-git/v6/utils/merkletrie/index"
	"github.com/go-git/go-git/v6/utils/merkletrie/noder"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

func TestDirectory_IndexedSiblingLookups(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.GitInit(t, root)
	const siblings = 128
	for i := range siblings {
		testutil.WriteFile(t, root, fmt.Sprintf("sibling-%03d/.gitignore", i), "ignored\n")
	}
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	var names int
	backing := &membershipCountingFS{
		Filesystem: newMembershipRoot(t, root), reads: make(map[string]int), names: &names,
		directoryChecks: make(map[string]int),
	}
	directory, err := openDirectory(t.Context(), location, backing, "")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, directory.Close()) }()
	indexed := names
	for range 2 {
		for _, prefix := range []string{"", "./", string(filepath.Separator)} {
			for i := range siblings {
				name := prefix + fmt.Sprintf("sibling-%03d/.gitignore", i)
				file, openErr := directory.observed.Open(name)
				assert.NoError(t, openErr)
				assert.NoError(t, file.Close())
				_, statErr := directory.observed.Lstat(name)
				assert.NoError(t, statErr)
			}
		}
	}
	assert.Equal(t, indexed > siblings, true)
	assert.Equal(t, names, indexed)
	assert.Equal(t, len(backing.reads), siblings+1)
	for _, reads := range backing.reads {
		assert.Equal(t, reads, 1)
	}
	assert.Equal(t, backing.lstats, 0)
	assert.Equal(t, backing.payloadOpens, 0)
	worktree, err := directory.Worktree(directory.Artifact().FS.(*FS))
	assert.NoError(t, err)
	checks := backing.directoryChecks["sibling-000"]
	fileChecks := backing.directoryChecks[filepath.Join("sibling-000", ".gitignore")]
	_, err = worktree.Filesystem().ReadDir("sibling-000")
	assert.NoError(t, err)
	assert.Equal(t, backing.directoryChecks["sibling-000"], checks)
	assert.Equal(t, backing.directoryChecks[filepath.Join("sibling-000", ".gitignore")], fileChecks)
}

func TestDirectory_SharedCaptureAndBaseline(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	for _, name := range []string{"nested/tracked", "tracked.ignored", "removed", "pruned/tracked"} {
		testutil.WriteFile(t, root, name, "before")
	}
	testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	index, err := repo.Storer.Index()
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "nested/tracked", "after with a different size")
	testutil.WriteFile(t, root, "tracked.ignored", "changed tracked match")
	assert.NoError(t, os.Remove(filepath.Join(root, "removed")))
	testutil.WriteFile(t, root, ".gitignore", "*.ignored\npruned/\n")
	testutil.WriteFile(t, root, ".git/info/exclude", "info-only\n")
	for _, name := range []string{"noise.ignored", "pruned/deep/noise", "info-only", "nested/added"} {
		testutil.WriteFile(t, root, name, "untracked")
	}
	assert.NoError(t, os.Mkdir(filepath.Join(root, "empty"), 0o755))
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	backing := &membershipCountingFS{
		Filesystem: newMembershipRoot(t, root), reads: make(map[string]int), directoryChecks: make(map[string]int),
	}
	directory, err := openDirectory(t.Context(), location, backing, "")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, directory.Close()) }()
	assert.Equal(t, backing.payloadOpens, 0)
	assert.Equal(t, backing.lstats, 0)
	worktree, err := directory.Worktree(directory.Artifact().FS.(*FS))
	assert.NoError(t, err)
	changes, err := merkletrie.DiffTreeContext(t.Context(), merkleindex.NewRootNode(index),
		merklefs.NewRootNodeWithOptions(worktree.Filesystem(), nil, merklefs.Options{Index: index}),
		func(a, b noder.Hasher) bool {
			var unknown [24]byte
			left, right := a.Hash(), b.Hash()
			return !bytes.Equal(left, unknown[:]) && !bytes.Equal(right, unknown[:]) && bytes.Equal(left, right)
		})
	assert.NoError(t, err)
	got := make(map[string]merkletrie.Action)
	for _, change := range changes {
		action, actionErr := change.Action()
		assert.NoError(t, actionErr)
		name := change.To.String()
		if name == "" {
			name = change.From.String()
		}
		got[name] = action
	}
	assert.Diff(t, got, map[string]merkletrie.Action{
		".gitignore": merkletrie.Insert, "nested/added": merkletrie.Insert,
		"nested/tracked": merkletrie.Modify, "tracked.ignored": merkletrie.Modify, "removed": merkletrie.Delete,
	})
	assert.Diff(t, backing.reads, map[string]int{
		".": 1, "empty": 1, "nested": 1, "pruned": 1, filepath.Join("pruned", "deep"): 1,
	})
	assert.Equal(t, backing.payloadOpens > 0, true)
	assert.Equal(t, backing.lstats, 1)
	checks := backing.directoryChecks["nested"]
	uncached, err := directory.Worktree(directory.Artifact().FS.(*FS))
	assert.NoError(t, err)
	_, err = uncached.Filesystem().Lstat("nested/tracked")
	assert.NoError(t, err)
	// Uncached access MUST admit its leading directory before the file.
	assert.Equal(t, backing.directoryChecks["nested"], checks+1)
	rawNested, err := directory.observed.ReadDir("nested")
	assert.NoError(t, err)
	selectedNested, err := worktree.Filesystem().ReadDir("nested")
	assert.NoError(t, err)
	assert.Equal(t, &selectedNested[0], &rawNested[0])
	raw, err := directory.observed.ReadDir(".")
	assert.NoError(t, err)
	foundGit := false
	for _, entry := range raw {
		foundGit = foundGit || entry.Name() == ".git"
	}
	assert.Equal(t, foundGit, true)
}

func TestDirectory_NestedContentRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "content/deep/tracked.ignored", "tracked")
	testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	testutil.WriteFile(t, root, ".gitignore", "*.ignored\n")
	for _, name := range []string{"outside.ignored", "outside", "content/deep/noise.ignored", "content/cache/data", "content/deep/kept"} {
		testutil.WriteFile(t, root, name, name)
	}
	content := filepath.Join(root, "content")
	location, err := gitrepo.Locate(t.Context(), content, content)
	assert.NoError(t, err)
	directory, err := OpenDirectory(t.Context(), location)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, directory.Close()) }()
	artifact := ExcludeDirectory(directory.Artifact(), filepath.Join(content, "cache"))
	input := artifact.FS.(*FS)
	assert.Diff(t, input.membership.ignored, map[string]struct{}{"deep/noise.ignored": {}})
	worktree, err := directory.Worktree(input)
	assert.NoError(t, err)
	for _, name := range []string{"content/deep/noise.ignored", "content/cache", "content/cache/data"} {
		_, statErr := worktree.Filesystem().Lstat(name)
		assert.Equal(t, errors.Is(statErr, fs.ErrNotExist), true)
	}
	for _, name := range []string{"outside", "outside.ignored", "content/deep/tracked.ignored", "content/deep/kept"} {
		_, statErr := worktree.Filesystem().Lstat(name)
		assert.NoError(t, statErr)
	}
	scoped, err := worktree.Filesystem().Chroot("content")
	assert.NoError(t, err)
	for _, name := range []string{"deep/noise.ignored", "cache/data"} {
		_, statErr := scoped.Lstat(name)
		assert.Equal(t, errors.Is(statErr, fs.ErrNotExist), true)
	}
	file, err := scoped.Open("deep/tracked.ignored")
	assert.NoError(t, err)
	assert.NoError(t, file.Close())
	entries, err := scoped.ReadDir("/")
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].Name(), "deep")
}

func TestDirectory_IndependentWorktrees(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, ".gitignore", "*.ignored\n")
	for _, name := range []string{"noise.ignored", "nested/kept", "nested/noise.ignored", "pruned/kept"} {
		testutil.WriteFile(t, root, name, name)
	}
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	for _, tt := range []struct {
		name  string
		order []int
	}{
		{name: "unfiltered first", order: []int{0, 1}},
		{name: "admitted first", order: []int{1, 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			directory, err := OpenDirectory(t.Context(), location)
			assert.NoError(t, err)
			defer func() { assert.NoError(t, directory.Close()) }()
			views := []struct {
				current *FS
				root    []string
				nested  []string
				err     error
			}{
				{
					current: newLocal(root, false, &localPolicy{}),
					root:    []string{".gitignore", "nested", "noise.ignored", "pruned"},
					nested:  []string{"kept", "noise.ignored"},
				},
				{
					current: ExcludeDirectory(directory.Artifact(), filepath.Join(root, "pruned")).FS.(*FS),
					root:    []string{".gitignore", "nested"}, nested: []string{"kept"}, err: fs.ErrNotExist,
				},
			}
			for _, index := range tt.order {
				view := views[index]
				worktree, worktreeErr := directory.Worktree(view.current)
				assert.NoError(t, worktreeErr)
				for _, listing := range []struct {
					name string
					want []string
				}{{".", view.root}, {"nested", view.nested}} {
					entries, readErr := worktree.Filesystem().ReadDir(listing.name)
					assert.NoError(t, readErr)
					names := make([]string, len(entries))
					for i, entry := range entries {
						names[i] = entry.Name()
					}
					assert.Diff(t, names, listing.want)
				}
				for _, name := range []string{"noise.ignored", "nested/noise.ignored", "pruned"} {
					_, statErr := worktree.Filesystem().Lstat(name)
					assert.Equal(t, errors.Is(statErr, view.err), true)
				}
				_, err = worktree.Filesystem().ReadDir("pruned")
				assert.Equal(t, errors.Is(err, view.err), true)
			}
		})
	}
}

func TestDirectory_ClosedObservation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "nested/deep/file", "before")
	assert.NoError(t, os.Symlink("file", filepath.Join(dir, "nested", "deep", "link")))
	assert.NoError(t, os.Symlink("nested", filepath.Join(dir, "alias")))
	location, err := gitrepo.Locate(t.Context(), dir, dir)
	assert.NoError(t, err)
	directory, err := OpenDirectory(t.Context(), location)
	assert.NoError(t, err)
	artifact := directory.Artifact()
	observed, err := directory.Worktree(artifact.FS.(*FS))
	assert.NoError(t, err)
	for _, name := range []string{"", "/"} {
		_, readErr := observed.Filesystem().ReadDir(name)
		assert.NoError(t, readErr)
	}
	scoped, err := observed.Filesystem().Chroot("nested")
	assert.NoError(t, err)
	_, err = scoped.Lstat("../alias/deep/file")
	assert.Equal(t, errors.Is(err, fs.ErrInvalid), true)
	info, err := scoped.Lstat("deep/link")
	assert.NoError(t, err)
	assert.Equal(t, info.Mode()&fs.ModeSymlink != 0, true)
	_, err = observed.Filesystem().ReadDir("alias")
	assert.NoError(t, err)
	_, err = observed.Filesystem().Lstat("alias/deep/file")
	assert.FatalIf(t, err == nil, "%s", "go-git accepted a leading symlink")
	_, err = scoped.ReadDir("deep")
	assert.NoError(t, err)
	_, err = scoped.Lstat("deep/file")
	assert.NoError(t, err)
	file, err := scoped.OpenFile("deep/file", os.O_RDONLY, 0)
	assert.NoError(t, err)
	assert.NoError(t, file.Close())
	assert.NoError(t, directory.Close())
	for _, name := range []string{"deep", "unread"} {
		_, err = scoped.ReadDir(name)
		assert.FatalIf(t, !errors.Is(err, os.ErrClosed), "closed observation remains readable: %v", err)
		_, err = scoped.Lstat(name)
		assert.Equal(t, errors.Is(err, os.ErrClosed), true)
		_, err = scoped.Open(name)
		assert.Equal(t, errors.Is(err, os.ErrClosed), true)
		_, err = scoped.OpenFile(name, os.O_RDONLY, 0)
		assert.Equal(t, errors.Is(err, os.ErrClosed), true)
		_, err = scoped.Readlink(name)
		assert.Equal(t, errors.Is(err, os.ErrClosed), true)
		_, err = scoped.Stat(name)
		assert.Equal(t, errors.Is(err, os.ErrClosed), true)
	}
	_, err = observed.Filesystem().Chroot("nested")
	assert.Equal(t, errors.Is(err, os.ErrClosed), true)
	assert.Equal(t, directory.observed.directories == nil, true)
	assert.Equal(t, directory.observed.ctx == nil, true)
	data, err := fs.ReadFile(artifact.FS, "nested/deep/file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "before")
}
