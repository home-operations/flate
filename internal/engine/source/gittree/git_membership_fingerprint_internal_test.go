package gittree

import (
	"errors"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func testFSContentFingerprintGitIgnoredNoise(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "nested/file", "tracked")
	testutil.WriteFile(t, root, ".gitignore", "*.noise\n")
	testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	first, err := admitLocal(t, root)
	assert.NoError(t, err)
	before, err := ContentFingerprint(t.Context(), first.FS)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "desktop.noise", "local")
	testutil.WriteFile(t, root, "nested/desktop.noise", "local")
	second, err := admitLocal(t, root)
	assert.NoError(t, err)
	after, err := ContentFingerprint(t.Context(), second.FS)
	assert.NoError(t, err)
	assert.Equal(t, before, after)
	assert.Equal(t, second.FS.(*FS).FileDigests()["desktop.noise"], "")
	assert.Equal(t, second.FS.(*FS).FileDigests()["nested/desktop.noise"], "")
}

func testFSGitContentEvidenceCommittedMembership(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "old.yaml", "historical")
	testutil.WriteFile(t, root, "excluded.yaml", "Flux excludes this tracked file")
	testutil.WriteFile(t, root, ".sourceignore", "excluded.yaml\n")
	commit := testutil.GitCommitAll(
		t,
		repo,
		"historical inputs",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
	)
	worktree, err := repo.Worktree()
	assert.NoError(t, err)
	_, err = worktree.Remove("old.yaml")
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "old.yaml", "local ignored bytes")
	testutil.WriteFile(t, root, ".gitignore", "*.yaml\n")
	testutil.WriteFile(t, root, ".sourceignore", "old.yaml\n")
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	var keys [2]string
	for i := range keys {
		local, localErr := admitLocal(t, root)
		assert.NoError(t, localErr)
		input, key, newErr := New(t.Context(), backing, commit, ViewOptions{
			Mode: Source, Current: local.FS, Changes: []string{"old.yaml", ".sourceignore"},
		})
		assert.NoError(t, newErr)
		data, readErr := input.ReadFile("old.yaml")
		assert.NoError(t, readErr)
		assert.Equal(t, string(data), "historical")
		_, readErr = input.ReadFile("excluded.yaml")
		assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
		keys[i], newErr = ContentFingerprint(t.Context(), input)
		assert.NoError(t, newErr)
		assert.Equal(t, keys[i], key)
		testutil.WriteFile(t, root, ".gitignore", "other.yaml\n")
		assert.NoError(t, worktree.AddWithOptions(&git.AddOptions{Path: "old.yaml", SkipStatus: true}))
	}
	assert.Equal(t, keys[0], keys[1])
	committed, _, err := New(t.Context(), backing, commit, ViewOptions{
		Mode:    Direct,
		Content: struct{ Revision string }{commit.String()}, Worktree: worktree.Filesystem(),
	})
	assert.NoError(t, err)
	_, err = committed.ReadFile("old.yaml")
	assert.NoError(t, err)
	assert.Equal(t, committed.membership.ignored == nil, true)
	assert.Equal(t, committed.membership.captured, false)
}

func TestContentFingerprint_ExcludedDirectoryOrder(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"cache/body":         "cache",
		"other/body":         "other",
		"cache-sibling/body": "cache-sibling",
		"option/body":        "option",
		"option-tail/body":   "option-tail",
	})
	repo := testutil.GitInit(t, root)
	commit := testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e"},
	})
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	pinned, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: Source, Current: artifact.FS})
	assert.NoError(t, err)
	exclude := func(input *store.SourceArtifact, directories ...string) *store.SourceArtifact {
		for _, directory := range directories {
			input = ExcludeDirectory(input, filepath.Join(root, directory))
		}
		return input
	}

	var keys [5]struct{ view, content string }
	t.Run("membership", func(t *testing.T) {
		for i, tc := range []struct {
			name string
			dirs []string
		}{
			{name: "empty"},
			{name: "single", dirs: []string{"cache"}},
			{name: "two exclusions", dirs: []string{"cache", "other"}},
			{name: "other first in tail", dirs: []string{"cache", "other", "cache-sibling"}},
			{name: "other last in tail", dirs: []string{"cache", "cache-sibling", "other"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				selected := exclude(artifact, tc.dirs...)
				overlay := exclude(&store.SourceArtifact{FS: pinned, LocalPath: root}, tc.dirs...)
				view, viewKey, err := New(t.Context(), backing, commit, ViewOptions{Mode: Source, Current: selected.FS})
				assert.NoError(t, err)
				contentKey, err := ContentFingerprint(t.Context(), overlay.FS)
				assert.NoError(t, err)
				assert.Equal(t, viewKey != "", true)
				assert.Equal(t, contentKey != "", true)
				keys[i].view, keys[i].content = viewKey, contentKey

				assertExcludedDirectoryListing(t, view, tc.dirs)
				assertExcludedDirectoryListing(t, overlay.FS.(*FS), tc.dirs)
			})
		}
	})
	for i := 1; i < 4; i++ {
		assert.Equal(t, keys[i].view != keys[i-1].view, true)
		assert.Equal(t, keys[i].content != keys[i-1].content, true)
	}
	assert.Equal(t, keys[3].view, keys[4].view)
	assert.Equal(t, keys[3].content, keys[4].content)

	t.Run("refresh precedence", func(t *testing.T) {
		t.Parallel()
		for _, directories := range [][2]string{{"cache", "other"}, {"other", "cache"}} {
			primary, retained := directories[0], directories[1]
			t.Run(primary, func(t *testing.T) {
				t.Parallel()
				current := exclude(artifact, primary, retained)
				option := exclude(artifact, "option", "option-tail")
				committed, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: Source, Current: current.FS})
				assert.NoError(t, err)
				view, key, err := New(t.Context(), backing, commit, ViewOptions{
					Mode: Source, Current: current.FS, membership: option.FS.(*FS).membership,
				})
				assert.NoError(t, err)
				effective := exclude(artifact, "option", retained, "option-tail")
				_, equivalent, err := New(
					t.Context(),
					backing,
					commit,
					ViewOptions{Mode: Source, Current: effective.FS},
				)
				assert.NoError(t, err)
				assert.Equal(t, key, equivalent)
				for _, tc := range []struct {
					name  string
					input fs.FS
				}{
					{name: "local", input: current.FS},
					{name: "nested Git", input: committed},
				} {
					t.Run(tc.name, func(t *testing.T) {
						t.Parallel()
						opts := ViewOptions{Mode: Direct, Current: tc.input, membership: option.FS.(*FS).membership}
						refreshed, _, _, err := opts.refreshCurrent(t.Context(), plumbing.ZeroHash)
						assert.NoError(t, err)
						assert.Diff(t, refreshed.membership.excluded, effective.FS.(*FS).membership.excluded)
						for _, input := range []*FS{refreshed, view} {
							assertExcludedDirectoryBodies(t, input, "option", retained, "option-tail")
							data, err := fs.ReadFile(input, primary+"/body")
							assert.NoError(t, err)
							assert.Equal(t, string(data), primary)
						}
					})
				}
			})
		}
	})

	t.Run("included bytes change", testExcludedDirectoryContentChange)

	t.Run("immutable view", func(t *testing.T) {
		t.Parallel()
		immutable, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: Source})
		assert.NoError(t, err)
		refreshed, err := immutable.freshInput(t.Context(), "cache")
		assert.NoError(t, err)
		assert.Equal(t, refreshed, immutable)
		data, err := fs.ReadFile(refreshed, "cache/body")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "cache")
	})

	t.Run("excluded scoped root", func(t *testing.T) {
		t.Parallel()
		scope, err := Sub(pinned, "cache-sibling")
		assert.NoError(t, err)
		scopedRoot := filepath.Join(root, "cache-sibling")
		excluded := ExcludeDirectory(&store.SourceArtifact{FS: scope, LocalPath: scopedRoot}, scopedRoot)
		_, err = excluded.FS.(*FS).FreshInputs(t.Context(), nil, nil)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	})
}

func assertExcludedDirectoryListing(t *testing.T, input *FS, dirs []string) {
	t.Helper()
	visible := []string{"cache", "cache-sibling", "option", "option-tail", "other"}
	visible = slices.DeleteFunc(visible, func(dir string) bool { return slices.Contains(dirs, dir) })
	visibleFiles := make([]string, len(visible))
	for i, dir := range visible {
		visibleFiles[i] = dir + "/body"
	}
	slices.Sort(visibleFiles)
	entries, err := fs.ReadDir(input, ".")
	assert.NoError(t, err)
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	assert.Diff(t, names, visible)
	observed, err := input.FreshInputs(t.Context(), nil, nil)
	assert.NoError(t, err)
	assert.Diff(t, slices.Sorted(maps.Keys(observed.Files)), visibleFiles)
	assertExcludedDirectoryBodies(t, input, dirs...)
}

func testExcludedDirectoryContentChange(t *testing.T) {
	t.Helper()
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "cache/body", "excluded")
	var keys [2]string
	for i, body := range []string{"before", "after"} {
		testutil.WriteFile(t, root, "body", body)
		input, err := admitLocal(t, root)
		assert.NoError(t, err)
		input = ExcludeDirectory(input, filepath.Join(root, "cache"))
		data, err := fs.ReadFile(input.FS, "body")
		assert.NoError(t, err)
		assert.Equal(t, string(data), body)
		assertExcludedDirectoryBodies(t, input.FS, "cache")
		keys[i], err = ContentFingerprint(t.Context(), input.FS)
		assert.NoError(t, err)
		assert.Equal(t, keys[i] != "", true)
	}
	assert.Equal(t, keys[0] != keys[1], true)
}

func assertExcludedDirectoryBodies(t *testing.T, input fs.FS, dirs ...string) {
	t.Helper()
	for _, dir := range dirs {
		_, err := fs.ReadFile(input, dir+"/body")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
}
