package gittree

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
	gitindex "github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/utils/merkletrie"
	merklefs "github.com/go-git/go-git/v5/utils/merkletrie/filesystem"
	merkleindex "github.com/go-git/go-git/v5/utils/merkletrie/index"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

func testNewEntryPolicyGitIgnoredMembership(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	for _, name := range []string{"tracked.ignored", "blocked/tracked.yaml"} {
		testutil.WriteFile(t, root, name, name)
	}
	testutil.GitCommitAll(
		t,
		repo,
		"tracked inputs",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
	)
	testutil.WriteFile(t, root, ".gitignore", "*.ignored\nblocked/\n!priority\n")
	testutil.WriteFile(t, root, "tracked.ignored", "modified tracked input")
	testutil.WriteFile(t, root, ".git/info/exclude", "info-only\npriority\n")
	testutil.WriteFile(t, root, "nested/.gitignore", "!keep.ignored\n")
	for _, name := range []string{"noise.ignored", "blocked/noise.yaml", "nested/noise.ignored", "info-only", "priority", "nested/keep.ignored", "notes.txt"} {
		testutil.WriteFile(t, root, name, name)
	}
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	artifact, err := AdmitDirectory(ctx, location)
	assert.NoError(t, err)
	cancel()
	index := &membershipIndexStorer{Storer: artifact.FS.(*FS).location.Repository.Storer, err: fs.ErrPermission}
	artifact.FS.(*FS).location.Repository.Storer = index
	var readers sync.WaitGroup
	for _, mode := range []ViewMode{Direct, Source} {
		readers.Go(func() {
			input, _, newErr := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: artifact.FS})
			assert.NoError(t, newErr)
			for _, name := range []string{"noise.ignored", "blocked/noise.yaml", "nested/noise.ignored", "info-only"} {
				_, readErr := input.ReadFile(name)
				assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
				_, statErr := input.Stat(name)
				assert.Equal(t, errors.Is(statErr, fs.ErrNotExist), true)
			}
			for _, name := range []string{"tracked.ignored", "blocked/tracked.yaml", "priority", "nested/keep.ignored", "notes.txt"} {
				data, readErr := input.ReadFile(name)
				assert.NoError(t, readErr)
				want := name
				if name == "tracked.ignored" {
					want = "modified tracked input"
				}
				assert.Equal(t, string(data), want)
			}
		})
	}
	readers.Wait()
	assert.Equal(t, index.calls, 0)
	input := artifact.FS.(*FS)
	for _, name := range []string{"noise.ignored", "blocked/noise.yaml"} {
		_, err = input.Open(name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		_, err = input.OpenRegular(name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		_, err = input.Lstat(name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		_, err = input.ReadLink(name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
	for _, view := range []*FS{input, input.OperationView()} {
		entries, readErr := view.ReadDir("blocked")
		assert.NoError(t, readErr)
		assert.Equal(t, len(entries), 1)
		assert.Equal(t, entries[0].Name(), "tracked.yaml")
		directory, openErr := view.Open("blocked")
		assert.NoError(t, openErr)
		listed, listErr := directory.(fs.ReadDirFile).ReadDir(1)
		assert.NoError(t, listErr)
		assert.Equal(t, len(listed), 1)
		assert.Equal(t, listed[0].Name(), "tracked.yaml")
		_, listErr = directory.(fs.ReadDirFile).ReadDir(1)
		assert.Equal(t, errors.Is(listErr, io.EOF), true)
		assert.NoError(t, directory.Close())
	}
	sub, err := Sub(input, "nested")
	assert.NoError(t, err)
	_, err = fs.ReadFile(sub, "noise.ignored")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	data, err := fs.ReadFile(sub, "keep.ignored")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "nested/keep.ignored")
	assert.Equal(t, index.calls, 0)
	independent, err := admitLocal(t, filepath.Join(root, "nested"))
	assert.NoError(t, err)
	_, err = fs.ReadFile(independent.FS, "noise.ignored")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
}

//nolint:gocognit // Each observation case retains its backing-I/O and upstream protection assertions.
func testNewEntryPolicyGitCaptureObservations(t *testing.T) {
	t.Helper()
	t.Run("shared traversals", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		for _, name := range []string{"tracked.ignored", "nested/tracked"} {
			testutil.WriteFile(t, root, name, "tracked")
		}
		testutil.GitCommitAll(
			t,
			repo,
			"tracked inputs",
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
		)
		testutil.WriteFile(t, root, "tracked.ignored", "changed bytes")
		testutil.WriteFile(t, root, ".gitignore", "*.ignored\npruned/\n")
		testutil.WriteFile(t, root, ".git/info/exclude", "info-only\n")
		for _, name := range []string{"noise.ignored", "pruned/deep/noise", "info-only"} {
			testutil.WriteFile(t, root, name, "untracked")
		}
		assert.NoError(t, os.Mkdir(filepath.Join(root, "empty"), 0o755))
		assert.NoError(t, os.Symlink("nested", filepath.Join(root, "alias")))
		backing := &membershipCountingFS{Filesystem: newMembershipRoot(t, root), reads: make(map[string]int)}
		observed := &membershipFilesystem{
			Filesystem:  backing,
			ctx:         t.Context(),
			directories: make(map[string]membershipDirectory),
		}
		patterns, err := gitignore.ReadPatterns(observed, nil)
		assert.NoError(t, err)
		assert.Equal(t, backing.opens, 2)
		assert.Equal(t, gitignore.NewMatcher(patterns).Match([]string{"info-only"}, false), true)
		assert.Equal(t, backing.reads["pruned"], 0)
		assert.Equal(t, backing.reads["empty"], 1)
		protected, err := git.Open(repo.Storer, observed)
		assert.NoError(t, err)
		worktree, err := protected.Worktree()
		assert.NoError(t, err)
		idx, err := repo.Storer.Index()
		assert.NoError(t, err)
		_, err = merkletrie.DiffTreeContext(
			t.Context(),
			merkleindex.NewRootNode(idx),
			merklefs.NewRootNode(worktree.Filesystem, nil),
			membershipEqual,
		)
		assert.NoError(t, err)
		assert.Equal(t, backing.payloadOpens, 0)
		assert.Equal(t, backing.lstats, 0)
		if diff := cmp.Diff(
			map[string]int{".": 1, "empty": 1, "nested": 1, "pruned": 1, filepath.Join("pruned", "deep"): 1},
			backing.reads,
		); diff != "" {
			t.Fatalf("directory reads (-want +got):\n%s", diff)
		}
		entries, err := observed.ReadDir("")
		assert.NoError(t, err)
		assert.Equal(
			t,
			slices.ContainsFunc(entries, func(entry fs.FileInfo) bool { return entry.Name() == ".git" }),
			true,
		)
		file, err := observed.Open(".git/info/exclude")
		assert.NoError(t, err)
		data, err := io.ReadAll(file)
		assert.NoError(t, err)
		assert.NoError(t, file.Close())
		assert.Equal(t, string(data), "info-only\n")
		_, err = observed.ReadDir(".git/info")
		assert.NoError(t, err)
		_, err = worktree.Filesystem.Open(".git/info/exclude")
		assert.Equal(t, err != nil, true)
		_, err = worktree.Filesystem.ReadDir(".git/info")
		assert.Equal(t, err != nil, true)
		assert.Equal(t, backing.reads["."], 1)
		assert.Equal(t, backing.reads[filepath.Join(".git", "info")], 1)
		_, err = observed.ReadDir("alias")
		assert.NoError(t, err)
		file, err = observed.Open("alias/tracked")
		assert.NoError(t, err)
		assert.NoError(t, file.Close())
		_, err = worktree.Filesystem.Open("alias/tracked")
		assert.Equal(t, err != nil, true)
	})
	for _, mode := range []string{"root", "empty", "error", "canceled", "canceled unread", "canceled open", "canceled open after observation", "canceled lstat"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "z", "z")
			testutil.WriteFile(t, root, "a", "a")
			assert.NoError(t, os.Mkdir(filepath.Join(root, "empty"), 0o755))
			backing := &membershipCountingFS{
				Filesystem: newMembershipRoot(t, root),
				reads:      make(map[string]int),
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			observed := &membershipFilesystem{
				Filesystem:  backing,
				ctx:         ctx,
				directories: make(map[string]membershipDirectory),
			}
			if mode == "canceled open after observation" {
				testutil.WriteFile(t, root, "empty/kept", "kept")
				_, err := observed.ReadDir(".")
				assert.NoError(t, err)
				backing.cancel = cancel
				_, err = observed.Open("empty/kept")
				assert.Equal(t, errors.Is(err, context.Canceled), true)
				assert.Equal(t, backing.payloadOpens, 0)
				assert.Equal(t, backing.reads["."], 1)
				assert.Equal(t, backing.reads["empty"], 1)
				return
			}
			if mode == "canceled lstat" {
				_, err := observed.ReadDir(".")
				assert.NoError(t, err)
				cancel()
				_, err = observed.Lstat("a")
				assert.Equal(t, errors.Is(err, context.Canceled), true)
				assert.Equal(t, backing.lstats, 0)
				return
			}
			if mode == "canceled unread" {
				cancel()
				_, err := observed.ReadDir("empty")
				assert.Equal(t, errors.Is(err, context.Canceled), true)
				assert.Equal(t, len(backing.reads), 0)
				return
			}
			if mode == "canceled open" {
				cancel()
				_, err := observed.Open("a")
				assert.Equal(t, errors.Is(err, context.Canceled), true)
				assert.Equal(t, backing.payloadOpens, 0)
				return
			}
			name := ""
			if mode == "empty" || mode == "error" {
				name = "empty"
			}
			if mode == "error" {
				backing.err = fs.ErrPermission
			}
			entries, err := observed.ReadDir(name)
			assert.Equal(t, errors.Is(err, fs.ErrPermission), mode == "error")
			if mode == "empty" || mode == "error" {
				_, statErr := observed.Lstat("empty/missing")
				assert.Equal(t, errors.Is(statErr, fs.ErrPermission), mode == "error")
				assert.Equal(t, errors.Is(statErr, fs.ErrNotExist), mode == "empty")
				assert.Equal(t, backing.lstats, 0)
			}
			if mode == "canceled" {
				cancel()
			}
			again, err := observed.ReadDir(observed.Join(".", name))
			switch mode {
			case "error":
				assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
			case "canceled":
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			default:
				assert.NoError(t, err)
				assert.Equal(t, len(again), len(entries))
				if mode == "root" {
					for i, entry := range entries {
						assert.Equal(t, again[i].Name(), entry.Name())
					}
				}
			}
			assert.Equal(t, backing.reads[observed.Join(".", name)], 1)
		})
	}
}

func testNewEntryPolicyGitMembershipReplacements(t *testing.T) {
	t.Helper()
	for _, trackedDirectory := range []bool{false, true} {
		name := "file to directory"
		if trackedDirectory {
			name = "directory to file"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			tracked := "swap"
			if trackedDirectory {
				tracked = "swap/tracked"
			}
			testutil.WriteFile(t, root, tracked, "tracked")
			testutil.GitCommitAll(
				t,
				repo,
				"tracked input",
				&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
			)
			assert.NoError(t, os.RemoveAll(filepath.Join(root, "swap")))
			testutil.WriteFile(t, root, ".gitignore", "swap\n")
			replacement := "swap/ignored"
			if trackedDirectory {
				replacement = "swap"
			}
			testutil.WriteFile(t, root, replacement, "untracked replacement")
			input, err := admitLocal(t, root)
			assert.NoError(t, err)
			_, err = fs.ReadFile(input.FS, replacement)
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		})
	}
}

type membershipCountingFS struct {
	billy.Filesystem

	reads        map[string]int
	opens        int
	lstats       int
	cancel       context.CancelFunc
	payloadOpens int
	err          error
}

func (f *membershipCountingFS) ReadDir(name string) ([]fs.FileInfo, error) {
	f.reads[f.Join(".", name)]++
	if f.err != nil {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: f.err}
	}
	entries, err := f.Filesystem.ReadDir(name)
	if f.cancel != nil {
		f.cancel()
	}
	return entries, err
}

func (f *membershipCountingFS) Lstat(name string) (fs.FileInfo, error) {
	f.lstats++
	return f.Filesystem.Lstat(name)
}

//nolint:ireturn // The billy filesystem contract requires billy.File.
func (f *membershipCountingFS) Open(name string) (billy.File, error) {
	f.opens++
	base := filepath.Base(name)
	if base != ".gitignore" && base != ".gitmodules" &&
		!strings.HasSuffix(filepath.ToSlash(name), ".git/info/exclude") {
		f.payloadOpens++
	}
	return f.Filesystem.Open(name)
}

func testNewEntryPolicyGitRuleCapture(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"no_rules", "index_error", "rule_reader_error", "canceled_before", "canceled_during", "canceled_index_error", "canceled_after"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "noise", "untracked")
			if mode != "no_rules" {
				testutil.WriteFile(t, root, ".gitignore", "noise\n")
			}
			location, err := gitrepo.Locate(t.Context(), root, root)
			assert.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			index := &membershipIndexStorer{Storer: repo.Storer, err: fs.ErrPermission}
			location.Repository.Storer = index
			switch mode {
			case "rule_reader_error":
				location.RepositoryRoot = filepath.Join(root, "missing")
			case "canceled_before":
				cancel()
			case "canceled_during":
				index.err, index.cancel = nil, cancel
			case "canceled_index_error":
				index.cancel = cancel
			case "canceled_after":
				index.err = nil
			case "no_rules", "index_error":
			}
			input, err := AdmitDirectory(ctx, location)
			switch mode {
			case "no_rules":
				assert.NoError(t, err)
				assert.Equal(t, index.calls, 0)
				data, readErr := fs.ReadFile(input.FS, "noise")
				assert.NoError(t, readErr)
				assert.Equal(t, string(data), "untracked")
			case "index_error", "canceled_index_error":
				assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
				assert.Equal(t, errors.Is(err, context.Canceled), false)
				assert.Equal(t, index.calls, 1)
			case "rule_reader_error":
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
				assert.Equal(t, index.calls, 0)
			case "canceled_before", "canceled_during":
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			case "canceled_after":
				assert.NoError(t, err)
				cancel()
				_, _, err = New(ctx, nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: input.FS})
				assert.Equal(t, errors.Is(err, context.Canceled), true)
				_, err = fs.ReadFile(input.FS, "noise")
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
		})
	}
}

type membershipIndexStorer struct {
	storage.Storer

	err    error
	cancel context.CancelFunc
	calls  int
}

func (s *membershipIndexStorer) Index() (*gitindex.Index, error) {
	s.calls++
	if s.cancel != nil {
		s.cancel()
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.Storer.Index()
}

func testNewEntryPolicyGitAndFluxPolicy(t *testing.T) {
	t.Helper()
	for _, ignoredPolicy := range []bool{false, true} {
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "tracked.yaml", "tracked")
		testutil.GitCommitAll(
			t,
			repo,
			"tracked input",
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
		)
		rules := "*.yaml\n"
		if ignoredPolicy {
			rules += ".sourceignore\n"
		}
		testutil.WriteFile(t, root, ".gitignore", rules)
		testutil.WriteFile(t, root, ".sourceignore", "tracked.yaml\n!noise.yaml\n")
		testutil.WriteFile(t, root, "noise.yaml", "ignored")
		artifact, err := admitLocal(t, root)
		assert.NoError(t, err)
		direct := artifact.FS.(*FS)
		data, err := direct.ReadFile("tracked.yaml")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "tracked")
		input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: direct})
		assert.NoError(t, err)
		_, err = input.ReadFile("noise.yaml")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		_, err = input.ReadFile("tracked.yaml")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), !ignoredPolicy)
		_, err = direct.ReadFile(".sourceignore")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), ignoredPolicy)
	}
}

//nolint:gocognit // The route table retains plain, live, and linked admission assertions together.
func testNewEntryPolicyGitAdmissionRoutes(t *testing.T) {
	t.Helper()
	for _, tt := range []struct {
		name string
		mode ViewMode
		live bool
	}{
		{name: "plain"},
		{name: "live_direct", mode: Direct, live: true},
		{name: "live_source", mode: Source, live: true},
		{name: "linked"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			var repo *git.Repository
			if tt.name != "plain" {
				repo = testutil.GitInit(t, root)
				testutil.WriteFile(t, root, "tracked", "tracked")
				testutil.GitCommitAll(
					t,
					repo,
					"fixture",
					&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
				)
			}
			if tt.name == "linked" {
				common := root
				root = t.TempDir()
				metadata := filepath.Join(common, ".git", "worktrees", "linked")
				testutil.WriteFile(t, root, ".git", "gitdir: "+metadata+"\n")
				testutil.WriteFile(t, metadata, "commondir", "../..\n")
				testutil.WriteFile(t, metadata, "HEAD", "ref: refs/heads/master\n")
				testutil.WriteFile(t, common, ".git/info/exclude", "common-only\n")
				testutil.WriteFile(t, root, "common-only", "approved linked-worktree limit")
			}
			testutil.WriteFile(t, root, ".gitignore", "noise\n")
			testutil.WriteFile(t, root, "noise", "untracked")
			location, err := gitrepo.Locate(t.Context(), root, root)
			assert.NoError(t, err)
			var index *membershipIndexStorer
			if repo != nil {
				index = &membershipIndexStorer{Storer: location.Repository.Storer}
				location.Repository.Storer = index
			}
			artifact, err := AdmitDirectory(t.Context(), location)
			assert.NoError(t, err)
			if index != nil {
				assert.Equal(t, index.calls, 1)
			}
			input := artifact.FS.(*FS)
			if tt.live {
				input, _, err = New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), plumbing.ZeroHash,
					ViewOptions{Mode: tt.mode, Worktree: osfs.New(root, osfs.WithBoundOS())})
				assert.NoError(t, err)
				index = &membershipIndexStorer{Storer: input.location.Repository.Storer}
				input.location.Repository.Storer = index
			}
			_, err = input.ReadFile("noise")
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), tt.name != "plain")
			if tt.live {
				assert.Equal(t, index.calls, 0)
			}
			if tt.name == "linked" {
				data, readErr := input.ReadFile("common-only")
				assert.NoError(t, readErr)
				assert.Equal(t, string(data), "approved linked-worktree limit")
			}
			if tt.live {
				testutil.WriteFile(t, root, ".gitignore", "later\n")
				testutil.WriteFile(t, root, "later", "new ignored file")
				observed, observeErr := input.FreshInputs(t.Context(), nil)
				assert.NoError(t, observeErr)
				assert.Equal(t, index.calls, 1)
				assert.Equal(t, observed.Files["later"], "")
				assert.Equal(t, observed.Files["noise"] != "", true)
				_, readErr := input.ReadFile("noise")
				assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
				assert.Equal(t, index.calls, 1)
			}
		})
	}
}

func newMembershipRoot(t *testing.T, directory string) *membershipRoot {
	t.Helper()
	root, err := os.OpenRoot(directory)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, root.Close()) })
	return &membershipRoot{Filesystem: osfs.New(directory, osfs.WithBoundOS()), root: root}
}
