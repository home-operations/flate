package gittree

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	gitindex "github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"

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
	testutil.WriteFile(t, root, ".git/info/exclude", "info-only\npriority\n")
	testutil.WriteFile(t, root, "nested/.gitignore", "!keep.ignored\n")
	for _, name := range []string{"noise.ignored", "blocked/noise.yaml", "nested/noise.ignored", "info-only", "priority", "nested/keep.ignored", "notes.txt"} {
		testutil.WriteFile(t, root, name, name)
	}
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
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
				assert.Equal(t, string(data), name)
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
	independent, err := admitLocal(t, filepath.Join(root, "nested"))
	assert.NoError(t, err)
	_, err = fs.ReadFile(independent.FS, "noise.ignored")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
}

func testNewEntryPolicyGitRuleCapture(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"no_rules", "index_error", "rule_reader_error", "canceled_before", "canceled_during", "canceled_after"} {
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
			case "index_error":
				assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
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
			artifact, err := admitLocal(t, root)
			assert.NoError(t, err)
			input := artifact.FS.(*FS)
			if tt.live {
				input, _, err = New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), plumbing.ZeroHash,
					ViewOptions{Mode: tt.mode, Worktree: osfs.New(root, osfs.WithBoundOS())})
				assert.NoError(t, err)
			}
			_, err = input.ReadFile("noise")
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), tt.name != "plain")
			if tt.name == "linked" {
				data, readErr := input.ReadFile("common-only")
				assert.NoError(t, readErr)
				assert.Equal(t, string(data), "approved linked-worktree limit")
			}
			if tt.live {
				testutil.WriteFile(t, root, ".gitignore", "later\n")
				testutil.WriteFile(t, root, "later", "new ignored file")
				observed, observeErr := input.FreshInputs(t.Context(), nil, nil)
				assert.NoError(t, observeErr)
				assert.Equal(t, observed.Files["later"], "")
				assert.Equal(t, observed.Files["noise"] != "", true)
				_, readErr := input.ReadFile("noise")
				assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
			}
		})
	}
}
