package gittree

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/store"
)

func testFSGitContentEvidenceCase(t *testing.T, mode *string,
) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "dir/file", "alpha")
	testutil.WriteFile(t, root, "dir/ignored", "ignored")
	testutil.WriteFile(t, root, ".sourceignore", "dir/ignored\n")
	old := time.Unix(946684800, 0)
	assert.NoError(t, os.Chtimes(filepath.Join(root, "dir", "file"), old, old))
	commit := testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: old}},
	)
	want := "alpha"
	if (*mode) == "dirty" || (*mode) == "staged" || (*mode) == "racy" ||
		(*mode) == "routed" {
		want = "bravo"
		testutil.WriteFile(t, root, "dir/file", want)
	}
	if (*mode) == "staged" {
		assert.NoError(t, os.Chtimes(filepath.Join(root, "dir", "file"), old, old))
		wt, err := repo.Worktree()
		assert.NoError(t, err)
		_, err = wt.Add("dir/file")
		assert.NoError(t, err)
	}
	if (*mode) == "racy" {
		assert.NoError(t, os.Chtimes(filepath.Join(root, "dir", "file"), old, old))
		assert.NoError(t, os.Chtimes(filepath.Join(root, ".git", "index"), old, old))
	}
	if (*mode) == "untracked" {
		testutil.WriteFile(t, root, "dir/new", "untracked")
	}
	contentRoot, name := root, "dir/file"
	if (*mode) == "subtree" {
		contentRoot, name = filepath.Join(root, "dir"), "file"
	}
	location, err := gitrepo.Locate(t.Context(), contentRoot, contentRoot)
	assert.NoError(t, err)
	reader := &contentReadBilly{Filesystem: osfs.New(root)}
	location.Repository, err = git.Open(repo.Storer, reader)
	assert.NoError(t, err)
	input, err := AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	objects := &contentReadBilly{Filesystem: repo.Storer.(*filesystem.Storage).Filesystem(), all: true}
	if (*mode) == "immutable" || (*mode) == "scoped_immutable" ||
		(*mode) == "direct_immutable" {
		viewMode := Source
		if (*mode) == "direct_immutable" {
			viewMode = Direct
		}
		input.FS, _, err = New(t.Context(), objects, commit, ViewOptions{Mode: viewMode})
		assert.NoError(t, err)
	}
	if (*mode) == "sdk" {
		want = "sdkdata"
		input.FS, _, err = New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{
				Mode:     Direct,
				Location: location,
				Current:  fstest.MapFS{"dir/file": {Data: []byte(want)}},
			},
		)
		assert.NoError(t, err)
	}
	testFSGitContentEvidenceCaseCompletion(
		t,
		&testFSGitContentEvidenceCaseCompletionInputs{
			mode:    &mode,
			input:   &input,
			name:    &name,
			want:    &want,
			commit:  &commit,
			reader:  &reader,
			err:     &err,
			objects: &objects,
			root:    &root,
		},
	)
}

func testFSGitContentEvidenceCaseCompletion(t *testing.T, captures *testFSGitContentEvidenceCaseCompletionInputs) {
	t.Helper()
	if (*(*captures.mode)) == "scoped_local" || (*(*captures.mode)) == "scoped_immutable" {
		current, subErr := Sub((*captures.input).FS, "dir")
		assert.NoError(t, subErr)
		(*captures.input).
			FS, _, subErr = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: current})
		assert.NoError(t, subErr)
		(*captures.name) = "file"
	}
	if (*(*captures.mode)) == "source_unreadable" || (*(*captures.mode)) == "routed" {
		opts := ViewOptions{Mode: Source, Current: (*captures.input).FS}
		ref := plumbing.ZeroHash
		if (*(*captures.mode)) == "routed" {
			(*captures.want), ref = "alpha", (*captures.commit)
			opts.Worktree, opts.Changes = (*captures.reader), []string{"dir/file"}
		}
		(*captures.input).
			FS, _, (*captures.err) = New(t.Context(), (*captures.objects), ref, opts)
		assert.NoError(t, (*captures.err))
	}
	selected := (*captures.input).FS.(*FS)
	if (*(*captures.mode)) == "unlocated" {
		selected.location = nil
	}
	_, (*captures.err) = selected.SourcePolicy(t.Context())
	assert.NoError(t, (*captures.err))
	(*captures.objects).
		reads.Store(0)
	if (*(*captures.mode)) == "unreadable_clean" || (*(*captures.mode)) == "source_unreadable" ||
		(*(*captures.mode)) == "subtree" ||
		(*(*captures.mode)) ==
			"scoped_local" {
		file := filepath.Join((*captures.root), "dir", "file")
		assert.NoError(t, os.Chmod(file, 0))
		t.Cleanup(func() { assert.NoError(t, os.Chmod(file, 0o644)) })
		if _, readFileErr3 := os.ReadFile(file); !errors.Is(readFileErr3, fs.ErrPermission) {
			t.Skipf("filesystem cannot enforce unreadable file: %v", readFileErr3)
		}
	}
	assert.Equal(t, selected.FileDigests() == nil, true)
	_, (*captures.err) = ContentFingerprint(t.Context(), selected)
	assert.NoError(t, (*captures.err))
	assert.Equal(
		t,
		selected.FileDigests()[(*captures.name)],
		manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte((*captures.want)))),
	)
	if (*(*captures.mode)) == "untracked" {
		assert.Equal(
			t,
			selected.FileDigests()["dir/new"],
			manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte("untracked"))),
		)
	}
	if (*(*captures.mode)) != "subtree" {
		_, ignored := selected.FileDigests()["dir/ignored"]
		assert.Equal(t, ignored, false)
	}
	reads := (*captures.reader).reads.Load()
	assert.Equal(t, (*captures.objects).reads.Load(), int64(0))
	_, (*captures.err) = ContentFingerprint(t.Context(), selected)
	assert.NoError(t, (*captures.err))
	assert.Equal(t, (*captures.reader).reads.Load(), reads)
	if (*(*captures.mode)) == "immutable" || (*(*captures.mode)) == "direct_immutable" {
		var group sync.WaitGroup
		for _, requested := range [][]string{nil, {(*captures.name)}} {
			group.Go(func() {
				observed, err := selected.FreshInputs(t.Context(), requested)
				assert.NoError(t, err)
				assert.Equal(t, observed.Files[(*captures.name)], selected.FileDigests()[(*captures.name)])
				assert.Equal(t, observed.Types["dir"].IsDir(), true)
				kind, present := observed.Types[(*captures.name)]
				assert.Equal(t, present && kind.IsRegular(), true)
			})
		}
		group.Wait()
		assert.Equal(t, (*captures.objects).reads.Load(), int64(0))
	}
}

type testFSGitContentEvidenceCaseCompletionInputs struct {
	mode    **string
	input   **store.SourceArtifact
	name    *string
	want    *string
	commit  *plumbing.Hash
	reader  **contentReadBilly
	err     *error
	objects **contentReadBilly
	root    *string
}
