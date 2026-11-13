package gittree

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func testNewWarningsSurviveFailureCase(t *testing.T, failure *string,
) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	entries := []object.TreeEntry{
		{Name: "module", Mode: filemode.Submodule, Hash: plumbing.NewHash(strings.Repeat("a", 40))},
	}
	if (*failure) == "traversal" {
		entries = append(
			entries,
			object.TreeEntry{
				Name: "zmissing",
				Mode: filemode.Dir,
				Hash: plumbing.NewHash(strings.Repeat("b", 40)),
			},
		)
	} else {
		encoded := repo.Storer.NewEncodedObject()
		assert.NoError(t, (&object.Tree{}).Encode(encoded))
		hash, err := repo.Storer.SetEncodedObject(encoded)
		assert.NoError(t, err)
		name := ".sourceignore"
		if (*failure) == "refresh" {
			name = "zsubtree"
		}
		entries = append(entries, object.TreeEntry{Name: name, Mode: filemode.Dir, Hash: hash})
	}
	encoded := repo.Storer.NewEncodedObject()
	sort.Sort(object.TreeEntrySorter(entries))
	assert.NoError(t, (&object.Tree{Entries: entries}).Encode(encoded))
	treeHash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, (&object.Commit{TreeHash: treeHash}).Encode(encoded))
	commit, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	opts := ViewOptions{}
	if (*failure) == "refresh" {
		opts = ViewOptions{Mode: Direct, Content: "captured", Current: fstest.MapFS{}}
	}
	if (*failure) == "identity" {
		opts.Content = make(chan int)
	}
	input, identity, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, opts)
	if (*failure) == "refresh" {
		assert.NoError(t, err)
		for _, entry := range entries {
			if entry.Name == "zsubtree" {
				oid := entry.Hash.String()
				assert.NoError(
					t,
					os.Remove(
						filepath.Join(
							repo.Storer.(*filesystem.Storage).Filesystem().Root(),
							"objects",
							oid[:2],
							oid[2:],
						),
					),
				)
			}
		}
		input, identity, err = New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{Mode: Direct, Current: input},
		)
	}
	assert.FatalIf(
		t,
		err == nil || identity != "" || input == nil || input.view != nil || len(input.Warnings) != 1 ||
			input.Warnings[0].Category != manifest.WarnSkippedSubmodule ||
			input.Warnings[0].Detail[0] != "module",
		"failed admission = %+v, %q, %v",
		input,
		identity,
		err,
	)
	assert.FatalIf(t, (*failure) == "identity" &&
		(!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || err.Error() != "git view content cannot be fingerprinted"), "identity diagnostic lost its cause or classification: %v", err)
	assert.FatalIf(
		t, (*failure) ==
			"traversal" && !errors.Is(err, plumbing.ErrObjectNotFound), "original error lost: %v",
		err,
	)
}
