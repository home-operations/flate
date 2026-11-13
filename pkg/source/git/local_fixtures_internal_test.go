package git

import (
	"sort"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

func localFixture(tb testing.TB) localRepository {
	tb.Helper()
	root := tb.TempDir()
	repo := testutil.GitInit(tb, root)
	var hashes [2]plumbing.Hash
	for i, value := range []string{"A", "B"} {
		testutil.WriteFile(tb, root, "value.txt", value)
		testutil.WriteFile(tb, root, "extra.txt", "extra")
		hashes[i] = testutil.GitCommitAll(tb, repo, value, &git.CommitOptions{
			Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
		})
		{
			err := repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName([]string{"v1.0.0", "v2.0.0"}[i]), hashes[i]),
			)
			assert.NoError(tb, err)
		}
	}
	testutil.WriteFile(tb, root, "value.txt", "dirty")
	return localRepository{root: root, repo: repo, base: hashes[0], head: hashes[1]}
}

func localLocation(tb testing.TB, root string) *gitrepo.Location {
	tb.Helper()
	location, err := gitrepo.Locate(tb.Context(), root, root)
	assert.Equal(tb, err, nil)
	return location
}

func submoduleCommit(t *testing.T, repo *git.Repository, hash plumbing.Hash) plumbing.Hash {
	t.Helper()
	commit, err := repo.CommitObject(hash)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	tree.Entries = append(tree.Entries, object.TreeEntry{Name: "module", Mode: filemode.Submodule, Hash: hash})
	sort.Sort(object.TreeEntrySorter(tree.Entries))
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, tree.Encode(encoded))
	commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	hash, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	return hash
}

type localRepository struct {
	root       string
	repo       *git.Repository
	base, head plumbing.Hash
}
