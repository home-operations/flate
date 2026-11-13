package git

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveLocal_WarningsSurviveFailure(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"current entry", "later entry"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			localFixtureResult := localFixture(t)
			root, repo, pin := localFixtureResult.root, localFixtureResult.repo, localFixtureResult.base
			commit, err := repo.CommitObject(pin)
			assert.NoError(t, err)
			tree, err := commit.Tree()
			assert.NoError(t, err)
			tree.Entries = append(tree.Entries, object.TreeEntry{Name: "module", Mode: filemode.Submodule, Hash: pin})
			if failure == "current entry" {
				tree.Entries = append(
					tree.Entries,
					object.TreeEntry{
						Name: "zmissing",
						Mode: filemode.Dir,
						Hash: plumbing.NewHash(strings.Repeat("f", 40)),
					},
				)
			}
			encoded := repo.Storer.NewEncodedObject()
			sort.Sort(object.TreeEntrySorter(tree.Entries))
			assert.NoError(t, tree.Encode(encoded))
			commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
			assert.NoError(t, err)
			encoded = repo.Storer.NewEncodedObject()
			assert.NoError(t, commit.Encode(encoded))
			pin, err = repo.Storer.SetEncodedObject(encoded)
			assert.NoError(t, err)
			first := localSource(&sourcev1.GitRepositoryRef{Commit: pin.String()})
			repositories := []*manifest.GitRepository{first}
			if failure == "later entry" {
				repositories = append(repositories, nil)
			}
			artifacts, err := ResolveLocal(
				t.Context(),
				localLocation(t, root),
				repositories,
				LocalOptions{Revision: ""},
			)
			assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "original failure lost: %v", err)
			artifact := artifacts[first.Named()]
			assert.FatalIf(
				t,
				artifact == nil || artifact.FS != nil || artifact.Key != "" || len(artifact.Warnings) != 1 ||
					artifact.Warnings[0].Resource != first.Named() ||
					artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule,
				"warning-only local result = %+v",
				artifact,
			)
		})
	}
}

func testResolveLocalUnusableObjectsAndStoreCase(
	t *testing.T, kind *string,
) {
	t.Helper()

	localFixtureResult2 := localFixture(t)
	root, repo, a := localFixtureResult2.root, localFixtureResult2.repo, localFixtureResult2.base
	ref := sourcev1.GitRepositoryRef{Tag: "v1.0.0"}
	switch *kind {
	case "tag-target":
		assert.Equal(
			t,
			repo.Storer.SetReference(
				plumbing.NewHashReference(
					plumbing.NewTagReferenceName("v1.0.0"),
					plumbing.NewHash(strings.Repeat("f", 40)),
				),
			),
			nil,
		)
	case "head":
		assert.Equal(
			t,
			repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.HEAD, plumbing.NewHash(strings.Repeat("f", 40))),
			),
			nil,
		)
	case "config":
		testutil.WriteFile(t, root, ".git/config", "[invalid\n")
	case "store", "lookup":
		assert.Equal(t, os.RemoveAll(filepath.Join(root, ".git")), nil)
		if (*kind) == "lookup" {
			assert.Equal(t, os.Symlink(".git", filepath.Join(root, ".git")), nil)
		} else {
			testutil.WriteFile(t, root, ".git", "corrupt gitdir")
		}
	case "tree", "tree-tag", "recursive-tree-tag":
		commit, err := repo.CommitObject(a)
		assert.NoError(t, err)

		commit.TreeHash = plumbing.NewHash(strings.Repeat("f", 40))
		encoded := repo.Storer.NewEncodedObject()
		assert.Equal(t, commit.Encode(encoded), nil)
		hash, err := repo.Storer.SetEncodedObject(encoded)
		assert.Equal(t, err, nil)
		if (*kind) == "tree" {
			ref = sourcev1.GitRepositoryRef{Commit: hash.String()}
		} else {
			assert.Equal(
				t,
				repo.Storer.SetReference(
					plumbing.NewHashReference(plumbing.NewTagReferenceName(ref.Tag), hash),
				),
				nil,
			)
		}
	}
	repository := localSource(&ref)
	repository.RecurseSubmodules = (*kind) == "recursive-tree-tag"
	repository.SparseCheckout = []string{"apps"}
	{
		got, err := ResolveLocal(
			t.Context(),
			localLocation(t, root),
			[]*manifest.GitRepository{repository},
			LocalOptions{Revision: ""},
		)
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
			got != nil, "corruption became fallback: %v, %v", got, err)
	}
}

func testResolveLocalOptionsFallbackAndGitIndirectionCase(
	t *testing.T, root *string,
	option *string,
) {
	t.Helper()
	for _, tag := range []string{"v1.0.0", "v2.0.0"} {
		repository := localSource(&sourcev1.GitRepositoryRef{Tag: tag})
		if (*option) == "sparse" {
			repository.SparseCheckout = []string{"./value.txt"}
		} else {
			repository.RecurseSubmodules = true
		}
		got, err := ResolveLocal(
			t.Context(),
			localLocation(t, (*root)),
			[]*manifest.GitRepository{repository},
			LocalOptions{Revision: ""},
		)
		assert.FatalIf(
			t,
			err != nil || (len(got) != 0) != (tag == "v2.0.0" || (*option) == "sparse"),
			"%s %s = %v, %v", (*option), tag,
			got,
			err,
		)
		if (*option) == "sparse" {
			input := got[repository.Named()]
			data, err := fs.ReadFile(input.FS, "value.txt")
			assert.NoError(t, err)
			want := "A"
			if tag == "v2.0.0" {
				want = "dirty"
			}
			assert.Equal(t, string(data), want)
			_, err = fs.Stat(input.FS, "extra.txt")
			if tag == "v1.0.0" {
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			} else {
				assert.NoError(t, err)
			}
			assert.Diff(t, repository.SparseCheckout, []string{"./value.txt"})
		}
	}
}
