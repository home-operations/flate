package git

import (
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func localFixture(tb testing.TB) (string, *git.Repository, plumbing.Hash, plumbing.Hash) {
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
	return root, repo, hashes[0], hashes[1]
}

func localLocation(tb testing.TB, root string) *gitrepo.Location {
	tb.Helper()
	location, err := gitrepo.Locate(tb.Context(), root, root)
	assert.Equal(tb, err, nil)
	return location
}

func TestResolveLocal_WarningsSurviveFailure(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"current entry", "later entry"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			root, repo, pin, _ := localFixture(t)
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
			artifacts, err := ResolveLocal(t.Context(), localLocation(t, root), repositories, "")
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

func testResolveLocalReferencesCase(t *testing.T, root *string,
	b *plumbing.Hash,
	tc *struct {
		name string
		ref  sourcev1.GitRepositoryRef
		want plumbing.Hash
		fail bool
	},
) {
	t.Helper()

	repository := localSource(&tc.ref)

	artifacts, err := ResolveLocal(
		t.Context(),
		localLocation(t, (*root)),
		[]*manifest.GitRepository{repository},
		"",
	)
	if tc.fail {
		assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux), "expected domain error, got %v", err)
		return
	}
	assert.NoError(t, err)

	artifact := artifacts[repository.Named()]
	if tc.want.IsZero() {
		assert.FatalIf(t, artifact != nil, "lookup miss installed an artifact: %+v", artifact)
		return
	}
	if artifact == nil || artifact.Revision != tc.want.String() || artifact.URL != repository.URL {
		t.Fatalf("artifact = %+v, want revision %s", artifact, tc.want)
	}
	assert.FatalIf(
		t,
		artifact.FS == nil || (artifact.Key == "") != (tc.want == (*b)),
		"FS/Key state did not distinguish HEAD from committed content: %+v",
		artifact,
	)
	view, pin, err := source.OperationView(t.Context(), artifact.FS)
	assert.NoError(t, err)
	assert.Equal(t, artifact.Content, pin)
	if artifact.Key == "" {
		assert.Equal(t, pin, "")
	} else {
		assert.Equal(t, pin, artifact.Key)
	}
	data, err := fs.ReadFile(view, "value.txt")
	assert.NoError(t, err)
	wantBody := "A"
	if tc.want == (*b) {
		wantBody = "dirty"
	}
	assert.Equal(t, string(data), wantBody)
	assert.FatalIf(t, artifact.ContentFingerprint == nil, "%s", "local source content callback is absent")

	fingerprint, err := artifact.ContentFingerprint(t.Context(), artifact.FS)
	assert.NoError(t, err)
	assert.Equal(t, fingerprint != "", true)
	assert.Equal(
		t,
		artifact.FS.(*gittree.FS).FileDigests()["value.txt"],
		manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte(wantBody))),
	)

	if (artifact.LocalPath == (*root)) != (tc.want == (*b)) {
		t.Fatalf("unexpected artifact path %q", artifact.LocalPath)
	}
}

func testResolveLocalReferencesCase2(t *testing.T, metadata *string,
) {
	t.Helper()

	root, _, _, head := localFixture(t)
	metadataRoot := filepath.Join(root, ".git")
	if (*metadata) == "gitfile" {
		metadataRoot = filepath.Join(t.TempDir(), "repository")
		assert.Equal(t, os.Rename(filepath.Join(root, ".git"), metadataRoot), nil)
		testutil.WriteFile(t, root, ".git", "gitdir: "+metadataRoot+"\n")
	}
	testutil.WriteFile(
		t,
		metadataRoot,
		"config",
		"[remote \"origin\"]\nurl = https://fixture.invalid/original\n",
	)
	testutil.WriteFile(t, root, "nested/.git", "gitdir: absent\n")
	location := localLocation(t, root)
	assert.Equal(t, os.Remove(filepath.Join(metadataRoot, "config")), nil)
	artifacts, err := ResolveLocal(
		t.Context(),
		location,
		[]*manifest.GitRepository{localSource(&sourcev1.GitRepositoryRef{Tag: "v2.0.0"})},
		"",
	)
	assert.Equal(t, err, nil)
	artifact := artifacts[localSource(&sourcev1.GitRepositoryRef{Tag: "v2.0.0"}).Named()]
	assert.FatalIf(
		t,
		artifact == nil || artifact.Revision != head.String(),
		"%s",
		"retained repository did not resolve HEAD",
	)

	assert.Diff(t, location.URLs, []string{"https://fixture.invalid/original"})
	for _, name := range []string{".git", ".git/config", "nested/.git"} {
		_, err := fs.Stat(artifact.FS, name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		f, err := artifact.FS.Open(name)
		if f != nil {
			f.Close()
		}
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		_, err = fs.ReadFile(artifact.FS, name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
	for _, name := range []string{".", "nested"} {
		entries, err := fs.ReadDir(artifact.FS, name)
		assert.Equal(t, err, nil)
		for _, entry := range entries {
			assert.FatalIf(t, entry.Name() == ".git", "ReadDir(%q) exposes Git metadata", name)
		}
	}
}

func testResolveLocalUnusableObjectsAndStoreCase(
	t *testing.T, kind *string,
) {
	t.Helper()

	root, repo, a, _ := localFixture(t)
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
			"",
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
			"",
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

func testCheckoutRefPreservesOriginFirstBranchLookupCase(
	t *testing.T, kind *string,
) {
	t.Helper()

	src, remote, first, second := localFixture(t)
	branch := plumbing.NewBranchReferenceName("pinned")
	assert.NoError(t, remote.Storer.SetReference(plumbing.NewHashReference(branch, first)))
	assert.NoError(t, remote.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, branch)))
	var advertisements atomic.Int32
	handler := gitHTTPHandler(t, src)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && advertisements.Add(1) == 2 {
			if err := remote.Storer.SetReference(plumbing.NewHashReference(branch, second)); err != nil {
				t.Error(err)
				http.Error(w, "move branch", http.StatusInternalServerError)
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	ref := &sourcev1.GitRepositoryRef{Name: branch.String()}
	if (*kind) == "commit-branch" {
		ref = &sourcev1.GitRepositoryRef{Commit: second.String(), Branch: "pinned"}
	}
	spec := &manifest.GitRepository{
		URL: srv.URL, Reference: ref, RecurseSubmodules: true,
	}
	artifact, err := f.Fetch(
		t.Context(),
		spec,
		(testInputs{}).readSet(spec),
		sourcecache.Policy{MaxAge: time.Hour},
	)
	assert.NoError(t, err)
	acquired := openGeneration(t, f, spec, artifact)
	local, err := acquired.Reference(branch, true)
	assert.NoError(t, err)
	origin, err := acquired.Reference(plumbing.NewRemoteReferenceName("origin", "pinned"), true)
	assert.NoError(t, err)
	assert.Equal(t, local.Hash(), second)
	assert.Equal(t, origin.Hash(), first)
	resolved, err := resolveRefHash(acquired, spec.Reference, true)
	assert.NoError(t, err)
	assert.Equal(t, resolved, second)
	assert.Equal(t, artifact.Revision, resolved.String())
	assertWorktree(t, artifact.FS, "value.txt", "B")
}
