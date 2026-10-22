package git

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

func TestResolveLocal_References(t *testing.T) {
	t.Parallel()
	root, repo, a, b := localFixture(t)
	for name, hash := range map[string]plumbing.Hash{
		"refs/heads/pinned": a, "refs/remotes/origin/pinned": b,
		"refs/remotes/origin/tracking": a, "refs/pull/1/head": a,
	} {
		if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), hash)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.CreateTag("annotated", a, &git.CreateTagOptions{
		Tagger: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}, Message: "pin",
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		ref  sourcev1.GitRepositoryRef
		want plumbing.Hash
		fail bool
	}{
		{"tag", sourcev1.GitRepositoryRef{Tag: "v1.0.0"}, a, false},
		{"annotated", sourcev1.GitRepositoryRef{Tag: "annotated"}, a, false},
		{"commit", sourcev1.GitRepositoryRef{Commit: a.String()}, a, false},
		{"name", sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"}, a, false},
		{"branch-local-first", sourcev1.GitRepositoryRef{Branch: "pinned"}, a, false},
		{"branch-origin", sourcev1.GitRepositoryRef{Branch: "tracking"}, a, false},
		{"name-origin", sourcev1.GitRepositoryRef{Name: "refs/heads/tracking"}, a, false},
		{"semver", sourcev1.GitRepositoryRef{SemVer: "<2.0.0"}, a, false},
		{"commit-over-name", sourcev1.GitRepositoryRef{Commit: a.String(), Name: "bad name", SemVer: "invalid", Tag: "v2.0.0"}, a, false},
		{"name-over-semver", sourcev1.GitRepositoryRef{Name: "refs/pull/1/head", SemVer: "invalid", Tag: "v2.0.0", Branch: "bad branch"}, a, false},
		{"semver-over-tag", sourcev1.GitRepositoryRef{SemVer: "<2.0.0", Tag: "bad tag", Branch: "bad branch"}, a, false},
		{"tag-over-branch", sourcev1.GitRepositoryRef{Tag: "v1.0.0", Branch: "bad branch"}, a, false},
		{"head", sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, b, false},
		{"commit-branch-reachable", sourcev1.GitRepositoryRef{Commit: a.String(), Branch: "master"}, a, false},
		{"commit-branch-unreachable", sourcev1.GitRepositoryRef{Commit: b.String(), Branch: "pinned"}, plumbing.ZeroHash, true},
		{"commit-branch-missing", sourcev1.GitRepositoryRef{Commit: a.String(), Branch: "missing"}, plumbing.ZeroHash, false},
		{"missing-tag", sourcev1.GitRepositoryRef{Tag: "missing"}, plumbing.ZeroHash, false},
		{"missing-name", sourcev1.GitRepositoryRef{Name: "refs/heads/missing"}, plumbing.ZeroHash, false},
		{"missing-branch", sourcev1.GitRepositoryRef{Branch: "missing"}, plumbing.ZeroHash, false},
		{"unmatched-semver", sourcev1.GitRepositoryRef{SemVer: ">3.0.0"}, plumbing.ZeroHash, false},
		{"invalid-semver", sourcev1.GitRepositoryRef{SemVer: "invalid"}, plumbing.ZeroHash, true},
		{"short-commit", sourcev1.GitRepositoryRef{Commit: a.String()[:12]}, plumbing.ZeroHash, true},
		{"nonhex-commit", sourcev1.GitRepositoryRef{Commit: strings.Repeat("g", 40)}, plumbing.ZeroHash, true},
		{"missing-commit", sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40)}, plumbing.ZeroHash, false},
		{"missing-commit-branch", sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40), Branch: "master"}, plumbing.ZeroHash, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repository := localSource(tc.ref)

			artifacts, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{repository}, "")
			if tc.fail {
				if !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("expected domain error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			artifact := artifacts[repository.Named()]
			if tc.want.IsZero() {
				if artifact != nil {
					t.Fatalf("lookup miss installed an artifact: %+v", artifact)
				}
				return
			}
			if artifact == nil || artifact.Revision != tc.want.String() || artifact.URL != repository.URL {
				t.Fatalf("artifact = %+v, want revision %s", artifact, tc.want)
			}
			if artifact.FS == nil || (artifact.Key == "") != (tc.want == b) {
				t.Fatalf("FS/Key state did not distinguish HEAD from committed content: %+v", artifact)
			}
			view, pin, err := source.OperationView(t.Context(), artifact.FS)
			assert.NoError(t, err)
			if artifact.Key == "" {
				assert.Equal(t, pin, "")
			} else {
				assert.Equal(t, pin, artifact.Key)
			}
			data, err := fs.ReadFile(view, "value.txt")
			assert.NoError(t, err)
			wantBody := "A"
			if tc.want == b {
				wantBody = "dirty"
			}
			assert.Equal(t, string(data), wantBody)
			if (artifact.LocalPath == root) != (tc.want == b) {
				t.Fatalf("unexpected artifact path %q", artifact.LocalPath)
			}
		})
	}
	for _, metadata := range []string{"directory", "gitfile"} {
		t.Run("metadata "+metadata, func(t *testing.T) {
			t.Parallel()
			root, _, _, head := localFixture(t)
			metadataRoot := filepath.Join(root, ".git")
			if metadata == "gitfile" {
				metadataRoot = filepath.Join(t.TempDir(), "repository")
				assert.Equal(t, os.Rename(filepath.Join(root, ".git"), metadataRoot), nil)
				testutil.WriteFile(t, root, ".git", "gitdir: "+metadataRoot+"\n")
			}
			testutil.WriteFile(t, metadataRoot, "config", "[remote \"origin\"]\nurl = https://fixture.invalid/original\n")
			testutil.WriteFile(t, root, "nested/.git", "gitdir: absent\n")
			location := localLocation(t, root)
			assert.Equal(t, os.Remove(filepath.Join(metadataRoot, "config")), nil)
			artifacts, err := ResolveLocal(t.Context(), location, []*manifest.GitRepository{localSource(sourcev1.GitRepositoryRef{Tag: "v2.0.0"})}, "")
			assert.Equal(t, err, nil)
			artifact := artifacts[localSource(sourcev1.GitRepositoryRef{Tag: "v2.0.0"}).Named()]
			if artifact == nil || artifact.Revision != head.String() {
				t.Fatal("retained repository did not resolve HEAD")
			}
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
					if entry.Name() == ".git" {
						t.Fatalf("ReadDir(%q) exposes Git metadata", name)
					}
				}
			}
		})
	}

}

func TestResolveRefHash_ObjectClassification(t *testing.T) {
	t.Parallel()
	_, repo, hash, _ := localFixture(t)
	missing := plumbing.NewHash(strings.Repeat("f", 40))
	commit, err := repo.CommitObject(hash)
	assert.NoError(t, err)
	tree := commit.TreeHash
	commit.TreeHash = missing
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	broken, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	for _, tc := range []struct {
		name        string
		hash        plumbing.Hash
		unavailable bool
	}{
		{"absent commit", missing, true},
		{"wrong object type", tree, false},
		{"missing tree", broken, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := resolveRefHash(repo, &sourcev1.GitRepositoryRef{Commit: tc.hash.String()}, false)
			if err == nil || errors.Is(err, errRefUnavailable) != tc.unavailable {
				t.Fatalf("resolution error = %v, want unavailable=%t", err, tc.unavailable)
			}
		})
	}
}

func TestResolveLocal_IncompleteHistory(t *testing.T) {
	t.Parallel()
	root, repo, a, b := localFixture(t)
	commit, err := repo.CommitObject(b)
	if err != nil {
		t.Fatal(err)
	}
	commit.ParentHashes = []plumbing.Hash{plumbing.NewHash(strings.Repeat("f", 40))}
	encoded := repo.Storer.NewEncodedObject()
	if err := commit.Encode(encoded); err != nil {
		t.Fatal(err)
	}
	hash, err := repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("master"), hash)); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveRefHash(repo, &sourcev1.GitRepositoryRef{Commit: a.String(), Branch: "master"}, false); !errors.Is(err, errRefUnavailable) {
		t.Errorf("incomplete history did not become unavailable: %v", err)
	}
	repository := localSource(sourcev1.GitRepositoryRef{Commit: a.String(), Branch: "master"})
	got, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{repository}, "")
	if err != nil || len(got) != 0 {
		t.Fatalf("incomplete history did not fall back: %v, %v", got, err)
	}
}

func TestResolveLocal_NoWarningAllocations(t *testing.T) {
	empty := []*manifest.GitRepository{localSource(sourcev1.GitRepositoryRef{})}
	authored := []*manifest.GitRepository{localSource(sourcev1.GitRepositoryRef{Tag: "v1.0.0"})}
	selected := (*manifest.GitRepositoryRef)(authored[0].Reference)
	emptyAllocs := testing.AllocsPerRun(1000, func() {
		_, err := ResolveLocal(t.Context(), nil, empty, "")
		assert.NoError(t, err)
	})
	validationAllocs := testing.AllocsPerRun(1000, func() {
		assert.NoError(t, selected.Validate())
	})
	authoredAllocs := testing.AllocsPerRun(1000, func() {
		_, err := ResolveLocal(t.Context(), nil, authored, "")
		assert.NoError(t, err)
	})
	if want := emptyAllocs + validationAllocs; authoredAllocs > want {
		t.Fatalf("reference formatting without warnings: %g allocations, want %g", authoredAllocs, want)
	}
}

func TestResolveLocal_NoStoreValidatesEffectiveSyntax(t *testing.T) {
	t.Parallel()
	for _, ref := range []sourcev1.GitRepositoryRef{
		{Commit: "abc"}, {Name: "bad name"}, {Tag: "bad tag"}, {Branch: "bad branch"}, {SemVer: "invalid"},
	} {
		t.Run((*manifest.GitRepositoryRef)(&ref).String(), func(t *testing.T) {
			t.Parallel()
			if _, err := ResolveLocal(t.Context(), localLocation(t, t.TempDir()), []*manifest.GitRepository{localSource(ref)}, ""); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("expected invalid spec, got %v", err)
			}
		})
	}
	parent, _, _, _ := localFixture(t)
	child := filepath.Join(parent, "exported")
	assert.Equal(t, os.Mkdir(child, 0o750), nil)
	repository := localSource(sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	got, err := ResolveLocal(t.Context(), localLocation(t, child), []*manifest.GitRepository{repository}, "")
	assert.Equal(t, err, nil)
	assert.Equal(t, len(got), 0)
	testutil.WriteFile(t, parent, ".git/config", "[invalid\n")
	got, err = ResolveLocal(t.Context(), localLocation(t, child), []*manifest.GitRepository{repository}, "")
	assert.Equal(t, err, nil)
	assert.Equal(t, len(got), 0)
	assert.Equal(t, os.RemoveAll(filepath.Join(parent, ".git")), nil)
	assert.Equal(t, os.Symlink(".git", filepath.Join(parent, ".git")), nil)
	got, err = ResolveLocal(t.Context(), localLocation(t, child), []*manifest.GitRepository{repository}, "")
	assert.Equal(t, err, nil)
	assert.Equal(t, len(got), 0)
}

func TestResolveLocal_RejectsNilSource(t *testing.T) {
	t.Parallel()
	if _, err := ResolveLocal(t.Context(), localLocation(t, t.TempDir()), []*manifest.GitRepository{nil}, ""); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("nil source = %v", err)
	}
}

func TestResolveLocal_CacheReuseIsolationAndMovedTags(t *testing.T) {
	t.Parallel()
	root, repo, a, b := localFixture(t)

	a = submoduleCommit(t, repo, a)
	assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), a)))

	first := localSource(sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	second := localSource(sourcev1.GitRepositoryRef{Commit: a.String()})
	second.Name = "second"
	ignored := localSource(sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	ignored.Name = "ignored"
	ignore := "/extra.txt\n"
	ignored.Ignore = &ignore
	got, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{first, second, ignored}, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []*manifest.GitRepository{first, second, ignored} {
		artifact := got[source.Named()]
		if len(artifact.Warnings) != 1 || artifact.Warnings[0].Resource != source.Named() || artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule {
			t.Fatalf("pin evidence lost: %+v", artifact)
		}
	}
	key := got[first.Named()].Key
	if key != got[second.Named()].Key || key == got[ignored.Named()].Key {
		t.Fatalf("cache did not deduplicate commit/options or isolate ignores: %v", got)
	}
	if _, err := fs.Stat(got[ignored.Named()].FS, "extra.txt"); !os.IsNotExist(err) {
		t.Fatalf("ignore not applied: %v", err)
	}
	if content, err := fs.ReadFile(got[first.Named()].FS, "value.txt"); err != nil || string(content) != "A" {
		t.Fatalf("dirty content leaked: %q, %v", content, err)
	}
	otherRoot, _, _, _ := localFixture(t)
	other, err := ResolveLocal(t.Context(), localLocation(t, otherRoot), []*manifest.GitRepository{first}, "")
	if err != nil || other[first.Named()].Key == key {
		t.Fatalf("repository identities share artifacts: %v, %v", other, err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), b)); err != nil {
		t.Fatal(err)
	}
	moved, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{first}, "")
	if err != nil || moved[first.Named()].LocalPath != root || moved[first.Named()].Revision != b.String() {
		t.Fatalf("moved tag used stale resolution: %v, %v", moved, err)
	}
}

func TestResolveLocal_AnnotatedSemverWinner(t *testing.T) {
	t.Parallel()
	root, repo, revision, _ := localFixture(t)
	if _, err := repo.CreateTag("v1.5.0", revision, &git.CreateTagOptions{
		Tagger: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}, Message: "pin",
	}); err != nil {
		t.Fatal(err)
	}
	repository := localSource(sourcev1.GitRepositoryRef{SemVer: ">=1.0.0 <2.0.0"})
	artifacts, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{repository}, "")
	if err != nil {
		t.Fatal(err)
	}
	if artifact := artifacts[repository.Named()]; artifact == nil || artifact.Revision != revision.String() {
		t.Fatalf("annotated semver winner = %+v, want revision %s", artifact, revision)
	}
}

func TestResolveSemver_EqualVersionCreationOrder(t *testing.T) {
	t.Parallel()
	for _, names := range [][]string{{"v1.0.0", "1.0.0"}, {"1.0.0", "v1.0.0"}} {
		t.Run(strings.Join(names, "_"), func(t *testing.T) {
			t.Parallel()
			_, repo, a, b := localFixture(t)
			hashes := map[string]plumbing.Hash{"1.0.0": a, "v1.0.0": b}
			var refs []*plumbing.Reference
			for _, name := range names {
				ref := plumbing.NewHashReference(plumbing.NewTagReferenceName(name), hashes[name])
				if err := repo.Storer.SetReference(ref); err != nil {
					t.Fatal(err)
				}
				refs = append(refs, ref)
			}
			// Filesystem ref iteration can sort away creation order; force both
			// orders so tie resolution cannot rely on the storage backend.
			repo.Storer = &orderedTagStore{Storer: repo.Storer, refs: refs}
			for range 10 {
				got, err := resolveSemver(repo, ">=1.0.0 <2.0.0")
				if err != nil || got != a {
					t.Fatalf("equal-version winner = %s, %v, want %s", got, err, a)
				}
			}
		})
	}
}

type orderedTagStore struct {
	storage.Storer
	refs []*plumbing.Reference
}

func (s *orderedTagStore) IterReferences() (storer.ReferenceIter, error) {
	return storer.NewReferenceSliceIter(s.refs), nil
}

func TestResolveLocal_ConcurrentReuseAndCancellation(t *testing.T) {
	t.Parallel()
	root, _, _, _ := localFixture(t)

	repository := localSource(sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	var paths [2]string
	var wg sync.WaitGroup
	for i := range paths {
		wg.Go(func() {
			got, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{repository}, "")
			if err != nil {
				t.Error(err)
				return
			}
			artifact := got[repository.Named()]
			paths[i] = artifact.Key
			for range 2 {
				content, err := fs.ReadFile(artifact.FS, "value.txt")
				if err != nil || string(content) != "A" {
					t.Errorf("concurrent committed read = %q, %v", content, err)
				}
			}
		})
	}
	wg.Wait()
	if paths[0] == "" || paths[0] != paths[1] {
		t.Fatalf("local artifact identities differ: %v", paths)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ResolveLocal(ctx, localLocation(t, root), []*manifest.GitRepository{repository}, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
}

func TestResolveLocal_UnusableObjectsAndStore(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"tag-target", "head", "store", "config", "lookup", "tree", "tree-tag", "recursive-tree-tag"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root, repo, a, _ := localFixture(t)
			ref := sourcev1.GitRepositoryRef{Tag: "v1.0.0"}
			switch kind {
			case "tag-target":
				assert.Equal(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), plumbing.NewHash(strings.Repeat("f", 40)))), nil)
			case "head":
				assert.Equal(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.HEAD, plumbing.NewHash(strings.Repeat("f", 40)))), nil)
			case "config":
				testutil.WriteFile(t, root, ".git/config", "[invalid\n")
			case "store", "lookup":
				assert.Equal(t, os.RemoveAll(filepath.Join(root, ".git")), nil)
				if kind == "lookup" {
					assert.Equal(t, os.Symlink(".git", filepath.Join(root, ".git")), nil)
				} else {
					testutil.WriteFile(t, root, ".git", "corrupt gitdir")
				}
			case "tree", "tree-tag", "recursive-tree-tag":
				commit, err := repo.CommitObject(a)
				if err != nil {
					t.Fatal(err)
				}
				commit.TreeHash = plumbing.NewHash(strings.Repeat("f", 40))
				encoded := repo.Storer.NewEncodedObject()
				assert.Equal(t, commit.Encode(encoded), nil)
				hash, err := repo.Storer.SetEncodedObject(encoded)
				assert.Equal(t, err, nil)
				if kind == "tree" {
					ref = sourcev1.GitRepositoryRef{Commit: hash.String()}
				} else {
					assert.Equal(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName(ref.Tag), hash)), nil)
				}
			}
			repository := localSource(ref)
			repository.RecurseSubmodules = kind == "recursive-tree-tag"
			repository.SparseCheckout = []string{"apps"}
			if got, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{repository}, ""); !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || got != nil {
				t.Fatalf("corruption became fallback: %v, %v", got, err)
			}
		})
	}
}

func TestResolveLocal_OptionsFallbackAndGitIndirection(t *testing.T) {
	root, _, _, _ := localFixture(t)
	for _, option := range []string{"sparse", "submodules"} {
		t.Run(option, func(t *testing.T) {
			for _, tag := range []string{"v1.0.0", "v2.0.0"} {
				repository := localSource(sourcev1.GitRepositoryRef{Tag: tag})
				if option == "sparse" {
					repository.SparseCheckout = []string{"apps"}
				} else {
					repository.RecurseSubmodules = true
				}
				got, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{repository}, "")
				if err != nil || (len(got) != 0) != (tag == "v2.0.0" || option == "sparse") {
					t.Fatalf("%s %s = %v, %v", option, tag, got, err)
				}
			}
		})
	}
	gitdir := filepath.Join(t.TempDir(), "objects")
	if err := os.Rename(filepath.Join(root, ".git"), gitdir); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, ".git", "gitdir: "+gitdir+"\n")
	got, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{localSource(sourcev1.GitRepositoryRef{Tag: "v2.0.0"})}, "")
	if err != nil || len(got) != 1 {
		t.Fatalf("gitdir indirection = %v, %v", got, err)
	}
}

func TestResolveLocal_MissingBlobFailsOnRead(t *testing.T) {
	t.Parallel()
	root, repo, a, _ := localFixture(t)
	inputTree := &object.Tree{Entries: []object.TreeEntry{{Name: "missing", Mode: filemode.Regular, Hash: plumbing.NewHash(strings.Repeat("f", 40))}}}
	encoded := repo.Storer.NewEncodedObject()
	if err := inputTree.Encode(encoded); err != nil {
		t.Fatal(err)
	}
	treeHash, err := repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := repo.CommitObject(a)
	if err != nil {
		t.Fatal(err)
	}
	commit.TreeHash = treeHash
	encoded = repo.Storer.NewEncodedObject()
	if err := commit.Encode(encoded); err != nil {
		t.Fatal(err)
	}
	hash, err := repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	repository := localSource(sourcev1.GitRepositoryRef{Commit: hash.String()})
	got, err := ResolveLocal(t.Context(), localLocation(t, root), []*manifest.GitRepository{repository}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.ReadFile(got[repository.Named()].FS, "missing"); !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Fatalf("missing blob: %v", err)
	}
}

func TestCheckoutRef_PreservesOriginFirstBranchLookup(t *testing.T) {
	t.Parallel()
	root, repo, a, b := localFixture(t)
	for name, hash := range map[string]plumbing.Hash{"refs/heads/pinned": a, "refs/remotes/origin/pinned": b} {
		if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), hash)); err != nil {
			t.Fatal(err)
		}
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if err := wt.Checkout(&git.CheckoutOptions{Hash: b, Force: true}); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveRefHash(repo, &sourcev1.GitRepositoryRef{Branch: "pinned"}, true)
	if err != nil || resolved != b {
		t.Fatalf("acquired branch resolved %s, %v, want %s", resolved, err, b)
	}
	if err := checkoutRef(repo, resolved, nil); err != nil {
		t.Fatal(err)
	}
	head, err := repo.Head()
	if err != nil || head.Hash() != resolved {
		t.Fatalf("checkout must use resolved hash: %v, %v (root %s)", head, err, root)
	}
	data, err := os.ReadFile(filepath.Join(root, "value.txt"))
	if err != nil || string(data) != "B" {
		t.Fatalf("checkout content = %q, %v, want B", data, err)
	}

	for _, kind := range []string{"name", "commit-branch"} {
		t.Run("acquisition/"+kind, func(t *testing.T) {
			t.Parallel()
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
			if kind == "commit-branch" {
				ref = &sourcev1.GitRepositoryRef{Commit: second.String(), Branch: "pinned"}
			}
			spec := &manifest.GitRepository{
				URL: srv.URL, Reference: ref, RecurseSubmodules: true,
			}
			artifact, err := f.Fetch(t.Context(), spec, sourcecache.Policy{MaxAge: time.Hour})
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
		})
	}
}

func TestResolveSemver_IgnoresUnusableLowerTag(t *testing.T) {
	t.Parallel()
	_, repo, a, b := localFixture(t)
	commit, err := repo.CommitObject(a)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), commit.TreeHash)); err != nil {
		t.Fatal(err)
	}
	got, err := resolveSemver(repo, "*")
	if err != nil || got != b {
		t.Fatalf("higher usable tag = %s, %v, want %s", got, err, b)
	}
}

func TestResolveSemver_DeterministicTiesAndReadErrors(t *testing.T) {
	t.Parallel()
	_, repo, a, b := localFixture(t)
	for name, hash := range map[string]plumbing.Hash{"1.0.0": a, "v1.0.0": b} {
		if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName(name), hash)); err != nil {
			t.Fatal(err)
		}
	}
	for range 10 {
		got, err := resolveSemver(repo, "<2.0.0")
		if err != nil || got != a {
			t.Fatalf("tie = %s, %v", got, err)
		}
	}
	failure := errors.New("storage unavailable")
	repo.Storer = &failingRefStore{Storer: repo.Storer, err: failure}
	if _, err := resolveSemver(repo, "*"); !errors.Is(err, failure) {
		t.Fatalf("iterator error lost: %v", err)
	}
	if _, err := lookupBranch(repo, "master", false); !errors.Is(err, failure) || errors.Is(err, errRefUnavailable) {
		t.Fatalf("reference read error became miss: %v", err)
	}
}

type failingRefStore struct {
	storage.Storer
	err error
}

func (s *failingRefStore) Reference(plumbing.ReferenceName) (*plumbing.Reference, error) {
	return nil, s.err
}

func (s *failingRefStore) IterReferences() (storer.ReferenceIter, error) { return nil, s.err }

func localSource(ref sourcev1.GitRepositoryRef) *manifest.GitRepository {
	repo := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	repo.URL = "git://fixture.invalid/cluster"
	repo.Reference = &ref
	return repo
}

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
		if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName([]string{"v1.0.0", "v2.0.0"}[i]), hashes[i])); err != nil {
			tb.Fatal(err)
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
				tree.Entries = append(tree.Entries, object.TreeEntry{Name: "zmissing", Mode: filemode.Dir, Hash: plumbing.NewHash(strings.Repeat("f", 40))})
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
			first := localSource(sourcev1.GitRepositoryRef{Commit: pin.String()})
			repositories := []*manifest.GitRepository{first}
			if failure == "later entry" {
				repositories = append(repositories, nil)
			}
			artifacts, err := ResolveLocal(t.Context(), localLocation(t, root), repositories, "")
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("original failure lost: %v", err)
			}
			artifact := artifacts[first.Named()]
			if artifact == nil || artifact.FS != nil || artifact.Key != "" || len(artifact.Warnings) != 1 || artifact.Warnings[0].Resource != first.Named() || artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule {
				t.Fatalf("warning-only local result = %+v", artifact)
			}
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
