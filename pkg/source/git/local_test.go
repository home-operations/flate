package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func TestResolveLocal_References(t *testing.T) {
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
		ref  manifest.GitRepositoryRef
		want plumbing.Hash
		fail bool
	}{
		{"tag", manifest.GitRepositoryRef{Tag: "v1.0.0"}, a, false},
		{"annotated", manifest.GitRepositoryRef{Tag: "annotated"}, a, false},
		{"commit", manifest.GitRepositoryRef{Commit: a.String()}, a, false},
		{"name", manifest.GitRepositoryRef{Name: "refs/pull/1/head"}, a, false},
		{"branch-local-first", manifest.GitRepositoryRef{Branch: "pinned"}, a, false},
		{"branch-origin", manifest.GitRepositoryRef{Branch: "tracking"}, a, false},
		{"name-origin", manifest.GitRepositoryRef{Name: "refs/heads/tracking"}, a, false},
		{"semver", manifest.GitRepositoryRef{SemVer: "<2.0.0"}, a, false},
		{"commit-over-name", manifest.GitRepositoryRef{Commit: a.String(), Name: "bad name", SemVer: "invalid", Tag: "v2.0.0"}, a, false},
		{"name-over-semver", manifest.GitRepositoryRef{Name: "refs/pull/1/head", SemVer: "invalid", Tag: "v2.0.0", Branch: "bad branch"}, a, false},
		{"semver-over-tag", manifest.GitRepositoryRef{SemVer: "<2.0.0", Tag: "bad tag", Branch: "bad branch"}, a, false},
		{"tag-over-branch", manifest.GitRepositoryRef{Tag: "v1.0.0", Branch: "bad branch"}, a, false},
		{"head", manifest.GitRepositoryRef{Tag: "v2.0.0"}, b, false},
		{"commit-branch-reachable", manifest.GitRepositoryRef{Commit: a.String(), Branch: "master"}, a, false},
		{"commit-branch-unreachable", manifest.GitRepositoryRef{Commit: b.String(), Branch: "pinned"}, plumbing.ZeroHash, true},
		{"commit-branch-missing", manifest.GitRepositoryRef{Commit: a.String(), Branch: "missing"}, plumbing.ZeroHash, false},
		{"missing-tag", manifest.GitRepositoryRef{Tag: "missing"}, plumbing.ZeroHash, false},
		{"missing-name", manifest.GitRepositoryRef{Name: "refs/heads/missing"}, plumbing.ZeroHash, false},
		{"missing-branch", manifest.GitRepositoryRef{Branch: "missing"}, plumbing.ZeroHash, false},
		{"unmatched-semver", manifest.GitRepositoryRef{SemVer: ">3.0.0"}, plumbing.ZeroHash, false},
		{"invalid-semver", manifest.GitRepositoryRef{SemVer: "invalid"}, plumbing.ZeroHash, true},
		{"short-commit", manifest.GitRepositoryRef{Commit: a.String()[:12]}, plumbing.ZeroHash, true},
		{"nonhex-commit", manifest.GitRepositoryRef{Commit: strings.Repeat("g", 40)}, plumbing.ZeroHash, true},
		{"missing-commit", manifest.GitRepositoryRef{Commit: strings.Repeat("f", 40)}, plumbing.ZeroHash, false},
		{"missing-commit-branch", manifest.GitRepositoryRef{Commit: strings.Repeat("f", 40), Branch: "master"}, plumbing.ZeroHash, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repository := localSource(tc.ref)
			cache := source.NewCache(cacheroot.New(t.TempDir()))
			artifacts, err := ResolveLocal(t.Context(), root, []*manifest.GitRepository{repository}, cache)
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
			if (artifact.LocalPath == root) != (tc.want == b) {
				t.Fatalf("unexpected artifact path %q", artifact.LocalPath)
			}
		})
	}
}

func TestValidateCommitBranch_UnavailableCommit(t *testing.T) {
	_, repo, _, _ := localFixture(t)
	missing := plumbing.NewHash(strings.Repeat("f", 40))
	if err := validateCommitBranch(repo, missing, "master"); !errors.Is(err, errRefUnavailable) {
		t.Fatalf("missing commit did not become unavailable: %v", err)
	}
}

func TestResolveLocal_IncompleteHistory(t *testing.T) {
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
	if err := validateCommitBranch(repo, a, "master"); !errors.Is(err, errRefUnavailable) {
		t.Errorf("incomplete history did not become unavailable: %v", err)
	}
	repository := localSource(manifest.GitRepositoryRef{Commit: a.String(), Branch: "master"})
	got, err := ResolveLocal(t.Context(), root, []*manifest.GitRepository{repository}, nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("incomplete history did not fall back: %v, %v", got, err)
	}
}

func TestResolveLocal_NoStoreValidatesEffectiveSyntax(t *testing.T) {
	for _, ref := range []manifest.GitRepositoryRef{
		{Commit: "abc"}, {Name: "bad name"}, {Tag: "bad tag"}, {Branch: "bad branch"}, {SemVer: "invalid"},
	} {
		t.Run(manifest.GitRefString(ref), func(t *testing.T) {
			if _, err := ResolveLocal(t.Context(), t.TempDir(), []*manifest.GitRepository{localSource(ref)}, nil); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("expected invalid spec, got %v", err)
			}
		})
	}
	parent, _, _, _ := localFixture(t)
	child := filepath.Join(parent, "exported")
	if err := os.Mkdir(child, 0o750); err != nil {
		t.Fatal(err)
	}
	got, err := ResolveLocal(t.Context(), child, []*manifest.GitRepository{localSource(manifest.GitRepositoryRef{Tag: "v1.0.0"})}, nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("borrowed parent store: %v, %v", got, err)
	}
}

func TestResolveLocal_RejectsNilSource(t *testing.T) {
	if _, err := ResolveLocal(t.Context(), t.TempDir(), []*manifest.GitRepository{nil}, nil); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("nil source = %v", err)
	}
}

func TestResolveLocal_CacheReuseIsolationAndMovedTags(t *testing.T) {
	root, repo, a, b := localFixture(t)
	cache := source.NewCache(cacheroot.New(t.TempDir()))
	first := localSource(manifest.GitRepositoryRef{Tag: "v1.0.0"})
	second := localSource(manifest.GitRepositoryRef{Commit: a.String()})
	second.Name = "second"
	ignored := localSource(manifest.GitRepositoryRef{Tag: "v1.0.0"})
	ignored.Name = "ignored"
	ignore := "/extra.txt\n"
	ignored.Ignore = &ignore
	got, err := ResolveLocal(t.Context(), root, []*manifest.GitRepository{first, second, ignored}, cache)
	if err != nil {
		t.Fatal(err)
	}
	path := got[first.Named()].LocalPath
	if path != got[second.Named()].LocalPath || path == got[ignored.Named()].LocalPath {
		t.Fatalf("cache did not deduplicate commit/options or isolate ignores: %v", got)
	}
	if _, err := os.Stat(filepath.Join(got[ignored.Named()].LocalPath, "extra.txt")); !os.IsNotExist(err) {
		t.Fatalf("ignore not applied: %v", err)
	}
	if content, err := os.ReadFile(filepath.Join(path, "value.txt")); err != nil || string(content) != "A" {
		t.Fatalf("dirty content leaked: %q, %v", content, err)
	}
	otherRoot, _, _, _ := localFixture(t)
	other, err := ResolveLocal(t.Context(), otherRoot, []*manifest.GitRepository{first}, cache)
	if err != nil || other[first.Named()].LocalPath == path {
		t.Fatalf("repository identities share artifacts: %v, %v", other, err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), b)); err != nil {
		t.Fatal(err)
	}
	moved, err := ResolveLocal(t.Context(), root, []*manifest.GitRepository{first}, cache)
	if err != nil || moved[first.Named()].LocalPath != root || moved[first.Named()].Revision != b.String() {
		t.Fatalf("moved tag used stale resolution: %v, %v", moved, err)
	}
}

func TestResolveLocal_ConcurrentReuseAndCancellation(t *testing.T) {
	root, _, _, _ := localFixture(t)
	cacheRoot := t.TempDir()
	cache := source.NewCache(cacheroot.New(cacheRoot))
	repository := localSource(manifest.GitRepositoryRef{Tag: "v1.0.0"})
	var paths [2]string
	var wg sync.WaitGroup
	for i := range paths {
		wg.Go(func() {
			got, err := ResolveLocal(t.Context(), root, []*manifest.GitRepository{repository}, cache)
			if err != nil {
				t.Error(err)
				return
			}
			paths[i] = got[repository.Named()].LocalPath
		})
	}
	wg.Wait()
	if paths[0] == "" || paths[0] != paths[1] {
		t.Fatalf("duplicate work did not share slot: %v", paths)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ResolveLocal(ctx, root, []*manifest.GitRepository{repository}, cache); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
}

func TestResolveLocal_UnusableObjectsAndStore(t *testing.T) {
	for _, kind := range []string{"tag-target", "head", "store", "tree"} {
		t.Run(kind, func(t *testing.T) {
			root, repo, a, _ := localFixture(t)
			ref := manifest.GitRepositoryRef{Tag: "v1.0.0"}
			switch kind {
			case "tag-target":
				if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), plumbing.NewHash(strings.Repeat("f", 40)))); err != nil {
					t.Fatal(err)
				}
			case "head":
				if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.HEAD, plumbing.NewHash(strings.Repeat("f", 40)))); err != nil {
					t.Fatal(err)
				}
			case "store":
				if err := os.RemoveAll(filepath.Join(root, ".git")); err != nil {
					t.Fatal(err)
				}
				testutil.WriteFile(t, root, ".git", "corrupt gitdir")
			case "tree":
				commit, err := repo.CommitObject(a)
				if err != nil {
					t.Fatal(err)
				}
				commit.TreeHash = plumbing.NewHash(strings.Repeat("f", 40))
				encoded := repo.Storer.NewEncodedObject()
				if err := commit.Encode(encoded); err != nil {
					t.Fatal(err)
				}
				hash, err := repo.Storer.SetEncodedObject(encoded)
				if err != nil {
					t.Fatal(err)
				}
				ref = manifest.GitRepositoryRef{Commit: hash.String()}
			}
			repository := localSource(ref)
			repository.SparseCheckout = []string{"apps"}
			if got, err := ResolveLocal(t.Context(), root, []*manifest.GitRepository{repository}, nil); !errors.Is(err, manifest.ErrInput) || got != nil {
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
				repository := localSource(manifest.GitRepositoryRef{Tag: tag})
				if option == "sparse" {
					repository.SparseCheckout = []string{"apps"}
				} else {
					repository.RecurseSubmodules = true
				}
				got, err := ResolveLocal(t.Context(), root, []*manifest.GitRepository{repository}, nil)
				if err != nil || (len(got) != 0) != (tag == "v2.0.0") {
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
	got, err := ResolveLocal(t.Context(), root, []*manifest.GitRepository{localSource(manifest.GitRepositoryRef{Tag: "v2.0.0"})}, nil)
	if err != nil || len(got) != 1 {
		t.Fatalf("gitdir indirection = %v, %v", got, err)
	}
}

func TestResolveLocal_MaterializationFailureLeavesNoArtifact(t *testing.T) {
	root, repo, a, _ := localFixture(t)
	tree := &object.Tree{Entries: []object.TreeEntry{{Name: "missing", Mode: filemode.Regular, Hash: plumbing.NewHash(strings.Repeat("f", 40))}}}
	encoded := repo.Storer.NewEncodedObject()
	if err := tree.Encode(encoded); err != nil {
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
	cacheRoot := t.TempDir()
	cache := source.NewCache(cacheroot.New(cacheRoot))
	got, err := ResolveLocal(t.Context(), root, []*manifest.GitRepository{localSource(manifest.GitRepositoryRef{Commit: hash.String()})}, cache)
	if err == nil || got != nil {
		t.Fatalf("materialization failure = %v, %v", got, err)
	}
	if err := filepath.WalkDir(cacheRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && strings.Contains(entry.Name(), ".tmp.") || entry.Name() == source.SlotMetaFile {
			t.Errorf("partial artifact remains at %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckoutRef_PreservesOriginFirstBranchLookup(t *testing.T) {
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
	if err := checkoutRef(repo, manifest.GitRepositoryRef{Branch: "pinned"}, nil); err != nil {
		t.Fatal(err)
	}
	head, err := repo.Head()
	if err != nil || head.Hash() != b {
		t.Fatalf("checkout must prefer origin: %v, %v (root %s)", head, err, root)
	}
}

func TestResolveSemver_IgnoresUnusableLowerTag(t *testing.T) {
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
	if _, err := lookupBranch(repo, "master"); !errors.Is(err, failure) || errors.Is(err, errRefUnavailable) {
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

func localSource(ref manifest.GitRepositoryRef) *manifest.GitRepository {
	repo := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	repo.URL = "git://fixture.invalid/cluster"
	repo.Reference = &ref
	return repo
}

func localFixture(t testing.TB) (string, *git.Repository, plumbing.Hash, plumbing.Hash) {
	t.Helper()
	root := t.TempDir()
	repo, err := git.PlainInit(root, false)
	if err != nil {
		t.Fatal(err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	var hashes [2]plumbing.Hash
	for i, value := range []string{"A", "B"} {
		testutil.WriteFile(t, root, "value.txt", value)
		testutil.WriteFile(t, root, "extra.txt", "extra")
		if _, err := wt.Add("."); err != nil {
			t.Fatal(err)
		}
		hashes[i], err = wt.Commit(value, &git.CommitOptions{
			Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName([]string{"v1.0.0", "v2.0.0"}[i]), hashes[i])); err != nil {
			t.Fatal(err)
		}
	}
	testutil.WriteFile(t, root, "value.txt", "dirty")
	return root, repo, hashes[0], hashes[1]
}
