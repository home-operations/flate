package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/source/git/mirror"
)

// TestMirror_BareClonePersistsAcrossFetches confirms that a Fetcher
// with Mirrors set creates the bare mirror once and reuses it on the
// second fetch — the per-URL mirror dir is present after run 1 and
// still present after run 2, while the slot worktree is materialized
// each time (the slot lives at a different path than the mirror).
func TestMirror_BareClonePersistsAcrossFetches(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cacheDir := t.TempDir()
	layout := cacheroot.New(cacheDir)
	mirrorDir := layout.GitMirrors()

	cache := source.NewCache(layout)
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
	}
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout)}

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch 1: %v", err)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "hello.txt")); err != nil {
		t.Errorf("worktree missing hello.txt: %v", err)
	}

	// The mirror directory must exist after the first fetch.
	entries, err := os.ReadDir(mirrorDir)
	if err != nil {
		t.Fatalf("mirror dir missing: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected one mirror dir, got %d: %v", len(entries), entries)
	}

	// Second fetch reuses the mirror. The mutable HEAD ref gets a fresh
	// worktree slot so no in-place reset can race consumers of the first
	// artifact.
	art2, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}
	if art2.Revision != art.Revision {
		t.Errorf("revision drifted: %s vs %s", art.Revision, art2.Revision)
	}
	entries2, _ := os.ReadDir(mirrorDir)
	if len(entries2) != 1 {
		t.Errorf("mirror dir count drifted: %d → %d", len(entries), len(entries2))
	}
}

// TestMirror_FallsBackForSubmodules confirms that RecurseSubmodules=true
// disables the mirror path so go-git's submodule support still works.
// We don't actually wire up a submodule repo — we only assert the
// branch decision via canUseMirror.
func TestMirror_FallsBackForSubmodules(t *testing.T) {
	f := &Fetcher{Mirrors: mirror.New(cacheroot.New(t.TempDir()))}
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL: "https://example.com/x", RecurseSubmodules: true,
		},
	}
	if f.canUseMirror(repo, "https://example.com/x") {
		t.Error("RecurseSubmodules should disable the mirror path")
	}
	repo.RecurseSubmodules = false
	repo.SparseCheckout = []string{"sub/"}
	if f.canUseMirror(repo, "https://example.com/x") {
		t.Error("SparseCheckout should disable the mirror path")
	}
	repo.SparseCheckout = nil
	if !f.canUseMirror(repo, "https://example.com/x") {
		t.Error("vanilla fetch should be mirror-eligible")
	}

	// Nil Mirrors → legacy.
	f2 := &Fetcher{}
	if f2.canUseMirror(repo, "https://example.com/x") {
		t.Error("nil Mirrors should disable the mirror path")
	}
}

// TestMirror_TagResolvesAcrossRefs covers the cross-ref reuse the
// mirror enables: a single bare clone covers both the main branch and
// a tag, and fetching each materializes the right commit.
func TestMirror_TagResolvesAcrossRefs(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	tagged := mustTagHEAD(t, src, "v1.0.0")

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout)}

	// First: fetch a tag.
	tagRepo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"},
		},
	}
	art, err := f.Fetch(context.Background(), tagRepo)
	if err != nil {
		t.Fatalf("Fetch tag: %v", err)
	}
	if art.Revision != tagged {
		t.Errorf("tag revision = %q, want %q", art.Revision, tagged)
	}

	// Second: fetch HEAD (same commit but different ref path).
	headRepo := &manifest.GitRepository{
		Name: "u", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
	}
	art2, err := f.Fetch(context.Background(), headRepo)
	if err != nil {
		t.Fatalf("Fetch HEAD: %v", err)
	}
	if art2.Revision != tagged {
		t.Errorf("HEAD revision = %q, want %q", art2.Revision, tagged)
	}
	if art.LocalPath == art2.LocalPath {
		t.Error("different refs should land in different slots")
	}
}

func TestMirror_TagRefreshFetchesOnlyRequestedTag(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	v1 := mustTagHEAD(t, src, "v1.0.0")

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout)}
	repo := &manifest.GitRepository{
		Name: "tagged", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"},
		},
	}
	if _, err := f.Fetch(context.Background(), repo); err != nil {
		t.Fatalf("Fetch v1: %v", err)
	}

	mustCommitFile(t, src, "v2.txt", "v2")
	mustTagHEAD(t, src, "v2.0.0")

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch v1 again: %v", err)
	}
	if art.Revision != v1 {
		t.Errorf("revision = %q, want %q", art.Revision, v1)
	}

	entries, err := os.ReadDir(layout.GitMirrors())
	if err != nil {
		t.Fatalf("ReadDir mirrors: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one mirror dir, got %d", len(entries))
	}
	mirrorRepo, err := git.PlainOpen(filepath.Join(layout.GitMirrors(), entries[0].Name()))
	if err != nil {
		t.Fatalf("PlainOpen mirror: %v", err)
	}
	if _, err := mirrorRepo.Tag("v2.0.0"); err == nil {
		t.Fatal("exact tag refresh fetched unrelated tag v2.0.0")
	}
}

func TestMirror_RefNameResolvesNonBranchRefs(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	want := mustSetRefToHEAD(t, src, "refs/pull/1/head")

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout)}
	repo := &manifest.GitRepository{
		Name: "pull", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"},
		},
	}

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch ref.name through mirror: %v", err)
	}
	if art.Revision != want {
		t.Errorf("revision = %q, want %q", art.Revision, want)
	}
}

func TestMirror_CommitMustBeReachableFromBranch(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	initial := mustHead(t, src)
	mainOnly := mustCommitFile(t, src, "main-only.txt", "main")
	mustSetRefToHash(t, src, "refs/heads/staging", initial)

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout)}
	repo := &manifest.GitRepository{
		Name: "constrained", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL: "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{
				Branch: "staging",
				Commit: mainOnly,
			},
		},
	}

	_, err := f.Fetch(context.Background(), repo)
	if err == nil {
		t.Fatal("expected branch-constrained commit fetch to fail through mirror")
	}
	if !strings.Contains(err.Error(), "not reachable from branch") {
		t.Fatalf("error should explain branch reachability, got %v", err)
	}
}

func TestResolveRefHash_TagAndBranchAreStrict(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	mustSetRefToHEAD(t, src, "refs/heads/branch-only")
	mustTagHEAD(t, src, "tag-only")

	repo, err := git.PlainOpen(src)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	if _, err := resolveRefHash(repo, &manifest.GitRepositoryRef{Tag: "branch-only"}); err == nil {
		t.Fatal("tag lookup should not fall back to a branch with the same name")
	}
	if _, err := resolveRefHash(repo, &manifest.GitRepositoryRef{Branch: "tag-only"}); err == nil {
		t.Fatal("branch lookup should not fall back to a tag with the same name")
	}
}

func mustSetRefToHEAD(t *testing.T, dir, refName string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := r.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	ref := plumbing.NewHashReference(plumbing.ReferenceName(refName), head.Hash())
	if err := r.Storer.SetReference(ref); err != nil {
		t.Fatalf("SetReference %s: %v", refName, err)
	}
	return head.Hash().String()
}
