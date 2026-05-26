package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func TestFetcher_LocalFileURL(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
	}

	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	sa := art
	if sa.LocalPath == "" || sa.URL == "" {
		t.Errorf("incomplete artifact: %+v", sa)
	}
	if _, err := os.Stat(filepath.Join(sa.LocalPath, "hello.txt")); err != nil {
		t.Errorf("expected checked-out file: %v", err)
	}

	art2, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	if art2.Revision != sa.Revision {
		t.Errorf("unchanged default ref revision changed: %s vs %s", sa.Revision, art2.Revision)
	}
}

func TestFetcher_MutableDefaultRefRefreshesCache(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
	}
	f := &Fetcher{Cache: cache}

	art1, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch first: %v", err)
	}
	want := mustCommitFile(t, src, "later.txt", "new content")
	art2, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	if art2.Revision == art1.Revision {
		t.Fatalf("mutable default ref reused stale revision %s", art2.Revision)
	}
	if art2.Revision != want {
		t.Fatalf("revision = %s, want %s", art2.Revision, want)
	}
	if _, err := os.Stat(filepath.Join(art2.LocalPath, "later.txt")); err != nil {
		t.Fatalf("refreshed checkout missing later.txt: %v", err)
	}
}

func TestFetcher_MutableRefUsesFreshIntervalCache(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:      "file://" + src,
			Interval: metav1.Duration{Duration: time.Hour},
		},
	}
	f := &Fetcher{Cache: cache}

	art1, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch first: %v", err)
	}
	mustCommitFile(t, src, "later.txt", "new content")
	art2, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	if art2.Revision != art1.Revision {
		t.Fatalf("fresh interval cache should reuse revision %s, got %s", art1.Revision, art2.Revision)
	}
	if _, err := os.Stat(filepath.Join(art2.LocalPath, "later.txt")); !os.IsNotExist(err) {
		t.Fatalf("fresh interval cache should not include later commit, stat err=%v", err)
	}
}

func TestFetcher_MutableRefreshFailureKeepsPreviousSlot(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
	}
	f := &Fetcher{Cache: cache}

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch first: %v", err)
	}
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Fetch(context.Background(), repo); err == nil {
		t.Fatal("expected refresh to fail after removing upstream repo")
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "hello.txt")); err != nil {
		t.Fatalf("failed mutable refresh removed previous committed slot: %v", err)
	}
}

// TestFetcher_RefByName exercises spec.ref.name handling — flate
// resolves it to a commit via git.ResolveRevision and checks out the
// resulting hash.
func TestFetcher_RefByName(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	tagged := mustTagHEAD(t, src, "v0.1.0")

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Name: "refs/tags/v0.1.0"},
		},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch by ref.name: %v", err)
	}
	if art.Revision != tagged {
		t.Errorf("Revision = %q, want %q (tag → commit)", art.Revision, tagged)
	}
}

// TestFetcher_RefByName_Unresolvable surfaces a clear error when the
// ref name can't be resolved.
func TestFetcher_RefByName_Unresolvable(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Name: "refs/heads/does-not-exist"},
		},
	}
	f := &Fetcher{Cache: cache}
	_, err := f.Fetch(context.Background(), repo)
	if err == nil {
		t.Fatalf("expected unresolvable-ref error")
	}
}

func TestFetcher_RefByNameFallbackFetchesExplicitRef(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/a/manifest.yaml": "kind: ConfigMap",
		"apps/b/manifest.yaml": "kind: Secret",
	})
	want := mustSetRefToHEAD(t, src, "refs/pull/1/head")

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:            "file://" + src,
			Reference:      &sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"},
			SparseCheckout: []string{"apps/a"},
		},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch non-branch ref.name with sparse checkout: %v", err)
	}
	if art.Revision != want {
		t.Errorf("revision = %q, want %q", art.Revision, want)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "apps", "a", "manifest.yaml")); err != nil {
		t.Errorf("sparse-included file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "apps", "b", "manifest.yaml")); !os.IsNotExist(err) {
		t.Errorf("sparse-excluded file should be absent; stat err = %v", err)
	}
}

func TestFetcher_SemVerRef(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	mustTagHEAD(t, src, "v1.0.0")
	want := mustCommitFile(t, src, "version.txt", "v1.1.0")
	mustTagHEAD(t, src, "v1.1.0")

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0", SemVer: ">=1.1.0"},
		},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch semver: %v", err)
	}
	if art.Revision != want {
		t.Errorf("semver revision = %q, want %q", art.Revision, want)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "version.txt")); err != nil {
		t.Errorf("semver checkout missing v1.1 file: %v", err)
	}
}

func TestFetcher_CommitPrecedesRefName(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	first := mustTagHEAD(t, src, "v1.0.0")
	mustCommitFile(t, src, "version.txt", "v1.1.0")
	mustTagHEAD(t, src, "v1.1.0")

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL: "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{
				Commit: first,
				Name:   "refs/tags/v1.1.0",
			},
		},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch commit+name: %v", err)
	}
	if art.Revision != first {
		t.Errorf("revision = %q, want commit %q", art.Revision, first)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "version.txt")); !os.IsNotExist(err) {
		t.Errorf("commit should have won over ref.name; version.txt stat err = %v", err)
	}
}

func TestFetcher_CommitPrecedesUnresolvableRefName(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	first := mustTagHEAD(t, src, "v1.0.0")
	mustCommitFile(t, src, "version.txt", "v1.1.0")

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL: "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{
				Commit: first,
				Name:   "refs/pull/does-not-exist/head",
			},
		},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch commit+unresolvable name: %v", err)
	}
	if art.Revision != first {
		t.Errorf("revision = %q, want commit %q", art.Revision, first)
	}
}

func TestFetcher_CommitMustBeReachableFromBranch(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	initial := mustHead(t, src)
	mainOnly := mustCommitFile(t, src, "main-only.txt", "main")
	mustSetRefToHash(t, src, "refs/heads/staging", initial)

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	f := &Fetcher{Cache: cache}

	commitOnly := &manifest.GitRepository{
		Name: "commit-only", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Commit: mainOnly},
		},
	}
	if _, err := f.Fetch(context.Background(), commitOnly); err != nil {
		t.Fatalf("commit-only fetch should populate the unconstrained cache slot: %v", err)
	}

	constrained := &manifest.GitRepository{
		Name: "constrained", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL: "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{
				Branch: "staging",
				Commit: mainOnly,
			},
		},
	}
	_, err := f.Fetch(context.Background(), constrained)
	if err == nil {
		t.Fatal("expected branch-constrained commit fetch to fail")
	}
	if !strings.Contains(err.Error(), "not reachable from branch") {
		t.Fatalf("error should explain branch reachability, got %v", err)
	}
}

// TestFetcher_SparseCheckout exercises spec.sparseCheckout — when
// the repo declares specific directories, the worktree contains only
// those (plus any tree-root metadata go-git keeps).
func TestFetcher_SparseCheckout(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/a/manifest.yaml":  "kind: ConfigMap",
		"apps/b/manifest.yaml":  "kind: ConfigMap",
		"infra/c/manifest.yaml": "kind: ConfigMap",
		"README.md":             "top-level",
	})

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:            "file://" + src,
			SparseCheckout: []string{"apps/a"},
		},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	// apps/a must be present; apps/b must not.
	if _, err := os.Stat(filepath.Join(art.LocalPath, "apps", "a", "manifest.yaml")); err != nil {
		t.Errorf("sparse-included file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "apps", "b", "manifest.yaml")); !os.IsNotExist(err) {
		t.Errorf("sparse-excluded file should be absent; stat err = %v", err)
	}
}

// TestFetcher_AppliesSpecIgnore exercises the spec.ignore wiring: a
// staged tree containing a tracked file matching the user-supplied
// gitignore pattern must have that file deleted post-checkout.
func TestFetcher_AppliesSpecIgnore(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/deployment.yaml": "kind: Deployment",
		"apps/scratch.tmp":     "noise",
		"docs/notes.md":        "drop-me",
	})

	patterns := "*.tmp\ndocs/\n"
	cache := source.NewCache(cacheroot.New(t.TempDir()))
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:    "file://" + src,
			Ignore: &patterns,
		},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "apps", "scratch.tmp")); !os.IsNotExist(err) {
		t.Errorf("expected *.tmp to be removed by spec.ignore; stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "docs")); !os.IsNotExist(err) {
		t.Errorf("expected docs/ to be removed by spec.ignore; stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "apps", "deployment.yaml")); err != nil {
		t.Errorf("non-ignored file should remain: %v", err)
	}
}

// `.flate-git-revision` marker survives ApplyIgnore (the default
// sourceignore patterns don't match `.flate-*`) and lets the next
// Fetch validate the cache without `.git/` — which the ignore step
// itself wipes via the VCS-excludes preset.
func TestFetcher_CacheMarkerSurvivesIgnore(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := source.NewCache(cacheroot.New(t.TempDir()))
	patterns := "/*\n!/hello.txt\n"
	commit := mustHead(t, src)
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Commit: commit},
			Ignore:    &patterns,
		},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	marker := filepath.Join(art.LocalPath, cachedRevisionFile)
	b, err := os.ReadFile(marker) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("expected marker %s to exist after Fetch with restrictive ignore: %v", cachedRevisionFile, err)
	}
	if string(b) == "" {
		t.Errorf("marker exists but is empty")
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, ".git")); err == nil {
		t.Errorf("expected .git/ to be wiped by ApplyIgnore but it still exists")
	}

	// Second Fetch should hit the cache via the marker.
	art2, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	if art2.Revision != string(b) {
		t.Errorf("warm Fetch returned a different revision: %q vs %q", art2.Revision, string(b))
	}
}

func TestFetcher_CommitCacheKeyIncludesIgnore(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"hello.txt": "hello",
		"docs.md":   "docs",
	})
	commit := mustHead(t, src)
	cache := source.NewCache(cacheroot.New(t.TempDir()))
	f := &Fetcher{Cache: cache}

	ignore := "docs.md\n"
	ignoredRepo := &manifest.GitRepository{
		Name: "ignored", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Commit: commit},
			Ignore:    &ignore,
		},
	}
	ignored, err := f.Fetch(context.Background(), ignoredRepo)
	if err != nil {
		t.Fatalf("Fetch ignored: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ignored.LocalPath, "docs.md")); !os.IsNotExist(err) {
		t.Fatalf("ignored checkout should not contain docs.md: %v", err)
	}

	plainRepo := &manifest.GitRepository{
		Name: "plain", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Commit: commit},
		},
	}
	plain, err := f.Fetch(context.Background(), plainRepo)
	if err != nil {
		t.Fatalf("Fetch plain: %v", err)
	}
	if plain.LocalPath == ignored.LocalPath {
		t.Fatalf("different ignore policies reused cache slot %s", plain.LocalPath)
	}
	if _, err := os.Stat(filepath.Join(plain.LocalPath, "docs.md")); err != nil {
		t.Fatalf("plain checkout lost docs.md through ignored cache slot: %v", err)
	}
}

func TestFetcher_CommitCacheKeyIncludesVerification(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	commit := mustHead(t, src)
	cache := source.NewCache(cacheroot.New(t.TempDir()))
	f := &Fetcher{Cache: cache}

	plainRepo := &manifest.GitRepository{
		Name: "plain", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Commit: commit},
		},
	}
	if _, err := f.Fetch(context.Background(), plainRepo); err != nil {
		t.Fatalf("Fetch plain: %v", err)
	}

	verifiedRepo := &manifest.GitRepository{
		Name: "verified", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       plainRepo.URL,
			Reference: &sourcev1.GitRepositoryRef{Commit: commit},
			Verification: &manifest.GitRepositoryVerify{
				Mode:      manifest.GitVerifyModeHEAD,
				SecretRef: manifest.LocalObjectReference{Name: "trusted-keys"},
			},
		},
	}
	if _, err := f.Fetch(context.Background(), verifiedRepo); err == nil {
		t.Fatal("verified fetch reused an unverified commit cache slot")
	} else if !strings.Contains(err.Error(), "source.SecretGetter") {
		t.Fatalf("verified fetch error = %v, want missing SecretGetter", err)
	}
}

func TestGitCacheKeyIncludesVerificationNamespace(t *testing.T) {
	ref := sourcev1.GitRepositoryRef{Commit: strings.Repeat("a", 40)}
	mkRepo := func(ns string) *manifest.GitRepository {
		return &manifest.GitRepository{
			Name: "repo", Namespace: ns,
			GitRepositorySpec: sourcev1.GitRepositorySpec{
				URL:       "https://example.com/repo.git",
				Reference: &ref,
				Verification: &manifest.GitRepositoryVerify{
					Mode:      manifest.GitVerifyModeHEAD,
					SecretRef: manifest.LocalObjectReference{Name: "trusted-keys"},
				},
			},
		}
	}

	a := gitCacheKey(mkRepo("ns-a"), gitRefLabel(ref))
	b := gitCacheKey(mkRepo("ns-b"), gitRefLabel(ref))
	if a == b {
		t.Fatalf("verification cache key ignored namespace: %q", a)
	}
}

// mustInitRepoWithFiles creates a git repo at dir with the given
// {path: contents} entries committed as one revision.
func mustInitRepoWithFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	r, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	for path, content := range files {
		full := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if _, err := wt.Add(path); err != nil {
			t.Fatalf("Add %s: %v", path, err)
		}
	}
	if _, err := wt.Commit("init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// mustTagHEAD creates an annotated tag pointing at the worktree HEAD
// and returns the tagged commit SHA.
func mustTagHEAD(t *testing.T, dir, tag string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := r.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if _, err := r.CreateTag(tag, head.Hash(), nil); err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	return head.Hash().String()
}

func mustHead(t *testing.T, dir string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	head, err := r.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	return head.Hash().String()
}

func mustSetRefToHash(t *testing.T, dir, refName, hash string) {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	ref := plumbing.NewHashReference(plumbing.ReferenceName(refName), plumbing.NewHash(hash))
	if err := r.Storer.SetReference(ref); err != nil {
		t.Fatalf("SetReference %s: %v", refName, err)
	}
}

func mustCommitFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	full := filepath.Join(dir, name)
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	if _, err := wt.Add(name); err != nil {
		t.Fatalf("Add %s: %v", name, err)
	}
	hash, err := wt.Commit("update "+name, &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(1, 0)},
	})
	if err != nil {
		t.Fatalf("Commit %s: %v", name, err)
	}
	return hash.String()
}

// mustInitRepo creates a minimal git repo at dir with one file and one
// commit.
func mustInitRepo(t *testing.T, dir string) {
	t.Helper()
	r, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	hello := filepath.Join(dir, "hello.txt")
	if err := os.WriteFile(hello, []byte("hi"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	if _, err := wt.Add("hello.txt"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := wt.Commit("init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}
