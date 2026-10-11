package git

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func TestFetcher_PackedContent(t *testing.T) {
	var blocks [8]string
	for i := range blocks {
		var block strings.Builder
		for line := range 200 {
			fmt.Fprintf(&block, "%x\n", sha256.Sum256(fmt.Appendf(nil, "%d-%d", i, line)))
		}
		blocks[i] = block.String()
	}
	files := map[string]string{"base.txt": strings.Join(blocks[:], ""), "reordered.txt": ""}
	for _, i := range []int{3, 0, 5, 1, 7, 2, 6, 4} {
		files["reordered.txt"] += blocks[i]
	}
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, files)
	for _, mode := range []string{"memory", "disk"} {
		t.Run(mode, func(t *testing.T) {
			root := ""
			if mode == "disk" {
				root = t.TempDir()
			}
			f := &Fetcher{Cache: newGitCache(t, root)}
			art, err := f.Fetch(t.Context(), &manifest.GitRepository{Name: "packed", Namespace: "flux-system", URL: "file://" + filepath.ToSlash(src)})
			if err != nil {
				t.Fatal(err)
			}
			var readers sync.WaitGroup
			for range 2 {
				readers.Go(func() {
					for file, want := range files {
						got, err := fs.ReadFile(art.FS, file)
						if err != nil || string(got) != want {
							t.Errorf("packed %s content differs: bytes=%d, err=%v", file, len(got), err)
						}
					}
				})
			}
			readers.Wait()
		})
	}
}

func TestFetcher_SourceLinksOmitted(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{"hello.txt": "hi", "apps/cm.yaml": "kind: ConfigMap"})
	for name, target := range map[string]string{"alias": "hello.txt", "dirlink": "apps", ".sourceignore": "hello.txt"} {
		if err := os.Symlink(target, filepath.Join(src, name)); err != nil {
			t.Fatal(err)
		}
	}
	r, err := git.PlainOpen(src)
	if err != nil {
		t.Fatal(err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Add("."); err != nil {
		t.Fatal(err)
	}
	mustCommitFile(t, src, "hello.txt", "hi")
	f := &Fetcher{Cache: newGitCache(t, "")}
	art, err := f.Fetch(t.Context(), &manifest.GitRepository{Name: "links", URL: "file://" + src})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alias", "dirlink", "dirlink/cm.yaml", ".sourceignore"} {
		if file, err := art.FS.Open(name); !errors.Is(err, fs.ErrNotExist) {
			if file != nil {
				_ = file.Close()
			}
			t.Errorf("Open(%s): %v", name, err)
		}
		if _, err := fs.Stat(art.FS, name); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("Stat(%s): %v", name, err)
		}
		if _, err := fs.ReadDir(art.FS, name); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("ReadDir(%s): %v", name, err)
		}
	}
	entries, err := fs.ReadDir(art.FS, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "alias" || entry.Name() == "dirlink" || entry.Name() == ".sourceignore" {
			t.Errorf("listed link %s", entry.Name())
		}
	}
}

func TestFetcher_LocalFileURL(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL: "file://" + src,
	}

	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	sa := art
	if sa.FS == nil || sa.Key == "" || sa.URL == "" {
		t.Errorf("incomplete artifact: %+v", sa)
	}
	if _, err := fs.Stat(sa.FS, path.Join("hello.txt")); err != nil {
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

// TestFetcher_RestrictSchemesBlocksFileURL: with RestrictSchemes on (the
// untrusted-render guard, #743), a file:// GitRepository is rejected before
// any clone with an ErrInput-wrapped error naming the scheme; with it off
// (the default) the same file:// source clones normally. Guards against a
// fork-PR file:// source disclosing an arbitrary in-pod git repo.
func TestFetcher_RestrictSchemesBlocksFileURL(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	repo := &manifest.GitRepository{
		Name: "evil", Namespace: "flux-system",
		URL: "file://" + src,
	}

	blocked := &Fetcher{Cache: newGitCache(t, t.TempDir()), RestrictSchemes: true}
	_, err := blocked.Fetch(context.Background(), repo)
	if err == nil {
		t.Fatal("RestrictSchemes: true must reject a file:// GitRepository")
	}
	if !errors.Is(err, manifest.ErrInput) {
		t.Errorf("error must wrap manifest.ErrInput; got %v", err)
	}
	if !strings.Contains(err.Error(), "scheme not allowed") {
		t.Errorf("error must name the rejected scheme; got %v", err)
	}

	allowed := &Fetcher{Cache: newGitCache(t, t.TempDir())} // RestrictSchemes defaults off
	if _, err := allowed.Fetch(context.Background(), repo); err != nil {
		t.Fatalf("RestrictSchemes off must allow file://: %v", err)
	}
}

// TestFetcher_RestrictSchemesAllowsRemoteURLs: RestrictSchemes only blocks
// local/plaintext schemes — an https:// or scp-style (git@host:path) URL
// passes the validateRepo scheme check even when the guard is on. Asserted at
// the validation layer to avoid a live clone of an unreachable remote.
func TestFetcher_RestrictSchemesAllowsRemoteURLs(t *testing.T) {
	f := &Fetcher{RestrictSchemes: true}
	for _, url := range []string{"https://github.com/o/r", "ssh://git@github.com/o/r", "git@github.com:o/r.git"} {
		repo := &manifest.GitRepository{
			Name: "r", Namespace: "flux-system",
			URL: url,
		}
		if err := f.validateRepo(repo); err != nil {
			t.Errorf("validateRepo(%q) under RestrictSchemes must pass: %v", url, err)
		}
	}
}

func TestGitSchemeAllowed(t *testing.T) {
	cases := map[string]bool{
		"https://github.com/o/r":    true,
		"ssh://git@github.com/o/r":  true,
		"git@github.com:o/r.git":    true, // scp-style → SSH
		"user@host.xz:path/to/repo": true,
		"file:///srv/repo":          false,
		"git://github.com/o/r":      false,
		"http://github.com/o/r":     false,
		"/bare/local/path":          false,
		"./relative":                false,
		"ssh-but-not-scheme://x":    false, // not the ssh:// prefix
		"":                          false,
	}
	for url, want := range cases {
		if got := gitSchemeAllowed(url); got != want {
			t.Errorf("gitSchemeAllowed(%q) = %v, want %v", url, got, want)
		}
	}
}

func TestFetcher_MutableDefaultRefRefreshesCache(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL: "file://" + src,
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
	if _, err := fs.Stat(art2.FS, path.Join("later.txt")); err != nil {
		t.Fatalf("refreshed checkout missing later.txt: %v", err)
	}
}

func TestFetcher_MutableRefUsesFreshIntervalCache(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:      "file://" + src,
		Interval: metav1.Duration{Duration: time.Hour},
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
	if _, err := fs.Stat(art2.FS, path.Join("later.txt")); !os.IsNotExist(err) {
		t.Fatalf("fresh interval cache should not include later commit, stat err=%v", err)
	}
}

func TestFetcher_MutableRefreshFailureKeepsHeldGeneration(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL: "file://" + src,
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
	if _, err := fs.Stat(art.FS, path.Join("hello.txt")); err != nil {
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

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Name: "refs/tags/v0.1.0"},
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

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Name: "refs/heads/does-not-exist"},
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

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:            "file://" + src,
		Reference:      &sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"},
		SparseCheckout: []string{"apps/a"},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch non-branch ref.name with sparse checkout: %v", err)
	}
	if art.Revision != want {
		t.Errorf("revision = %q, want %q", art.Revision, want)
	}
	if _, err := fs.Stat(art.FS, path.Join("apps", "a", "manifest.yaml")); err != nil {
		t.Errorf("sparse-included file missing: %v", err)
	}
	if _, err := fs.Stat(art.FS, path.Join("apps", "b", "manifest.yaml")); !os.IsNotExist(err) {
		t.Errorf("sparse-excluded file should be absent; stat err = %v", err)
	}
}

func TestFetcher_SemVerRef(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	mustTagHEAD(t, src, "v1.0.0")
	want := mustCommitFile(t, src, "version.txt", "v1.1.0")
	mustTagHEAD(t, src, "v1.1.0")

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0", SemVer: ">=1.1.0"},
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch semver: %v", err)
	}
	if art.Revision != want {
		t.Errorf("semver revision = %q, want %q", art.Revision, want)
	}
	if _, err := fs.Stat(art.FS, path.Join("version.txt")); err != nil {
		t.Errorf("semver checkout missing v1.1 file: %v", err)
	}
}

func TestFetcher_CommitPrecedesRefName(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	first := mustTagHEAD(t, src, "v1.0.0")
	mustCommitFile(t, src, "version.txt", "v1.1.0")
	mustTagHEAD(t, src, "v1.1.0")

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL: "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{
			Commit: first,
			Name:   "refs/tags/v1.1.0",
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
	if _, err := fs.Stat(art.FS, path.Join("version.txt")); !os.IsNotExist(err) {
		t.Errorf("commit should have won over ref.name; version.txt stat err = %v", err)
	}
}

func TestFetcher_CommitPrecedesUnresolvableRefName(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	first := mustTagHEAD(t, src, "v1.0.0")
	mustCommitFile(t, src, "version.txt", "v1.1.0")

	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL: "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{
			Commit: first,
			Name:   "refs/pull/does-not-exist/head",
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

	cache := newGitCache(t, t.TempDir())
	f := &Fetcher{Cache: cache}

	commitOnly := &manifest.GitRepository{
		Name: "commit-only", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Commit: mainOnly},
	}
	if _, err := f.Fetch(context.Background(), commitOnly); err != nil {
		t.Fatalf("commit-only fetch should populate the unconstrained cache slot: %v", err)
	}

	constrained := &manifest.GitRepository{
		Name: "constrained", Namespace: "flux-system",
		URL: "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{
			Branch: "staging",
			Commit: mainOnly,
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

func TestFetcher_SparseCheckout(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/a/manifest.yaml":     "kind: ConfigMap",
		"apps/b/manifest.yaml":     "kind: ConfigMap",
		"apps-extra/manifest.yaml": "kind: ConfigMap",
		"infra/c/manifest.yaml":    "kind: ConfigMap",
	})
	f := &Fetcher{Cache: newGitCache(t, t.TempDir())}
	for _, prefix := range []string{"apps/a", "apps"} {
		t.Run(prefix, func(t *testing.T) {
			art, err := f.Fetch(t.Context(), &manifest.GitRepository{
				Name: "test", Namespace: "flux-system", URL: "file://" + src,
				SparseCheckout: []string{prefix},
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range []string{"apps/a/manifest.yaml", "apps/b/manifest.yaml", "apps-extra/manifest.yaml", "infra/c/manifest.yaml"} {
				_, err := fs.Stat(art.FS, file)
				if strings.HasPrefix(file, prefix) {
					if err != nil {
						t.Errorf("included %s: %v", file, err)
					}
				} else if !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("excluded %s: %v", file, err)
				}
			}
		})
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
	cache := newGitCache(t, t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:    "file://" + src,
		Ignore: &patterns,
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if _, err := fs.Stat(art.FS, path.Join("apps", "scratch.tmp")); !os.IsNotExist(err) {
		t.Errorf("expected *.tmp to be removed by spec.ignore; stat err = %v", err)
	}
	if _, err := fs.Stat(art.FS, path.Join("docs")); !os.IsNotExist(err) {
		t.Errorf("expected docs/ to be removed by spec.ignore; stat err = %v", err)
	}
	if _, err := fs.Stat(art.FS, path.Join("apps", "deployment.yaml")); err != nil {
		t.Errorf("non-ignored file should remain: %v", err)
	}
}

// `.flate-git-revision` marker survives ApplyIgnore (the default
// sourceignore patterns don't match `.flate-*`) and lets the next
// Fetch validate the cache without `.git/` — which the ignore step
// itself wipes via the VCS-excludes preset.
func TestFetcher_CommitHitSurvivesIgnore(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := newGitCache(t, t.TempDir())
	patterns := "/*\n!/hello.txt\n"
	commit := mustHead(t, src)
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Commit: commit},
		Ignore:    &patterns,
	}
	f := &Fetcher{Cache: cache}
	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if _, err := fs.Stat(art.FS, ".git"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("git metadata visible: %v", err)
	}
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	art2, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	if art2.Revision != art.Revision {
		t.Errorf("warm Fetch returned a different revision: %q vs %q", art2.Revision, art.Revision)
	}
}

func TestFetcher_CommitCacheKeyIncludesIgnore(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"hello.txt": "hello",
		"docs.md":   "docs",
	})
	commit := mustHead(t, src)
	cache := newGitCache(t, t.TempDir())
	f := &Fetcher{Cache: cache}

	ignore := "docs.md\n"
	ignoredRepo := &manifest.GitRepository{
		Name: "ignored", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Commit: commit},
		Ignore:    &ignore,
	}
	ignored, err := f.Fetch(context.Background(), ignoredRepo)
	if err != nil {
		t.Fatalf("Fetch ignored: %v", err)
	}
	if _, err := fs.Stat(ignored.FS, path.Join("docs.md")); !os.IsNotExist(err) {
		t.Fatalf("ignored checkout should not contain docs.md: %v", err)
	}

	plainRepo := &manifest.GitRepository{
		Name: "plain", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Commit: commit},
	}
	plain, err := f.Fetch(context.Background(), plainRepo)
	if err != nil {
		t.Fatalf("Fetch plain: %v", err)
	}
	if plain.Key == ignored.Key {
		t.Fatalf("different ignore policies reused cache slot %s", plain.Key)
	}
	if _, err := fs.Stat(plain.FS, path.Join("docs.md")); err != nil {
		t.Fatalf("plain checkout lost docs.md through ignored cache slot: %v", err)
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

func newGitCache(t testing.TB, root string) *sourcecache.Cache {
	t.Helper()
	c, err := sourcecache.New(t.Context(), cacheroot.New(root))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
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
