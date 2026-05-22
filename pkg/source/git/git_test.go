package git

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

func TestFetcher_LocalFileURL(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := source.NewCache(t.TempDir())
	repo := &manifest.GitRepository{
		Name:      "test",
		Namespace: "flux-system",
		URL:       "file://" + src,
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

	// Second call should reuse cache.
	art2, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch second: %v", err)
	}
	if art2.LocalPath != sa.LocalPath {
		t.Errorf("cache slot changed: %s vs %s", sa.LocalPath, art2.LocalPath)
	}
}

// TestFetcher_RefByName exercises spec.ref.name handling — flate
// resolves it to a commit via git.ResolveRevision and checks out the
// resulting hash.
func TestFetcher_RefByName(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	tagged := mustTagHEAD(t, src, "v0.1.0")

	cache := source.NewCache(t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL: "file://" + src,
		Ref: manifest.GitRepositoryRef{Name: "refs/tags/v0.1.0"},
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

	cache := source.NewCache(t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL: "file://" + src,
		Ref: manifest.GitRepositoryRef{Name: "refs/heads/does-not-exist"},
	}
	f := &Fetcher{Cache: cache}
	_, err := f.Fetch(context.Background(), repo)
	if err == nil {
		t.Fatalf("expected unresolvable-ref error")
	}
}

// TestFetcher_SparseCheckout exercises spec.sparseCheckout — when
// the repo declares specific directories, the worktree contains only
// those (plus any tree-root metadata go-git keeps).
func TestFetcher_SparseCheckout(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/a/manifest.yaml":   "kind: ConfigMap",
		"apps/b/manifest.yaml":   "kind: ConfigMap",
		"infra/c/manifest.yaml":  "kind: ConfigMap",
		"README.md":              "top-level",
	})

	cache := source.NewCache(t.TempDir())
	repo := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system",
		URL:            "file://" + src,
		SparseCheckout: []string{"apps/a"},
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
