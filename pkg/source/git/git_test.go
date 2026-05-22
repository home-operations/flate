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
