package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestFetchGit_LocalFileURL(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cache := NewCache(t.TempDir())
	repo := &manifest.GitRepository{
		Name:      "test",
		Namespace: "flux-system",
		URL:       "file://" + src,
	}

	art, err := FetchGit(context.Background(), cache, repo, nil)
	if err != nil {
		t.Fatalf("FetchGit: %v", err)
	}
	if art.LocalPath == "" || art.URL == "" {
		t.Errorf("incomplete artifact: %+v", art)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "hello.txt")); err != nil {
		t.Errorf("expected checked-out file: %v", err)
	}

	// Second call should reuse cache.
	art2, err := FetchGit(context.Background(), cache, repo, nil)
	if err != nil {
		t.Fatalf("FetchGit second: %v", err)
	}
	if art2.LocalPath != art.LocalPath {
		t.Errorf("cache slot changed: %s vs %s", art.LocalPath, art2.LocalPath)
	}
}

func TestSlugifyRepo(t *testing.T) {
	cases := map[string]string{
		"https://github.com/owner/cluster.git":                "cluster",
		"git@github.com:owner/cluster.git":                    "cluster",
		"https://example.com/long-path/with/slashes/repo.git": "repo",
		"oci://ghcr.io/stefanprodan/charts/podinfo":           "podinfo",
		"": "repo",
	}
	for in, want := range cases {
		if got := slugifyRepo(in); got != want {
			t.Errorf("slugifyRepo(%q) = %q want %q", in, got, want)
		}
	}
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
