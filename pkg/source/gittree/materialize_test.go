package gittree

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// TestMaterialize_ParallelWritesPreserveContent: seed a repo with N
// files at deep paths, materialize, assert every file lands with the
// right bytes. Exercises the worker fan-out under concurrency.
func TestMaterialize_ParallelWritesPreserveContent(t *testing.T) {
	src := t.TempDir()
	repo := mustInit(t, src)
	files := map[string]string{
		"a.txt":          "alpha",
		"b/c.txt":        "beta-charlie",
		"x/y/z.txt":      "deep",
		"docs/README.md": "# hi",
		"empty.txt":      "",
	}
	for path, content := range files {
		full := filepath.Join(src, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	hash := mustCommit(t, repo, src)

	dst := t.TempDir()
	if err := Materialize(context.Background(), repo, hash, dst, Options{Workers: 4}); err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	for path, want := range files {
		got, err := os.ReadFile(filepath.Join(dst, path)) //nolint:gosec // path under t.TempDir
		if err != nil {
			t.Errorf("read %q: %v", path, err)
			continue
		}
		if string(got) != want {
			t.Errorf("%q content drift: got %q want %q", path, got, want)
		}
	}
}

// TestMaterialize_SubmoduleCallbackWiredCorrectly: a real repo without
// submodules must NOT trip the callback. (Fabricating a submodule
// entry without go-git's submodule API is hard; this is the minimal
// guard against accidentally invoking OnSubmodule for regular files.)
func TestMaterialize_SubmoduleCallbackWiredCorrectly(t *testing.T) {
	src := t.TempDir()
	repo := mustInit(t, src)
	if err := os.WriteFile(filepath.Join(src, "x.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	hash := mustCommit(t, repo, src)

	called := make(map[string]bool)
	if err := Materialize(context.Background(), repo, hash, t.TempDir(), Options{
		Workers:     1,
		OnSubmodule: func(path string) { called[path] = true },
	}); err != nil {
		t.Fatal(err)
	}
	if len(called) != 0 {
		t.Errorf("OnSubmodule fired on a repo without submodules: %v", called)
	}
}

// TestMaterialize_RespectsCtxCancel: a pre-cancelled ctx aborts the
// materialization with a context error.
func TestMaterialize_RespectsCtxCancel(t *testing.T) {
	src := t.TempDir()
	repo := mustInit(t, src)
	for i := range 50 {
		_ = i
		_ = os.WriteFile(filepath.Join(src, "f"+filepath.Base(t.TempDir())), []byte("x"), 0o600)
	}
	hash := mustCommit(t, repo, src)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Materialize(ctx, repo, hash, t.TempDir(), Options{Workers: 2}); err == nil {
		t.Error("expected error from cancelled ctx")
	}
}

func mustInit(t *testing.T, dir string) *git.Repository {
	t.Helper()
	r, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	return r
}

func mustCommit(t *testing.T, repo *git.Repository, dir string) plumbing.Hash {
	t.Helper()
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if d.IsDir() {
			if rel == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		_, err = wt.Add(rel)
		return err
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	h, err := wt.Commit("seed", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return h
}
