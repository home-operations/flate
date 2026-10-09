package gittree

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
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
	for range 50 {
		_ = os.WriteFile(filepath.Join(src, "f"+filepath.Base(t.TempDir())), []byte("x"), 0o600)
	}
	hash := mustCommit(t, repo, src)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Materialize(ctx, repo, hash, t.TempDir(), Options{Workers: 2}); err == nil {
		t.Error("expected error from cancelled ctx")
	}
}

func TestMaterialize_PreCancelledContextAvoidsRootAccess(t *testing.T) {
	src := t.TempDir()
	repo := mustInit(t, src)
	testutil.WriteFile(t, src, "value", "committed")
	hash := mustCommit(t, repo, src)
	for _, tt := range []struct {
		name   string
		exists bool
	}{
		{name: "existing", exists: true},
		{name: "missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "staging")
			if tt.exists {
				if err := os.Mkdir(root, 0o750); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := Materialize(ctx, repo, hash, root, Options{Workers: 2}); !errors.Is(err, context.Canceled) {
				t.Fatalf("pre-cancelled materialization = %v, want context.Canceled", err)
			}
			entries, err := os.ReadDir(root)
			if tt.exists {
				if err != nil || len(entries) != 0 {
					t.Fatalf("cancelled root changed: %v, %v", entries, err)
				}
			} else if !os.IsNotExist(err) {
				t.Fatalf("cancelled materialization created root: %v", err)
			}
		})
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

func TestMaterialize_RejectsMalformedEntries(t *testing.T) {
	names := []string{"..", "../escape", "/escape", "a/b", "."}
	if os.PathSeparator == '\\' {
		names = append(names, `a\b`)
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			repo := mustInit(t, t.TempDir())
			parent := t.TempDir()
			root := filepath.Join(parent, "staging")
			if err := os.Mkdir(root, 0o750); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []filemode.FileMode{filemode.Regular, filemode.Dir} {
				hash := craftedCommit(t, repo, object.TreeEntry{Name: name, Mode: mode})
				if err := Materialize(t.Context(), repo, hash, root, Options{Workers: 2}); err == nil {
					t.Fatalf("accepted malformed %s entry %q", mode, name)
				}
			}
			if _, err := os.Stat(filepath.Join(parent, "escape")); !os.IsNotExist(err) {
				t.Fatalf("write escaped staging: %v", err)
			}
		})
	}
	if os.PathSeparator != '\\' {
		t.Run(`a\b`, func(t *testing.T) {
			repo := mustInit(t, t.TempDir())
			hash := craftedCommit(t, repo, object.TreeEntry{Name: `a\b`, Mode: filemode.Regular})
			root := t.TempDir()
			if err := Materialize(t.Context(), repo, hash, root, Options{Workers: 2}); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, `a\b`)
			if info, err := os.Lstat(path); err != nil || !info.Mode().IsRegular() {
				t.Fatalf("backslash entry is not a regular file: %v, %v", info, err)
			}
			if content, err := os.ReadFile(path); err != nil || string(content) != "malicious" {
				t.Fatalf("backslash file content = %q, %v", content, err)
			}
		})
	}
}

func TestMaterialize_RejectsSymlinkDestinations(t *testing.T) {
	for _, target := range []string{"dir", "dir/file", "file"} {
		t.Run(target, func(t *testing.T) {
			src := t.TempDir()
			repo := mustInit(t, src)
			testutil.WriteFile(t, src, target+"/value", "committed")
			hash := mustCommit(t, repo, src)
			root, outside := t.TempDir(), t.TempDir()
			if err := os.Symlink(outside, filepath.Join(root, "dir")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(root, "file")); err != nil {
				t.Fatal(err)
			}
			if err := Materialize(t.Context(), repo, hash, root, Options{Workers: 4}); err == nil {
				t.Fatal("accepted a symlink ancestor outside staging")
			}
			entries, err := os.ReadDir(outside)
			if err != nil || len(entries) != 0 {
				t.Fatalf("outside tree changed: %v, %v", entries, err)
			}
		})
	}
}

func TestMaterialize_PreservesSymlinksAndExecutableModes(t *testing.T) {
	src := t.TempDir()
	repo := mustInit(t, src)
	testutil.WriteFile(t, src, "bin/script", "#!/bin/sh\nexit 0\n")
	if err := os.Chmod(filepath.Join(src, "bin/script"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("bin/script", filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}
	hash := mustCommit(t, repo, src)
	root := t.TempDir()
	if err := Materialize(t.Context(), repo, hash, root, Options{Workers: 4}); err != nil {
		t.Fatal(err)
	}
	if target, err := os.Readlink(filepath.Join(root, "link")); err != nil || target != "bin/script" {
		t.Fatalf("symlink = %q, %v", target, err)
	}
	if info, err := os.Stat(filepath.Join(root, "bin/script")); err != nil || info.Mode()&0o100 == 0 {
		t.Fatalf("executable mode lost: %v, %v", info, err)
	}
}

func TestMaterialize_RejectsExistingSymlinkFile(t *testing.T) {
	src := t.TempDir()
	repo := mustInit(t, src)
	testutil.WriteFile(t, src, "value", "committed")
	hash := mustCommit(t, repo, src)
	root, outside := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, outside, "value", "untouched")
	if err := os.Symlink(filepath.Join(outside, "value"), filepath.Join(root, "value")); err != nil {
		t.Fatal(err)
	}
	if err := Materialize(t.Context(), repo, hash, root, Options{Workers: 2}); err == nil {
		t.Fatal("followed an existing symlink file")
	}
	if content, err := os.ReadFile(filepath.Join(outside, "value")); err != nil || string(content) != "untouched" {
		t.Fatalf("outside file changed: %q, %v", content, err)
	}
}

func TestMaterialize_RejectsUnreadableDirectoryObjects(t *testing.T) {
	repo := mustInit(t, t.TempDir())
	hash := craftedCommit(t, repo, object.TreeEntry{Name: "dir", Mode: filemode.Dir})
	commit, err := repo.CommitObject(hash)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatal(err)
	}
	tree.Entries[0].Hash = plumbing.NewHash("ffffffffffffffffffffffffffffffffffffffff")
	encoded := repo.Storer.NewEncodedObject()
	if err := tree.Encode(encoded); err != nil {
		t.Fatal(err)
	}
	commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	encoded = repo.Storer.NewEncodedObject()
	if err := commit.Encode(encoded); err != nil {
		t.Fatal(err)
	}
	hash, err = repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := Materialize(t.Context(), repo, hash, t.TempDir(), Options{Workers: 2}); !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Fatalf("unreadable directory became successful EOF: %v", err)
	}
}

func craftedCommit(t *testing.T, repo *git.Repository, entry object.TreeEntry) plumbing.Hash {
	t.Helper()
	blob := repo.Storer.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, err := blob.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("malicious")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	entry.Hash, err = repo.Storer.SetEncodedObject(blob)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Mode == filemode.Dir {
		child := repo.Storer.NewEncodedObject()
		if err := (&object.Tree{}).Encode(child); err != nil {
			t.Fatal(err)
		}
		entry.Hash, err = repo.Storer.SetEncodedObject(child)
		if err != nil {
			t.Fatal(err)
		}
	}
	tree := &object.Tree{Entries: []object.TreeEntry{entry}}
	encoded := repo.Storer.NewEncodedObject()
	if err := tree.Encode(encoded); err != nil {
		t.Fatal(err)
	}
	treeHash, err := repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	commit := &object.Commit{TreeHash: treeHash, Author: object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}}
	encoded = repo.Storer.NewEncodedObject()
	if err := commit.Encode(encoded); err != nil {
		t.Fatal(err)
	}
	hash, err := repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return hash
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
