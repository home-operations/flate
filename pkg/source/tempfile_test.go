package source

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTempFiles_WritePermsAndContent(t *testing.T) {
	tf := NewTempFiles(t.TempDir())
	path, err := tf.Write("creds-*.json", "hello")
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if path == "" {
		t.Fatal("expected non-empty path for non-empty content")
	}
	b, err := os.ReadFile(path) //nolint:gosec // test-controlled path
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(b) != "hello" {
		t.Fatalf("content = %q, want %q", b, "hello")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// os.CreateTemp yields 0o600; the helper must not loosen it since it
	// holds secret material.
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("perm = %o, want 0600", perm)
	}
}

func TestTempFiles_DirIsRespected(t *testing.T) {
	dir := t.TempDir()
	tf := NewTempFiles(dir)
	path, err := tf.Write("tls-*.pem", "pem")
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := filepath.Dir(path); got != dir {
		t.Fatalf("file written to %q, want under %q", got, dir)
	}
}

func TestTempFiles_EmptyContentWritesNoFile(t *testing.T) {
	dir := t.TempDir()
	tf := NewTempFiles(dir)
	path, err := tf.Write("ca-*.pem", "")
	if err != nil {
		t.Fatalf("Write(empty): %v", err)
	}
	if path != "" {
		t.Fatalf("empty content produced path %q, want \"\"", path)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty content created %d file(s), want 0", len(entries))
	}
	// Cleanup is a no-op when nothing was written.
	tf.Cleanup()
}

func TestTempFiles_CleanupRemovesAll(t *testing.T) {
	tf := NewTempFiles(t.TempDir())
	var paths []string
	for _, c := range []string{"a", "b", "c"} {
		p, err := tf.Write("f-*.txt", c)
		if err != nil {
			t.Fatalf("Write %q: %v", c, err)
		}
		paths = append(paths, p)
	}
	tf.Cleanup()
	for _, p := range paths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("file %q still present after Cleanup (err=%v)", p, err)
		}
	}
}

// TestTempFiles_FailedWriteKeepsPriorForCleanup is the load-bearing
// contract: a mid-sequence Write failure must NOT discard files from
// earlier successful Writes — the caller reaps them with a deferred
// Cleanup. A pattern containing a path separator makes os.CreateTemp
// fail deterministically without touching the filesystem.
func TestTempFiles_FailedWriteKeepsPriorForCleanup(t *testing.T) {
	tf := NewTempFiles(t.TempDir())
	first, err := tf.Write("ok-*.pem", "first")
	if err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := tf.Write("bad/slash-*.pem", "second"); err == nil {
		t.Fatal("expected error from pattern with path separator, got nil")
	}
	// The failed Write created no file and left the prior registration
	// intact; Cleanup must still remove the first file.
	tf.Cleanup()
	if _, err := os.Stat(first); !os.IsNotExist(err) {
		t.Fatalf("first file not cleaned after a later failed Write (err=%v)", err)
	}
}

func TestTempFiles_CreateTempErrorReturnsNoPath(t *testing.T) {
	tf := NewTempFiles(filepath.Join(t.TempDir(), "does-not-exist"))
	path, err := tf.Write("x-*.tmp", "data")
	if err == nil {
		t.Fatal("expected error writing into a non-existent dir, got nil")
	}
	if path != "" {
		t.Fatalf("error path returned %q, want \"\"", path)
	}
	// Nothing registered, so Cleanup is a harmless no-op.
	tf.Cleanup()
}
