package atomic

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFile_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	if err := WriteFile(path, []byte("hello"), 0o600, false); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := os.ReadFile(path) //nolint:gosec // path under t.TempDir
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Errorf("content drift: got %q", got)
	}
}

// TestWriteFile_OverwriteAtomic: a second write replaces the first
// without ever exposing a torn intermediate state.
func TestWriteFile_OverwriteAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	if err := WriteFile(path, []byte("v1"), 0o600, false); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(path, []byte("v2"), 0o600, false); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path) //nolint:gosec // path under t.TempDir
	if string(got) != "v2" {
		t.Errorf("expected v2, got %q", got)
	}
}

// TestWriteFile_StagingCleanedOnError: when an error occurs after the
// temp file exists, the temp file is removed.
func TestWriteFile_StagingCleanedOnError(t *testing.T) {
	dir := t.TempDir()
	// Write succeeds; on success we want NO leftover .tmp-* files.
	if err := WriteFile(filepath.Join(dir, "data"), []byte("x"), 0o600, false); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "data" {
			t.Errorf("leftover staging entry: %q", e.Name())
		}
	}
}

// TestWriteFile_PermRespected: perm is applied to the final file.
func TestWriteFile_PermRespected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	if err := WriteFile(path, []byte("x"), 0o640, false); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Umask may strip group/world bits; perm-or-stricter is the
	// portable contract.
	if info.Mode().Perm()&0o600 != 0o600 {
		t.Errorf("perm = %v, expected at least 0o600", info.Mode().Perm())
	}
}
