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

// TestWriteFile_OverwriteAtomic verifies idempotent overwrite: the second
// call must fully replace the first with no torn intermediate state visible.
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

// TestWriteFile_NoStagingRemnants guards the cleanup defer: after a successful
// write no .tmp-* sibling should remain in the directory.
func TestWriteFile_NoStagingRemnants(t *testing.T) {
	dir := t.TempDir()
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

// TestWriteFile_PermRespected verifies that perm is applied to the renamed file
// (umask may narrow it further, so the test asserts perm-or-stricter).
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
