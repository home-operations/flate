// Package testutil is shared fixture scaffolding for tests across the
// repo — kept minimal so each test file stays self-describing.
package testutil

import (
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// WriteFile writes body to root/rel, creating any missing parent
// directories. Fails the test on any I/O error.
//
// Accepts testing.TB so benchmarks (*testing.B) can share the same
// fixture helper as unit tests (*testing.T) — fixture building is
// part of the bench's setup phase, not the measured loop.
func WriteFile(tb testing.TB, root, rel, body string) {
	tb.Helper()
	writeFileWithParents(tb, filepath.Join(root, rel), body)
}

// WriteFileAt writes body to path (absolute or cwd-relative), creating
// any missing parent directories. Companion to WriteFile for callers
// that already hold a full path rather than a root+rel split.
func WriteFileAt(tb testing.TB, path, body string) {
	tb.Helper()
	writeFileWithParents(tb, path, body)
}

func writeFileWithParents(tb testing.TB, path, body string) {
	tb.Helper()
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		tb.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		tb.Fatalf("write %s: %v", path, err)
	}
}

// CopyTree copies src into dst using fixture directory and file modes.
func CopyTree(tb testing.TB, src, dst string) {
	tb.Helper()
	err := filepath.WalkDir(src, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		data, err := fs.ReadFile(os.DirFS(src), filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		WriteFileAt(tb, target, string(data))
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
}

// WriteTree writes files beneath a symlink-resolved temporary directory.
func WriteTree(tb testing.TB, files map[string]string) string {
	tb.Helper()
	root, err := filepath.EvalSymlinks(tb.TempDir())
	if err != nil {
		tb.Fatalf("evalsymlinks tempdir: %v", err)
	}
	for _, name := range slices.Sorted(maps.Keys(files)) {
		WriteFile(tb, root, name, files[name])
	}
	return root
}
