// Package testutil is shared fixture scaffolding for tests across the
// repo — kept minimal so each test file stays self-describing.
package testutil

import (
	"os"
	"path/filepath"
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
