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
func WriteFile(t testing.TB, root, rel, body string) {
	t.Helper()
	writeFileWithParents(t, filepath.Join(root, rel), body)
}

// WriteFileAt writes body to path (absolute or cwd-relative), creating
// any missing parent directories. Companion to WriteFile for callers
// that already hold a full path rather than a root+rel split.
func WriteFileAt(t testing.TB, path, body string) {
	t.Helper()
	writeFileWithParents(t, path, body)
}

// writeFileWithParents is the shared mkdir-parents + write that both
// WriteFile and WriteFileAt delegate to once they've computed the path.
func writeFileWithParents(t testing.TB, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
