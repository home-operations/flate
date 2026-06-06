package source

import (
	"fmt"
	"os"
)

// TempFiles collects temp files written from secret material so a
// fetcher can hand their paths to a downstream tool that expects
// on-disk files — helm's getter (WithTLSClientConfig takes paths) or
// docker's file-backed credential store — then remove them all once
// the fetch completes.
//
// dir selects the parent passed to os.CreateTemp: "" uses the system
// temp dir; a non-empty dir scopes the files under a caller-owned
// location (e.g. helm's per-fetch tmpDir). Files land with
// os.CreateTemp's default 0o600 perms.
//
// One TempFiles belongs to one in-flight fetch; it is not safe for
// concurrent use.
type TempFiles struct {
	dir   string
	files []string
}

// NewTempFiles returns a collector that writes into dir. An empty dir
// uses the system temp directory.
func NewTempFiles(dir string) *TempFiles {
	return &TempFiles{dir: dir}
}

// Write materializes content to a fresh temp file (os.CreateTemp
// pattern semantics — the last "*" is replaced by a random string) and
// returns its path, registering it for Cleanup.
//
// Empty content writes no file and returns ("", nil): callers
// materializing an optional secret key (e.g. ca.crt with no key) rely
// on the empty path to mean "absent". A write that fails removes only
// the partial file it just created; files from earlier successful
// Writes stay registered so the caller's deferred Cleanup reaps them.
func (t *TempFiles) Write(pattern, content string) (string, error) {
	if content == "" {
		return "", nil
	}
	f, err := os.CreateTemp(t.dir, pattern)
	if err != nil {
		return "", fmt.Errorf("create temp %s: %w", pattern, err)
	}
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("write temp %s: %w", pattern, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("close temp %s: %w", pattern, err)
	}
	t.files = append(t.files, f.Name())
	return f.Name(), nil
}

// Cleanup removes every file a successful Write registered. It is a
// no-op when nothing was written, so it is safe to defer immediately
// after construction.
func (t *TempFiles) Cleanup() {
	for _, p := range t.files {
		_ = os.Remove(p)
	}
}
