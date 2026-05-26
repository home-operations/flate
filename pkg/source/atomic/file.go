// Package atomic provides a stage-then-rename WriteFile primitive. Using a
// sibling temp file + os.Rename collapses what would otherwise be a
// create-write-close-chmod-rename sequence into a single visible transition,
// satisfying POSIX atomicity for cache writers that must never expose a partial
// or zero-byte file to concurrent readers.
package atomic

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteFile writes data to path atomically. The file is first written to a
// sibling temp file in the same directory (ensuring rename stays on the same
// filesystem / inode device), then renamed into place. Readers always observe
// either the previous complete contents or the new complete contents.
//
// perm is applied to the staged file before rename; umask still applies via
// CreateTemp. On any error after staging, the temp file is removed and path is
// left untouched.
//
// syncDir controls the durability guarantee. When true the function calls
// Sync on the staged file before rename, then opens and fsyncs the directory
// after rename — this flushes the rename journal entry so the new name
// survives a power loss. Set false for high-churn caches whose values are
// cheap to re-derive on the next reconcile.
func WriteFile(path string, data []byte, perm os.FileMode, syncDir bool) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("atomic stage: %w", err)
	}
	tmpName := tmp.Name()
	// Guard ensures the staging file is removed on any error path without
	// requiring every return site to call os.Remove explicitly.
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("atomic write: %w", err)
	}
	if syncDir {
		// Flush data to the storage device before rename so a crash
		// between Sync and Rename doesn't leave the old name pointing
		// at a partially-written inode.
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("atomic sync: %w", err)
		}
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("atomic close: %w", err)
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return fmt.Errorf("atomic chmod: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("atomic rename: %w", err)
	}
	committed = true
	if syncDir {
		// Fsyncing the directory persists the rename's directory entry
		// update. Without this, a crash after Rename but before the
		// journal flushes can leave the directory pointing at the old
		// name. Best-effort: some filesystems (tmpfs, network FS) ignore
		// fsync on directories without returning an error.
		if d, derr := os.Open(dir); derr == nil { //nolint:gosec // dir = filepath.Dir(path); caller-controlled path
			_ = d.Sync()
			_ = d.Close()
		}
	}
	return nil
}
