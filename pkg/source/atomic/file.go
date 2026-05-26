// Package atomic carries the small "stage-then-rename" file write
// helper every flate cache writer used to reinvent (helm.writeAtomic,
// oci.writeCachedDigest, oci.writeVerifyMarker, blob.Refs.Put). One
// implementation, one set of error shapes, one place to revisit if
// the durability story needs to change.
package atomic

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteFile installs data at path via a sibling temp file + atomic
// rename. Concurrent readers either see the previous contents or the
// new ones — never a torn write, never a zero-byte file from a
// power-loss between create and rename.
//
// perm is the final file's permission bits (umask still applies via
// CreateTemp). On any error, the staging file is removed and path is
// left untouched.
//
// When syncDir is true, the function fsyncs both the staged file's
// contents AND the containing directory's entry after the rename so
// the rename survives power loss. Set false for high-churn caches
// where durability doesn't justify the I/O barrier (e.g. ref pointers
// that are cheap to re-derive on the next reconcile).
func WriteFile(path string, data []byte, perm os.FileMode, syncDir bool) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("atomic stage: %w", err)
	}
	tmpName := tmp.Name()
	// On any error path we want the staging file gone. The double-
	// defer with a sentinel keeps the success path's defer a no-op
	// without the caller having to think about it.
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
		// Best-effort directory fsync — see writeAtomic's history
		// for why this matters and why it's allowed to fail on
		// filesystems that don't expose the operation.
		if d, derr := os.Open(dir); derr == nil { //nolint:gosec // dir = filepath.Dir(path); caller-controlled path
			_ = d.Sync()
			_ = d.Close()
		}
	}
	return nil
}
