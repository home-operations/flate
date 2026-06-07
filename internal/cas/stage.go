// Package cas holds the atomic content-addressed staging dance used by
// baseline materialization: build into a sibling temp dir, atomically rename
// it into the final slot, and — when the rename loses a cross-process race —
// discard the temp and adopt the winner's already-finalized directory.
//
// What "build" does and how a race is detected are caller-supplied callbacks
// (baseline writes a git tree and adopts on a finalized directory). The
// temp-dir lifecycle, the rename, and the discard-on-failure cleanup live here.
package cas

import (
	"fmt"
	"os"
	"path/filepath"
)

// Stage builds final atomically. It creates a sibling temp dir
// (filepath.Base(final)+".tmp.*") under parent, runs build against that
// temp dir, then renames it onto final. parent must already exist.
//
//   - The MkdirTemp error is wrapped with tmpPrefix ("<tmpPrefix>: %w").
//   - On a build error the temp dir is removed and the error is returned
//     unwrapped — the build callback owns its own error context.
//   - On rename success final is the built tree.
//   - On rename failure the temp dir is removed and adopt is consulted:
//     when adopt reports true a peer already finalized final and we
//     adopt it (no error); otherwise the rename error is wrapped with
//     finalizePrefix ("<finalizePrefix>: %w").
//
// The two prefixes keep each call site's existing error strings intact
// ("baseline staging"/"baseline finalize", "stage tmp"/"stage finalize").
//
// adopted reports whether the returned slot came from adopting a peer's
// finalized directory (false on the normal rename-wins path); callers
// that distinguish "we built it" from "we adopted it" use it, others
// ignore it.
func Stage(parent, final, tmpPrefix, finalizePrefix string, build func(staging string) error, adopt func() bool) (adopted bool, err error) {
	staging, err := os.MkdirTemp(parent, filepath.Base(final)+".tmp.*")
	if err != nil {
		return false, fmt.Errorf("%s: %w", tmpPrefix, err)
	}
	if err := build(staging); err != nil {
		_ = os.RemoveAll(staging)
		return false, err
	}
	if err := os.Rename(staging, final); err != nil {
		_ = os.RemoveAll(staging)
		// A racing peer process beat us to the rename. Content
		// addressing guarantees the trees are equivalent, so when the
		// caller confirms the winner finalized, adopt it instead of
		// re-materializing or surfacing the lost-race error.
		if adopt() {
			return true, nil
		}
		return false, fmt.Errorf("%s: %w", finalizePrefix, err)
	}
	return false, nil
}
