// Package safepath provides a path-traversal guard used by the OCI and
// bucket source packages. Both packages must prevent a malicious remote
// (a crafted tar archive or a mis-curated bucket) from writing files
// outside the caller's designated cache slot.
package safepath

import (
	"fmt"
	"path/filepath"
	"strings"
)

// SafeJoin joins base and rel, verifying that the resulting path stays
// strictly inside base. It defends against two escape shapes:
//
//   - Relative traversal: `../../escape.txt` — filepath.Clean collapses
//     the dots; Rel then reports a `..` prefix which is rejected.
//   - Absolute path: `/etc/passwd` — rejected before the Join when
//     rejectAbsolute is true (tar-header callers). When rejectAbsolute is
//     false (bucket-key callers), filepath.Join's component-boundary
//     handling silently strips the leading slash and keeps the result
//     inside base; Rel still validates containment after the join.
//
// The rejectAbsolute flag exists because the two callers differ in
// semantics:
//
//   - OCI tar extraction (rejectAbsolute = true): a tar header with an
//     absolute path (e.g. `/etc/passwd`) is a sign of a malicious
//     archive; it must be rejected, not silently redirected.
//   - Bucket key download (rejectAbsolute = false): bucket object names
//     are not filesystem paths; an object literally named "/etc/passwd"
//     is contained safely by filepath.Join and should not error.
//
// Returns the cleaned absolute path on success, or an error if the
// path would escape base.
func SafeJoin(base, rel string, rejectAbsolute bool) (string, error) {
	// Normalize forward slashes so that keys from cross-platform sources
	// (e.g. S3 bucket objects stored with forward slashes on Windows) are
	// handled correctly before any path manipulation.
	rel = filepath.FromSlash(rel)

	if rejectAbsolute {
		clean := filepath.Clean(rel)
		if filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" {
			return "", fmt.Errorf("path escapes target directory: %q", rel)
		}
		target := filepath.Join(base, clean)
		relInside, err := filepath.Rel(base, target)
		if err != nil || isEscaped(relInside) {
			return "", fmt.Errorf("path escapes target directory: %q", rel)
		}
		return target, nil
	}

	// rejectAbsolute = false: let filepath.Join handle leading slashes by
	// treating them as component boundaries (not re-rooting at /). filepath.Rel
	// cleans its base argument internally, so base needs no pre-cleaning.
	target := filepath.Join(base, rel)
	relInside, err := filepath.Rel(base, target)
	if err != nil {
		return "", fmt.Errorf("path resolution: %w", err)
	}
	if isEscaped(relInside) {
		return "", fmt.Errorf("path traversal: %q escapes target directory", rel)
	}
	return target, nil
}

func isEscaped(rel string) bool {
	return rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// Contains reports whether target lies within root. Both must already be
// absolute and symlink-resolved by the caller; Contains is pure (no I/O) and
// only does the algebraic containment check (filepath.Rel + the same `..`-prefix
// test SafeJoin uses). root == target counts as contained. Used by the kustomize
// remote-base copy to confine a symlink's resolved target to the base root.
func Contains(root, target string) bool {
	rel, err := filepath.Rel(root, target)
	return err == nil && !isEscaped(rel)
}
