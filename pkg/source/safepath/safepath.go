// Package safepath validates archive names and confines disk paths to a root.
package safepath

import (
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
)

// ArchiveName rejects absolute, drive-qualified, traversal and invalid-separator
// names before cleaning. Errors wrap fs.ErrInvalid. The result "." is valid
// only for a caller's non-file root entry.
func ArchiveName(name string) (string, error) {
	if name == "" || strings.ContainsAny(name, "\x00\\") {
		return "", fmt.Errorf("invalid archive name %q: %w", name, fs.ErrInvalid)
	}
	for component := range strings.SplitSeq(name, "/") {
		if component == ".." {
			return "", fmt.Errorf("invalid archive name %q: %w", name, fs.ErrInvalid)
		}
	}
	for strings.HasPrefix(name, "./") {
		name = name[2:]
	}
	drive := len(name) >= 2 && name[1] == ':' &&
		(name[0] >= 'a' && name[0] <= 'z' || name[0] >= 'A' && name[0] <= 'Z')
	if strings.HasPrefix(name, "/") || drive {
		return "", fmt.Errorf("invalid archive name %q: %w", name, fs.ErrInvalid)
	}
	return path.Clean(name), nil
}

// SafeJoin confines a disk path to base and rejects absolute names.
func SafeJoin(base, rel string) (string, error) {
	// Normalize forward slashes so that keys from cross-platform sources
	// (e.g. S3 bucket objects stored with forward slashes on Windows) are
	// handled correctly before any path manipulation.
	rel = filepath.FromSlash(rel)

	clean := filepath.Clean(rel)
	if filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" {
		return "", fmt.Errorf("path escapes target directory: %q", rel)
	}
	target := filepath.Join(base, clean)
	relInside, err := filepath.Rel(base, target)
	if err != nil || Escaped(relInside) {
		return "", fmt.Errorf("path escapes target directory: %q", rel)
	}
	return target, nil
}

// Escaped reports whether a filepath.Rel result escapes its base: the rel is
// exactly ".." or begins with a ".." path component (OS separator). It is the
// shared containment predicate used by SafeJoin, Contains, and callers that
// keep the rel string but must reject repo/root escapes.
func Escaped(rel string) bool {
	return rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// Contains reports whether target lies within root. Both must already be
// absolute and symlink-resolved by the caller; Contains is pure (no I/O) and
// only does the algebraic containment check (filepath.Rel + the same `..`-prefix
// test SafeJoin uses). root == target counts as contained. Used by the kustomize
// remote-base copy to confine a symlink's resolved target to the base root.
func Contains(root, target string) bool {
	rel, err := filepath.Rel(root, target)
	return err == nil && !Escaped(rel)
}
