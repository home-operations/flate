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

// Escaped reports whether a filepath.Rel result leaves its base.
func Escaped(rel string) bool {
	return rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// Contains reports whether target lies within an absolute, symlink-resolved root.
func Contains(root, target string) bool {
	rel, err := filepath.Rel(root, target)
	return err == nil && !Escaped(rel)
}
