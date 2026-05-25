package discovery

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ResolveScanPath normalizes a user-supplied --path / --path-orig:
// absolute, with symlinks resolved. Without symlink resolution
// filepath.WalkDir doesn't follow root-level symlinks, producing an
// empty manifest set without any error indication — a footgun.
func ResolveScanPath(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return abs, nil
		}
		return "", fmt.Errorf("resolve --path %q: %w", p, err)
	}
	return resolved, nil
}

// FindRepoRoot walks upward from p looking for a .git directory; falls
// back to p itself when there isn't one.
func FindRepoRoot(p string) string {
	for cur := p; ; {
		if _, err := os.Stat(filepath.Join(cur, ".git")); err == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return p
		}
		cur = parent
	}
}

// stripDotSlash strips leading `./` / `/` / bare `.` segments from a
// Flux spec.path so it can be joined against repoRoot without
// re-rooting. `..` is preserved — the caller validates traversal via
// pathUnderRoot.
func stripDotSlash(p string) string {
	for {
		switch {
		case strings.HasPrefix(p, "./"):
			p = p[2:]
		case p == ".":
			return ""
		case strings.HasPrefix(p, "/"):
			p = p[1:]
		default:
			return p
		}
	}
}

// pathUnderRoot reports whether target resolves inside root. Both are
// trailing-slash normalized so a sibling path with a prefix-matching
// name (`/repo/apps` vs `/repo/apps-staging`) doesn't get a false
// positive from raw HasPrefix.
func pathUnderRoot(target, root string) bool {
	t := filepath.Clean(target) + string(filepath.Separator)
	r := filepath.Clean(root) + string(filepath.Separator)
	return strings.HasPrefix(t, r)
}
