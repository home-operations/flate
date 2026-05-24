package discovery

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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

func stripDotSlash(p string) string {
	for len(p) > 0 && (p[0] == '.' || p[0] == '/') {
		if p[0] == '.' && (len(p) == 1 || p[1] == '/') {
			p = p[1:]
			continue
		}
		if p[0] == '/' {
			p = p[1:]
			continue
		}
		break
	}
	return p
}

func pathUnderRoot(target, root string) bool {
	t := filepath.Clean(target) + string(filepath.Separator)
	r := filepath.Clean(root) + string(filepath.Separator)
	return len(t) >= len(r) && t[:len(r)] == r
}
