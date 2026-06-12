package kustomize

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/pkg/source/safepath"
)

// skipStageDir reports whether a directory base name is excluded when a source
// tree is walked: `node_modules` and every dot-prefixed dir (which captures
// `.git`, `.flate-cache`, IDE state, etc.). Apply to non-root directories only;
// callers own root detection.
func skipStageDir(base string) bool {
	return base == "node_modules" || strings.HasPrefix(base, ".")
}

// walkSourceFiles walks root and invokes visit(rel, body) for every regular
// file (symlinks dereferenced but confined to root — issue #741; dangling,
// escaping, and irregular entries skipped silently), descending all directories
// except those skipStageDir excludes. rel is relative to root, using the OS
// separator. Used to copy a cloned git base into a render's in-memory fs (see
// copyDirIntoFS).
func walkSourceFiles(root string, visit func(rel string, body []byte) error) error {
	// A remote git base is untrusted: a fork PR can plant a symlink like
	// creds.yaml -> /proc/self/environ in the fetched worktree, and this copy
	// reads through symlinks. Resolve the base root once (Abs mirrors flux's
	// isSecurePath; EvalSymlinks is load-bearing because cache/temp dirs sit
	// under symlinked prefixes like /var -> /private/var) so each link's real
	// target can be confined to it. The main source path gets this for free from
	// the secure on-disk FS (MakeFsOnDiskSecure); this copy walks the real tree
	// and must enforce containment itself. The confinement is sound only because
	// gittree.Materialize writes blob bytes via WriteFile and links via
	// os.Symlink, never os.Link — so a regular-file walk entry can never be a
	// hardlink to a host file (which would bypass the symlink check).
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	rootResolved, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return err
	}
	return filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(absRoot, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			if skipStageDir(d.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		readPath := path
		if d.Type()&fs.ModeSymlink != 0 {
			// Resolve the full link chain and require the real target to stay
			// inside the base root, then read that resolved path directly (not
			// back through the link, so there is no re-follow). An escaping link
			// is skipped silently — erroring would leak host-path existence to
			// the attacker via render success/failure.
			target, terr := filepath.EvalSymlinks(path)
			if terr != nil {
				if errors.Is(terr, fs.ErrNotExist) {
					return nil // dangling — skip
				}
				return terr
			}
			if !safepath.Contains(rootResolved, target) {
				return nil // escapes the base root — skip
			}
			info, serr := os.Stat(target)
			if serr != nil {
				if errors.Is(serr, fs.ErrNotExist) {
					return nil
				}
				return serr
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			readPath = target
		} else if !d.Type().IsRegular() {
			return nil
		}
		body, rerr := os.ReadFile(readPath) //nolint:gosec // symlink targets are confined to root via safepath.Contains; regular entries are walk results under root
		if rerr != nil {
			return rerr
		}
		return visit(rel, body)
	})
}
