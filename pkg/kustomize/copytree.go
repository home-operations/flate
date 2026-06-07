package kustomize

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// SkipStageDir reports whether a directory base name is excluded when a source
// tree is walked: `node_modules` and every dot-prefixed dir (which captures
// `.git`, `.flate-cache`, IDE state, etc.). Apply to non-root directories only;
// callers own root detection.
func SkipStageDir(base string) bool {
	return base == "node_modules" || strings.HasPrefix(base, ".")
}

// walkSourceFiles walks root and invokes visit(rel, body) for every regular
// file (symlinks dereferenced; dangling and irregular entries skipped silently),
// descending all directories except those SkipStageDir excludes. rel is
// relative to root, using the OS separator. Used to copy a cloned git base into
// a render's in-memory fs (see copyDirIntoFS).
func walkSourceFiles(root string, visit func(rel string, body []byte) error) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			if SkipStageDir(d.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			info, serr := os.Stat(path)
			if serr != nil {
				if errors.Is(serr, fs.ErrNotExist) {
					return nil
				}
				return serr
			}
			if !info.Mode().IsRegular() {
				return nil
			}
		} else if !d.Type().IsRegular() {
			return nil
		}
		body, rerr := os.ReadFile(path) //nolint:gosec // path is a walk result under root
		if rerr != nil {
			return rerr
		}
		return visit(rel, body)
	})
}
