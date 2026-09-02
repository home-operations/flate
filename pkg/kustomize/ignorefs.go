package kustomize

// ignorefs.go hides source-controller-excluded paths from the disk layer of a
// working-tree build. A GitRepository artifact only contains the files that
// survive source-controller's ignore matcher (its defaults plus every in-tree
// .sourceignore), so a Kustomization whose spec.path — or whose kustomization
// resources — resolve into an excluded directory fails on the cluster even
// though the files exist in the checkout. Filtering at the filesystem rather
// than in the resource scan makes every read the build performs (the spec.path
// check, kustomize's own resource resolution, the auto-generate scan) see the
// artifact's shape, so the render fails where Flux would.

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

// ignoreFS wraps a disk filesystem rooted at root and reports every path the
// matcher excludes as absent. Writes pass through untouched (the overlay never
// writes to disk). Safe for concurrent use.
type ignoreFS struct {
	disk    filesys.FileSystem
	root    string
	matcher *sourceignore.Matcher
	// dirs memoizes the survivor scan a matched directory needs (see hidden);
	// the tree is static for the life of the FS, so a verdict never changes.
	dirs *sync.Map // rel dir -> hidden bool
}

func newIgnoreFS(disk filesys.FileSystem, root string, matcher *sourceignore.Matcher) filesys.FileSystem {
	return ignoreFS{disk: disk, root: root, matcher: matcher, dirs: &sync.Map{}}
}

var errSurvivorFound = errors.New("survivor found")

// hidden reports whether path is excluded from the artifact. A matched file is
// always hidden. A matched directory is hidden only when nothing under it is
// re-included by a deeper `!` pattern: source-controller's archive walk skips
// the directory entry itself but keeps descending, so an allowlist such as
// `/*` + `!/charts/x/` still ships charts/x/** (with charts/ recreated as its
// parent). Paths at or outside root are never hidden.
func (ig ignoreFS) hidden(path string, isDir bool) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(ig.root, abs)
	if err != nil || rel == "." || safepath.Escaped(rel) {
		return false
	}
	if !ig.matcher.Match(rel, isDir) {
		return false
	}
	if !isDir {
		return true
	}
	if v, ok := ig.dirs.Load(rel); ok {
		return v.(bool)
	}
	h := !ig.hasSurvivor(abs)
	ig.dirs.Store(rel, h)
	return h
}

// hasSurvivor reports whether any regular file under dir escapes the matcher.
func (ig ignoreFS) hasSurvivor(dir string) bool {
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		rel, rerr := filepath.Rel(ig.root, p)
		if rerr != nil {
			return nil
		}
		if !ig.matcher.Match(rel, false) {
			return errSurvivorFound
		}
		return nil
	})
	return errors.Is(err, errSurvivorFound)
}

func notExist(op, path string) error {
	return &fs.PathError{Op: op, Path: path, Err: fs.ErrNotExist}
}

func (ig ignoreFS) Create(path string) (filesys.File, error) { return ig.disk.Create(path) }
func (ig ignoreFS) Mkdir(path string) error                  { return ig.disk.Mkdir(path) }
func (ig ignoreFS) MkdirAll(path string) error               { return ig.disk.MkdirAll(path) }
func (ig ignoreFS) RemoveAll(path string) error              { return ig.disk.RemoveAll(path) }
func (ig ignoreFS) WriteFile(path string, d []byte) error    { return ig.disk.WriteFile(path, d) }

func (ig ignoreFS) Exists(path string) bool {
	return ig.disk.Exists(path) && !ig.hidden(path, ig.disk.IsDir(path))
}

func (ig ignoreFS) IsDir(path string) bool {
	return ig.disk.IsDir(path) && !ig.hidden(path, true)
}

func (ig ignoreFS) Open(path string) (filesys.File, error) {
	if ig.hidden(path, ig.disk.IsDir(path)) {
		return nil, notExist("open", path)
	}
	return ig.disk.Open(path)
}

func (ig ignoreFS) ReadFile(path string) ([]byte, error) {
	if ig.hidden(path, ig.disk.IsDir(path)) {
		return nil, notExist("open", path)
	}
	return ig.disk.ReadFile(path)
}

func (ig ignoreFS) CleanedAbs(path string) (filesys.ConfirmedDir, string, error) {
	d, f, err := ig.disk.CleanedAbs(path)
	if err != nil {
		return d, f, err
	}
	full := d.String()
	if f != "" {
		full = filepath.Join(full, f)
	}
	if ig.hidden(full, f == "") {
		return "", "", notExist("stat", path)
	}
	return d, f, nil
}

func (ig ignoreFS) ReadDir(path string) ([]string, error) {
	if ig.hidden(path, true) {
		return nil, notExist("open", path)
	}
	entries, err := ig.disk.ReadDir(path)
	if err != nil {
		return nil, err
	}
	kept := entries[:0]
	for _, e := range entries {
		child := filepath.Join(path, e)
		if !ig.hidden(child, ig.disk.IsDir(child)) {
			kept = append(kept, e)
		}
	}
	return kept, nil
}

func (ig ignoreFS) Glob(pattern string) ([]string, error) {
	paths, err := ig.disk.Glob(pattern)
	if err != nil {
		return nil, err
	}
	kept := paths[:0]
	for _, p := range paths {
		if !ig.hidden(p, ig.disk.IsDir(p)) {
			kept = append(kept, p)
		}
	}
	return kept, nil
}

// Walk skips hidden files and prunes hidden directories. A matched directory
// that is NOT hidden (it has a re-included descendant) is still descended, with
// its excluded children dropped individually.
func (ig ignoreFS) Walk(path string, walkFn filepath.WalkFunc) error {
	return ig.disk.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err == nil && info != nil && ig.hidden(p, info.IsDir()) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		return walkFn(p, info, err)
	})
}
