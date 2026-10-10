package kustomize

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"
	"github.com/home-operations/flate/pkg/tree"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// diskInput exposes only secure reads to the common overlay. Paths must retain
// unresolved parent components until Flux's CleanedAbs resolves them.
type diskInput struct {
	filesys.FileSystem
	root     string
	resolved map[string]diskResolved
}

type diskResolved struct {
	abs  string
	path string
	err  error
}

func (d diskInput) mounted(name string) string {
	if r, ok := d.resolved[name]; ok {
		return r.abs
	}
	if name == "." {
		return d.root
	}
	return d.root + string(filepath.Separator) + filepath.FromSlash(name)
}

func (d diskInput) Resolve(name string) (string, error) {
	if r, ok := d.resolved[name]; ok {
		return r.path, r.err
	}
	abs := d.mounted(name)
	dir, file, err := d.CleanedAbs(abs)
	p := dir.String()
	if file != "" {
		p += string(filepath.Separator) + file
	}
	if _, ok := errors.AsType[*fluxfilesys.ConstraintError](err); ok {
		err = fmt.Errorf("%w: %w", tree.ErrEscape, err)
	}
	rel := ""
	if err == nil {
		rel, err = filepath.Rel(d.root, p)
		rel = filepath.ToSlash(rel)
	}
	if d.resolved != nil {
		d.resolved[name] = diskResolved{abs: abs, path: rel, err: err}
	}
	return rel, err
}

func (d diskInput) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	return d.FileSystem.Open(d.mounted(name))
}

func (d diskInput) Stat(name string) (fs.FileInfo, error) {
	p, err := d.Resolve(name)
	if err != nil {
		return nil, err
	}
	return os.Stat(d.mounted(p))
}

func (d diskInput) raw(name string) (string, error) {
	dir := path.Dir(name)
	parent, err := d.Resolve(dir)
	if err != nil {
		return "", err
	}
	if name == "." {
		return d.root, nil
	}
	if parent == dir {
		return d.mounted(name), nil
	}
	return d.mounted(path.Join(parent, path.Base(name))), nil
}

func (d diskInput) Lstat(name string) (fs.FileInfo, error) {
	p, err := d.raw(name)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(p)
	if err == nil && info.Mode()&fs.ModeSymlink == 0 {
		if ig, ok := d.FileSystem.(ignoreFS); ok && ig.hidden(p, info.IsDir()) {
			return nil, notExist("lstat", name)
		}
		if d.resolved != nil {
			abs, rel := p, name
			dir := path.Dir(name)
			if d.resolved[dir].path != dir {
				abs = d.mounted(name)
				var e error
				rel, e = filepath.Rel(d.root, p)
				if e != nil {
					return nil, e
				}
				rel = filepath.ToSlash(rel)
			}
			d.resolved[name] = diskResolved{abs: abs, path: rel}
		}
	}
	return info, err
}
func (d diskInput) ReadLink(name string) (string, error) {
	p, err := d.raw(name)
	if err != nil {
		return "", err
	}
	return os.Readlink(p)
}

func (d diskInput) ReadFile(name string) ([]byte, error) {
	return d.FileSystem.ReadFile(d.mounted(name))
}

func (d diskInput) ReadDir(name string) ([]fs.DirEntry, error) {
	p, err := d.Resolve(name)
	if err != nil {
		return nil, err
	}
	abs := d.mounted(p)
	entries, err := os.ReadDir(abs)
	if err != nil {
		return nil, err
	}
	if ig, ok := d.FileSystem.(ignoreFS); ok {
		kept := entries[:0]
		for _, e := range entries {
			child := abs + string(filepath.Separator) + e.Name()
			if !ig.hidden(child, ig.disk.IsDir(child)) {
				kept = append(kept, e)
			}
		}
		entries = kept
	}
	return entries, nil
}
