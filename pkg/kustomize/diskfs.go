package kustomize

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/home-operations/flate/pkg/tree"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// diskInput exposes the regular files and directories a Flux source archive
// retains. Metadata is memoized only within one render's immutable input view.
type diskInput struct {
	filesys.FileSystem
	root string
	info map[string]fs.FileInfo
}

func (d diskInput) mounted(name string) string {
	if name == "." {
		return d.root
	}
	return d.root + string(filepath.Separator) + filepath.FromSlash(name)
}

func (d diskInput) Open(name string) (fs.File, error) {
	info, err := d.Stat(name)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return tree.NewBuilder(d).Open(name)
	}
	return d.FileSystem.Open(d.mounted(name))
}

func (d diskInput) Stat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	if info := d.info[name]; info != nil {
		return info, nil
	}
	if name != "." {
		parent, err := d.Stat(path.Dir(name))
		if err != nil {
			return nil, err
		}
		if !parent.IsDir() {
			return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
		}
	}
	abs := d.mounted(name)
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, err
	}
	if !archived(info.Mode()) {
		return nil, notExist("stat", name)
	}
	if ig, ok := d.FileSystem.(ignoreFS); ok && ig.hidden(abs, info.IsDir()) {
		return nil, notExist("stat", name)
	}
	d.info[name] = info
	return info, nil
}

func (d diskInput) ReadFile(name string) ([]byte, error) {
	if _, err := d.Stat(name); err != nil {
		return nil, err
	}
	return d.FileSystem.ReadFile(d.mounted(name))
}

func (d diskInput) ReadDir(name string) ([]fs.DirEntry, error) {
	if _, err := d.Stat(name); err != nil {
		return nil, err
	}
	abs := d.mounted(name)
	entries, err := os.ReadDir(abs)
	if err != nil {
		return nil, err
	}
	ig, ignored := d.FileSystem.(ignoreFS)
	kept := entries[:0]
	for _, e := range entries {
		if !archived(e.Type()) {
			continue
		}
		if ignored && ig.hidden(abs+string(filepath.Separator)+e.Name(), e.IsDir()) {
			continue
		}
		kept = append(kept, e)
	}
	return kept, nil
}

func archived(mode fs.FileMode) bool {
	return mode.IsRegular() || mode.IsDir()
}
