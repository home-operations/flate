package gittree

import (
	"cmp"
	"errors"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/source/gitrepo"
	"github.com/home-operations/flate/internal/store"
)

// ExcludeDirectory retains immutable source membership without the resolved active cache directory.
// Callers MUST supply the cache owner's directory before source observations begin.
func ExcludeDirectory(input *store.SourceArtifact, directory string) *store.SourceArtifact {
	if directory == "" {
		return input
	}
	root := cmp.Or(input.LocalRoot, input.LocalPath)
	g, native := input.FS.(*FS)
	if native {
		var view *gitView
		subdir, origin, physical := ".", ".", ""
		g.fingerprintOrigin(&view, &subdir, &origin, &physical)
		root = cmp.Or(physical, root)
	}
	rel, err := filepath.Rel(root, directory)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return input
	}
	rel = filepath.ToSlash(rel)
	if native && rel == g.excluded {
		return input
	}
	selected := &FS{excluded: rel, snapshot: &localPolicy{}}
	if native {
		selected.view, selected.location = g.view, g.location
		selected.policy, selected.Warnings = g.policy, g.Warnings
	} else {
		selected.view = &directView{input.FS}
		selected.location = &gitrepo.Location{ContentRoot: root}
	}
	captured := *input
	captured.FS = selected
	return &captured
}

func (g *FS) excludes(name string) bool {
	if g.excluded == "" {
		return false
	}
	rest, matched := strings.CutPrefix(name, g.excluded)
	return g.excluded == "." || matched && (rest == "" || rest[0] == '/')
}

// Open keeps excluded paths absent from file and directory handles.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) Open(name string) (fs.File, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	file, err := g.view.Open(name)
	if err != nil || g.excluded == "" {
		return file, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if !info.IsDir() {
		return file, nil
	}
	entries, readErr := g.ReadDir(name)
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, err
	}
	return &gitFile{input: g, info: info, entries: entries}, nil
}

// OpenRegular applies membership before the regular-file shortcut.
func (g *FS) OpenRegular(name string) (fs.File, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.OpenRegular(name)
}

// ReadFile keeps optimized file reads within admitted membership.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) ReadFile(name string) ([]byte, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.ReadFile(name)
}

// Stat keeps excluded paths absent from metadata reads.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) Stat(name string) (fs.FileInfo, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.Stat(name)
}

// Lstat keeps excluded paths absent without following aliases.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) Lstat(name string) (fs.FileInfo, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.Lstat(name)
}

// ReadLink keeps excluded paths absent from link reads.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) ReadLink(name string) (string, error) {
	if g.excludes(name) {
		return "", &fs.PathError{Op: readLinkOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.ReadLink(name)
}

// ReadDir omits the excluded directory without mutating shared listings.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	entries, err := g.view.ReadDir(name)
	if g.excluded != "" && name == path.Dir(g.excluded) {
		index := slices.IndexFunc(entries, func(entry fs.DirEntry) bool {
			return entry.Name() == path.Base(g.excluded)
		})
		if index >= 0 {
			entries = slices.Delete(slices.Clone(entries), index, index+1)
		}
	}
	return entries, err
}
