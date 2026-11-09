package gittree

import (
	"cmp"
	"errors"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/store"
)

type membership struct {
	directory string
	ignored   map[string]struct{}
	captured  bool
}

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
	if native && rel == g.membership.directory {
		return input
	}
	selected := &FS{snapshot: &localPolicy{}}
	if native {
		selected.view, selected.location = g.view, g.location
		selected.policy, selected.Warnings = g.policy, g.Warnings
		selected.membership = g.membership
	} else {
		selected.view = &directView{input.FS}
		selected.location = &gitrepo.Location{ContentRoot: root}
	}
	selected.membership.directory = rel
	captured := *input
	captured.FS = selected
	return &captured
}

func (g *FS) excludes(name string) bool {
	if _, ignored := g.membership.ignored[name]; ignored {
		return true
	}
	directory := g.membership.directory
	if directory == "" {
		return false
	}
	rest, matched := strings.CutPrefix(name, directory)
	return directory == "." || matched && (rest == "" || rest[0] == '/')
}

// Open MUST keep excluded paths absent from file and directory handles.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) Open(name string) (fs.File, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	file, err := g.view.Open(name)
	if err != nil || g.membership.directory == "" && len(g.membership.ignored) == 0 {
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

// OpenRegular MUST enforce membership even when the caller already observed metadata.
func (g *FS) OpenRegular(name string) (fs.File, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.OpenRegular(name)
}

// ReadFile MUST keep optimized reads within the captured membership.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) ReadFile(name string) ([]byte, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.ReadFile(name)
}

// Stat MUST report excluded paths as absent even when backing metadata exists.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) Stat(name string) (fs.FileInfo, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.Stat(name)
}

// Lstat MUST enforce membership without following aliases.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) Lstat(name string) (fs.FileInfo, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.Lstat(name)
}

// ReadLink MUST report excluded paths as absent before resolving an alias.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) ReadLink(name string) (string, error) {
	if g.excludes(name) {
		return "", &fs.PathError{Op: readLinkOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.ReadLink(name)
}

// ReadDir MUST never modify shared listings when it removes excluded entries.
//
//nolint:wrapcheck // Filesystem adapters retain the original I/O error contracts.
func (g *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	entries, err := g.view.ReadDir(name)
	if g.membership.directory == "" && len(g.membership.ignored) == 0 {
		return entries, err
	}
	index := slices.IndexFunc(
		entries,
		func(entry fs.DirEntry) bool { return g.excludes(path.Join(name, entry.Name())) },
	)
	if index < 0 {
		return entries, err
	}
	selected := slices.Clone(entries[:index])
	for _, entry := range entries[index+1:] {
		if !g.excludes(path.Join(name, entry.Name())) {
			selected = append(selected, entry)
		}
	}
	return selected, err
}
