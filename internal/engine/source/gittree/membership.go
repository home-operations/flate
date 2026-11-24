package gittree

import (
	"cmp"
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/source/gitrepo"
	"github.com/home-operations/flate/internal/engine/store"
)

type membership struct {
	directory string
	extra     []string
	ignored   map[string]struct{}
	captured  bool
}

func sourceOrigin(previous *FS, mode ViewMode, commit plumbing.Hash) *FS {
	if previous == nil || mode != Source || commit != plumbing.ZeroHash {
		return nil
	}
	switch prior := previous.view.(type) {
	case *localView, *directView:
		return cmp.Or(previous.sameFiles, previous)
	case *gitView:
		if prior.mode == Direct {
			return previous
		}
	}
	return nil
}

// PreservesFiles reports whether admission retained the same files at the same path.
func PreservesFiles(before, after *store.SourceArtifact) bool {
	if before == after {
		return true
	}
	if before == nil || after == nil || before.LocalPath != after.LocalPath {
		return false
	}
	view, native := after.FS.(*FS)
	return native && view.sameFiles != nil && view.sameFiles == before.FS
}

func openRegular(input fs.FS, name string) (fs.File, error) {
	if regular, ok := input.(interface {
		OpenRegular(string) (fs.File, error)
	}); ok {
		return regular.OpenRegular(name)
	}
	//nolint:wrapcheck // Preserve the admitted filesystem's path errors.
	return input.Open(name)
}

func (g *gitView) OpenRegular(name string) (fs.File, error) {
	if node := g.nodes[name]; node != nil && node.mode.IsRegular() && g.current != nil && !g.changes[name] {
		return openRegular(g.current, name)
	}
	return g.Open(name)
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
	if native && (rel == g.membership.directory || slices.Contains(g.membership.extra, rel)) {
		return input
	}
	selected := &FS{snapshot: &localPolicy{}}
	if native {
		selected.view, selected.location = g.view, g.location
		selected.policy, selected.policyKey, selected.Warnings = g.policy, g.policyKey, g.Warnings
		selected.membership = g.membership
	} else {
		selected.view = &directView{input.FS}
		selected.location = &gitrepo.Location{ContentRoot: root}
	}
	if selected.membership.directory == "" {
		selected.membership.directory = rel
	} else {
		selected.membership.extra = append(slices.Clone(selected.membership.extra), rel)
	}
	return input.WithSourceView(selected)
}

func (g *FS) excludes(name string) bool {
	if _, ignored := g.membership.ignored[name]; ignored {
		return true
	}
	directory := g.membership.directory
	if directory == "" {
		if scoped, ok := g.view.(*scopedView); ok {
			return scoped.parent.excludes(path.Join(scoped.path, name))
		}
		return false
	}
	if inDirectory(name, directory) {
		return true
	}
	return slices.ContainsFunc(g.membership.extra, func(dir string) bool { return inDirectory(name, dir) })
}

func inDirectory(name, directory string) bool {
	rest, matched := strings.CutPrefix(name, directory)
	return directory == "." || matched && (rest == "" || rest[0] == '/')
}

// Open MUST keep excluded paths absent from file and directory handles.
//
//nolint:wrapcheck // Open forwards admitted files and PathError fields and retains joined read and close errors.
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
//nolint:wrapcheck // ReadFile forwards admitted bytes and the underlying PathError fields.
func (g *FS) ReadFile(name string) ([]byte, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.ReadFile(name)
}

// Stat MUST report excluded paths as absent even when backing metadata exists.
func (g *FS) Stat(name string) (fs.FileInfo, error) {
	info, _, err := g.inspect(name)
	return info, err
}

func (g *FS) inspect(name string) (fs.FileInfo, string, error) {
	if g.excludes(name) {
		return nil, "", &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.inspect(name)
}

// InspectedFile retains the path admitted by the source filesystem.
type InspectedFile struct {
	input fs.FS
	name  string
	path  string
}

// InspectFile ties metadata and the open operation to the same filesystem view.
func InspectFile(input fs.FS, name string) (fs.FileInfo, InspectedFile, error) {
	file := InspectedFile{input: input, name: name}
	if source, ok := input.(*FS); ok {
		info, absolute, err := source.inspect(name)
		if err == nil && info.Mode().IsRegular() {
			file.path = absolute
		}
		return info, file, err
	}
	info, err := fs.Stat(input, name)
	return info, file, err //nolint:wrapcheck // Preserve the filesystem's path errors.
}

// Open requires a regular file and an unchanged source view since inspection.
func (f InspectedFile) Open() (fs.File, error) {
	if f.path != "" {
		return openLocalFile(f.path)
	}
	return openRegular(f.input, f.name)
}

// Lstat MUST enforce membership without following aliases.
//
//nolint:wrapcheck // Lstat forwards admitted metadata and the underlying PathError fields.
func (g *FS) Lstat(name string) (fs.FileInfo, error) {
	if g.excludes(name) {
		return nil, &fs.PathError{Op: statOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.Lstat(name)
}

// ReadLink MUST report excluded paths as absent before resolving an alias.
//
//nolint:wrapcheck // ReadLink forwards admitted targets and the underlying PathError fields.
func (g *FS) ReadLink(name string) (string, error) {
	if g.excludes(name) {
		return "", &fs.PathError{Op: readLinkOperation, Path: name, Err: fs.ErrNotExist}
	}
	return g.view.ReadLink(name)
}

// ReadDir MUST never modify shared listings when it removes excluded entries.
//
//nolint:wrapcheck // ReadDir retains partial listings and the underlying PathError fields while filtering membership.
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

type memberFilesystem struct {
	billy.Filesystem

	names fs.FS
	input *FS
	root  string
}

// Only native Billy backends may enumerate the host directory.
func (g *FS) memberFilesystem(files billy.Filesystem, root string) *memberFilesystem {
	bridge := &memberFilesystem{Filesystem: files, input: g, root: root}
	for selected := g; selected.membership.directory == ""; {
		scoped, ok := selected.view.(*scopedView)
		if !ok {
			return bridge
		}
		selected = scoped.parent
	}
	backend, err := files.Chroot(".")
	if err != nil {
		return bridge
	}
	native := false
	switch backend := backend.(type) {
	case *osfs.BoundOS, *osfs.RootOS:
		native = true
	case *chroot.ChrootHelper:
		switch backend.Underlying().(type) {
		case *osfs.BoundOS, *osfs.RootOS:
			native = true
		}
	}
	if native {
		bridge.names = os.DirFS(files.Root())
	}
	return bridge
}

// ReadDir MUST filter excluded names before metadata access.
// Repository coordinates MUST stay intact for the index and .git/info/exclude.
//
//nolint:wrapcheck // ReadDir forwards backing ReadDir and DirEntry.Info PathError fields while retaining repository coordinates.
func (f *memberFilesystem) ReadDir(name string) ([]fs.DirEntry, error) {
	if f.names == nil {
		return f.Filesystem.ReadDir(name)
	}
	relative, err := filepath.Rel(f.root, filepath.Join(f.Filesystem.Root(), name))
	if err != nil {
		return nil, err
	}
	relative = filepath.ToSlash(relative)
	if f.input.excludes(relative) {
		return nil, &fs.PathError{Op: openOperation, Path: name, Err: fs.ErrNotExist}
	}
	entries, err := fs.ReadDir(f.names, filepath.ToSlash(filepath.Clean(name)))
	if err != nil {
		return nil, err
	}
	selected := make([]fs.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if f.input.excludes(path.Join(relative, entry.Name())) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		selected = append(selected, fs.FileInfoToDirEntry(info))
	}
	return selected, nil
}
