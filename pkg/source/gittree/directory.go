package gittree

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/store"
)

// Directory owns one nonconcurrent admission and comparison lifetime.
// The checkout MUST remain stable until Close returns.
// Borrowed worktrees and chroot views MUST close with it. Artifacts remain detached.
type Directory struct {
	root       *os.Root
	location   *gitrepo.Location
	observed   *membershipFilesystem
	membership membership
	artifact   *store.SourceArtifact
}

// OpenDirectory completes admission before returning the scoped resource.
// Synthetic source labels MUST use their already admitted filesystem instead.
func OpenDirectory(ctx context.Context, location *gitrepo.Location) (*Directory, error) {
	return openDirectoryAt(ctx, location, "")
}

//nolint:wrapcheck // Admission preserves filesystem and cancellation error identities.
func openDirectoryAt(ctx context.Context, location *gitrepo.Location, excluded string) (*Directory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := os.Stat(location.ContentRoot); err != nil {
		return nil, err
	}
	var root *os.Root
	var backing billy.Filesystem
	if location.Repository != nil {
		var err error
		root, err = os.OpenRoot(location.RepositoryRoot)
		if err != nil {
			return nil, err
		}
		backing = &membershipRoot{Filesystem: osfs.New(location.RepositoryRoot, osfs.WithBoundOS()), root: root}
	}
	d, err := openDirectory(ctx, location, backing, excluded)
	if err != nil {
		if root != nil {
			_ = root.Close()
		}
		return nil, err
	}
	d.root = root
	return d, nil
}

func openDirectory(
	ctx context.Context,
	location *gitrepo.Location,
	backing billy.Filesystem,
	excluded string,
) (*Directory, error) {
	if backing != nil && excluded != "" {
		input := &FS{membership: membership{directory: excluded}}
		backing = input.memberFilesystem(backing, location.ContentRoot)
	}
	d := &Directory{
		location: location,
		observed: &membershipFilesystem{
			Filesystem:  backing,
			ctx:         ctx,
			directories: make(map[string]membershipDirectory),
		},
	}
	captured, err := d.captureMembership()
	if err != nil {
		return nil, err
	}
	captured.directory = excluded
	d.membership = captured
	root := location.ContentRoot
	input := newLocal(root, false, &localPolicy{})
	input.location, input.membership = location, d.membership
	d.artifact = &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + root, LocalPath: root, LocalRoot: root,
		FS: input, ContentFingerprint: ContentFingerprint,
	}
	return d, nil
}

// Artifact retains only the detached descriptor and immutable membership.
func (d *Directory) Artifact() *store.SourceArtifact {
	return d.artifact
}

// Worktree requires a Git directory and the same admission's selected descriptor.
// The caller MUST apply cache exclusion to current before comparison.
//
//nolint:wrapcheck // The adapter preserves upstream repository error identities.
func (d *Directory) Worktree(current *FS) (*git.Worktree, error) {
	if err := d.observed.err(); err != nil {
		return nil, err
	}
	prefix, err := filepath.Rel(d.location.RepositoryRoot, d.location.ContentRoot)
	if err != nil {
		return nil, err
	}
	input := &directoryFilesystem{
		Filesystem: d.observed, observed: d.observed, current: current,
		content: filepath.ToSlash(prefix), selected: make(map[string]directorySelection),
	}
	repo, err := git.Open(d.location.Repository.Storer, input)
	if err != nil {
		return nil, err
	}
	return repo.Worktree()
}

// Close invalidates all borrowed views, including cached metadata.
//
//nolint:wrapcheck // Closing retains the root's error identity.
func (d *Directory) Close() error {
	d.observed.closed = true
	d.observed.directories = nil
	d.observed.ctx = nil
	if d.root != nil {
		return d.root.Close()
	}
	return nil
}

type directoryFilesystem struct {
	billy.Filesystem

	observed *membershipFilesystem
	current  *FS
	selected map[string]directorySelection
	prefix   string
	content  string
}

type directorySelection struct {
	entries   []fs.FileInfo
	info      fs.FileInfo
	directory bool
}

func (f *directoryFilesystem) Lstat(name string) (fs.FileInfo, error) {
	if err := f.observed.err(); err != nil {
		return nil, err
	}
	name = directoryName(name)
	if !fs.ValidPath(filepath.ToSlash(name)) {
		return nil, &fs.PathError{Op: lstatOperation, Path: name, Err: fs.ErrInvalid}
	}
	name = f.Join(f.prefix, directoryName(name))
	if selected, ok := f.selected[name]; ok {
		return selected.info, nil
	}
	parts := name
	directory := ""
	// Only real directories establish ancestor protection; rule reads can follow aliases.
	if parent := filepath.Dir(name); f.selected[parent].directory {
		directory, parts = parent, filepath.Base(name)
	}
	for part := range strings.SplitSeq(parts, string(filepath.Separator)) {
		directory = f.Join(directory, part)
		if part == gitDirectory {
			return nil, &fs.PathError{Op: lstatOperation, Path: name, Err: fs.ErrNotExist}
		}
		info, err := f.observed.Lstat(directory)
		if err != nil {
			return nil, err
		}
		if !f.admits("", directory, info) {
			return nil, &fs.PathError{Op: lstatOperation, Path: name, Err: fs.ErrNotExist}
		}
		if directory == name {
			return info, nil
		}
		if !info.IsDir() {
			return nil, &fs.PathError{Op: lstatOperation, Path: name, Err: fs.ErrPermission}
		}
	}
	panic("unreachable directory path")
}

func (f *directoryFilesystem) ReadDir(name string) ([]fs.FileInfo, error) {
	info, err := f.Lstat(name)
	if err != nil {
		return nil, err
	}
	name = f.Join(f.prefix, directoryName(name))
	if selected, ok := f.selected[name]; ok {
		return selected.entries, nil
	}
	entries, err := f.observed.ReadDir(name)
	if err != nil {
		return nil, err
	}
	selected := entries
	index := slices.IndexFunc(
		entries,
		func(entry fs.FileInfo) bool { return !f.admits(name, entry.Name(), entry) },
	)
	if index >= 0 {
		selected = make([]fs.FileInfo, 0, len(entries)-1)
		selected = append(selected, entries[:index]...)
		for _, entry := range entries[index+1:] {
			if f.admits(name, entry.Name(), entry) {
				selected = append(selected, entry)
			}
		}
	}
	f.selected[name] = directorySelection{entries: selected, info: info, directory: info.IsDir()}
	return selected, nil
}

//nolint:ireturn // The upstream filesystem contract requires billy.File.
func (f *directoryFilesystem) Open(name string) (billy.File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

//nolint:ireturn // The upstream filesystem contract requires billy.File.
func (f *directoryFilesystem) OpenFile(name string, flag int, mode fs.FileMode) (billy.File, error) {
	if _, err := f.Lstat(name); err != nil {
		return nil, err
	}
	return f.observed.OpenFile(f.Join(f.prefix, directoryName(name)), flag, mode)
}

func (f *directoryFilesystem) Readlink(name string) (string, error) {
	if _, err := f.Lstat(name); err != nil {
		return "", err
	}
	return f.observed.Readlink(f.Join(f.prefix, directoryName(name)))
}

//nolint:ireturn,wrapcheck // Chroot implements the upstream filesystem interface and retains observation lifetime.
func (f *directoryFilesystem) Chroot(name string) (billy.Filesystem, error) {
	if _, err := f.Lstat(name); err != nil {
		return nil, err
	}
	scoped, err := f.Filesystem.Chroot(name)
	if err != nil {
		return nil, err
	}
	return &directoryFilesystem{
		Filesystem: scoped, observed: f.observed, current: f.current,
		prefix: f.Join(f.prefix, directoryName(name)), content: f.content, selected: f.selected,
	}, nil
}

func (f *directoryFilesystem) admits(parent, name string, info fs.FileInfo) bool {
	if filepath.Base(name) == gitDirectory {
		return false
	}
	if f.current.membership.directory == "" && len(f.current.membership.ignored) == 0 {
		return true
	}
	if !info.Mode().IsRegular() && !info.IsDir() {
		return true
	}
	name = f.Join(parent, name)
	rel, inside := strings.CutPrefix(filepath.ToSlash(name), f.content+"/")
	if f.content == "." {
		rel, inside = filepath.ToSlash(name), true
	} else if filepath.ToSlash(name) == f.content {
		rel, inside = ".", true
	}
	return !inside || !f.current.excludes(rel)
}

func (f *directoryFilesystem) Stat(name string) (fs.FileInfo, error) {
	info, err := f.Lstat(name)
	if err != nil || info.Mode()&fs.ModeSymlink == 0 {
		return info, err
	}
	return f.observed.Stat(f.Join(f.prefix, directoryName(name)))
}

func directoryName(name string) string {
	if name == "" || name == "/" {
		return "."
	}
	return name
}
