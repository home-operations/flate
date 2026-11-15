package gittree

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/gitignore"
	"github.com/go-git/go-git/v6/utils/merkletrie"
	merklefs "github.com/go-git/go-git/v6/utils/merkletrie/filesystem"
	merkleindex "github.com/go-git/go-git/v6/utils/merkletrie/index"
	"github.com/go-git/go-git/v6/utils/merkletrie/noder"
)

func (d *Directory) captureMembership() (membership, error) {
	ctx, location, observed := d.observed.ctx, d.location, d.observed
	switch err := ctx.Err(); {
	case err != nil, location == nil, location.Repository == nil:
		return membership{captured: err == nil}, err
	}
	//nolint:staticcheck // Admission MUST capture ignore errors and observations before index access.
	patterns, err := gitignore.ReadPatterns(observed, nil)
	if err != nil {
		return membership{}, fmt.Errorf("read git ignore patterns: %w", err)
	}
	switch ctxErr := ctx.Err(); {
	case ctxErr != nil, len(patterns) == 0:
		return membership{captured: ctxErr == nil}, ctxErr
	}
	idx, err := location.Repository.Storer.Index()
	if err != nil {
		return membership{}, fmt.Errorf("read git index: %w", err)
	}
	repo, err := git.Open(location.Repository.Storer, observed)
	if err != nil {
		return membership{}, fmt.Errorf("open membership repository: %w", err)
	}
	worktree, err := repo.Worktree()
	if err != nil {
		return membership{}, fmt.Errorf("open membership worktree: %w", err)
	}
	modules, err := worktree.Submodules()
	if err != nil {
		return membership{}, fmt.Errorf("read membership submodules: %w", err)
	}
	status, err := modules.Status()
	if err != nil {
		return membership{}, fmt.Errorf("read membership submodule status: %w", err)
	}
	submodules := make(map[string]plumbing.Hash, len(status))
	for _, module := range status {
		submodules[module.Path] = cmp.Or(module.Current, module.Expected)
	}
	changes, err := merkletrie.DiffTreeContext(
		ctx,
		merkleindex.NewRootNode(idx),
		merklefs.NewRootNodeWithOptions(worktree.Filesystem(), submodules, merklefs.Options{}),
		membershipEqual,
	)
	if err != nil {
		return membership{}, fmt.Errorf("compare git membership: %w", errors.Join(err, ctx.Err()))
	}
	var (
		matcher = gitignore.NewMatcher(patterns)
		ignored map[string]struct{}
	)
	for _, change := range changes {
		if len(change.From) != 0 || len(change.To) == 0 {
			continue
		}
		parts := make([]string, len(change.To))
		for i, node := range change.To {
			parts[i] = node.Name()
		}
		if !matcher.Match(parts, change.To.IsDir()) {
			continue
		}
		name, err := filepath.Rel(
			location.ContentRoot, filepath.Join(location.RepositoryRoot, filepath.FromSlash(change.To.String())),
		)
		switch {
		case err != nil:
			return membership{}, fmt.Errorf("relate ignored path %q to content root: %w", change.To, err)
		case name == "..", strings.HasPrefix(name, ".."+string(filepath.Separator)):
			continue
		case ignored == nil:
			ignored = make(map[string]struct{})
		}
		ignored[filepath.ToSlash(name)] = struct{}{}
	}
	return membership{ignored: ignored, captured: true}, ctx.Err()
}

//nolint:errcheck // Upstream comparison supplies noder.Path values implementing noder.Noder.
func membershipEqual(a, b noder.Hasher) bool {
	// Membership MUST descend through either directory without reading file contents.
	return !a.(noder.Noder).IsDir() &&
		!b.(noder.Noder).IsDir()
}

type membershipFilesystem struct {
	billy.Filesystem

	ctx         context.Context //nolint:containedctx // The upstream filesystem interface has no context parameter; this adapter never leaves its directory operation.
	directories map[string]membershipDirectory
	closed      bool
}

type membershipDirectory struct {
	entries []fs.DirEntry
	byName  map[string]fs.DirEntry
	err     error
}

//nolint:wrapcheck // ReadDir reuses the backing directory's entries and fs.PathError without changing its operation or path.
func (f *membershipFilesystem) ReadDir(name string) ([]fs.DirEntry, error) {
	if err := f.err(); err != nil {
		return nil, err
	}
	name = f.Join(".", name)
	if directory, ok := f.directories[name]; ok {
		return directory.entries, directory.err
	}
	entries, err := f.Filesystem.ReadDir(name)
	byName := make(map[string]fs.DirEntry, len(entries))
	for _, entry := range entries {
		byName[entry.Name()] = entry
	}
	f.directories[name] = membershipDirectory{entries: entries, byName: byName, err: err}
	return entries, err
}

func (f *membershipFilesystem) Open(name string) (billy.File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

//nolint:wrapcheck // OpenFile forwards billy.Filesystem errors and returns open fs.PathError for an absent observed entry.
func (f *membershipFilesystem) OpenFile(name string, flag int, mode fs.FileMode) (billy.File, error) {
	if err := f.err(); err != nil {
		return nil, err
	}
	name = f.Join(".", name)
	metadata := strings.HasPrefix(name, gitDirectory+string(filepath.Separator))
	if name != "." && !metadata && len(f.directories) != 0 {
		directory := "."
		for part := range strings.SplitSeq(name, string(filepath.Separator)) {
			_, err := f.ReadDir(directory)
			if err != nil {
				return nil, err
			}
			if f.directories[directory].byName[part] == nil {
				return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
			}
			directory = f.Join(directory, part)
		}
	}
	if err := f.err(); err != nil {
		return nil, err
	}
	return f.Filesystem.OpenFile(name, flag, mode)
}

//nolint:wrapcheck // Lstat forwards cached ReadDir, DirEntry.Info, and backing Lstat errors with their original fs.PathError fields.
func (f *membershipFilesystem) Lstat(name string) (fs.FileInfo, error) {
	if err := f.err(); err != nil {
		return nil, err
	}
	name = f.Join(".", name)
	directory, ok := f.directories[filepath.Dir(name)]
	if name == "." || !ok {
		return f.Filesystem.Lstat(name)
	}
	if directory.err != nil {
		return nil, directory.err
	}
	base := filepath.Base(name)
	if entry := directory.byName[base]; entry != nil {
		return entry.Info()
	}
	return nil, &fs.PathError{Op: lstatOperation, Path: name, Err: fs.ErrNotExist}
}

type membershipRoot struct {
	billy.Filesystem

	root *os.Root
}

//nolint:wrapcheck // Root.Open and File.Readdir supply fs.PathError with the failed directory operation and path.
func (f *membershipRoot) ReadDir(name string) ([]fs.DirEntry, error) {
	directory, err := f.root.Open(cmp.Or(name, "."))
	if err != nil {
		return nil, err
	}
	defer func() { _ = directory.Close() }()
	// Capture metadata before closing the root-bound directory.
	infos, err := directory.Readdir(-1)
	if err != nil {
		return nil, err
	}
	entries := make([]fs.DirEntry, len(infos))
	for i, info := range infos {
		entries[i] = fs.FileInfoToDirEntry(info)
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return entries, nil
}

func (f *membershipFilesystem) err() error {
	if f.closed {
		return os.ErrClosed
	}
	return f.ctx.Err()
}

//nolint:wrapcheck // Readlink forwards billy.Filesystem errors with their original fs.PathError operation and path.
func (f *membershipFilesystem) Readlink(name string) (string, error) {
	if err := f.err(); err != nil {
		return "", err
	}
	return f.Filesystem.Readlink(name)
}

//nolint:wrapcheck // Stat forwards billy.Filesystem errors with their original fs.PathError operation and path for ignore-rule reads.
func (f *membershipFilesystem) Stat(name string) (fs.FileInfo, error) {
	if err := f.err(); err != nil {
		return nil, err
	}
	return f.Filesystem.Stat(name)
}
