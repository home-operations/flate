package gittree

import (
	"cmp"
	"context"
	"errors"
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

//nolint:funlen,gocognit,wrapcheck // Native capture keeps its index, submodule, and ignore decisions in one admission lifetime and preserves their error identities.
func (d *Directory) captureMembership() (membership, error) {
	ctx, location, observed := d.observed.ctx, d.location, d.observed
	if err := ctx.Err(); err != nil {
		return membership{}, err
	}
	if location == nil || location.Repository == nil {
		return membership{captured: true}, nil
	}
	//nolint:staticcheck // Admission MUST capture ignore errors and observations before index access.
	patterns, err := gitignore.ReadPatterns(
		observed,
		nil,
	)
	if err != nil {
		return membership{}, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return membership{}, ctxErr
	}
	if len(patterns) == 0 {
		return membership{captured: true}, nil
	}
	idx, err := location.Repository.Storer.Index()
	if err != nil {
		return membership{}, err
	}
	repo, err := git.Open(location.Repository.Storer, observed)
	if err != nil {
		return membership{}, err
	}
	worktree, err := repo.Worktree()
	if err != nil {
		return membership{}, err
	}
	modules, err := worktree.Submodules()
	if err != nil {
		return membership{}, err
	}
	status, err := modules.Status()
	if err != nil {
		return membership{}, err
	}
	submodules := make(map[string]plumbing.Hash, len(status))
	for _, module := range status {
		submodules[module.Path] = cmp.Or(module.Current, module.Expected)
	}
	current := merklefs.NewRootNodeWithOptions(worktree.Filesystem(), submodules, merklefs.Options{})
	changes, err := merkletrie.DiffTreeContext(
		ctx,
		merkleindex.NewRootNode(idx),
		current,
		membershipEqual,
	)
	if err != nil {
		return membership{}, errors.Join(err, ctx.Err())
	}
	matcher := gitignore.NewMatcher(patterns)
	var ignored map[string]struct{}
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
			location.ContentRoot,
			filepath.Join(location.RepositoryRoot, filepath.FromSlash(change.To.String())),
		)
		if err != nil {
			return membership{}, err
		}
		if name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
			continue
		}
		if ignored == nil {
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

//nolint:wrapcheck // Capture-local I/O preserves filesystem error identities.
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

//nolint:ireturn // The billy filesystem contract requires billy.File.
func (f *membershipFilesystem) Open(name string) (billy.File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

//nolint:ireturn,wrapcheck // The billy filesystem contract requires billy.File and preserves filesystem error identities.
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

//nolint:wrapcheck // Capture-local metadata preserves filesystem error identities.
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

//nolint:wrapcheck // Capture-local directory reads preserve OS error identities.
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

//nolint:wrapcheck // Borrowed views preserve cancellation error identities.
func (f *membershipFilesystem) err() error {
	if f.closed {
		return os.ErrClosed
	}
	return f.ctx.Err()
}

//nolint:wrapcheck // The raw adapter preserves filesystem error identities.
func (f *membershipFilesystem) Readlink(name string) (string, error) {
	if err := f.err(); err != nil {
		return "", err
	}
	return f.Filesystem.Readlink(name)
}

//nolint:wrapcheck // Raw stat retains the rule reader's filesystem semantics.
func (f *membershipFilesystem) Stat(name string) (fs.FileInfo, error) {
	if err := f.err(); err != nil {
		return nil, err
	}
	return f.Filesystem.Stat(name)
}
