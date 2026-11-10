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

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
	"github.com/go-git/go-git/v5/utils/merkletrie"
	merklefs "github.com/go-git/go-git/v5/utils/merkletrie/filesystem"
	merkleindex "github.com/go-git/go-git/v5/utils/merkletrie/index"
	"github.com/go-git/go-git/v5/utils/merkletrie/noder"

	"github.com/home-operations/flate/pkg/source/gitrepo"
)

//nolint:funlen,gocognit,wrapcheck // Native capture keeps its index, submodule, and ignore decisions in one admission lifetime and preserves their error identities.
func captureMembership(ctx context.Context, location *gitrepo.Location) (membership, error) {
	if err := ctx.Err(); err != nil {
		return membership{}, err
	}
	if location == nil || location.Repository == nil {
		return membership{captured: true}, nil
	}
	root, err := os.OpenRoot(location.RepositoryRoot)
	if err != nil {
		return membership{}, err
	}
	defer func() { _ = root.Close() }()
	// The protected worktree hides .git/info/exclude from the native rule reader.
	observed := &membershipFilesystem{
		Filesystem:  &membershipRoot{Filesystem: osfs.New(location.RepositoryRoot, osfs.WithBoundOS()), root: root},
		ctx:         ctx,
		directories: make(map[string]membershipDirectory),
	}
	patterns, err := gitignore.ReadPatterns(observed, nil)
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
	current := merklefs.NewRootNode(worktree.Filesystem, submodules)
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

	ctx         context.Context //nolint:containedctx // The upstream filesystem interface has no context parameter; this adapter never leaves capture.
	directories map[string]membershipDirectory
}

type membershipDirectory struct {
	entries []fs.FileInfo
	err     error
}

//nolint:wrapcheck // Capture-local I/O preserves filesystem error identities.
func (f *membershipFilesystem) ReadDir(name string) ([]fs.FileInfo, error) {
	if err := f.ctx.Err(); err != nil {
		return nil, err
	}
	name = f.Join(".", name)
	if directory, ok := f.directories[name]; ok {
		return directory.entries, directory.err
	}
	entries, err := f.Filesystem.ReadDir(name)
	f.directories[name] = membershipDirectory{entries: entries, err: err}
	return entries, err
}

//nolint:ireturn,wrapcheck // The billy filesystem contract requires billy.File and preserves filesystem error identities.
func (f *membershipFilesystem) Open(name string) (billy.File, error) {
	if err := f.ctx.Err(); err != nil {
		return nil, err
	}
	name = f.Join(".", name)
	if name != "." && len(f.directories) != 0 {
		directory := "."
		for part := range strings.SplitSeq(name, string(filepath.Separator)) {
			entries, err := f.ReadDir(directory)
			if err != nil {
				return nil, err
			}
			if !slices.ContainsFunc(entries, func(entry fs.FileInfo) bool { return entry.Name() == part }) {
				return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
			}
			directory = f.Join(directory, part)
		}
	}
	if err := f.ctx.Err(); err != nil {
		return nil, err
	}
	return f.Filesystem.Open(name)
}

//nolint:wrapcheck // Capture-local metadata preserves filesystem error identities.
func (f *membershipFilesystem) Lstat(name string) (fs.FileInfo, error) {
	if err := f.ctx.Err(); err != nil {
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
	for _, entry := range directory.entries {
		if entry.Name() == base {
			return entry, nil
		}
	}
	return nil, &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrNotExist}
}

type membershipRoot struct {
	billy.Filesystem

	root *os.Root
}

//nolint:wrapcheck // Capture-local directory reads preserve OS error identities.
func (f *membershipRoot) ReadDir(name string) ([]fs.FileInfo, error) {
	directory, err := f.root.Open(cmp.Or(name, "."))
	if err != nil {
		return nil, err
	}
	defer func() { _ = directory.Close() }()
	// The native tree needs FileInfo without resolving each entry through its full path.
	entries, err := directory.Readdir(-1)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b fs.FileInfo) int { return strings.Compare(a.Name(), b.Name()) })
	return entries, nil
}
