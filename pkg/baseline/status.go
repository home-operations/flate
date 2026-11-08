package baseline

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	gitindex "github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/utils/merkletrie"
	merklefs "github.com/go-git/go-git/v5/utils/merkletrie/filesystem"
	merkleindex "github.com/go-git/go-git/v5/utils/merkletrie/index"
	"github.com/go-git/go-git/v5/utils/merkletrie/noder"
)

//nolint:wrapcheck // The adapter preserves upstream filesystem and repository error identities.
func (s selection) worktreeChanges(
	ctx context.Context,
	worktree *git.Worktree,
	base *gitindex.Index,
) (merkletrie.Changes, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(worktree.Filesystem.Root())
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	input := &worktreeMetadata{
		Filesystem: osfs.New(root.Name()), root: root, ctx: ctx, observations: map[string]os.FileInfo{},
	}
	observed, err := git.Open(s.repo.Storer, input)
	if err == nil {
		worktree, err = observed.Worktree()
	}
	if err != nil {
		return nil, err
	}
	index, err := s.repo.Storer.Index()
	if err != nil {
		return nil, err
	}
	// Source policy admits regular files inside checked-out submodules.
	current := merklefs.NewRootNodeWithOptions(worktree.Filesystem, nil, merklefs.Options{Index: index})
	return merkletrie.DiffTreeContext(ctx, merkleindex.NewRootNode(base), current, func(a, b noder.Hasher) bool {
		// Upstream filesystem directories have an unknown zero hash and MUST be traversed.
		var unknown [24]byte
		left, right := a.Hash(), b.Hash()
		return !bytes.Equal(left, unknown[:]) && !bytes.Equal(right, unknown[:]) && bytes.Equal(left, right)
	})
}

// Baseline requires a stable checkout. Metadata observations MUST end with this comparison.
type worktreeMetadata struct {
	billy.Filesystem

	root         *os.Root
	ctx          context.Context //nolint:containedctx // The upstream filesystem interface has no context parameter; this view shares one operation lifetime.
	prefix       string
	observations map[string]os.FileInfo
}

//nolint:wrapcheck // Filesystem adapters preserve I/O and cancellation error identities.
func (w *worktreeMetadata) ReadDir(name string) ([]os.FileInfo, error) {
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	if name == "" || name == "/" {
		name = "."
	}
	file, err := w.root.Open(path.Join(w.prefix, name))
	if err != nil {
		return nil, err
	}
	entries, readErr := file.Readdir(-1)
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b os.FileInfo) int { return strings.Compare(a.Name(), b.Name()) })
	for _, info := range entries {
		if info.IsDir() {
			w.observations[path.Join(w.prefix, name, info.Name())] = info
		}
	}
	return entries, nil
}

//nolint:wrapcheck // Filesystem adapters preserve I/O and cancellation error identities.
func (w *worktreeMetadata) Lstat(name string) (os.FileInfo, error) {
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	if name == "" || name == "/" {
		name = "."
	}
	name = path.Join(w.prefix, name)
	if info := w.observations[name]; info != nil {
		return info, nil
	}
	info, err := w.root.Lstat(name)
	if err == nil && info.IsDir() {
		w.observations[name] = info
	}
	return info, err
}

//nolint:ireturn,wrapcheck // Chroot implements the upstream filesystem interface and preserves its diagnostics.
func (w *worktreeMetadata) Chroot(name string) (billy.Filesystem, error) {
	scoped, err := w.Filesystem.Chroot(name)
	if err != nil {
		return nil, err
	}
	return &worktreeMetadata{
		Filesystem: scoped, root: w.root, ctx: w.ctx, prefix: path.Join(w.prefix, name), observations: w.observations,
	}, nil
}
