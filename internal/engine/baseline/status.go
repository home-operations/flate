package baseline

import (
	"bytes"
	"context"
	"fmt"

	"github.com/go-git/go-git/v6"
	gitindex "github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/utils/merkletrie"
	merklefs "github.com/go-git/go-git/v6/utils/merkletrie/filesystem"
	merkleindex "github.com/go-git/go-git/v6/utils/merkletrie/index"
	"github.com/go-git/go-git/v6/utils/merkletrie/noder"
)

func (s *selection) worktreeChanges(
	ctx context.Context,
	worktree *git.Worktree,
	base *gitindex.Index,
) (merkletrie.Changes, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
	index, err := s.repo.Storer.Index()
	if err != nil {
		return nil, fmt.Errorf("index: %w", err)
	}
	// Source policy admits regular files inside checked-out submodules.
	current := merklefs.NewRootNodeWithOptions(worktree.Filesystem(), nil, merklefs.Options{Index: index})
	changes, err := merkletrie.DiffTreeContext(
		ctx,
		merkleindex.NewRootNode(base),
		current,
		func(a, b noder.Hasher) bool {
			// Upstream filesystem directories have an unknown zero hash and MUST be traversed.
			var unknown [24]byte
			left, right := a.Hash(), b.Hash()
			return !bytes.Equal(left, unknown[:]) && !bytes.Equal(right, unknown[:]) && bytes.Equal(left, right)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("diff worktree: %w", err)
	}
	return changes, nil
}
