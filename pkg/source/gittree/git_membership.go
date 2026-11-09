package gittree

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
	"github.com/go-git/go-git/v5/utils/merkletrie"
	merklefs "github.com/go-git/go-git/v5/utils/merkletrie/filesystem"
	merkleindex "github.com/go-git/go-git/v5/utils/merkletrie/index"
	"github.com/go-git/go-git/v5/utils/merkletrie/noder"

	"github.com/home-operations/flate/pkg/source/gitrepo"
)

//nolint:gocognit,wrapcheck // Native capture keeps its index, submodule, and ignore decisions in one admission lifetime and preserves their error identities.
func captureMembership(ctx context.Context, location *gitrepo.Location) (membership, error) {
	if err := ctx.Err(); err != nil {
		return membership{}, err
	}
	if location == nil || location.Repository == nil {
		return membership{captured: true}, nil
	}
	// The protected worktree hides .git/info/exclude from the native rule reader.
	patterns, err := gitignore.ReadPatterns(osfs.New(location.RepositoryRoot, osfs.WithBoundOS()), nil)
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
	worktree, err := location.Repository.Worktree()
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
	current := merklefs.NewRootNodeWithOptions(worktree.Filesystem, submodules, merklefs.Options{Index: idx})
	changes, err := merkletrie.DiffTreeContext(
		ctx,
		merkleindex.NewRootNode(idx),
		current,
		func(a, b noder.Hasher) bool {
			// Upstream directories have unknown hashes and MUST be traversed.
			var unknown [24]byte
			left, right := a.Hash(), b.Hash()
			return !bytes.Equal(left, unknown[:]) && !bytes.Equal(right, unknown[:]) && bytes.Equal(left, right)
		},
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
