package git

import (
	"context"
	"slices"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
)

func checkoutRef(repo *git.Repository, hash plumbing.Hash, sparse []string) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	return wt.Checkout(&git.CheckoutOptions{Hash: hash, SparseCheckoutDirectories: slices.Clone(sparse)})
}

// updateSubmodules initializes and pulls submodules in the cloned
// worktree. Mirrors `git submodule update --init --recursive`. The
// parent's auth is reused for each submodule's fetch — Flux's
// behavior assumes a single credential source per GitRepository CR,
// even when submodules live on different hosts.
func updateSubmodules(ctx context.Context, repo *git.Repository, auth transport.AuthMethod) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	subs, err := wt.Submodules()
	if err != nil {
		return err
	}
	return subs.UpdateContext(ctx, &git.SubmoduleUpdateOptions{
		Init:              true,
		RecurseSubmodules: git.DefaultSubmoduleRecursionDepth,
		Auth:              auth,
	})
}
