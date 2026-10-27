package git

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func checkoutRef(repo *git.Repository, hash plumbing.Hash, sparse []string) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	return wt.Checkout(&git.CheckoutOptions{Hash: hash, SparseCheckoutDirectories: gittree.NormalizeSparsePaths(sparse)})
}

// Flux reuses the parent's credentials for submodules on every host.
func (f *Fetcher) updateSubmodules(ctx context.Context, repo *git.Repository, auth transport.AuthMethod, depth git.SubmoduleRescursivity) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	subs, err := wt.Submodules()
	if err != nil {
		return err
	}
	slices.SortFunc(subs, func(a, b *git.Submodule) int {
		return strings.Compare(a.Config().Name, b.Config().Name)
	})
	for _, sub := range subs {
		if err := sub.Init(); err != nil && !errors.Is(err, git.ErrSubmoduleAlreadyInitialized) {
			return err
		}
		child, err := sub.Repository()
		if err != nil {
			return err
		}
		remote, err := child.Remote(git.DefaultRemoteName)
		if err != nil {
			return err
		}
		if _, err := f.validateRepo(&manifest.GitRepository{URL: remote.Config().URLs[0]}); err != nil {
			return fmt.Errorf("validate submodule %q: %w", sub.Config().Name, err)
		}
		if err := sub.UpdateContext(ctx, &git.SubmoduleUpdateOptions{Auth: auth}); err != nil {
			return err
		}
		if depth > 0 {
			if err := f.updateSubmodules(ctx, child, auth, depth-1); err != nil {
				return err
			}
		}
	}
	return nil
}
