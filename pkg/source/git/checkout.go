package git

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func checkoutRef(repo *git.Repository, hash plumbing.Hash, sparse []string) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err //nolint:wrapcheck // The go-git checkout and submodule adapters retain upstream protocol errors for source classification.
	}
	return wt.Checkout( //nolint:wrapcheck // The checkout adapter must preserve upstream protocol error identity and messages.
		&git.CheckoutOptions{Hash: hash, SparseCheckoutDirectories: gittree.NormalizeSparsePaths(sparse)},
	)
}

// Flux reuses the parent's credentials for submodules on every host.
func (f *Fetcher) updateSubmodules(ctx context.Context, repo *git.Repository, options submoduleOptions) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err //nolint:wrapcheck // The go-git checkout and submodule adapters retain upstream protocol errors for source classification.
	}
	subs, err := wt.Submodules()
	if err != nil {
		return err //nolint:wrapcheck // The go-git checkout and submodule adapters retain upstream protocol errors for source classification.
	}
	client, err := newClient(&ClientConfig{Transports: f.GitTransports, Network: f.Network}, options.auth)
	if err != nil {
		return err
	}
	slices.SortFunc(subs, func(a, b *git.Submodule) int {
		return strings.Compare(a.Config().Name, b.Config().Name)
	})
	for _, sub := range subs {
		if err := sub.Init(); err != nil && !errors.Is(err, git.ErrSubmoduleAlreadyInitialized) {
			return err //nolint:wrapcheck // The go-git checkout and submodule adapters retain upstream protocol errors for source classification.
		}
		child, err := sub.Repository()
		if err != nil {
			return err //nolint:wrapcheck // The go-git checkout and submodule adapters retain upstream protocol errors for source classification.
		}
		remote, err := child.Remote(git.DefaultRemoteName)
		if err != nil {
			return err //nolint:wrapcheck // The go-git checkout and submodule adapters retain upstream protocol errors for source classification.
		}
		endpoint, err := f.validateRepo(
			manifest.NewGitRepository(
				sourcev1.GitRepository{Spec: sourcev1.GitRepositorySpec{URL: remote.Config().URLs[0]}},
			), false,
		)
		if err != nil {
			return fmt.Errorf("validate submodule %q: %w", sub.Config().Name, err)
		}
		err = sub.UpdateContext(ctx, &git.SubmoduleUpdateOptions{ClientOptions: client.options})
		recordTransfer(options.work, endpoint.Scheme, err)
		if err != nil {
			return err //nolint:wrapcheck // The go-git checkout and submodule adapters retain upstream protocol errors for source classification.
		}
		if options.depth <= 0 {
			continue
		}
		if err := f.updateSubmodules(ctx, child, submoduleOptions{
			auth: options.auth, depth: options.depth - 1, work: options.work,
		}); err != nil {
			return err
		}
	}
	return nil
}

type submoduleOptions struct {
	auth  any
	depth git.SubmoduleRecursivity
	work  *manifest.AcquisitionWork
}
