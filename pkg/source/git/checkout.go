package git

import (
	"fmt"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/home-operations/flate/pkg/manifest"
)

// checkoutRef checks out the worktree at the requested ref, applying
// sparse-checkout directories when configured. Mirrors source-
// controller's ref-resolution order: Name > Commit > Tag > Branch >
// SemVer (unsupported here), falling back to HEAD when no ref is set.
func checkoutRef(repo *git.Repository, ref manifest.GitRepositoryRef, sparse []string) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	// newOpts builds a CheckoutOptions pre-populated with sparse-checkout
	// directories when configured. Repeated per call-site because
	// CheckoutOptions is consumed by each Checkout invocation.
	newOpts := func() *git.CheckoutOptions {
		opts := &git.CheckoutOptions{}
		if len(sparse) > 0 {
			opts.SparseCheckoutDirectories = append(opts.SparseCheckoutDirectories, sparse...)
		}
		return opts
	}
	checkout := func(set func(*git.CheckoutOptions)) error {
		opts := newOpts()
		set(opts)
		return wt.Checkout(opts)
	}
	switch {
	case ref.Name != "":
		// Full ref name takes precedence (e.g. "refs/pull/420/head",
		// "refs/tags/v1.2.3"). Resolve against the cloned repo so the
		// remote-tracking layer (refs/remotes/origin/...) is checked
		// too — go-git's PlainClone fetches all refs but normalizes
		// branches under refs/remotes/origin.
		rev := plumbing.Revision(ref.Name)
		if h, err := repo.ResolveRevision(rev); err == nil {
			return checkout(func(o *git.CheckoutOptions) { o.Hash = *h })
		}
		// Fall through: try resolving as a remote-tracking ref. This
		// handles refs/heads/<branch> by mapping to refs/remotes/origin/<branch>.
		if rest, ok := strings.CutPrefix(ref.Name, "refs/heads/"); ok {
			remote := plumbing.NewRemoteReferenceName("origin", rest)
			if h, err := repo.ResolveRevision(plumbing.Revision(remote)); err == nil {
				return checkout(func(o *git.CheckoutOptions) { o.Hash = *h })
			}
		}
		return fmt.Errorf("%w: unable to resolve git ref %q", manifest.ErrInput, ref.Name)
	case ref.Commit != "":
		return checkout(func(o *git.CheckoutOptions) { o.Hash = plumbing.NewHash(ref.Commit) })
	case ref.Tag != "":
		if h, err := repo.ResolveRevision(plumbing.Revision("refs/tags/" + ref.Tag)); err == nil {
			return checkout(func(o *git.CheckoutOptions) { o.Hash = *h })
		}
		return checkout(func(o *git.CheckoutOptions) { o.Branch = plumbing.NewTagReferenceName(ref.Tag) })
	case ref.Branch != "":
		remoteRef := plumbing.NewRemoteReferenceName("origin", ref.Branch)
		if h, err := repo.ResolveRevision(plumbing.Revision(remoteRef)); err == nil {
			return checkout(func(o *git.CheckoutOptions) { o.Hash = *h })
		}
		return checkout(func(o *git.CheckoutOptions) { o.Branch = plumbing.NewBranchReferenceName(ref.Branch) })
	case ref.SemVer != "":
		return fmt.Errorf("%w: GitRepository semver ref is not supported yet", manifest.ErrInput)
	}
	// No ref: just check out HEAD (with sparse, if configured).
	return checkout(func(*git.CheckoutOptions) {})
}

// updateSubmodules initializes and pulls submodules in the cloned
// worktree. Mirrors `git submodule update --init --recursive`. The
// parent's auth is reused for each submodule's fetch — Flux's
// behavior assumes a single credential source per GitRepository CR,
// even when submodules live on different hosts.
func updateSubmodules(repo *git.Repository, auth transport.AuthMethod) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	subs, err := wt.Submodules()
	if err != nil {
		return err
	}
	return subs.Update(&git.SubmoduleUpdateOptions{
		Init:              true,
		RecurseSubmodules: git.DefaultSubmoduleRecursionDepth,
		Auth:              auth,
	})
}
