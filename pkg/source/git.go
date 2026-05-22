package source

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// GitFetcher is the Fetcher implementation for KindGitRepository.
// It owns a shared Cache so multiple GitRepository CRs writing to the
// same cache root serialize on slot allocation correctly.
type GitFetcher struct {
	Cache *Cache
}

// Fetch implements source.Fetcher for *manifest.GitRepository.
func (f *GitFetcher) Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	repo, ok := obj.(*manifest.GitRepository)
	if !ok {
		return nil, fmt.Errorf("%w: GitFetcher: unexpected payload %T", manifest.ErrInput, obj)
	}
	return FetchGit(ctx, f.Cache, repo)
}

// FetchGit clones the GitRepository referenced by repo into the supplied
// cache and returns a populated *store.SourceArtifact. If a usable cached
// copy already exists, it is reused.
//
// Supported transports: public HTTPS, ssh-with-agent (handled by go-git
// transparently), and file:// URLs. Per-host credential lookups via a
// Secret reference will be added when a use case demands it.
func FetchGit(ctx context.Context, cache *Cache, repo *manifest.GitRepository) (*store.SourceArtifact, error) {
	if repo == nil {
		return nil, errors.New("git repository is nil")
	}
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: GitRepository %s missing url", manifest.ErrInput, repo.RepoName())
	}

	refStr := repo.Ref.RefString()
	if refStr == "" {
		refStr = "HEAD"
	}

	slot, exists, err := cache.Slot(repo.URL, refStr)
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", repo.URL, err)
	}

	if exists {
		// Validate it's a usable repo before reusing.
		if _, err := git.PlainOpen(slot); err == nil {
			rev, _ := readResolvedRevision(slot)
			return &store.SourceArtifact{
				Kind: manifest.KindGitRepository,
				URL:  repo.URL, LocalPath: slot, Revision: rev,
			}, nil
		}
		// Stale slot — wipe and re-clone.
		_ = cache.Reset(slot)
		if err := os.MkdirAll(slot, 0o750); err != nil {
			return nil, err
		}
	}

	url := repo.URL
	// file:// URLs are accepted by go-git as bare filesystem paths.
	if rest, ok := strings.CutPrefix(url, "file://"); ok {
		url = rest
	}

	cloneOpts := &git.CloneOptions{URL: url, NoCheckout: true}
	cloned, err := git.PlainCloneContext(ctx, slot, false, cloneOpts)
	if err != nil {
		_ = cache.Reset(slot)
		return nil, fmt.Errorf("clone %s: %w", url, err)
	}

	if err := checkoutRef(cloned, repo.Ref); err != nil {
		return nil, fmt.Errorf("checkout %s: %w", refStr, err)
	}

	rev, _ := readResolvedRevision(slot)
	return &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  repo.URL, LocalPath: slot, Revision: rev,
	}, nil
}

func checkoutRef(repo *git.Repository, ref manifest.GitRepositoryRef) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	switch {
	case ref.Commit != "":
		return wt.Checkout(&git.CheckoutOptions{Hash: plumbing.NewHash(ref.Commit)})
	case ref.Tag != "":
		// Tags are typically reachable via "refs/tags/<tag>".
		if h, err := repo.ResolveRevision(plumbing.Revision("refs/tags/" + ref.Tag)); err == nil {
			return wt.Checkout(&git.CheckoutOptions{Hash: *h})
		}
		return wt.Checkout(&git.CheckoutOptions{Branch: plumbing.NewTagReferenceName(ref.Tag)})
	case ref.Branch != "":
		// Default-branch HEAD is already checked out when NoCheckout=false.
		// With NoCheckout we must resolve the remote branch.
		remoteRef := plumbing.NewRemoteReferenceName("origin", ref.Branch)
		if h, err := repo.ResolveRevision(plumbing.Revision(remoteRef)); err == nil {
			return wt.Checkout(&git.CheckoutOptions{Hash: *h})
		}
		// Fallback: maybe the repo has a local branch (file:// case).
		return wt.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(ref.Branch)})
	case ref.Semver != "":
		return fmt.Errorf("%w: GitRepository semver ref is not supported yet", manifest.ErrInput)
	}
	// No ref: just check out HEAD.
	return wt.Checkout(&git.CheckoutOptions{})
}

// readResolvedRevision returns the current commit SHA at the worktree.
// Best-effort: returns empty string on any failure.
func readResolvedRevision(slot string) (string, error) {
	repo, err := git.PlainOpen(slot)
	if err != nil {
		return "", err
	}
	h, err := repo.Head()
	if err != nil {
		return "", err
	}
	return h.Hash().String(), nil
}
