package git

import (
	"cmp"
	"context"
	"fmt"
	"time"

	"errors"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// RemoteBaseRequest carries a public base's effective repository and command policy.
type RemoteBaseRequest struct {
	URL        string
	Ref        string
	Timeout    time.Duration
	Submodules bool
}

// FetchRemoteBase returns an immutable lazy view of the whole public repository.
// An error may retain warnings without usable content.
func (f *Fetcher) FetchRemoteBase(ctx context.Context, request RemoteBaseRequest, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	if f.Cache == nil {
		return nil, fmt.Errorf("%w: remote git base requires cache", manifest.ErrInput)
	}
	repo := &manifest.GitRepository{}
	repo.URL = request.URL
	repo.RecurseSubmodules = request.Submodules
	if err := f.validateRepo(repo); err != nil {
		return nil, err
	}
	if request.Ref != "" {
		repo.Reference = &sourcev1.GitRepositoryRef{Name: request.Ref}
		if plumbing.IsHash(request.Ref) {
			repo.Reference = &sourcev1.GitRepositoryRef{Commit: request.Ref}
		}
	}
	pinned := (*manifest.GitRepositoryRef)(repo.Reference).Pinned()
	if pinned {
		policy.MaxAge = sourcecache.Forever
	}
	content := struct {
		URL, Ref   string
		Submodules bool
	}{request.URL, request.Ref, request.Submodules}
	key := sourcecache.GitKey{Content: content, Ref: cmp.Or(request.Ref, "HEAD"), RecurseSubmodules: request.Submodules}
	policy.Timeout = nil
	var input *gittree.FS
	generation, id, err := f.Cache.Git(ctx, key, policy, func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		input = nil
		// Opaque base refs require upstream rev-parse over all advertised heads and tags.
		acquisition := *repo
		acquisition.RecurseSubmodules = false
		if !pinned {
			acquisition.Reference = nil
		}
		cloned, err := f.acquire(ctx, storer, worktree, gitAcquisition{spec: &acquisition, options: git.CloneOptions{URL: request.URL, NoCheckout: true}, timeout: request.Timeout})
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if !pinned {
			fetchCtx := ctx
			var cancel context.CancelFunc
			if request.Timeout > 0 {
				fetchCtx, cancel = context.WithTimeout(ctx, request.Timeout)
			}
			err := cloned.FetchContext(fetchCtx, &git.FetchOptions{RefSpecs: []config.RefSpec{"+refs/*:refs/*"}, Tags: git.NoTags})
			if cancel != nil {
				cancel()
			}
			if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
				return plumbing.ZeroHash, err
			}
		}
		hash, err := resolveRefHash(cloned, repo.Reference, true)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if request.Submodules {
			if err := checkoutRef(cloned, hash, nil); err != nil {
				return plumbing.ZeroHash, err
			}
			updateCtx := ctx
			var cancel context.CancelFunc
			if request.Timeout > 0 {
				updateCtx, cancel = context.WithTimeout(ctx, request.Timeout)
			}
			err := f.updateSubmodules(updateCtx, cloned, nil, git.DefaultSubmoduleRecursionDepth)
			if cancel != nil {
				cancel()
			}
			if err != nil {
				return plumbing.ZeroHash, err
			}
		}
		input, _, err = gittree.New(ctx, storer.(*filesystem.Storage).Filesystem(), hash, gittree.ViewOptions{Content: content, Mode: gittree.RemoteBase, Worktree: worktree})
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("%w: validate git view: %w", manifest.ErrInput, err)
		}
		return hash, nil
	})
	if err != nil {
		return gitWarningArtifact(input, manifest.NamedResource{}, request.URL, request.Ref), err
	}
	if input == nil {
		input, _, err = gittree.New(ctx, generation.Backing, generation.Commit, gittree.ViewOptions{
			Content: content, Mode: gittree.RemoteBase, Worktree: generation.Worktree,
		})
	}
	if err != nil {
		return gitWarningArtifact(input, manifest.NamedResource{}, request.URL, request.Ref), fmt.Errorf("%w: remote git base: %w", manifest.ErrInput, err)
	}

	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: request.URL, Revision: generation.Commit.String(), Key: id, FS: input, Warnings: gitWarnings(input, manifest.NamedResource{}, request.URL, request.Ref)}, nil
}
