package git

import (
	"cmp"
	"context"
	"fmt"

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

// RemoteBaseRequest carries a public base's identity.
type RemoteBaseRequest struct {
	URL string
	Ref string
}

// FetchRemoteBase returns an immutable lazy view of the whole public repository.
// An error may retain warnings without usable content.
func (f *Fetcher) FetchRemoteBase(ctx context.Context, request RemoteBaseRequest, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	if f.Cache == nil {
		return nil, fmt.Errorf("%w: remote git base requires cache", manifest.ErrInput)
	}
	repo := &manifest.GitRepository{}
	repo.URL = request.URL
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
	content := struct{ URL, Ref string }{request.URL, request.Ref}
	key := sourcecache.GitKey{Content: content, Ref: cmp.Or(request.Ref, "HEAD")}
	var input *gittree.FS
	generation, id, err := f.Cache.Git(ctx, key, policy, func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		input = nil
		// Opaque base refs require upstream rev-parse over all advertised heads and tags.
		acquisition := *repo
		if !pinned {
			acquisition.Reference = nil
		}
		cloned, err := f.acquire(ctx, storer, worktree, gitAcquisition{spec: &acquisition, options: git.CloneOptions{URL: request.URL, NoCheckout: true}})
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if !pinned {
			err := cloned.FetchContext(ctx, &git.FetchOptions{RefSpecs: []config.RefSpec{"+refs/*:refs/*"}, Tags: git.NoTags})
			if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
				return plumbing.ZeroHash, err
			}
		}
		hash, err := resolveRefHash(cloned, repo.Reference, true)
		if err != nil {
			return plumbing.ZeroHash, err
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
