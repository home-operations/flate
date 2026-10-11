package git

import (
	"cmp"
	"context"
	"fmt"
	"time"

	"errors"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// RemoteBaseRequest carries a public base's identity and caller freshness bound.
type RemoteBaseRequest struct {
	URL    string
	Ref    string
	MaxAge time.Duration
}

// FetchRemoteBase returns an immutable lazy view of the whole public repository.
func (f *Fetcher) FetchRemoteBase(ctx context.Context, request RemoteBaseRequest) (*store.SourceArtifact, error) {
	if f.Cache == nil {
		return nil, fmt.Errorf("%w: remote git base requires cache", manifest.ErrInput)
	}
	repo := &manifest.GitRepository{}
	repo.URL = request.URL
	if err := f.validateRepo(repo); err != nil {
		return nil, err
	}
	if request.Ref != "" {
		repo.Reference = &manifest.GitRepositoryRef{Name: request.Ref}
		if plumbing.IsHash(request.Ref) {
			repo.Reference = &manifest.GitRepositoryRef{Commit: request.Ref}
			request.MaxAge = sourcecache.Forever
		}
	}
	h, err := source.CacheKeyHash([]string{request.URL, request.Ref, "remote-base-v1"}, 32)
	if err != nil {
		return nil, err
	}
	key := sourcecache.GitKey{ID: "git-base-" + h, Ref: cmp.Or(request.Ref, "HEAD")}
	var input *gittree.FS
	generation, err := f.Cache.Git(ctx, key, request.MaxAge, func(storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		// Opaque base refs require upstream rev-parse over all advertised heads and tags.
		acquisition := *repo
		if !plumbing.IsHash(request.Ref) {
			acquisition.Reference = nil
		}
		cloned, err := f.acquire(ctx, storer, worktree, gitAcquisition{&acquisition, git.CloneOptions{URL: request.URL, NoCheckout: true}})
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if !plumbing.IsHash(request.Ref) {
			err := cloned.FetchContext(ctx, &git.FetchOptions{RefSpecs: []config.RefSpec{"+refs/*:refs/*"}, Tags: git.NoTags})
			if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
				return plumbing.ZeroHash, err
			}
		}
		hash, err := resolveRefHash(cloned, repo.Reference)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		input, err = generationView(ctx, &sourcecache.GitGeneration{Backing: storer.(*filesystem.Storage).Filesystem(), Commit: hash}, gittree.ViewOptions{Mode: gittree.RemoteBase}, nil)
		return hash, err
	})
	if err != nil {
		return nil, err
	}
	input, err = generationView(ctx, generation, gittree.ViewOptions{Mode: gittree.RemoteBase}, input)
	if err != nil {
		return nil, fmt.Errorf("%w: remote git base: %w", manifest.ErrInput, err)
	}

	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: request.URL, Revision: generation.Commit.String(), Key: key.ID + "@" + generation.Commit.String(), FS: input}, nil
}
