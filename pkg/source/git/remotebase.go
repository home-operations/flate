package git

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

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
func (f *Fetcher) FetchRemoteBase(
	ctx context.Context,
	request RemoteBaseRequest,
	policy sourcecache.Policy,
) (*store.SourceArtifact, error) {
	if f.Cache == nil {
		return nil, manifest.NewError(manifest.ErrInput, nil, "remote git base requires cache")
	}
	repo := manifest.NewGitRepository(
		sourcev1.GitRepository{
			Spec: sourcev1.GitRepositorySpec{URL: request.URL, RecurseSubmodules: request.Submodules},
		},
	)
	if _, err := f.validateRepo(repo); err != nil {
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
	var identity string
	generation, id, err := f.Cache.Git(
		ctx,
		key,
		policy,
		func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
			return f.produceRemoteGitBase(ctx, storer, worktree, &remoteBaseProducerState{
				request: request, repo: repo, pinned: pinned,
				input: &input, identity: &identity, content: content,
			})
		},
	)
	if err != nil {
		return gitWarningArtifact(input, manifest.NamedResource{}, request.URL, request.Ref), err
	}
	if input == nil {
		input, identity, err = gittree.New(ctx, generation.Backing, generation.Commit, gittree.ViewOptions{
			Content: content, Mode: gittree.RemoteBase, Worktree: generation.Worktree,
		})
	}
	if err != nil {
		return gitWarningArtifact(
				input,
				manifest.NamedResource{},
				request.URL,
				request.Ref,
			), manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("remote git base: %w", err),
				"",
			)
	}

	return &store.SourceArtifact{
		Kind:     manifest.KindGitRepository,
		URL:      request.URL,
		Revision: generation.Commit.String(),
		Key:      id,
		Content:  identity,
		FS:       input,
		Warnings: gitWarnings(input, manifest.NamedResource{}, request.URL, request.Ref),
	}, nil
}

type remoteBaseProducerState struct {
	request  RemoteBaseRequest
	repo     *manifest.GitRepository
	pinned   bool
	input    **gittree.FS
	identity *string
	content  struct {
		URL, Ref   string
		Submodules bool
	}
}

func (f *Fetcher) produceRemoteGitBase(
	ctx context.Context,
	storer storage.Storer,
	worktree billy.Filesystem,
	scope *remoteBaseProducerState,
) (plumbing.Hash, error) {
	*scope.input = nil
	// Opaque base refs require upstream rev-parse over all advertised heads and tags.
	acquisition := *scope.repo
	acquisition.RecurseSubmodules = false
	if !scope.pinned {
		acquisition.Reference = nil
	}
	cloned, err := f.acquire(
		ctx,
		storer,
		worktree, &gitAcquisition{
			spec:    &acquisition,
			options: git.CloneOptions{URL: scope.request.URL, NoCheckout: true},
			timeout: scope.request.Timeout,
		},
	)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if !scope.pinned {
		fetchContextErr := (&gitAcquisition{timeout: scope.request.Timeout}).fetchRevision(
			ctx, cloned, &git.FetchOptions{RefSpecs: []config.RefSpec{fetchAllRefs}, Tags: git.NoTags},
		)
		if fetchContextErr != nil && !errors.Is(fetchContextErr, git.NoErrAlreadyUpToDate) {
			return plumbing.ZeroHash, fmt.Errorf("fetch git revision: %w", fetchContextErr)
		}
	}
	hash, err := resolveRefHash(cloned, scope.repo.Reference, true)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if scope.request.Submodules {
		if checkoutRefErr := checkoutRef(cloned, hash, nil); checkoutRefErr != nil {
			return plumbing.ZeroHash, checkoutRefErr
		}
		updateCtx := ctx
		var cancel context.CancelFunc
		if scope.request.Timeout > 0 {
			updateCtx, cancel = context.WithTimeout(ctx, scope.request.Timeout)
		}
		updateSubmodulesErr := f.updateSubmodules(updateCtx, cloned, nil, git.DefaultSubmoduleRecursionDepth)
		if cancel != nil {
			cancel()
		}
		if updateSubmodulesErr != nil {
			return plumbing.ZeroHash, updateSubmodulesErr
		}
	}
	*scope.input, *scope.identity, err = gittree.New(
		ctx,
		storer.(*filesystem.Storage).Filesystem(), //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
		hash,
		gittree.ViewOptions{Content: scope.content, Mode: gittree.RemoteBase, Worktree: worktree},
	)
	if err != nil {
		return plumbing.ZeroHash, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("validate git view: %w", err),
			"",
		)
	}
	return hash, nil
}
