package git

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// ResolveLocal resolves authored refs from an admitted location's own repository.
// Only matching mutable HEAD inputs may expose working-tree edits; committed
// artifacts retain their own object storage and use LocalPath solely for ownership.
// On error, returned artifacts contain only warnings and no usable content.
func ResolveLocal(
	ctx context.Context,
	location *gitrepo.Location,
	repositories []*manifest.GitRepository,
	sideRevision string,
) (map[manifest.NamedResource]*store.SourceArtifact, error) {
	if len(repositories) == 0 {
		return nil, nil //nolint:nilnil // An empty inventory has no locally resolved artifacts.
	}
	r := localResolver{
		location:     location,
		sideRevision: sideRevision,
		resolved:     make(map[sourcev1.GitRepositoryRef]localResolution),
	}
	if location != nil {
		r.root = location.ContentRoot
	}
	artifacts := make(map[manifest.NamedResource]*store.SourceArtifact, len(repositories))
	for _, repository := range repositories {
		artifact, err := r.resolve(ctx, repository)
		if artifact != nil {
			artifacts[repository.Named()] = artifact
		}
		if err != nil {
			return failedLocal(artifacts), err
		}
	}
	return artifacts, nil
}

type localResolution struct {
	hash plumbing.Hash
	err  error
}

type localResolver struct {
	location     *gitrepo.Location
	sideRevision string
	root         string
	repo         *git.Repository
	head         plumbing.Hash
	local        fs.FS
	opened       bool
	resolved     map[sourcev1.GitRepositoryRef]localResolution
}

func (r *localResolver) resolve(
	ctx context.Context,
	repository *manifest.GitRepository,
) (*store.SourceArtifact, error) {
	if repository == nil {
		return nil, manifest.NewError(manifest.ErrInput, nil, "local GitRepository is nil")
	}
	selected := (*manifest.GitRepositoryRef)(repository.Reference)
	ref := selected.Normalized()
	if reference := selected.Effective(); reference.Kind == "" {
		return nil, nil //nolint:nilnil // Repositories without an authored reference do not select local content.
	}
	if err := selected.Validate(); err != nil {
		return nil, localError(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, localError(err)
	}
	if !r.opened {
		r.opened = true
		if err := r.open(); err != nil {
			return nil, localError(err)
		}
	}
	if r.repo == nil {
		return nil, nil //nolint:nilnil // An unavailable repository leaves remote acquisition responsible for this source.
	}
	resolution, ok := r.resolved[ref]
	if !ok {
		resolution.hash, resolution.err = resolveRefHash(r.repo, &ref, false)
		r.resolved[ref] = resolution
	}
	if errors.Is(resolution.err, errRefUnavailable) {
		return nil, nil //nolint:nilnil // An unavailable local ref leaves remote acquisition responsible for this source.
	}
	if resolution.err != nil {
		return nil, localError(resolution.err)
	}
	if resolution.hash == r.head && (r.sideRevision == "" || r.sideRevision == resolution.hash.String()) {
		return r.directory(ctx, repository, resolution.hash)
	}
	if repository.RecurseSubmodules {
		if _, err := readCommit(r.repo, resolution.hash); err != nil {
			return nil, localError(err)
		}
		return nil, nil //nolint:nilnil // Non-HEAD submodule sources require remote acquisition.
	}
	return r.committed(ctx, repository, resolution.hash, selected)
}

func (r *localResolver) open() error {
	if r.location != nil {
		if r.location.LookupErr != nil {
			return r.location.LookupErr
		}
		if r.location.RepositoryRoot == r.root {
			r.repo = r.location.Repository
		}
	}
	if r.repo == nil {
		return nil
	}
	headRef, err := r.repo.Head()
	if err != nil {
		return fmt.Errorf("read local git HEAD: %w", err)
	}
	r.head = headRef.Hash()
	_, err = readCommit(r.repo, r.head)
	return err
}

func (r *localResolver) directory(
	ctx context.Context,
	repository *manifest.GitRepository,
	hash plumbing.Hash,
) (*store.SourceArtifact, error) {
	if r.local == nil {
		input, err := gittree.AdmitDirectory(ctx, r.location)
		if err != nil {
			return nil, localError(err)
		}
		r.local = input.FS
	}
	return &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: repository.URL, LocalPath: r.root, Revision: hash.String(),
		FS: r.local, ContentFingerprint: gittree.ContentFingerprint,
	}, nil
}

func (r *localResolver) committed(
	ctx context.Context, repository *manifest.GitRepository, hash plumbing.Hash, selected *manifest.GitRepositoryRef,
) (*store.SourceArtifact, error) {
	ignore := ""
	if repository.Ignore != nil {
		ignore = *repository.Ignore
	}
	input, key, err := gittree.New(
		ctx,
		r.repo.Storer.(*filesystem.Storage).Filesystem(), //nolint:errcheck // The admitted repository uses the filesystem storage selected by discovery.
		hash,
		gittree.ViewOptions{
			Location: r.location,
			Content:  struct{ Root string }{r.root},
			Sparse:   repository.SparseCheckout,
			Ignore:   ignore,
		},
	)
	var warnings []manifest.Warning
	if input != nil && len(input.Warnings) > 0 {
		warnings = gitWarnings(input, repository.Named(), repository.URL, selected.String())
	}
	if err != nil {
		return &store.SourceArtifact{Warnings: warnings}, localError(err)
	}
	label, _ := gittree.Label(gittree.LabelOptions{Root: r.root, Mode: gittree.Source, Key: key})
	return &store.SourceArtifact{
		Kind:               manifest.KindGitRepository,
		URL:                repository.URL,
		LocalPath:          label,
		Revision:           hash.String(),
		Key:                key,
		Content:            key,
		FS:                 input,
		ContentFingerprint: gittree.ContentFingerprint,
		Warnings:           warnings,
	}, nil
}

func failedLocal(
	artifacts map[manifest.NamedResource]*store.SourceArtifact,
) map[manifest.NamedResource]*store.SourceArtifact {
	for id, artifact := range artifacts {
		if len(artifact.Warnings) == 0 {
			delete(artifacts, id)
		} else {
			artifacts[id] = &store.SourceArtifact{Warnings: artifact.Warnings}
		}
	}
	if len(artifacts) == 0 {
		return nil
	}
	return artifacts
}

func localError(err error) error {
	return manifest.NewError(manifest.ErrInput, fmt.Errorf("resolve local git: %w", err), "")
}
