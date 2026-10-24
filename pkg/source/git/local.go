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
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// ResolveLocal resolves authored refs from an admitted location's own repository.
// Only matching mutable HEAD inputs may expose working-tree edits; committed
// artifacts retain their own object storage and use LocalPath solely for ownership.
// On error, returned artifacts contain only warnings and no usable content.
func ResolveLocal(ctx context.Context, location *gitrepo.Location, repositories []*manifest.GitRepository, sideRevision string) (map[manifest.NamedResource]*store.SourceArtifact, error) {
	if len(repositories) == 0 {
		return nil, nil
	}
	var repo *git.Repository
	root := ""
	if location != nil {
		root = location.ContentRoot
	}
	var local fs.FS
	var head plumbing.Hash
	opened := false
	type resolution struct {
		hash plumbing.Hash
		err  error
	}
	resolved := make(map[sourcev1.GitRepositoryRef]resolution)
	artifacts := make(map[manifest.NamedResource]*store.SourceArtifact, len(repositories))
	for _, repository := range repositories {
		if repository == nil {
			return failedLocal(artifacts), fmt.Errorf("%w: local GitRepository is nil", manifest.ErrInput)
		}
		selected := (*manifest.GitRepositoryRef)(repository.Reference)
		ref := selected.Normalized()
		if kind, _ := selected.Effective(); kind == "" {
			continue
		}
		if err := selected.Validate(); err != nil {
			return failedLocal(artifacts), localError(repository, err)
		}
		if err := ctx.Err(); err != nil {
			return failedLocal(artifacts), localError(repository, err)
		}
		if !opened {
			opened = true
			if location != nil {
				if location.LookupErr != nil {
					return failedLocal(artifacts), localError(repository, location.LookupErr)
				}
				if location.RepositoryRoot == root {
					repo = location.Repository
				}
			}
			if repo != nil {
				headRef, err := repo.Head()
				if err != nil {
					return failedLocal(artifacts), localError(repository, err)
				}
				head = headRef.Hash()
				if _, err := readCommit(repo, head); err != nil {
					return failedLocal(artifacts), localError(repository, err)
				}
			}
		}
		if repo == nil {
			continue
		}
		r, ok := resolved[ref]
		if !ok {
			r.hash, r.err = resolveRefHash(repo, &ref, false)
			resolved[ref] = r
		}
		if errors.Is(r.err, errRefUnavailable) {
			continue
		}
		if r.err != nil {
			return failedLocal(artifacts), localError(repository, r.err)
		}
		if r.hash == head && (sideRevision == "" || sideRevision == r.hash.String()) {
			if local == nil {
				input, err := gittree.AdmitDirectory(ctx, location)
				if err != nil {
					return failedLocal(artifacts), localError(repository, err)
				}
				local = input.FS
			}
			artifacts[repository.Named()] = &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: repository.URL, LocalPath: root, Revision: r.hash.String(), FS: local, ContentFingerprint: gittree.ContentFingerprint}
			continue
		}
		if repository.RecurseSubmodules {
			if _, err := readCommit(repo, r.hash); err != nil {
				return failedLocal(artifacts), localError(repository, err)
			}
			continue
		}
		ignore := ""
		if repository.Ignore != nil {
			ignore = *repository.Ignore
		}
		input, key, err := gittree.New(ctx, repo.Storer.(*filesystem.Storage).Filesystem(), r.hash, gittree.ViewOptions{Location: location, Content: struct{ Root string }{root}, Sparse: repository.SparseCheckout, Ignore: ignore})
		var warnings []manifest.Warning
		if input != nil && len(input.Warnings) > 0 {
			warnings = gitWarnings(input, repository.Named(), repository.URL, selected.String())
		}
		if err != nil {
			artifacts[repository.Named()] = &store.SourceArtifact{Warnings: warnings}
			return failedLocal(artifacts), localError(repository, err)
		}
		label, _ := gittree.Label(gittree.LabelOptions{Root: root, Mode: gittree.Source, Key: key})
		artifact := &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: repository.URL, LocalPath: label, Revision: r.hash.String(), Key: key, FS: input, ContentFingerprint: gittree.ContentFingerprint, Warnings: warnings}
		artifacts[repository.Named()] = artifact
	}
	return artifacts, nil
}

func failedLocal(artifacts map[manifest.NamedResource]*store.SourceArtifact) map[manifest.NamedResource]*store.SourceArtifact {
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

func localError(repo *manifest.GitRepository, err error) error {
	return fmt.Errorf("%w: resolve local %s: %w", manifest.ErrInput, source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name), err)
}
