package git

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Masterminds/semver/v3"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// ResolveLocal resolves matching authored sources against root's own repository.
// A root-path artifact preserves working-tree edits; a committed artifact's
// FS supplies its bytes and LocalPath labels ownership. Missing entries require
// normal fetching. Invalid input, unusable objects and view-construction failures
// return domain errors.
func ResolveLocal(ctx context.Context, root string, repositories []*manifest.GitRepository) (map[manifest.NamedResource]*store.SourceArtifact, error) {
	if len(repositories) == 0 {
		return nil, nil
	}
	var repo *git.Repository
	var head plumbing.Hash
	opened := false
	type resolution struct {
		hash plumbing.Hash
		err  error
	}
	resolved := make(map[manifest.GitRepositoryRef]resolution)
	artifacts := make(map[manifest.NamedResource]*store.SourceArtifact, len(repositories))
	for _, repository := range repositories {
		if repository == nil {
			return nil, fmt.Errorf("%w: local GitRepository is nil", manifest.ErrInput)
		}
		ref := effectiveRef(repository.Reference)
		if manifest.GitRefString(ref) == "" {
			continue
		}
		if err := validateEffectiveRef(ref); err != nil {
			return nil, localError(repository, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, localError(repository, err)
		}
		if !opened {
			opened = true
			if _, err := os.Lstat(filepath.Join(root, ".git")); err != nil {
				if !os.IsNotExist(err) {
					return nil, localError(repository, err)
				}
			} else {
				var err error
				repo, err = git.PlainOpenWithOptions(root, &git.PlainOpenOptions{EnableDotGitCommonDir: true})
				if err != nil {
					return nil, localError(repository, err)
				}
				headRef, err := repo.Head()
				if err != nil {
					return nil, localError(repository, err)
				}
				head = headRef.Hash()
				if _, err := readCommit(repo, head); err != nil {
					return nil, localError(repository, err)
				}
			}
		}
		if repo == nil {
			continue
		}
		r, ok := resolved[ref]
		if !ok {
			r.hash, r.err = resolveRefHash(repo, &ref)
			resolved[ref] = r
		}
		if errors.Is(r.err, errRefUnavailable) {
			continue
		}
		if r.err != nil {
			return nil, localError(repository, r.err)
		}
		if r.hash == head {
			artifacts[repository.Named()] = &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: repository.URL, LocalPath: root, Revision: r.hash.String()}
			continue
		}
		if repository.RecurseSubmodules {
			if _, err := readCommit(repo, r.hash); err != nil {
				return nil, localError(repository, err)
			}
			continue
		}
		identity, err := filepath.EvalSymlinks(root)
		if err != nil {
			return nil, localError(repository, err)
		}
		identity, err = filepath.Abs(identity)
		if err != nil {
			return nil, localError(repository, err)
		}
		ignore := ""
		if repository.Ignore != nil {
			ignore = *repository.Ignore
		}
		input, err := gittree.New(ctx, repo.Storer.(*filesystem.Storage).Filesystem(), r.hash, gittree.ViewOptions{Sparse: repository.SparseCheckout, Ignore: ignore})
		if err != nil {
			return nil, localError(repository, err)
		}
		key, err := source.CacheKeyHash([]string{identity, r.hash.String(), gitCacheKey(repository, r.hash.String(), sourceignore.RulesVersion)}, 32)
		if err != nil {
			return nil, localError(repository, err)
		}
		artifact := &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: repository.URL, LocalPath: filepath.Join(filepath.Dir(identity), ".flate-sources", key), Revision: r.hash.String(), Key: "local-git-" + key, FS: input}
		artifacts[repository.Named()] = artifact
	}
	return artifacts, nil
}

func effectiveRef(ref *manifest.GitRepositoryRef) manifest.GitRepositoryRef {
	if ref == nil {
		return manifest.GitRepositoryRef{}
	}
	switch {
	case ref.Commit != "":
		return manifest.GitRepositoryRef{Commit: ref.Commit, Branch: ref.Branch}
	case ref.Name != "":
		return manifest.GitRepositoryRef{Name: ref.Name}
	case ref.SemVer != "":
		return manifest.GitRepositoryRef{SemVer: ref.SemVer}
	case ref.Tag != "":
		return manifest.GitRepositoryRef{Tag: ref.Tag}
	default:
		return manifest.GitRepositoryRef{Branch: ref.Branch}
	}
}

func validateEffectiveRef(ref manifest.GitRepositoryRef) error {
	if ref.Commit != "" && !plumbing.IsHash(ref.Commit) {
		return fmt.Errorf("invalid full commit hash %q", ref.Commit)
	}
	var name plumbing.ReferenceName
	switch {
	case ref.Name != "":
		name = plumbing.ReferenceName(ref.Name)
	case ref.SemVer != "":
		_, err := semver.NewConstraint(ref.SemVer)
		return err
	case ref.Tag != "":
		name = plumbing.NewTagReferenceName(ref.Tag)
	case ref.Branch != "":
		name = plumbing.NewBranchReferenceName(ref.Branch)
	default:
		return nil
	}
	if err := name.Validate(); err != nil {
		return fmt.Errorf("invalid reference %q: %w", name, err)
	}
	return nil
}

func localError(repo *manifest.GitRepository, err error) error {
	return fmt.Errorf("%w: resolve local %s: %w", manifest.ErrInput, gitID(repo), err)
}
