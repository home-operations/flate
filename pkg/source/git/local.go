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

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// ResolveLocal resolves matching authored sources against root's own repository.
// A root-path artifact preserves working-tree edits; a distinct path contains
// the committed tree. Missing entries require normal fetching. Invalid input,
// unusable objects and materialization failures return domain errors.
func ResolveLocal(ctx context.Context, root string, repositories []*manifest.GitRepository, cache *source.Cache) (map[manifest.NamedResource]*store.SourceArtifact, error) {
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
			if r.err == nil {
				_, r.err = readCommit(repo, r.hash)
			}
			resolved[ref] = r
		}
		if errors.Is(r.err, errRefUnavailable) {
			continue
		}
		if r.err != nil {
			return nil, localError(repository, r.err)
		}
		if r.hash == head {
			artifacts[repository.Named()] = gitArtifact(repository.URL, root, r.hash.String())
			continue
		}
		if repository.RecurseSubmodules || len(repository.SparseCheckout) != 0 {
			continue
		}
		if cache == nil {
			cache = source.NewCache(cacheroot.New(cacheroot.Default()))
		}
		artifact, err := materializeLocal(ctx, repo, cache, localMaterialization{root, r.hash, repository})
		if err != nil {
			return nil, localError(repository, err)
		}
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

type localMaterialization struct {
	root       string
	hash       plumbing.Hash
	repository *manifest.GitRepository
}

func materializeLocal(ctx context.Context, repo *git.Repository, cache *source.Cache, local localMaterialization) (*store.SourceArtifact, error) {
	identity, err := filepath.EvalSymlinks(local.root)
	if err != nil {
		return nil, err
	}
	identity, err = filepath.Abs(identity)
	if err != nil {
		return nil, err
	}
	revision := local.hash.String()
	slot, err := cache.Slot(ctx, "local-tree://"+identity, gitCacheKey(local.repository, revision), "")
	if err != nil {
		return nil, err
	}
	defer slot.Release()
	if slot.Exists {
		if readCachedRevision(slot.Path) == revision {
			return gitArtifact(local.repository.URL, slot.Path, revision), nil
		}
		if err := slot.Refresh(); err != nil {
			return nil, err
		}
	}
	if err := gittree.Materialize(ctx, repo, local.hash, slot.Path, gittree.Options{}); err != nil {
		return nil, err
	}
	artifact := gitArtifact(local.repository.URL, slot.Path, revision)
	if err := applyIgnoreAndMark(local.repository, artifact); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return commitArtifact(local.repository, slot, artifact)
}

func localError(repo *manifest.GitRepository, err error) error {
	return fmt.Errorf("%w: resolve local %s: %w", manifest.ErrInput, gitID(repo), err)
}
