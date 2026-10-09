package git

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/pkg/manifest"
)

var errRefUnavailable = errors.New("local git reference unavailable")

// resolveRefHash translates a Flux GitRepositoryRef into a concrete
// commit hash within repo (a bare mirror). Ordering matches upstream
// source-controller:
//
//   - explicit Commit always wins
//   - Name resolves a full git ref like refs/heads/main or refs/pull/1/head
//   - SemVer picks the highest tag satisfying the constraint
//   - Tag resolves by name (annotated or lightweight)
//   - Branch resolves by name
//   - empty/missing → HEAD (the mirror's default branch)
//
// Returns a wrapped error if no match exists; the caller surfaces it
// to the user with the originating CR's identity.
func resolveRefHash(repo *git.Repository, ref *manifest.GitRepositoryRef) (plumbing.Hash, error) {
	if ref != nil {
		switch {
		case ref.Commit != "":
			if !plumbing.IsHash(ref.Commit) {
				return plumbing.ZeroHash, fmt.Errorf("%w: invalid full commit hash %q", manifest.ErrInput, ref.Commit)
			}
			hash := plumbing.NewHash(ref.Commit)
			if _, err := readCommit(repo, hash); err != nil {
				if errors.Is(err, plumbing.ErrObjectNotFound) {
					if _, objectErr := repo.Object(plumbing.AnyObject, hash); errors.Is(objectErr, plumbing.ErrObjectNotFound) {
						return plumbing.ZeroHash, fmt.Errorf("%w: commit %s", errRefUnavailable, hash)
					}
				}
				return plumbing.ZeroHash, err
			}
			if err := validateCommitBranch(repo, hash, ref.Branch); err != nil {
				return plumbing.ZeroHash, err
			}
			return hash, nil
		case ref.Name != "":
			if strings.HasPrefix(ref.Name, "refs/") || ref.Name == "HEAD" {
				return lookupNamedRef(repo, ref.Name)
			}
			h, err := repo.ResolveRevision(plumbing.Revision(ref.Name))
			if err != nil {
				return plumbing.ZeroHash, fmt.Errorf("resolve revision %q: %w", ref.Name, err)
			}
			return *h, nil
		case ref.SemVer != "":
			return resolveSemver(repo, ref.SemVer)
		case ref.Tag != "":
			return lookupTag(repo, ref.Tag)
		case ref.Branch != "":
			return lookupBranch(repo, ref.Branch)
		}
	}
	head, err := repo.Head()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("resolve HEAD: %w", err)
	}
	return head.Hash(), nil
}

func validateCommitBranch(repo *git.Repository, commit plumbing.Hash, branch string) error {
	if branch == "" {
		return nil
	}
	branchHash, err := lookupBranch(repo, branch)
	if err != nil {
		return fmt.Errorf("branch %q for commit %s: %w", branch, commit, err)
	}
	commitObj, err := readCommit(repo, commit)
	if err != nil {
		if errors.Is(err, plumbing.ErrObjectNotFound) {
			if _, objectErr := repo.Object(plumbing.AnyObject, commit); errors.Is(objectErr, plumbing.ErrObjectNotFound) {
				return fmt.Errorf("%w: commit %s", errRefUnavailable, commit)
			}
		}
		return fmt.Errorf("commit %s not found: %w", commit, err)
	}
	branchObj, err := readCommit(repo, branchHash)
	if err != nil {
		return fmt.Errorf("branch %q target %s is not a commit: %w", branch, branchHash, err)
	}
	reachable, err := commitObj.IsAncestor(branchObj)
	if err != nil {
		return fmt.Errorf("check commit %s reachability from branch %q: %w", commit, branch, err)
	}
	if !reachable {
		return fmt.Errorf("commit %s is not reachable from branch %q", commit, branch)
	}
	return nil
}

func lookupTag(repo *git.Repository, name string) (plumbing.Hash, error) {
	return lookupReference(repo, plumbing.NewTagReferenceName(name))
}

func lookupBranch(repo *git.Repository, name string) (plumbing.Hash, error) {
	return lookupReference(repo, plumbing.NewBranchReferenceName(name), plumbing.NewRemoteReferenceName("origin", name))
}

func lookupNamedRef(repo *git.Repository, name string) (plumbing.Hash, error) {
	if branch, ok := strings.CutPrefix(name, "refs/heads/"); ok {
		return lookupReference(repo, plumbing.ReferenceName(name), plumbing.NewRemoteReferenceName("origin", branch))
	}
	return lookupReference(repo, plumbing.ReferenceName(name))
}

func lookupReference(repo *git.Repository, names ...plumbing.ReferenceName) (plumbing.Hash, error) {
	for _, name := range names {
		ref, err := repo.Reference(name, false)
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			continue
		}
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("read reference %q: %w", name, err)
		}
		if ref.Type() == plumbing.SymbolicReference {
			ref, err = repo.Reference(name, true)
			if err != nil {
				return plumbing.ZeroHash, fmt.Errorf("resolve reference %q: %w", name, err)
			}
		}
		return peelCommit(repo, ref.Hash())
	}
	return plumbing.ZeroHash, fmt.Errorf("%w: %s", errRefUnavailable, names[0])
}

func peelCommit(repo *git.Repository, hash plumbing.Hash) (plumbing.Hash, error) {
	for {
		obj, err := repo.Object(plumbing.AnyObject, hash)
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("read reference target %s: %w", hash, err)
		}
		switch obj := obj.(type) {
		case *object.Commit:
			return hash, nil
		case *object.Tag:
			hash = obj.Target
		default:
			return plumbing.ZeroHash, fmt.Errorf("reference target %s is not a commit or tag", hash)
		}
	}
}

func readCommit(repo *git.Repository, hash plumbing.Hash) (*object.Commit, error) {
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("read commit %s: %w", hash, err)
	}
	if _, err := commit.Tree(); err != nil {
		return nil, fmt.Errorf("read tree for commit %s: %w", hash, err)
	}
	return commit, nil
}

// resolveSemver picks the highest tag in repo satisfying expr.
func resolveSemver(repo *git.Repository, expr string) (plumbing.Hash, error) {
	constraint, err := semver.NewConstraint(expr)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("parse semver %q: %w", expr, err)
	}
	tags, err := repo.Tags()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("list tags: %w", err)
	}
	var best *semver.Version
	var bestHash plumbing.Hash
	var bestName string
	if err := tags.ForEach(func(ref *plumbing.Reference) error {
		name := ref.Name().Short()
		v, verr := semver.NewVersion(name)
		if verr != nil {
			return nil
		}
		if !constraint.Check(v) {
			return nil
		}
		if best == nil || v.GreaterThan(best) || (v.Equal(best) && name < bestName) {
			best, bestHash, bestName = v, ref.Hash(), name
		}
		return nil
	}); err != nil {
		return plumbing.ZeroHash, err
	}
	if best == nil {
		return plumbing.ZeroHash, fmt.Errorf("%w: no tag satisfies semver %q", errRefUnavailable, expr)
	}
	return peelCommit(repo, bestHash)
}
