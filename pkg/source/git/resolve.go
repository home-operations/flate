package git

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/pkg/manifest"
)

var errRefUnavailable = errors.New("local git reference unavailable")

func resolveRefHash(repo *git.Repository, ref *sourcev1.GitRepositoryRef, originFirst bool) (plumbing.Hash, error) {
	selected := (*manifest.GitRepositoryRef)(ref)
	kind, value := selected.Effective()
	switch kind {
	case "commit":
		if err := selected.Validate(); err != nil {
			return plumbing.ZeroHash, fmt.Errorf("%w: %w", manifest.ErrInput, err)
		}
		hash := plumbing.NewHash(value)
		commit, err := readCommit(repo, hash)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if err := validateCommitBranch(repo, commit, ref.Branch); err != nil {
			return plumbing.ZeroHash, err
		}
		return hash, nil
	case "name":
		if strings.HasPrefix(value, "refs/") || value == "HEAD" {
			return lookupNamedRef(repo, value)
		}
		resolved, err := repo.ResolveRevision(plumbing.Revision(value))
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("resolve revision %q: %w", value, err)
		}
		return *resolved, nil
	case "semver":
		return resolveSemver(repo, value)
	case "tag":
		return lookupReference(repo, plumbing.NewTagReferenceName(value))
	case "branch":
		return lookupBranch(repo, value, originFirst)
	}
	head, err := repo.Head()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("resolve HEAD: %w", err)
	}
	return head.Hash(), nil
}

func validateCommitBranch(repo *git.Repository, commit *object.Commit, branch string) error {
	if branch == "" {
		return nil
	}
	branchHash, err := lookupBranch(repo, branch, false)
	if err != nil {
		return fmt.Errorf("branch %q for commit %s: %w", branch, commit.Hash, err)
	}
	branchObj, err := readCommit(repo, branchHash)
	if err != nil {
		return fmt.Errorf("branch %q target %s is not a commit: %w", branch, branchHash, err)
	}
	reachable, err := commit.IsAncestor(branchObj)
	if err != nil {
		if errors.Is(err, plumbing.ErrObjectNotFound) {
			return fmt.Errorf("%w: incomplete history for commit %s from branch %q", errRefUnavailable, commit.Hash, branch)
		}
		return fmt.Errorf("check commit %s reachability from branch %q: %w", commit.Hash, branch, err)
	}
	if !reachable {
		return fmt.Errorf("commit %s is not reachable from branch %q", commit.Hash, branch)
	}
	return nil
}

func lookupBranch(repo *git.Repository, name string, originFirst bool) (plumbing.Hash, error) {
	if originFirst {
		return lookupReference(repo, plumbing.NewRemoteReferenceName("origin", name), plumbing.NewBranchReferenceName(name))
	}
	return lookupReference(repo, plumbing.NewBranchReferenceName(name), plumbing.NewRemoteReferenceName("origin", name))
}

func lookupNamedRef(repo *git.Repository, name string) (plumbing.Hash, error) {
	if branch, ok := strings.CutPrefix(name, "refs/heads/"); ok {
		return lookupBranch(repo, branch, false)
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
		if errors.Is(err, plumbing.ErrObjectNotFound) {
			if _, objectErr := repo.Object(plumbing.AnyObject, hash); errors.Is(objectErr, plumbing.ErrObjectNotFound) {
				return nil, fmt.Errorf("%w: commit %s", errRefUnavailable, hash)
			}
		}
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
