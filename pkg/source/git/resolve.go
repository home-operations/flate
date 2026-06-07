package git

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
)

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
			hash := plumbing.NewHash(ref.Commit)
			if err := validateCommitBranch(repo, hash, ref.Branch); err != nil {
				return plumbing.ZeroHash, err
			}
			return hash, nil
		case ref.Name != "":
			h, err := repo.ResolveRevision(plumbing.Revision(ref.Name))
			if err == nil {
				return *h, nil
			}
			return plumbing.ZeroHash, fmt.Errorf("ref %q not found in mirror", ref.Name)
		case ref.SemVer != "":
			return resolveSemver(repo, ref.SemVer)
		case ref.Tag != "":
			if h, ok := lookupTag(repo, ref.Tag); ok {
				return h, nil
			}
			return plumbing.ZeroHash, fmt.Errorf("tag %q not found in mirror", ref.Tag)
		case ref.Branch != "":
			if h, ok := lookupBranch(repo, ref.Branch); ok {
				return h, nil
			}
			return plumbing.ZeroHash, fmt.Errorf("branch %q not found in mirror", ref.Branch)
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
	branchHash, ok := lookupBranch(repo, branch)
	if !ok {
		return fmt.Errorf("branch %q not found for commit %s", branch, commit)
	}
	commitObj, err := repo.CommitObject(commit)
	if err != nil {
		return fmt.Errorf("commit %s not found: %w", commit, err)
	}
	branchObj, err := repo.CommitObject(branchHash)
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

func lookupTag(repo *git.Repository, name string) (plumbing.Hash, bool) {
	tag, err := repo.Tag(name)
	if err != nil {
		return plumbing.ZeroHash, false
	}
	// Annotated tags point at a tag object whose Target is the commit.
	if obj, oerr := repo.TagObject(tag.Hash()); oerr == nil {
		return obj.Target, true
	}
	return tag.Hash(), true
}

func lookupBranch(repo *git.Repository, name string) (plumbing.Hash, bool) {
	for _, refName := range []plumbing.ReferenceName{
		plumbing.NewBranchReferenceName(name),
		plumbing.NewRemoteReferenceName("origin", name),
	} {
		r, err := repo.Reference(refName, true)
		if err == nil {
			return r.Hash(), true
		}
	}
	return plumbing.ZeroHash, false
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
	if err := tags.ForEach(func(ref *plumbing.Reference) error {
		name := ref.Name().Short()
		v, verr := semver.NewVersion(name)
		if verr != nil {
			return nil
		}
		if !constraint.Check(v) {
			return nil
		}
		if best == nil || v.GreaterThan(best) {
			best = v
			if h, ok := lookupTag(repo, name); ok {
				bestHash = h
			}
		}
		return nil
	}); err != nil {
		return plumbing.ZeroHash, err
	}
	if best == nil {
		return plumbing.ZeroHash, fmt.Errorf("no tag satisfies semver %q", expr)
	}
	return bestHash, nil
}
