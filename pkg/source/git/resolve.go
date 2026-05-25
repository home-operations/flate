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
//   - SemVer picks the highest tag satisfying the constraint
//   - Tag resolves by name (annotated or lightweight)
//   - Branch resolves by name
//   - empty/missing → HEAD (the mirror's default branch)
//
// Returns a wrapped error if no match exists; the caller surfaces it
// to the user with the originating CR's identity.
func resolveRefHash(repo *git.Repository, ref *manifest.GitRepositoryRef) (plumbing.Hash, error) {
	if ref == nil {
		return resolveHEAD(repo)
	}
	switch {
	case ref.Commit != "":
		return plumbing.NewHash(ref.Commit), nil
	case ref.SemVer != "":
		return resolveSemver(repo, ref.SemVer)
	case ref.Tag != "":
		return resolveTagOrBranch(repo, ref.Tag, true)
	case ref.Branch != "":
		return resolveTagOrBranch(repo, ref.Branch, false)
	}
	return resolveHEAD(repo)
}

func resolveHEAD(repo *git.Repository) (plumbing.Hash, error) {
	head, err := repo.Head()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("resolve HEAD: %w", err)
	}
	return head.Hash(), nil
}

// resolveTagOrBranch tries tag-first then branch-first depending on
// preferTag. Annotated tags are dereferenced via TagObject when
// present; lightweight tags resolve directly.
func resolveTagOrBranch(repo *git.Repository, name string, preferTag bool) (plumbing.Hash, error) {
	if preferTag {
		if h, ok := lookupTag(repo, name); ok {
			return h, nil
		}
		if h, ok := lookupBranch(repo, name); ok {
			return h, nil
		}
		return plumbing.ZeroHash, fmt.Errorf("tag %q not found in mirror", name)
	}
	if h, ok := lookupBranch(repo, name); ok {
		return h, nil
	}
	if h, ok := lookupTag(repo, name); ok {
		return h, nil
	}
	return plumbing.ZeroHash, fmt.Errorf("branch %q not found in mirror", name)
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
	r, err := repo.Reference(plumbing.NewBranchReferenceName(name), true)
	if err != nil {
		return plumbing.ZeroHash, false
	}
	return r.Hash(), true
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
