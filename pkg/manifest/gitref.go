package manifest

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
)

// GitRepositoryRef exposes the selected Flux Git reference without changing its schema.
type GitRepositoryRef sourcev1.GitRepositoryRef

// Effective returns the selected reference category and value, or empty strings for HEAD.
func (r *GitRepositoryRef) Effective() (string, string) {
	if r != nil {
		switch {
		case r.Commit != "":
			return "commit", r.Commit
		case r.Name != "":
			return "name", r.Name
		case r.SemVer != "":
			return "semver", r.SemVer
		case r.Tag != "":
			return "tag", r.Tag
		case r.Branch != "":
			return "branch", r.Branch
		}
	}
	return "", ""
}

// Normalized returns only the selected reference and any retained commit branch.
func (r *GitRepositoryRef) Normalized() sourcev1.GitRepositoryRef {
	kind, value := r.Effective()
	ref := sourcev1.GitRepositoryRef{}
	switch kind {
	case "commit":
		ref.Commit, ref.Branch = value, r.Branch
	case "name":
		ref.Name = value
	case "semver":
		ref.SemVer = value
	case "tag":
		ref.Tag = value
	case "branch":
		ref.Branch = value
	}
	return ref
}

// String returns the selected kind:value diagnostic, or empty for HEAD.
func (r *GitRepositoryRef) String() string {
	kind, value := r.Effective()
	if kind == "" {
		return ""
	}
	return kind + ":" + value
}

// Label returns the cache label including branch context for a selected commit.
func (r *GitRepositoryRef) Label() string {
	kind, value := r.Effective()
	if kind == "commit" && r.Branch != "" {
		return "branch:" + r.Branch + "@commit:" + value
	}
	return r.String()
}

// RefSpec returns a narrow fetch refspec, or empty when full/default acquisition is needed.
func (r *GitRepositoryRef) RefSpec() config.RefSpec {
	kind, value := r.Effective()
	var name string
	switch kind {
	case "name":
		name = value
	case "semver":
		name = "refs/tags/*"
	case "tag":
		name = plumbing.NewTagReferenceName(value).String()
	case "branch":
		name = plumbing.NewBranchReferenceName(value).String()
	default:
		return ""
	}
	return config.RefSpec("+" + name + ":" + name)
}

// Validate checks selected reference syntax and a commit's retained branch context.
func (r *GitRepositoryRef) Validate() error {
	kind, value := r.Effective()
	var name plumbing.ReferenceName
	switch kind {
	case "commit":
		if !plumbing.IsHash(value) {
			return fmt.Errorf("invalid full commit hash %q", value)
		}
		if r.Branch == "" {
			return nil
		}
		name = plumbing.NewBranchReferenceName(r.Branch)
	case "name":
		name = plumbing.ReferenceName(value)
	case "semver":
		_, err := semver.NewConstraint(value)
		return err
	case "tag":
		name = plumbing.NewTagReferenceName(value)
	case "branch":
		name = plumbing.NewBranchReferenceName(value)
	default:
		return nil
	}
	if err := name.Validate(); err != nil {
		return fmt.Errorf("invalid reference %q: %w", name, err)
	}
	return nil
}

// Pinned reports the conservative commit-only cache retention policy.
func (r *GitRepositoryRef) Pinned() bool {
	kind, _ := r.Effective()
	return kind == "commit" && r.Branch == "" && r.Tag == "" && r.SemVer == "" && r.Name == ""
}
