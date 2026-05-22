package manifest

import (
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

// GitRepositoryRef defines the ref used for pull and checkout.
type GitRepositoryRef struct {
	Branch string `json:"branch,omitempty" yaml:"branch,omitempty"`
	Tag    string `json:"tag,omitempty" yaml:"tag,omitempty"`
	Semver string `json:"semver,omitempty" yaml:"semver,omitempty"`
	Commit string `json:"commit,omitempty" yaml:"commit,omitempty"`
}

// RefString returns "branch:main", "tag:v1.2.3", etc., or empty when the
// ref is empty. Precedence: commit > tag > branch > semver.
func (r GitRepositoryRef) RefString() string {
	switch {
	case r.Commit != "":
		return "commit:" + r.Commit
	case r.Tag != "":
		return "tag:" + r.Tag
	case r.Branch != "":
		return "branch:" + r.Branch
	case r.Semver != "":
		return "semver:" + r.Semver
	}
	return ""
}

// IsEmpty reports whether the ref selects no specific commit.
func (r GitRepositoryRef) IsEmpty() bool { return r == GitRepositoryRef{} }

// GitRepository is the Flux GitRepository CRD.
type GitRepository struct {
	Name      string           `json:"name" yaml:"name"`
	Namespace string           `json:"namespace" yaml:"namespace"`
	URL       string           `json:"url" yaml:"url"`
	Ref       GitRepositoryRef `json:"ref,omitzero" yaml:"ref,omitempty"`
	Suspend   bool             `json:"-" yaml:"-"`
}

// Named identifies the GitRepository.
func (g *GitRepository) Named() NamedResource {
	return NamedResource{Kind: KindGitRepository, Namespace: g.Namespace, Name: g.Name}
}

// RepoName is "<namespace>-<name>".
func (g *GitRepository) RepoName() string { return g.Namespace + "-" + g.Name }

// ParseGitRepository decodes a GitRepository CR via the Flux typed
// schema (source-controller/api/v1), then projects the fields flate
// uses into the local struct.
func ParseGitRepository(doc map[string]any) (*GitRepository, error) {
	if err := checkAPIVersion(doc, SourceDomain); err != nil {
		return nil, err
	}
	var cr sourcev1.GitRepository
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("GitRepository decode: %v", err)
	}
	if cr.Name == "" {
		return nil, inputf("GitRepository missing metadata.name")
	}
	if cr.Spec.URL == "" {
		return nil, inputf("GitRepository missing spec.url")
	}
	var ref GitRepositoryRef
	if r := cr.Spec.Reference; r != nil {
		ref = GitRepositoryRef{
			Branch: r.Branch,
			Tag:    r.Tag,
			Semver: r.SemVer,
			Commit: r.Commit,
		}
	}
	return &GitRepository{
		Name:      cr.Name,
		Namespace: cr.Namespace,
		URL:       cr.Spec.URL,
		Ref:       ref,
		Suspend:   cr.Spec.Suspend,
	}, nil
}

// OCIRepositoryRef points at a specific OCI artifact version.
type OCIRepositoryRef struct {
	Digest       string `json:"digest,omitempty" yaml:"digest,omitempty"`
	Tag          string `json:"tag,omitempty" yaml:"tag,omitempty"`
	Semver       string `json:"semver,omitempty" yaml:"semver,omitempty"`
	SemverFilter string `json:"semverFilter,omitempty" yaml:"semverFilter,omitempty"`
}

// IsEmpty reports whether the ref is empty.
func (r OCIRepositoryRef) IsEmpty() bool { return r == OCIRepositoryRef{} }

// OCIRepository is the Flux OCIRepository CRD.
type OCIRepository struct {
	Name      string                `json:"name" yaml:"name"`
	Namespace string                `json:"namespace" yaml:"namespace"`
	URL       string                `json:"url" yaml:"url"`
	Ref       OCIRepositoryRef      `json:"ref,omitzero" yaml:"ref,omitempty"`
	SecretRef *LocalObjectReference `json:"secretRef,omitempty" yaml:"secretRef,omitempty"`
	Suspend   bool                  `json:"-" yaml:"-"`
}

// Named identifies the OCIRepository.
func (o *OCIRepository) Named() NamedResource {
	return NamedResource{Kind: KindOCIRepository, Namespace: o.Namespace, Name: o.Name}
}

// RepoName is "<namespace>-<name>".
func (o *OCIRepository) RepoName() string { return o.Namespace + "-" + o.Name }

// Version returns the digest, tag, or semver expression in that order.
// A semver expression is returned verbatim — callers wanting a concrete
// tag must resolve it against remote tag listing (pkg/source).
func (o *OCIRepository) Version() (string, error) {
	if o.Ref.IsEmpty() {
		return "", nil
	}
	switch {
	case o.Ref.Digest != "":
		return o.Ref.Digest, nil
	case o.Ref.Tag != "":
		return o.Ref.Tag, nil
	case o.Ref.Semver != "":
		return o.Ref.Semver, nil
	}
	return "", nil
}

// VersionedURL appends the version with the correct separator: "@" for
// digests, ":" for tags and semver.
func (o *OCIRepository) VersionedURL() string {
	if o.Ref.IsEmpty() {
		return o.URL
	}
	switch {
	case o.Ref.Digest != "":
		return o.URL + "@" + o.Ref.Digest
	case o.Ref.Tag != "":
		return o.URL + ":" + o.Ref.Tag
	case o.Ref.Semver != "":
		return o.URL + ":" + o.Ref.Semver
	}
	return o.URL
}

// ParseOCIRepository decodes an OCIRepository CR.
func ParseOCIRepository(doc map[string]any) (*OCIRepository, error) {
	if err := checkAPIVersion(doc, SourceDomain); err != nil {
		return nil, err
	}
	var cr sourcev1.OCIRepository
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("OCIRepository decode: %v", err)
	}
	if cr.Name == "" {
		return nil, inputf("OCIRepository missing metadata.name")
	}
	if cr.Spec.URL == "" {
		return nil, inputf("OCIRepository missing spec.url")
	}
	out := &OCIRepository{
		Name:      cr.Name,
		Namespace: cr.Namespace,
		URL:       cr.Spec.URL,
		Suspend:   cr.Spec.Suspend,
	}
	if r := cr.Spec.Reference; r != nil {
		out.Ref = OCIRepositoryRef{
			Digest:       r.Digest,
			Tag:          r.Tag,
			Semver:       r.SemVer,
			SemverFilter: r.SemverFilter,
		}
	}
	if cr.Spec.SecretRef != nil && cr.Spec.SecretRef.Name != "" {
		out.SecretRef = &LocalObjectReference{Name: cr.Spec.SecretRef.Name}
	}
	return out, nil
}
