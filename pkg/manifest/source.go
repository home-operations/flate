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
	// Name is the full Git reference (e.g. "refs/pull/420/head" or
	// "refs/tags/v0.1.0"). When set, takes precedence over
	// Branch/Tag/SemVer and any Commit specified — flate resolves the
	// remote ref to a commit before checkout.
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
}

// RefString returns "branch:main", "tag:v1.2.3", etc., or empty when
// the ref is empty. Precedence (matches Flux source-controller):
// name > commit > tag > branch > semver.
func (r GitRepositoryRef) RefString() string {
	switch {
	case r.Name != "":
		return "name:" + r.Name
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
	Name              string                `json:"name" yaml:"name"`
	Namespace         string                `json:"namespace" yaml:"namespace"`
	URL               string                `json:"url" yaml:"url"`
	Ref               GitRepositoryRef      `json:"ref,omitzero" yaml:"ref,omitempty"`
	Provider          string                `json:"provider,omitempty" yaml:"provider,omitempty"`
	SecretRef         *LocalObjectReference `json:"secretRef,omitempty" yaml:"secretRef,omitempty"`
	RecurseSubmodules bool                  `json:"recurseSubmodules,omitempty" yaml:"recurseSubmodules,omitempty"`
	// SparseCheckout limits the checkout to the listed repo-relative
	// directories. When empty, the full tree is checked out (default).
	SparseCheckout []string `json:"sparseCheckout,omitempty" yaml:"sparseCheckout,omitempty"`
	Suspend        bool     `json:"-" yaml:"-"`
}

// Named identifies the GitRepository.
func (g *GitRepository) Named() NamedResource {
	return NamedResource{Kind: KindGitRepository, Namespace: g.Namespace, Name: g.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (g *GitRepository) Suspended() bool { return g.Suspend }

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
			Name:   r.Name,
		}
	}
	provider := cr.Spec.Provider
	if provider == "" {
		provider = GitProviderGeneric
	}
	out := &GitRepository{
		Name:              cr.Name,
		Namespace:         cr.Namespace,
		URL:               cr.Spec.URL,
		Ref:               ref,
		Provider:          provider,
		RecurseSubmodules: cr.Spec.RecurseSubmodules,
		SparseCheckout:    cr.Spec.SparseCheckout,
		Suspend:           cr.Spec.Suspend,
	}
	if cr.Spec.SecretRef != nil && cr.Spec.SecretRef.Name != "" {
		out.SecretRef = &LocalObjectReference{Name: cr.Spec.SecretRef.Name}
	}
	return out, nil
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
	Name          string                `json:"name" yaml:"name"`
	Namespace     string                `json:"namespace" yaml:"namespace"`
	URL           string                `json:"url" yaml:"url"`
	Ref           OCIRepositoryRef      `json:"ref,omitzero" yaml:"ref,omitempty"`
	Provider      string                `json:"provider,omitempty" yaml:"provider,omitempty"`
	SecretRef     *LocalObjectReference `json:"secretRef,omitempty" yaml:"secretRef,omitempty"`
	CertSecretRef *LocalObjectReference `json:"certSecretRef,omitempty" yaml:"certSecretRef,omitempty"`
	Verify        *OCIRepositoryVerify  `json:"verify,omitempty" yaml:"verify,omitempty"`
	LayerSelector *OCILayerSelector     `json:"layerSelector,omitempty" yaml:"layerSelector,omitempty"`
	Insecure      bool                  `json:"insecure,omitempty" yaml:"insecure,omitempty"`
	Suspend       bool                  `json:"-" yaml:"-"`
}

// OCILayerSelector mirrors source-controller's spec.layerSelector.
// When set, the fetcher selects the first layer matching MediaType
// and processes it per Operation:
//   - "extract" (default): the layer's tarball is unpacked into the
//     artifact directory.
//   - "copy": the layer's compressed blob is persisted verbatim,
//     under the filename "layer.tar.gz".
type OCILayerSelector struct {
	MediaType string `json:"mediaType,omitempty" yaml:"mediaType,omitempty"`
	Operation string `json:"operation,omitempty" yaml:"operation,omitempty"`
}

// OCILayerOperationExtract and OCILayerOperationCopy are the two
// values source-controller accepts on LayerSelector.Operation.
const (
	OCILayerOperationExtract = "extract"
	OCILayerOperationCopy    = "copy"
)

// OCIRepositoryVerify mirrors source-controller's spec.verify on
// OCIRepository. Flate implements keyed mode only:
//   - Provider: "cosign" (the upstream default)
//   - SecretRef: a Secret whose keys hold one or more PEM-encoded
//     public keys (cosign.pub or any *.pub key).
//
// The "notation" provider and keyless (OIDC) flows parse here for
// round-trip fidelity but are not enforced at Fetch time. MatchOIDCIdentity
// is preserved verbatim for the same reason.
type OCIRepositoryVerify struct {
	Provider          string                `json:"provider,omitempty" yaml:"provider,omitempty"`
	SecretRef         *LocalObjectReference `json:"secretRef,omitempty" yaml:"secretRef,omitempty"`
	MatchOIDCIdentity []OIDCIdentityMatch   `json:"matchOIDCIdentity,omitempty" yaml:"matchOIDCIdentity,omitempty"`
}

// OIDCIdentityMatch is the keyless-mode identity matcher. Parsed for
// fidelity; flate does not perform keyless verification.
type OIDCIdentityMatch struct {
	Issuer  string `json:"issuer" yaml:"issuer"`
	Subject string `json:"subject" yaml:"subject"`
}

// Named identifies the OCIRepository.
func (o *OCIRepository) Named() NamedResource {
	return NamedResource{Kind: KindOCIRepository, Namespace: o.Namespace, Name: o.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (o *OCIRepository) Suspended() bool { return o.Suspend }

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
	provider := cr.Spec.Provider
	if provider == "" {
		provider = OCIProviderGeneric
	}
	out := &OCIRepository{
		Name:      cr.Name,
		Namespace: cr.Namespace,
		URL:       cr.Spec.URL,
		Provider:  provider,
		Insecure:  cr.Spec.Insecure,
		Suspend:   cr.Spec.Suspend,
	}
	if cr.Spec.CertSecretRef != nil && cr.Spec.CertSecretRef.Name != "" {
		out.CertSecretRef = &LocalObjectReference{Name: cr.Spec.CertSecretRef.Name}
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
	if v := cr.Spec.Verify; v != nil {
		out.Verify = &OCIRepositoryVerify{Provider: v.Provider}
		if v.SecretRef != nil && v.SecretRef.Name != "" {
			out.Verify.SecretRef = &LocalObjectReference{Name: v.SecretRef.Name}
		}
		for _, m := range v.MatchOIDCIdentity {
			out.Verify.MatchOIDCIdentity = append(out.Verify.MatchOIDCIdentity,
				OIDCIdentityMatch{Issuer: m.Issuer, Subject: m.Subject})
		}
	}
	if ls := cr.Spec.LayerSelector; ls != nil {
		out.LayerSelector = &OCILayerSelector{
			MediaType: ls.MediaType,
			Operation: ls.Operation,
		}
	}
	return out, nil
}

// ExternalArtifactSourceRef identifies the upstream CR that produces
// the artifact. Mirrors Flux's meta.NamespacedObjectKindReference.
type ExternalArtifactSourceRef struct {
	APIVersion string `json:"apiVersion,omitempty" yaml:"apiVersion,omitempty"`
	Kind       string `json:"kind" yaml:"kind"`
	Namespace  string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name       string `json:"name" yaml:"name"`
}

// ExternalArtifact is the Flux ExternalArtifact CRD
// (source.toolkit.fluxcd.io/v1). It is the contract a third-party
// controller uses to publish an on-cluster artifact under Flux's
// source-controller schema. flate's offline reconcile cannot fetch
// from a live cluster — if a downstream Kustomization or HelmRelease
// references an ExternalArtifact via sourceRef.kind=ExternalArtifact,
// the user must supply status.artifact in the YAML (typical workflow:
// pre-bake a file:// URL) for flate to resolve a local path.
type ExternalArtifact struct {
	Name      string                     `json:"name" yaml:"name"`
	Namespace string                     `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	SourceRef *ExternalArtifactSourceRef `json:"sourceRef,omitempty" yaml:"sourceRef,omitempty"`
	// ArtifactURL / Revision / Digest come from status.artifact when
	// pre-populated in the YAML. Empty otherwise (the normal case for a
	// freshly-written CR); flate then fails to resolve any consumer.
	ArtifactURL string `json:"-" yaml:"-"`
	Revision    string `json:"-" yaml:"-"`
	Digest      string `json:"-" yaml:"-"`
}

// Named identifies the ExternalArtifact.
func (e *ExternalArtifact) Named() NamedResource {
	return NamedResource{Kind: KindExternalArtifact, Namespace: e.Namespace, Name: e.Name}
}

// Suspended is always false for ExternalArtifact — the CR has no
// spec.suspend field per Flux's schema. Defined so the source
// controller's Suspendable check is uniform.
func (e *ExternalArtifact) Suspended() bool { return false }

// ParseExternalArtifact decodes an ExternalArtifact CR via the
// source-controller typed schema.
func ParseExternalArtifact(doc map[string]any) (*ExternalArtifact, error) {
	if err := checkAPIVersion(doc, SourceDomain); err != nil {
		return nil, err
	}
	var cr sourcev1.ExternalArtifact
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("ExternalArtifact decode: %v", err)
	}
	if cr.Name == "" {
		return nil, inputf("ExternalArtifact missing metadata.name")
	}
	out := &ExternalArtifact{
		Name:      cr.Name,
		Namespace: cr.Namespace,
	}
	if cr.Spec.SourceRef != nil {
		out.SourceRef = &ExternalArtifactSourceRef{
			APIVersion: cr.Spec.SourceRef.APIVersion,
			Kind:       cr.Spec.SourceRef.Kind,
			Namespace:  cr.Spec.SourceRef.Namespace,
			Name:       cr.Spec.SourceRef.Name,
		}
	}
	if a := cr.Status.Artifact; a != nil {
		out.ArtifactURL = a.URL
		out.Revision = a.Revision
		out.Digest = a.Digest
	}
	return out, nil
}
