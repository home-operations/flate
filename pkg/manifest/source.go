package manifest

import (
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

// GitRepositoryRef is the Flux GitRepositoryRef from source-controller.
type GitRepositoryRef = sourcev1.GitRepositoryRef

// GitRefString returns "branch:main", "tag:v1.2.3", etc., or empty when
// the ref is empty. Precedence (matches Flux source-controller):
// name > commit > tag > branch > semver.
func GitRefString(r GitRepositoryRef) string {
	switch {
	case r.Name != "":
		return "name:" + r.Name
	case r.Commit != "":
		return "commit:" + r.Commit
	case r.Tag != "":
		return "tag:" + r.Tag
	case r.Branch != "":
		return "branch:" + r.Branch
	case r.SemVer != "":
		return "semver:" + r.SemVer
	}
	return ""
}

// GitRepository is the Flux GitRepository CRD. The embedded
// sourcev1.GitRepositorySpec promotes URL, Reference, Verification,
// Provider, SecretRef, ProxySecretRef, RecurseSubmodules,
// SparseCheckout, Suspend etc. to the top level.
type GitRepository struct {
	Name      string `json:"name"      yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`

	sourcev1.GitRepositorySpec `json:",inline" yaml:",inline"`
}

// GitRepositoryVerify is the Flux GitRepositoryVerification type.
type GitRepositoryVerify = sourcev1.GitRepositoryVerification

// Git verification modes — typed re-exports of sourcev1.GitVerificationMode.
const (
	GitVerifyModeHEAD       = sourcev1.ModeGitHEAD
	GitVerifyModeTag        = sourcev1.ModeGitTag
	GitVerifyModeTagAndHEAD = sourcev1.ModeGitTagAndHEAD
)

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
		return nil, inputf("GitRepository decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("GitRepository missing metadata.name")
	}
	if cr.Spec.URL == "" {
		return nil, inputf("GitRepository missing spec.url")
	}
	if cr.Spec.Provider == "" {
		cr.Spec.Provider = sourcev1.GitProviderGeneric
	}
	if v := cr.Spec.Verification; v != nil {
		// GetMode normalizes the legacy "head" alias to "HEAD" and
		// applies the schema default. Write it back so consumers see a
		// canonical mode regardless of source casing.
		v.Mode = v.GetMode()
	}
	return &GitRepository{
		Name:              cr.Name,
		Namespace:         cr.Namespace,
		GitRepositorySpec: cr.Spec,
	}, nil
}

// OCIRepositoryRef is the Flux OCIRepositoryRef from source-controller.
type OCIRepositoryRef = sourcev1.OCIRepositoryRef

// OCIRepository is the Flux OCIRepository CRD. The embedded
// sourcev1.OCIRepositorySpec promotes URL, Reference, LayerSelector,
// Provider, SecretRef, CertSecretRef, ProxySecretRef, Verify, Insecure,
// Suspend etc. to the top level.
type OCIRepository struct {
	Name      string `json:"name"      yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`

	sourcev1.OCIRepositorySpec `json:",inline" yaml:",inline"`
}

// OCILayerSelector is the Flux OCILayerSelector from source-controller.
// "extract" (default) unpacks the layer's tarball into the artifact
// directory; "copy" persists the compressed blob verbatim under the
// filename "layer.tar.gz".
type OCILayerSelector = sourcev1.OCILayerSelector

// OCILayerOperationExtract and OCILayerOperationCopy are the two
// values source-controller accepts on LayerSelector.Operation.
const (
	OCILayerOperationExtract = "extract"
	OCILayerOperationCopy    = "copy"
)

// OCIRepositoryVerify is the Flux OCIRepositoryVerification from
// source-controller. Flate implements keyed cosign mode only: keyless
// (OIDC) parses for round-trip fidelity, logs a warn at Fetch time,
// and proceeds with rendering. Notation is also unenforced.
type OCIRepositoryVerify = sourcev1.OCIRepositoryVerification

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
	r := o.Reference
	if r == nil {
		return "", nil
	}
	switch {
	case r.Digest != "":
		return r.Digest, nil
	case r.Tag != "":
		return r.Tag, nil
	case r.SemVer != "":
		return r.SemVer, nil
	}
	return "", nil
}

// ParseOCIRepository decodes an OCIRepository CR.
func ParseOCIRepository(doc map[string]any) (*OCIRepository, error) {
	if err := checkAPIVersion(doc, SourceDomain); err != nil {
		return nil, err
	}
	var cr sourcev1.OCIRepository
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("OCIRepository decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("OCIRepository missing metadata.name")
	}
	if cr.Spec.URL == "" {
		return nil, inputf("OCIRepository missing spec.url")
	}
	if cr.Spec.Provider == "" {
		cr.Spec.Provider = sourcev1.GenericOCIProvider
	}
	return &OCIRepository{
		Name:              cr.Name,
		Namespace:         cr.Namespace,
		OCIRepositorySpec: cr.Spec,
	}, nil
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
	Name      string `json:"name"                yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`

	// ExternalArtifactSpec embeds the upstream spec, exposing
	// SourceRef at the top level.
	sourcev1.ExternalArtifactSpec `json:",inline" yaml:",inline"`

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
		return nil, inputf("ExternalArtifact decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("ExternalArtifact missing metadata.name")
	}
	out := &ExternalArtifact{
		Name:                 cr.Name,
		Namespace:            cr.Namespace,
		ExternalArtifactSpec: cr.Spec,
	}
	if a := cr.Status.Artifact; a != nil {
		out.ArtifactURL = a.URL
		out.Revision = a.Revision
		out.Digest = a.Digest
	}
	return out, nil
}
