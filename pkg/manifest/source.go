package manifest

import (
	"slices"

	meta "github.com/fluxcd/pkg/apis/meta"
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


// GitRepository is the Flux GitRepository CRD.
type GitRepository struct {
	Name              string                `json:"name" yaml:"name"`
	Namespace         string                `json:"namespace" yaml:"namespace"`
	URL               string                `json:"url" yaml:"url"`
	Ref               GitRepositoryRef      `json:"ref,omitzero" yaml:"ref,omitempty"`
	Provider          string                `json:"provider,omitempty" yaml:"provider,omitempty"`
	SecretRef         *LocalObjectReference `json:"secretRef,omitempty" yaml:"secretRef,omitempty"`
	ProxySecretRef    *LocalObjectReference `json:"proxySecretRef,omitempty" yaml:"proxySecretRef,omitempty"`
	Verify            *GitRepositoryVerify  `json:"verify,omitempty" yaml:"verify,omitempty"`
	RecurseSubmodules bool                  `json:"recurseSubmodules,omitempty" yaml:"recurseSubmodules,omitempty"`
	// SparseCheckout limits the checkout to the listed repo-relative
	// directories. When empty, the full tree is checked out (default).
	SparseCheckout []string `json:"sparseCheckout,omitempty" yaml:"sparseCheckout,omitempty"`
	Suspend        bool     `json:"-" yaml:"-"`
}

// GitRepositoryVerify mirrors source-controller's GitRepositoryVerification.
// The Secret named by SecretRef holds one or more PEM-armored PGP
// public keys (typically "*.asc"); flate concatenates them into a
// single keyring and verifies the resolved commit/tag signatures
// against it.
type GitRepositoryVerify struct {
	// Mode is "HEAD", "Tag", or "TagAndHEAD". The legacy "head"
	// alias normalizes to "HEAD" during parse.
	Mode      string                `json:"mode,omitempty" yaml:"mode,omitempty"`
	SecretRef *LocalObjectReference `json:"secretRef,omitempty" yaml:"secretRef,omitempty"`
}

// Git verification modes — sourced from sourcev1.GitVerificationMode so
// flate's bare-string Mode field interops with the typed upstream API.
var (
	GitVerifyModeHEAD       = string(sourcev1.ModeGitHEAD)
	GitVerifyModeTag        = string(sourcev1.ModeGitTag)
	GitVerifyModeTagAndHEAD = string(sourcev1.ModeGitTagAndHEAD)
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
		ref = *r
	}
	provider := cr.Spec.Provider
	if provider == "" {
		provider = sourcev1.GitProviderGeneric
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
		out.SecretRef = cr.Spec.SecretRef
	}
	if cr.Spec.ProxySecretRef != nil && cr.Spec.ProxySecretRef.Name != "" {
		out.ProxySecretRef = cr.Spec.ProxySecretRef
	}
	if v := cr.Spec.Verification; v != nil {
		mode := string(v.GetMode())
		if mode == "head" { // legacy alias
			mode = GitVerifyModeHEAD
		}
		out.Verify = &GitRepositoryVerify{Mode: mode}
		if v.SecretRef.Name != "" {
			ref := v.SecretRef
			out.Verify.SecretRef = &ref
		}
	}
	return out, nil
}

// OCIRepositoryRef is the Flux OCIRepositoryRef from source-controller.
type OCIRepositoryRef = sourcev1.OCIRepositoryRef

// OCIRefIsEmpty reports whether the ref is empty.
func OCIRefIsEmpty(r OCIRepositoryRef) bool { return r == OCIRepositoryRef{} }

// OCIRepository is the Flux OCIRepository CRD.
type OCIRepository struct {
	Name           string                `json:"name" yaml:"name"`
	Namespace      string                `json:"namespace" yaml:"namespace"`
	URL            string                `json:"url" yaml:"url"`
	Ref            OCIRepositoryRef      `json:"ref,omitzero" yaml:"ref,omitempty"`
	Provider       string                `json:"provider,omitempty" yaml:"provider,omitempty"`
	SecretRef      *LocalObjectReference `json:"secretRef,omitempty" yaml:"secretRef,omitempty"`
	CertSecretRef  *LocalObjectReference `json:"certSecretRef,omitempty" yaml:"certSecretRef,omitempty"`
	ProxySecretRef *LocalObjectReference `json:"proxySecretRef,omitempty" yaml:"proxySecretRef,omitempty"`
	Verify         *OCIRepositoryVerify  `json:"verify,omitempty" yaml:"verify,omitempty"`
	LayerSelector  *OCILayerSelector     `json:"layerSelector,omitempty" yaml:"layerSelector,omitempty"`
	Insecure       bool                  `json:"insecure,omitempty" yaml:"insecure,omitempty"`
	Suspend        bool                  `json:"-" yaml:"-"`
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
// (OIDC) and notation parse for round-trip fidelity but are not
// enforced at Fetch time.
type OCIRepositoryVerify = sourcev1.OCIRepositoryVerification

// OIDCIdentityMatch is the keyless-mode identity matcher. Parsed for
// fidelity; flate does not perform keyless verification.
type OIDCIdentityMatch = sourcev1.OIDCIdentityMatch

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
	if OCIRefIsEmpty(o.Ref) {
		return "", nil
	}
	switch {
	case o.Ref.Digest != "":
		return o.Ref.Digest, nil
	case o.Ref.Tag != "":
		return o.Ref.Tag, nil
	case o.Ref.SemVer != "":
		return o.Ref.SemVer, nil
	}
	return "", nil
}

// VersionedURL appends the version with the correct separator: "@" for
// digests, ":" for tags and semver.
func (o *OCIRepository) VersionedURL() string {
	if OCIRefIsEmpty(o.Ref) {
		return o.URL
	}
	switch {
	case o.Ref.Digest != "":
		return o.URL + "@" + o.Ref.Digest
	case o.Ref.Tag != "":
		return o.URL + ":" + o.Ref.Tag
	case o.Ref.SemVer != "":
		return o.URL + ":" + o.Ref.SemVer
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
		provider = sourcev1.GenericOCIProvider
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
		out.CertSecretRef = cr.Spec.CertSecretRef
	}
	if r := cr.Spec.Reference; r != nil {
		out.Ref = *r
	}
	if cr.Spec.SecretRef != nil && cr.Spec.SecretRef.Name != "" {
		out.SecretRef = cr.Spec.SecretRef
	}
	if cr.Spec.ProxySecretRef != nil && cr.Spec.ProxySecretRef.Name != "" {
		out.ProxySecretRef = cr.Spec.ProxySecretRef
	}
	if v := cr.Spec.Verify; v != nil {
		clone := *v
		clone.MatchOIDCIdentity = slices.Clone(v.MatchOIDCIdentity)
		out.Verify = &clone
	}
	if ls := cr.Spec.LayerSelector; ls != nil {
		clone := *ls
		out.LayerSelector = &clone
	}
	return out, nil
}

// ExternalArtifactSourceRef identifies the upstream CR that produces
// the artifact — Flux's meta.NamespacedObjectKindReference.
type ExternalArtifactSourceRef = meta.NamespacedObjectKindReference

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
		ref := *cr.Spec.SourceRef
		out.SourceRef = &ref
	}
	if a := cr.Status.Artifact; a != nil {
		out.ArtifactURL = a.URL
		out.Revision = a.Revision
		out.Digest = a.Digest
	}
	return out, nil
}
