package manifest

import (
	"cmp"

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

// validateSecretRefName rejects a Secret/LocalObjectReference whose
// Name is empty. Without this check, a typo'd YAML like
// `secretRef: {}` would fall through to a runtime lookup of
// "<ns>/" — and with --allow-missing-secrets enabled, would silently
// soft-skip the source. That converts a schema typo into invisible
// behavior change. Reject upfront with a clear pointer to the field.
//
// kind/owner/field are formatted into the error: e.g. ("OCIRepository",
// "default/private-app", "spec.secretRef").
func validateSecretRefName(kind, owner, field, name string) error {
	if name == "" {
		return inputf("%s %s: %s.name is empty", kind, owner, field)
	}
	return nil
}

// parseGitRepository decodes a GitRepository CR via the Flux typed
// schema (source-controller/api/v1), then projects the fields flate
// uses into the local struct.
func parseGitRepository(doc map[string]any) (*GitRepository, error) {
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
	cr.Spec.Provider = cmp.Or(cr.Spec.Provider, sourcev1.GitProviderGeneric)
	cr.Spec.URL = ResolveEnvsubstDefaults(cr.Spec.URL)
	if r := cr.Spec.Reference; r != nil {
		r.Branch = ResolveEnvsubstDefaults(r.Branch)
		r.Tag = ResolveEnvsubstDefaults(r.Tag)
		r.SemVer = ResolveEnvsubstDefaults(r.SemVer)
		r.Commit = ResolveEnvsubstDefaults(r.Commit)
		r.Name = ResolveEnvsubstDefaults(r.Name)
	}
	if v := cr.Spec.Verification; v != nil {
		// GetMode normalizes the legacy "head" alias to "HEAD" and
		// applies the schema default. Write it back so consumers see a
		// canonical mode regardless of source casing.
		v.Mode = v.GetMode()
	}
	owner := cr.Namespace + "/" + cr.Name
	if r := cr.Spec.SecretRef; r != nil {
		if err := validateSecretRefName("GitRepository", owner, "spec.secretRef", r.Name); err != nil {
			return nil, err
		}
	}
	if r := cr.Spec.ProxySecretRef; r != nil {
		if err := validateSecretRefName("GitRepository", owner, "spec.proxySecretRef", r.Name); err != nil {
			return nil, err
		}
	}
	if v := cr.Spec.Verification; v != nil {
		if err := validateSecretRefName("GitRepository", owner, "spec.verify.secretRef", v.SecretRef.Name); err != nil {
			return nil, err
		}
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
	cr.Spec.Provider = cmp.Or(cr.Spec.Provider, sourcev1.GenericOCIProvider)
	// Pre-resolve envsubst defaults on ref fields so a chart pin like
	// `tag: "${FLUXCD_VERSION:=v2.8.5}"` becomes "v2.8.5" before the
	// fetcher tries to resolve it. Bare ${VAR} (no default) is left
	// for postBuild substitution.
	cr.Spec.URL = ResolveEnvsubstDefaults(cr.Spec.URL)
	if r := cr.Spec.Reference; r != nil {
		r.Tag = ResolveEnvsubstDefaults(r.Tag)
		r.SemVer = ResolveEnvsubstDefaults(r.SemVer)
		r.Digest = ResolveEnvsubstDefaults(r.Digest)
	}
	owner := cr.Namespace + "/" + cr.Name
	if r := cr.Spec.SecretRef; r != nil {
		if err := validateSecretRefName("OCIRepository", owner, "spec.secretRef", r.Name); err != nil {
			return nil, err
		}
	}
	if r := cr.Spec.CertSecretRef; r != nil {
		if err := validateSecretRefName("OCIRepository", owner, "spec.certSecretRef", r.Name); err != nil {
			return nil, err
		}
	}
	if r := cr.Spec.ProxySecretRef; r != nil {
		if err := validateSecretRefName("OCIRepository", owner, "spec.proxySecretRef", r.Name); err != nil {
			return nil, err
		}
	}
	if v := cr.Spec.Verify; v != nil && v.SecretRef != nil {
		if err := validateSecretRefName("OCIRepository", owner, "spec.verify.secretRef", v.SecretRef.Name); err != nil {
			return nil, err
		}
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

// parseExternalArtifact decodes an ExternalArtifact CR via the
// source-controller typed schema.
func parseExternalArtifact(doc map[string]any) (*ExternalArtifact, error) {
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
