package manifest

import (
	"cmp"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

// GitRepositoryRef is the Flux GitRepositoryRef from source-controller.
type GitRepositoryRef = sourcev1.GitRepositoryRef

// GitRefString returns "branch:main", "tag:v1.2.3", etc., or empty when
// the ref is empty. Precedence matches Flux source-controller:
// commit > name > semver > tag > branch.
func GitRefString(r GitRepositoryRef) string {
	switch {
	case r.Commit != "":
		return "commit:" + r.Commit
	case r.Name != "":
		return "name:" + r.Name
	case r.SemVer != "":
		return "semver:" + r.SemVer
	case r.Tag != "":
		return "tag:" + r.Tag
	case r.Branch != "":
		return "branch:" + r.Branch
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

// RepoName delegates to the canonical NamedResource helper so all
// three source kinds (Git, OCI, Helm) produce identical strings on
// the same id.
func (g *GitRepository) RepoName() string { return g.Named().FluxResourceName() }

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

// secretRefCheck pairs a spec.field path with a pointer to the
// LocalObjectReference at that path. Used in slice form with
// validateOptionalRefs to batch every Secret-ref validation at the
// edge of a parser.
type secretRefCheck struct {
	Field string
	Ref   *LocalObjectReference
}

// validateOptionalRefs runs validateSecretRefName for each non-nil
// ref in checks. Caller hands the parser's full list of pointer-to-
// LocalObjectReference fields in one shot so adding a new ref to a
// source kind's schema is a one-line edit rather than 3-5 new
// boilerplate blocks. Value-type refs (e.g. Verification.SecretRef,
// nested inside a *Verification) stay inline since they don't
// share the nil-pointer-guard shape.
func validateOptionalRefs(kind, owner string, checks ...secretRefCheck) error {
	for _, c := range checks {
		if c.Ref == nil {
			continue
		}
		if err := validateSecretRefName(kind, owner, c.Field, c.Ref.Name); err != nil {
			return err
		}
	}
	return nil
}

// parseGitRepository decodes a GitRepository CR via the Flux typed
// schema (source-controller/api/v1), then projects the fields flate
// uses into the local struct.
func parseGitRepository(doc map[string]any) (*GitRepository, error) {
	var cr sourcev1.GitRepository
	if err := decodeCR(doc, &cr, "GitRepository", SourceDomain); err != nil {
		return nil, err
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
	if err := validateOptionalRefs("GitRepository", owner,
		secretRefCheck{Field: "spec.secretRef", Ref: cr.Spec.SecretRef},
		secretRefCheck{Field: "spec.proxySecretRef", Ref: cr.Spec.ProxySecretRef},
	); err != nil {
		return nil, err
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
// source-controller. Flate enforces cosign verification — keyed
// (secretRef → trusted public keys) and keyless (matchOIDCIdentity →
// Fulcio/Rekor via sigstore-go, see pkg/source/oci/keyless.go). Notation
// is unenforced (hard error if requested).
type OCIRepositoryVerify = sourcev1.OCIRepositoryVerification

// OIDCIdentityMatch is the Flux keyless identity matcher (issuer + subject
// regex pair) carried in OCIRepositoryVerify.MatchOIDCIdentity.
type OIDCIdentityMatch = sourcev1.OIDCIdentityMatch

// Named identifies the OCIRepository.
func (o *OCIRepository) Named() NamedResource {
	return NamedResource{Kind: KindOCIRepository, Namespace: o.Namespace, Name: o.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (o *OCIRepository) Suspended() bool { return o.Suspend }

// RepoName delegates to the canonical NamedResource helper.
func (o *OCIRepository) RepoName() string { return o.Named().FluxResourceName() }

// Version returns the digest, semver expression, or tag in that order.
// A semver expression is returned verbatim — callers wanting a concrete
// tag must resolve it against remote tag listing (pkg/source).
func (o *OCIRepository) Version() string {
	if o.Reference == nil {
		return ""
	}
	return cmp.Or(o.Reference.Digest, o.Reference.SemVer, o.Reference.Tag)
}

// ParseOCIRepository decodes an OCIRepository CR.
func ParseOCIRepository(doc map[string]any) (*OCIRepository, error) {
	var cr sourcev1.OCIRepository
	if err := decodeCR(doc, &cr, "OCIRepository", SourceDomain); err != nil {
		return nil, err
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
	if err := validateOptionalRefs("OCIRepository", owner,
		secretRefCheck{Field: "spec.secretRef", Ref: cr.Spec.SecretRef},
		secretRefCheck{Field: "spec.certSecretRef", Ref: cr.Spec.CertSecretRef},
		secretRefCheck{Field: "spec.proxySecretRef", Ref: cr.Spec.ProxySecretRef},
	); err != nil {
		return nil, err
	}
	if v := cr.Spec.Verify; v != nil {
		if err := validateOptionalRefs("OCIRepository", owner,
			secretRefCheck{Field: "spec.verify.secretRef", Ref: v.SecretRef},
		); err != nil {
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
	var cr sourcev1.ExternalArtifact
	if err := decodeCR(doc, &cr, "ExternalArtifact", SourceDomain); err != nil {
		return nil, err
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
