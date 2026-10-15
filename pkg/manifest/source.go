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

// Named identifies the GitRepository.
func (g *GitRepository) Named() NamedResource {
	return NamedResource{Kind: KindGitRepository, Namespace: g.Namespace, Name: g.Name}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (g *GitRepository) WithNamespace(namespace string) BaseManifest {
	out := *g
	out.Namespace = namespace
	return &out
}

// Suspended reports whether reconciliation is paused on this resource.
func (g *GitRepository) Suspended() bool { return g.Suspend }

// Active source credentials MUST name a Secret, even when missing secrets are allowed.
func validateSecretReference(id NamedResource, err *error) func(Reference) bool {
	return func(ref Reference) bool {
		if ref.Kind == KindSecret && ref.Name == "" {
			*err = inputf("%s %s/%s: spec.%s.name is empty", id.Kind, id.Namespace, id.Name, ref.Use)
			return false
		}
		return true
	}
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
	cr.Spec.URL = resolveEnvsubstDefaults(cr.Spec.URL)
	if r := cr.Spec.Reference; r != nil {
		r.Branch = resolveEnvsubstDefaults(r.Branch)
		r.Tag = resolveEnvsubstDefaults(r.Tag)
		r.SemVer = resolveEnvsubstDefaults(r.SemVer)
		r.Commit = resolveEnvsubstDefaults(r.Commit)
		r.Name = resolveEnvsubstDefaults(r.Name)
	}
	out := &GitRepository{
		Name:              cr.Name,
		Namespace:         cr.Namespace,
		GitRepositorySpec: cr.Spec,
	}
	var err error
	out.references(validateSecretReference(out.Named(), &err))
	if err != nil {
		return nil, err
	}
	return out, nil
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

// Named identifies the OCIRepository.
func (o *OCIRepository) Named() NamedResource {
	return NamedResource{Kind: KindOCIRepository, Namespace: o.Namespace, Name: o.Name}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (o *OCIRepository) WithNamespace(namespace string) BaseManifest {
	out := *o
	out.Namespace = namespace
	return &out
}

// Suspended reports whether reconciliation is paused on this resource.
func (o *OCIRepository) Suspended() bool { return o.Suspend }

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
	cr.Spec.URL = resolveEnvsubstDefaults(cr.Spec.URL)
	if r := cr.Spec.Reference; r != nil {
		r.Tag = resolveEnvsubstDefaults(r.Tag)
		r.SemVer = resolveEnvsubstDefaults(r.SemVer)
		r.Digest = resolveEnvsubstDefaults(r.Digest)
	}
	out := &OCIRepository{
		Name:              cr.Name,
		Namespace:         cr.Namespace,
		OCIRepositorySpec: cr.Spec,
	}
	var err error
	out.references(validateSecretReference(out.Named(), &err))
	if err != nil {
		return nil, err
	}
	return out, nil
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

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (e *ExternalArtifact) WithNamespace(namespace string) BaseManifest {
	out := *e
	out.Namespace = namespace
	return &out
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

func (g *GitRepository) references(yield func(Reference) bool) {
	credentialReferences(yield, g.Namespace, g.SecretRef, nil, g.ProxySecretRef)
}

func (o *OCIRepository) references(yield func(Reference) bool) {
	credentialReferences(yield, o.Namespace, o.SecretRef, o.CertSecretRef, o.ProxySecretRef)
}

func (g *GitRepository) kind() KindInfo { return kindTable[kindGitRepository].KindInfo }

func (o *OCIRepository) kind() KindInfo { return kindTable[kindOCIRepository].KindInfo }

func (e *ExternalArtifact) kind() KindInfo { return kindTable[kindExternalArtifact].KindInfo }
