package manifest

import (
	"cmp"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

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
	return NamedResource{Group: kindTable[kindGitRepository].Group, Kind: KindGitRepository, Namespace: g.Namespace, Name: g.Name}
}

// BootstrapAlias returns a local copy and the borrowed immutable Git descriptor.
// A non-nil descriptor selects repository-root policy and supplies ref/view inputs.
func (g *GitRepository) BootstrapAlias(root string) (BaseManifest, *GitRepository) {
	out := *g
	out.URL = "file://" + root
	return &out, g
}

// Content returns the source inputs that define the selected Git content.
func (g *GitRepository) Content(ref sourcev1.GitRepositoryRef) any {
	var auth []NamedResource
	g.references(func(ref Reference) bool {
		if ref.Content && ref.Name != "" {
			auth = append(auth, ref.NamedResource)
		}
		return true
	})
	ignore := ""
	if g.Ignore != nil {
		ignore = *g.Ignore
	}
	return struct {
		URL               string
		Ref               sourcev1.GitRepositoryRef
		Auth              []NamedResource
		Ignore            string
		SparseCheckout    []string `json:",omitempty"`
		RecurseSubmodules bool
	}{g.URL, ref, auth, ignore, g.SparseCheckout, g.RecurseSubmodules}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (g *GitRepository) WithNamespace(namespace string) BaseManifest {
	out := *g
	out.Namespace = namespace
	return &out
}

// Suspended reports whether reconciliation is paused on this resource.
func (g *GitRepository) Suspended() bool { return g.Suspend }

// ExistenceOnly reports whether readiness requires no artifact acquisition.
func (g *GitRepository) ExistenceOnly() bool { return false }

// SourceInterval returns the authored source refresh interval.
func (g *GitRepository) SourceInterval() time.Duration { return g.Interval.Duration }

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

// OCIRepositoryRef holds the Flux OCI reference and its cache-retention fact.
type OCIRepositoryRef sourcev1.OCIRepositoryRef

// Pinned reports whether a digest fixes the selected content.
func (r *OCIRepositoryRef) Pinned() bool { return r != nil && r.Digest != "" }

// OCIRepository is the Flux OCIRepository CRD. The embedded
// sourcev1.OCIRepositorySpec promotes URL, Reference, LayerSelector,
// Provider, SecretRef, CertSecretRef, ProxySecretRef, Verify, Insecure,
// Suspend etc. to the top level.
type OCIRepository struct {
	Name      string `json:"name"      yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`

	sourcev1.OCIRepositorySpec `json:",inline" yaml:",inline"`
}

// LayerOperation returns the Flux layer operation, defaulting to extraction.
func (o *OCIRepository) LayerOperation() string {
	return (&sourcev1.OCIRepository{Spec: o.OCIRepositorySpec}).GetLayerOperation()
}

// LayerMediaType returns the authored layer media type selector.
func (o *OCIRepository) LayerMediaType() string {
	return (&sourcev1.OCIRepository{Spec: o.OCIRepositorySpec}).GetLayerMediaType()
}

// OCILayerSelector is the Flux OCILayerSelector from source-controller.
// "extract" (default) unpacks the layer's tarball into the artifact
// directory; "copy" persists the compressed blob verbatim under the
// filename "layer.tar.gz".
type OCILayerSelector = sourcev1.OCILayerSelector

// Named identifies the OCIRepository.
func (o *OCIRepository) Named() NamedResource {
	return NamedResource{Group: kindTable[kindOCIRepository].Group, Kind: KindOCIRepository, Namespace: o.Namespace, Name: o.Name}
}

// BootstrapAlias returns a local copy; a nil Git descriptor selects a derived frozen root.
func (o *OCIRepository) BootstrapAlias(root string) (BaseManifest, *GitRepository) {
	out := *o
	out.URL = "file://" + root
	return &out, nil
}

// Content returns the source inputs that define the selected OCI content.
func (o *OCIRepository) Content(ref sourcev1.OCIRepositoryRef) any {
	var auth []NamedResource
	o.references(func(ref Reference) bool {
		if ref.Content && ref.Name != "" {
			auth = append(auth, ref.NamedResource)
		}
		return true
	})
	ignore := ""
	if o.Ignore != nil {
		ignore = *o.Ignore
	}
	return struct {
		URL           string
		Ref           sourcev1.OCIRepositoryRef
		Auth          []NamedResource
		Insecure      bool
		LayerSelector *OCILayerSelector
		Ignore        string
	}{o.URL, ref, auth, o.Insecure, o.LayerSelector, ignore}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (o *OCIRepository) WithNamespace(namespace string) BaseManifest {
	out := *o
	out.Namespace = namespace
	return &out
}

// Suspended reports whether reconciliation is paused on this resource.
func (o *OCIRepository) Suspended() bool { return o.Suspend }

// ExistenceOnly reports whether readiness requires no artifact acquisition.
func (o *OCIRepository) ExistenceOnly() bool { return false }

// SourceInterval returns the authored source refresh interval.
func (o *OCIRepository) SourceInterval() time.Duration { return o.Interval.Duration }

// ChartVersion returns the digest, semver constraint or tag declared by the source.
func (o *OCIRepository) ChartVersion() string {
	if o.Reference == nil {
		return ""
	}
	return cmp.Or(o.Reference.Digest, o.Reference.SemVer, o.Reference.Tag)
}

// parseOCIRepository decodes an OCIRepository CR.
func parseOCIRepository(doc map[string]any) (*OCIRepository, error) {
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

// ExternalArtifact is a source whose content is published by its materializer.
type ExternalArtifact struct {
	Name      string `json:"name"                yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`

	// ExternalArtifactSpec embeds the upstream spec, exposing
	// SourceRef at the top level.
	sourcev1.ExternalArtifactSpec `json:",inline" yaml:",inline"`
}

// Named identifies the ExternalArtifact.
func (e *ExternalArtifact) Named() NamedResource {
	return NamedResource{Group: kindTable[kindExternalArtifact].Group, Kind: KindExternalArtifact, Namespace: e.Namespace, Name: e.Name}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (e *ExternalArtifact) WithNamespace(namespace string) BaseManifest {
	out := *e
	out.Namespace = namespace
	return &out
}

// Suspended reports whether reconciliation is paused on this resource.
func (e *ExternalArtifact) Suspended() bool { return false }

// ExistenceOnly reports whether readiness requires no artifact acquisition.
func (e *ExternalArtifact) ExistenceOnly() bool { return false }

// SourceInterval returns the authored source refresh interval.
func (e *ExternalArtifact) SourceInterval() time.Duration { return 0 }

func parseExternalArtifact(doc map[string]any) (*ExternalArtifact, error) {
	var cr sourcev1.ExternalArtifact
	if err := decodeCR(doc, &cr, "ExternalArtifact", SourceDomain); err != nil {
		return nil, err
	}
	return &ExternalArtifact{
		Name:                 cr.Name,
		Namespace:            cr.Namespace,
		ExternalArtifactSpec: cr.Spec,
	}, nil
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

func (e *ExternalArtifact) references(yield func(Reference) bool) {
	if e.SourceRef == nil {
		return
	}
	ref := e.SourceRef
	yield(Reference{NamedResource: Resolve(NamedResource{Group: referenceGroup(ref.Kind, ref.APIVersion), Kind: ref.Kind, Name: ref.Name, Namespace: ref.Namespace}, "", e.Namespace), Use: "materializer", Gate: GateInputs})
}
