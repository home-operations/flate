package manifest

import (
	"cmp"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GitRepository is the Flux GitRepository CRD. The embedded
// sourcev1.GitRepositorySpec promotes URL, Reference, Verification,
// Provider, SecretRef, ProxySecretRef, RecurseSubmodules,
// SparseCheckout, Suspend etc. to the top level.
type GitRepository struct {
	Name        string            `json:"name"      yaml:"name"`
	Namespace   string            `json:"namespace" yaml:"namespace"`
	Labels      map[string]string `json:"-" yaml:"-"`
	Annotations map[string]string `json:"-" yaml:"-"`

	sourcev1.GitRepositorySpec `json:",inline" yaml:",inline"`
}

// NewGitRepository normalizes an owned typed GitRepository declaration.
func NewGitRepository(cr sourcev1.GitRepository) *GitRepository {
	cr.Spec.Provider = cmp.Or(cr.Spec.Provider, sourcev1.GitProviderGeneric)
	return &GitRepository{Name: cr.Name, Namespace: cr.Namespace, Labels: cr.Labels, Annotations: cr.Annotations, GitRepositorySpec: cr.Spec}
}

// GetLabels returns the GitRepository's metadata.labels.
func (g *GitRepository) GetLabels() map[string]string { return g.Labels }

// GetAnnotations returns the GitRepository's metadata.annotations.
func (g *GitRepository) GetAnnotations() map[string]string { return g.Annotations }

// Named identifies the GitRepository.
func (g *GitRepository) Named() NamedResource {
	return NamedResource{Group: kindTable[kindGitRepository].Group, Kind: KindGitRepository, Namespace: g.Namespace, Name: g.Name}
}

// BootstrapAlias returns a local copy.
func (g *GitRepository) BootstrapAlias(root string) BaseManifest {
	spec := g.GitRepositorySpec
	spec.URL = "file://" + root
	return NewGitRepository(sourcev1.GitRepository{Name: g.Name, Namespace: g.Namespace, Labels: g.Labels, Annotations: g.Annotations, Spec: spec})
}

// BootstrapFacts describes a source's local coordinates, revision and filtering.
type BootstrapFacts struct {
	URL         string
	DerivedRoot bool
	RefKind     string
	Ref         string
	Ignore      string
	Sparse      []string
	Recurse     bool
}

// BootstrapFacts returns the borrowed immutable Git view inputs.
func (g *GitRepository) BootstrapFacts() BootstrapFacts {
	selected := (*GitRepositoryRef)(g.Reference)
	kind, _ := selected.Effective()
	if selected.Validate() != nil {
		kind = "name"
	}
	facts := BootstrapFacts{URL: g.URL, RefKind: kind, Ref: (*GitRepositoryRef)(g.Reference).String(), Sparse: g.SparseCheckout, Recurse: g.RecurseSubmodules}
	if g.Ignore != nil {
		facts.Ignore = *g.Ignore
	}
	return facts
}

// BootstrapInput returns the authored inputs that shape a local Git alias.
func (g *GitRepository) BootstrapInput() any {
	var ref sourcev1.GitRepositoryRef
	if g.Reference != nil {
		ref = *g.Reference
	}
	return g.Content(ref)
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

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (g *GitRepository) ProjectionCompatible(BaseManifest) bool { return true }

// Suspended reports whether reconciliation is paused on this resource.
func (g *GitRepository) Suspended() bool { return g.Suspend }

// ExistenceOnly reports whether readiness requires no artifact acquisition.
func (g *GitRepository) ExistenceOnly() bool { return false }

// SourceInterval returns the authored source refresh interval.
func (g *GitRepository) SourceInterval() time.Duration { return g.Interval.Duration }

// OperationTimeout returns the authored operation timeout or the Flux default.
func (g *GitRepository) OperationTimeout() time.Duration { return operationTimeout(g.Timeout) }

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

func parseGitRepository(doc map[string]any) (*GitRepository, error) {
	var cr sourcev1.GitRepository
	if err := decodeCR(doc, &cr, "GitRepository", SourceDomain); err != nil {
		return nil, err
	}
	if cr.Spec.URL == "" {
		return nil, inputf("GitRepository missing spec.url")
	}
	out := NewGitRepository(cr)
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
	diagnosticOwner NamedResource
	diagnosticPath  string
	hasDiagnostic   bool
	Name            string            `json:"name"      yaml:"name"`
	Namespace       string            `json:"namespace" yaml:"namespace"`
	Labels          map[string]string `json:"-" yaml:"-"`
	Annotations     map[string]string `json:"-" yaml:"-"`

	sourcev1.OCIRepositorySpec `json:",inline" yaml:",inline"`
}

// NewOCIRepository normalizes an owned typed OCIRepository declaration.
func NewOCIRepository(cr sourcev1.OCIRepository) *OCIRepository {
	cr.Spec.Provider = cmp.Or(cr.Spec.Provider, sourcev1.GenericOCIProvider)
	if cr.Spec.Verify != nil && cr.Spec.Verify.Provider == "" {
		verify := *cr.Spec.Verify
		verify.Provider = "cosign"
		cr.Spec.Verify = &verify
	}
	return &OCIRepository{Name: cr.Name, Namespace: cr.Namespace, Labels: cr.Labels, Annotations: cr.Annotations, OCIRepositorySpec: cr.Spec}
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

// GetLabels returns the OCIRepository's metadata.labels.
func (o *OCIRepository) GetLabels() map[string]string { return o.Labels }

// GetAnnotations returns the OCIRepository's metadata.annotations.
func (o *OCIRepository) GetAnnotations() map[string]string { return o.Annotations }

// Named identifies the OCIRepository.
func (o *OCIRepository) Named() NamedResource {
	return NamedResource{Group: kindTable[kindOCIRepository].Group, Kind: KindOCIRepository, Namespace: o.Namespace, Name: o.Name}
}

// BootstrapAlias returns a local copy.
func (o *OCIRepository) BootstrapAlias(root string) BaseManifest {
	spec := o.OCIRepositorySpec
	spec.URL = "file://" + root
	out := NewOCIRepository(sourcev1.OCIRepository{Name: o.Name, Namespace: o.Namespace, Labels: o.Labels, Annotations: o.Annotations, Spec: spec})
	out.diagnosticOwner, out.diagnosticPath, out.hasDiagnostic = o.diagnosticOwner, o.diagnosticPath, o.hasDiagnostic
	return out
}

// BootstrapFacts returns the borrowed immutable OCI view inputs.
func (o *OCIRepository) BootstrapFacts() BootstrapFacts {
	facts := BootstrapFacts{URL: o.URL, DerivedRoot: true}
	if ref := o.Reference; ref != nil {
		switch {
		case ref.Digest != "":
			facts.RefKind, facts.Ref = "digest", ref.Digest
		case ref.SemVer != "":
			facts.RefKind, facts.Ref = "semver", ref.SemVer
		case ref.Tag != "":
			facts.RefKind, facts.Ref = "tag", ref.Tag
		}
	}
	if o.Ignore != nil {
		facts.Ignore = *o.Ignore
	}
	return facts
}

// BootstrapInput returns the authored inputs that shape a local OCI alias.
func (o *OCIRepository) BootstrapInput() any {
	var ref sourcev1.OCIRepositoryRef
	if o.Reference != nil {
		ref = *o.Reference
	}
	return o.Content(ref)
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

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (o *OCIRepository) ProjectionCompatible(previous BaseManifest) bool {
	p := previous.(*OCIRepository)
	return o.diagnosticOwner == p.diagnosticOwner && o.diagnosticPath == p.diagnosticPath && o.hasDiagnostic == p.hasDiagnostic
}

// Suspended reports whether reconciliation is paused on this resource.
func (o *OCIRepository) Suspended() bool { return o.Suspend }

// ExistenceOnly reports whether readiness requires no artifact acquisition.
func (o *OCIRepository) ExistenceOnly() bool { return false }

// SourceInterval returns the authored source refresh interval.
func (o *OCIRepository) SourceInterval() time.Duration {
	return (&sourcev1.OCIRepository{Spec: o.OCIRepositorySpec}).GetRequeueAfter()
}

// OperationTimeout returns the authored operation timeout or the Flux default.
func (o *OCIRepository) OperationTimeout() time.Duration { return operationTimeout(o.Timeout) }

// ChartVersion returns the digest, semver constraint or tag declared by the source.
func (o *OCIRepository) ChartVersion() string {
	return ociReferenceVersion(o.Reference)
}

func ociReferenceVersion(ref *sourcev1.OCIRepositoryRef) string {
	if ref == nil {
		return ""
	}
	return cmp.Or(ref.Digest, ref.SemVer, ref.Tag)
}

func parseOCIRepository(doc map[string]any) (*OCIRepository, error) {
	var cr sourcev1.OCIRepository
	if err := decodeCR(doc, &cr, "OCIRepository", SourceDomain); err != nil {
		return nil, err
	}
	if cr.Spec.URL == "" {
		return nil, inputf("OCIRepository missing spec.url")
	}
	out := NewOCIRepository(cr)
	var err error
	out.references(validateSecretReference(out.Named(), &err))
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ExternalArtifact is a source whose content is published by its materializer.
type ExternalArtifact struct {
	Name        string            `json:"name"                yaml:"name"`
	Namespace   string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Labels      map[string]string `json:"-" yaml:"-"`
	Annotations map[string]string `json:"-" yaml:"-"`

	// ExternalArtifactSpec embeds the upstream spec, exposing
	// SourceRef at the top level.
	sourcev1.ExternalArtifactSpec `json:",inline" yaml:",inline"`
}

// NewExternalArtifact constructs an owned typed ExternalArtifact declaration.
func NewExternalArtifact(cr sourcev1.ExternalArtifact) *ExternalArtifact {
	return &ExternalArtifact{Name: cr.Name, Namespace: cr.Namespace, Labels: cr.Labels, Annotations: cr.Annotations, ExternalArtifactSpec: cr.Spec}
}

// GetLabels returns the ExternalArtifact's metadata.labels.
func (e *ExternalArtifact) GetLabels() map[string]string { return e.Labels }

// GetAnnotations returns the ExternalArtifact's metadata.annotations.
func (e *ExternalArtifact) GetAnnotations() map[string]string { return e.Annotations }

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

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (e *ExternalArtifact) ProjectionCompatible(BaseManifest) bool { return true }

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
	return NewExternalArtifact(cr), nil
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

func operationTimeout(timeout *metav1.Duration) time.Duration {
	if timeout != nil {
		return timeout.Duration
	}
	return time.Minute
}
