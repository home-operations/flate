package manifest

import (
	"cmp"
	"iter"
	"reflect"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	meta "github.com/fluxcd/pkg/apis/meta"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

// LocalObjectReference is a name-only reference to a same-namespace
// resource (Secret, ConfigMap, ...). Aliased to fluxcd/pkg/apis/meta
// so flate's structs unmarshal Flux CRs without a field-by-field copy.
type LocalObjectReference = meta.LocalObjectReference

// ValuesReference is a reference to a values-bearing ConfigMap or
// Secret on a HelmRelease.spec.valuesFrom. Aliased to upstream meta
// for the same reason — and to inherit GetValuesKey().
type ValuesReference = meta.ValuesReference

// SubstituteReference is a postBuild.substituteFrom entry. Aliased to
// kustomize-controller's own type — flate's parser previously copied
// the same three fields (Kind, Name, Optional) into a local twin.
type SubstituteReference = kustomizev1.SubstituteReference

// NamedResource uniquely identifies a Kubernetes resource by group, kind,
// namespace and name. Empty Group means core; version is not identity.
// Values are comparable and safe to use as map keys.
type NamedResource struct {
	Group     string `json:"group,omitempty"      yaml:"group,omitempty"`
	Kind      string `json:"kind"                yaml:"kind"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name      string `json:"name"                yaml:"name"`
}

// IsCRD reports whether the identity is a Kubernetes CustomResourceDefinition.
func (n NamedResource) IsCRD() bool {
	return n.Group == apix.GroupName && n.Kind == KindCustomResourceDefinition
}

// ReportHidden reports whether the identity is internal to reconciliation reports.
func (n NamedResource) ReportHidden() bool {
	return n == BootstrapSourceID || n.Group == SourceDomain && n.Kind == KindHelmChart
}

// Resolve applies forced, authored and fallback namespace precedence without changing other fields.
func Resolve(id NamedResource, forced, fallback string) NamedResource {
	id.Namespace = cmp.Or(forced, id.Namespace, fallback)
	return id
}

// NamespacedName returns "namespace/name", or just "name" when
// cluster-scoped.
func (n NamedResource) NamespacedName() string {
	if n.Namespace == "" {
		return n.Name
	}
	return n.Namespace + "/" + n.Name
}

// KindLabel returns "kind.group", omitting an empty group.
func (n NamedResource) KindLabel() string {
	kind := n.Kind
	if n.Group != "" {
		kind += "." + n.Group
	}
	return kind
}

// String displays "kind.group/namespace/name", omitting empty group and namespace.
func (n NamedResource) String() string {
	return n.KindLabel() + "/" + n.NamespacedName()
}

// Compare orders NamedResources by (kind, group, namespace, name), returning
// -1, 0, or +1 per cmp.Compare semantics.
func (n NamedResource) Compare(other NamedResource) int {
	return cmp.Or(
		cmp.Compare(n.Kind, other.Kind),
		cmp.Compare(n.Group, other.Group),
		cmp.Compare(n.Namespace, other.Namespace),
		cmp.Compare(n.Name, other.Name),
	)
}

// BaseManifest is the sealed interface for parsed domain objects.
// Implementations own their dependency walk and MUST keep embedded specs immutable.
type BaseManifest interface {
	// WithNamespace changes only the internal namespace, retaining immutable borrowed payloads.
	WithNamespace(string) BaseManifest
	// ProjectionCompatible reports whether the desired receiver can reuse the previous projection.
	// Both MUST be non-nil, the same concrete type and eligible under the caller's
	// source-state and projected-identity lifecycle; metadata namespaces may differ.
	ProjectionCompatible(previous BaseManifest) bool
	Named() NamedResource
	// SourcePath returns the source identity and authored path, or zero values when absent.
	SourcePath() (NamedResource, string, bool)
	Suspended() bool
	ReferenceCapacity() (int, int)
	kind() KindInfo
	producerTargets([]NamedResource) []ProducerTarget
}

// Equal compares manifest content without derived projection-cache state.
func Equal(a, b BaseManifest) bool {
	if a == b {
		return true
	}
	if k, ok := a.(*Kustomization); ok {
		other, ok := b.(*Kustomization)
		return ok && k.equal(other)
	}
	return reflect.DeepEqual(a, b)
}

// DependencyRef is a compact prerequisite identity and optional CEL gate.
type DependencyRef struct {
	NamedResource
	// ReadyExpr borrows an immutable authored expression and replaces built-in readiness when non-empty.
	ReadyExpr *string
	// Artifact requires acquired filesystem content after readiness.
	Artifact bool
	// ArtifactOnly accepts readable content before readiness and rejects ready sources without it.
	ArtifactOnly bool
	// UnknownArtifactRoot preserves the unresolved source-root diagnostic at drain.
	UnknownArtifactRoot bool
	// ChartSource retains chart-source context on acquisition failures and skips.
	ChartSource bool
}

// GateStage selects the prerequisites needed at a reconciliation checkpoint.
type GateStage uint8

// GateParents checks structural owners before any input preparation.
const GateParents GateStage = 0

const (
	// GateInputs waits for ordering and synchronous preparation inputs.
	GateInputs GateStage = 1 << iota
	// GateArtifacts waits for effective rendering source content.
	GateArtifacts
)

// ReferenceView selects relationships or all reconciliation prerequisites.
type ReferenceView uint8

const (
	// ReferenceRelations selects authored dependencies and resolved source relationships.
	ReferenceRelations ReferenceView = iota
	// ReferenceAll also includes implicit reconciliation prerequisites.
	ReferenceAll
)

// Reference contains transient facts for one authored dependency or resolved source.
// Consumers MUST retain only DependencyRef or NamedResource, never the full value.
type Reference struct {
	DependencyRef
	// Use is source, chart, dependsOn, valuesFrom, substituteFrom, secretRef,
	// certSecretRef, proxySecretRef, inputsFrom, artifact or materializer; empty selects an implicit gate only.
	Use string
	// Position indexes the authored field; credentials use secret=0, cert=1, proxy=2.
	Position int
	// Optional permits an absent target under the reference's data policy.
	Optional bool
	// Content selects changed-only content dependents; ordering edges are false.
	Content bool
	// Gate selects reconciliation checkpoints without changing Content edges.
	Gate GateStage
	// ArtifactStages selects checkpoints requiring filesystem content.
	ArtifactStages GateStage
	// Ordering permits changed-only pruning of an ordering prerequisite.
	Ordering bool
	// Files borrows immutable source-relative input declarations.
	Files []string
	// ChartPath declares a source-relative chart directory or archive; dot selects the root.
	ChartPath string
}

// References yields dependencies in source-field order for the selected view without deduplication.
// The object and its embedded spec MUST remain immutable during iteration.
// Direct SDK objects MUST satisfy their reference schemas, including valid explicit API versions.
// Before walking a directly constructed ResourceSet, prepare each dependsOn GVK with
// KindOf on a RawObject carrying its APIVersion and Kind, then Namespaced. ParseDoc does this at admission.
// Concrete dispatch keeps yield out of interface calls so iteration closures stay on the stack.
func References(obj BaseManifest, view ReferenceView) iter.Seq[Reference] {
	return func(yield func(Reference) bool) {
		switch obj := obj.(type) {
		case *Kustomization:
			obj.references(yield, view)
		case *HelmRelease:
			obj.references(yield)
		case *ResourceSet:
			obj.references(yield)
		case *HelmChartSource:
			obj.references(yield)
		case *GitRepository:
			obj.references(yield)
		case *OCIRepository:
			obj.references(yield)
		case *HelmRepository:
			obj.references(yield)
		case *Bucket:
			obj.references(yield)
		case *ExternalArtifact:
			obj.references(yield)
		case *RawObject:
			obj.references(yield)
		}
	}
}

// MapDependsOn transforms dependency fields without mutating the object; a nil namespace preserves it.
func MapDependsOn(obj BaseManifest, name, namespace func(string) string) BaseManifest {
	if ks, ok := obj.(*Kustomization); ok {
		return ks.mapDependsOn(name, namespace)
	}
	return obj
}

func credentialReferences(yield func(Reference) bool, namespace string, refs ...*LocalObjectReference) {
	uses := [...]string{"secretRef", "certSecretRef", "proxySecretRef"}
	for i, ref := range refs {
		if ref != nil && !yield(Reference{NamedResource: Resolve(NamedResource{Kind: KindSecret, Name: ref.Name}, "", namespace),
			Use: uses[i], Position: i, Content: true}) {
			return
		}
	}
}

// DrainLevel controls dependency failure classification at the DAG fixpoint.
type DrainLevel uint8

const (
	// DrainNone parks unsatisfied dependencies during normal operation.
	DrainNone DrainLevel = iota
	// DrainCascade fails absent dependencies and unsatisfied ready expressions.
	DrainCascade
	// DrainForce also fails present dependencies that remain pending.
	DrainForce
)

// RawObject is the fallback for any Kubernetes document that doesn't
// match a more specific type.
type RawObject struct {
	Kind       string         `json:"kind"                yaml:"kind"`
	APIVersion string         `json:"apiVersion"          yaml:"apiVersion"`
	Name       string         `json:"name"                yaml:"name"`
	Namespace  string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Spec       map[string]any `json:"spec,omitempty"      yaml:"spec,omitempty"`
}

// Named identifies the resource.
func (r *RawObject) Named() NamedResource {
	return NamedResource{Group: apiGroup(r.APIVersion), Kind: r.Kind, Namespace: r.Namespace, Name: r.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (r *RawObject) Suspended() bool { return false }

// ReconcileDocument reports whether reconciliation consumes the authoritative document.
func (r *RawObject) ReconcileDocument() bool { return r.isArtifactGenerator() }

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (r *RawObject) WithNamespace(namespace string) BaseManifest {
	out := *r
	out.Namespace = namespace
	return &out
}

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (r *RawObject) ProjectionCompatible(BaseManifest) bool { return true }

// Clone deep-copies the spec to preserve store and parsed-document immutability.
func (r *RawObject) Clone() *RawObject {
	out := *r
	out.Spec = DeepCopyMap(r.Spec)
	return &out
}

func parseRawObject(doc map[string]any) (*RawObject, error) {
	apiVersion := DocAPIVersion(doc)
	if err := checkAPIVersion(doc, apiGroup(apiVersion)); err != nil {
		return nil, err
	}
	kind := DocKind(doc)
	if kind == "" {
		return nil, inputf("missing kind")
	}
	name, ns, missing := requireMetadata(doc)
	if missing != "" {
		return nil, inputf("missing %s for %s", missing, kind)
	}
	spec, _ := doc["spec"].(map[string]any)
	spec = DeepCopyMap(spec)
	return &RawObject{
		Kind:       kind,
		APIVersion: apiVersion,
		Name:       name,
		Namespace:  ns,
		Spec:       spec,
	}, nil
}

func (r *RawObject) kind() KindInfo {
	info := KindInfo{Group: apiGroup(r.APIVersion), Kind: r.Kind, apiVersion: r.APIVersion}
	if r.Kind == kindTable[kindArtifactGenerator].Kind && info.Group == kindTable[kindArtifactGenerator].Group {
		info = kindTable[kindArtifactGenerator].KindInfo
		info.apiVersion = r.APIVersion
	}
	return info
}

func (r *RawObject) isArtifactGenerator() bool {
	return r.Kind == kindTable[kindArtifactGenerator].Kind && apiGroup(r.APIVersion) == kindTable[kindArtifactGenerator].Group
}

func referenceExpression(expr *string) *string {
	if *expr == "" {
		return nil
	}
	return expr
}
