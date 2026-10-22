package manifest

import (
	"cmp"
	"iter"
	"sync/atomic"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	meta "github.com/fluxcd/pkg/apis/meta"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
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

// String displays "kind.group/namespace/name", omitting empty group and namespace.
func (n NamedResource) String() string {
	kind := n.Kind
	if n.Group != "" {
		kind += "." + n.Group
	}
	return kind + "/" + n.NamespacedName()
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
	Named() NamedResource
	Suspended() bool
	kind() KindInfo
	producerTargets([]NamedResource) []ProducerTarget
}

// DependencyRef is a compact prerequisite identity and optional CEL gate.
type DependencyRef struct {
	NamedResource
	// ReadyExpr replaces the built-in readiness check when non-empty.
	ReadyExpr string
}

// Reference contains transient facts for one authored dependency or resolved source.
// Consumers MUST retain only DependencyRef or NamedResource, never the full value.
type Reference struct {
	DependencyRef
	// Use is source, chart, dependsOn, valuesFrom, substituteFrom, secretRef,
	// certSecretRef, proxySecretRef or inputsFrom.
	Use string
	// Position indexes the authored field; credentials use secret=0, cert=1, proxy=2.
	Position int
	// Optional permits an absent target under the reference's data policy.
	Optional bool
	// Content selects changed-only content dependents; ordering edges are false.
	Content bool
	// Files borrows immutable source-relative input declarations.
	Files []string
}

// References yields dependencies in source-field order without deduplication.
// The object and its embedded spec MUST remain immutable during iteration.
// Direct SDK objects MUST satisfy their reference schemas, including valid explicit API versions.
// Before walking a directly constructed ResourceSet, prepare each dependsOn GVK with
// KindOf on a RawObject carrying its APIVersion and Kind, then Namespaced. ParseDoc does this at admission.
// Concrete dispatch keeps yield out of interface calls so iteration closures stay on the stack.
func References(obj BaseManifest) iter.Seq[Reference] {
	return func(yield func(Reference) bool) {
		switch obj := obj.(type) {
		case *Kustomization:
			obj.references(yield)
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

// Opportunity borrows a scheduler-owned supplier word and its observed epoch.
// Only the scheduler may write Word; low bit one means admitted work remains.
type Opportunity struct {
	NamedResource
	Word          *atomic.Uint64
	Observed, Cut uint64
}

// ContentWait carries immutable content targets, supplier observations and declaration.
// The scheduler owns any copy it binds; dispatchers must not mutate borrowed payloads.
type ContentWait struct {
	Targets     []NamedResource
	Suppliers   []Opportunity
	Declaration BaseManifest
}

// ReconcileAttempt carries invocation admission and borrowed content bindings.
type ReconcileAttempt struct {
	Drain   DrainLevel
	Content *ContentWait
}

// ReconcileWait separates required causes from content wake and binding identities.
type ReconcileWait struct {
	Deps    []NamedResource
	Content *ContentWait
}

// Empty reports whether no ordinary or content work remains.
func (w ReconcileWait) Empty() bool {
	return len(w.Deps) == 0 && (w.Content == nil ||
		len(w.Content.Targets) == 0 && len(w.Content.Suppliers) == 0 && w.Content.Declaration == nil)
}

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

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (r *RawObject) WithNamespace(namespace string) BaseManifest {
	out := *r
	out.Namespace = namespace
	return &out
}

// Clone deep-copies the spec to preserve store and parsed-document immutability.
func (r *RawObject) Clone() *RawObject {
	out := *r
	out.Spec = DeepCopyMap(r.Spec)
	return &out
}

// parseRawObject decodes any Kubernetes document into RawObject.
func parseRawObject(doc map[string]any) (*RawObject, error) {
	apiVersion := DocAPIVersion(doc)
	if apiVersion == "" {
		return nil, inputf("missing apiVersion")
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil || gv.Version == "" {
		return nil, inputf("invalid %w", field.Invalid(field.NewPath("apiVersion"), apiVersion, "must have a valid version"))
	}
	kind := DocKind(doc)
	if kind == "" {
		return nil, inputf("missing kind")
	}
	metadata, _ := doc["metadata"].(map[string]any)
	if metadata == nil {
		return nil, inputf("missing metadata for %s", kind)
	}
	name, _ := metadata["name"].(string)
	if name == "" {
		return nil, inputf("missing metadata.name for %s", kind)
	}
	ns, _ := metadata["namespace"].(string)
	spec, _ := doc["spec"].(map[string]any)
	// Deep-copy the spec map so RawObject doesn't alias the loader's
	// parsed YAML — mutating r.Spec then corrupts the multi-doc
	// stream the loader reuses across passes. Cheap; spec sub-trees
	// are small relative to chart-style HR.Values.
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
