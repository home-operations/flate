package manifest

import (
	"cmp"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	meta "github.com/fluxcd/pkg/apis/meta"
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

// NamedResource uniquely identifies a Kubernetes resource by kind +
// namespace + name. Values are comparable and safe to use as map keys.
type NamedResource struct {
	Kind      string `json:"kind"                yaml:"kind"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name      string `json:"name"                yaml:"name"`
}

// NamespacedName returns "namespace/name", or just "name" when
// cluster-scoped.
func (n NamedResource) NamespacedName() string {
	if n.Namespace == "" {
		return n.Name
	}
	return n.Namespace + "/" + n.Name
}

// FluxResourceName is the canonical "<namespace>-<name>" identifier
// every Flux source CR uses for cache slots, OCI auth scopes, etc.
// Single source of truth so GitRepository / OCIRepository /
// HelmRepository all return identical strings on the same id and a
// future format change (e.g. adding a kind prefix) only touches this
// function.
func (n NamedResource) FluxResourceName() string {
	return n.Namespace + "-" + n.Name
}

// String returns "kind/namespace/name" — the canonical id form.
func (n NamedResource) String() string {
	return n.Kind + "/" + n.NamespacedName()
}

// Compare orders NamedResources by (kind, namespace, name) — returns
// -1, 0, or +1 per cmp.Compare semantics.
func (n NamedResource) Compare(other NamedResource) int {
	return cmp.Or(
		cmp.Compare(n.Kind, other.Kind),
		cmp.Compare(n.Namespace, other.Namespace),
		cmp.Compare(n.Name, other.Name),
	)
}

// BaseManifest is the marker interface every domain object implements.
// Concrete handling is done via type assertions in each controller.
type BaseManifest interface {
	Named() NamedResource
}

// DependencyRef is a Kustomization or HelmRelease dependency entry —
// the target resource plus an optional CEL expression that overrides
// the built-in Ready check (per Flux's spec.dependsOn[].readyExpr).
type DependencyRef struct {
	NamedResource
	// ReadyExpr is the CEL expression to evaluate against the dep's
	// projected status. When non-empty, the built-in Ready=True check
	// is replaced. Empty means "use the built-in check."
	ReadyExpr string
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
	return NamedResource{Kind: r.Kind, Namespace: r.Namespace, Name: r.Name}
}

// Clone returns a deep copy safe for mutation without aliasing the
// store-owned source. The Spec map is recursively cloned so
// downstream transformations (e.g. namespace inheritance,
// substitution overlays) don't mutate the loader's parsed doc.
//
// The package doc advertises store-stored objects as immutable
// (store.AddObject's reflect.DeepEqual dedup compares against shared
// pointers), but RawObject's pre-Clone construction aliased the
// loader's `doc[spec]` map directly — any consumer that wrote
// through r.Spec corrupted the underlying multi-doc YAML the loader
// reused across passes. Clone makes the immutability contract
// enforceable instead of nominal.
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
	ns := stringOr(metadata, "namespace", DefaultNamespace)
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
