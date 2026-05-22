package manifest

import (
	"cmp"
	"strings"
)

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

// String returns "kind/namespace/name" — the canonical id form.
func (n NamedResource) String() string {
	return n.Kind + "/" + n.NamespacedName()
}

// Compare orders NamedResources by (kind, namespace, name) — returns
// -1, 0, or +1 per cmp.Compare semantics.
func (n NamedResource) Compare(other NamedResource) int {
	if c := cmp.Compare(n.Kind, other.Kind); c != 0 {
		return c
	}
	if c := cmp.Compare(n.Namespace, other.Namespace); c != 0 {
		return c
	}
	return cmp.Compare(n.Name, other.Name)
}

// Less is the sort.Interface-style predicate.
func (n NamedResource) Less(other NamedResource) bool { return n.Compare(other) < 0 }

// SplitNamespacedName parses "namespace/name". When s has no separator
// the fallback namespace is used. ok is false when the string is empty
// or one of its parts is.
func SplitNamespacedName(s, fallback string) (ns, name string, ok bool) {
	if before, after, found := strings.Cut(s, "/"); found {
		return before, after, before != "" && after != ""
	}
	return fallback, s, s != ""
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

// ParseRawObject decodes any Kubernetes document into RawObject.
func ParseRawObject(doc map[string]any) (*RawObject, error) {
	apiVersion, _ := doc["apiVersion"].(string)
	if apiVersion == "" {
		return nil, inputf("missing apiVersion")
	}
	kind, _ := doc["kind"].(string)
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
	return &RawObject{
		Kind:       kind,
		APIVersion: apiVersion,
		Name:       name,
		Namespace:  ns,
		Spec:       spec,
	}, nil
}
