package manifest

import (
	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
)

// ResourceSet is the flux-operator ResourceSet CRD
// (fluxcd.controlplane.io/v1). A ResourceSet templates a fixed set of
// resources across a matrix of input values — the controller renders
// spec.resources / spec.resourcesTemplate once per input set and emits
// the resulting objects with metadata.namespace defaulted to the
// ResourceSet's own namespace when absent.
//
// The embedded fluxopv1.ResourceSetSpec promotes CommonMetadata,
// Inputs, InputsFrom, Resources, ResourcesTemplate, InputStrategy,
// DependsOn, ServiceAccountName, Wait to the top level for ergonomic
// access.
type ResourceSet struct {
	Name      string `json:"name"                yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`

	fluxopv1.ResourceSetSpec `json:",inline" yaml:",inline"`

	// Labels mirrors metadata.labels from the source manifest so
	// downstream consumers can read them without re-parsing the raw
	// document.
	Labels map[string]string `json:"-" yaml:"-"`
}

// Named identifies the ResourceSet.
func (r *ResourceSet) Named() NamedResource {
	return NamedResource{Kind: KindResourceSet, Namespace: r.Namespace, Name: r.Name}
}

// NamespacedName is "<namespace>/<name>".
func (r *ResourceSet) NamespacedName() string { return r.Namespace + "/" + r.Name }

// ParseResourceSet decodes a ResourceSet CR via the flux-operator
// typed schema (controlplane.io/v1).
func ParseResourceSet(doc map[string]any) (*ResourceSet, error) {
	if err := checkAPIVersion(doc, FluxOperatorDomain); err != nil {
		return nil, err
	}
	var cr fluxopv1.ResourceSet
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("ResourceSet decode: %v", err)
	}
	if cr.Name == "" {
		return nil, inputf("ResourceSet missing metadata.name")
	}
	ns := cr.Namespace
	if ns == "" {
		ns = DefaultNamespace
	}
	return &ResourceSet{
		Name:            cr.Name,
		Namespace:       ns,
		ResourceSetSpec: cr.Spec,
		Labels:          cr.Labels,
	}, nil
}
