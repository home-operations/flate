package manifest

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

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

// parseResourceSet decodes a ResourceSet CR via the flux-operator
// typed schema (controlplane.io/v1).
func parseResourceSet(doc map[string]any) (*ResourceSet, error) {
	if err := checkAPIVersion(doc, FluxOperatorDomain); err != nil {
		return nil, err
	}
	var cr fluxopv1.ResourceSet
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("ResourceSet decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("ResourceSet missing metadata.name")
	}
	return &ResourceSet{
		Name:            cr.Name,
		Namespace:       cmp.Or(cr.Namespace, DefaultNamespace),
		ResourceSetSpec: cr.Spec,
		Labels:          cr.Labels,
	}, nil
}

// ResourceSetInputProvider is the flux-operator
// ResourceSetInputProvider CRD (fluxcd.controlplane.io/v1). It supplies
// inputs to one or more ResourceSets via spec.inputsFrom.
//
// flate evaluates the Static type fully — defaultValues becomes a
// single exported input set. Dynamic types (GitHubBranch, OCIArtifactTag,
// ExternalService, …) require live API access to remote services and
// are intentionally treated as empty providers here; the referencing
// ResourceSet still renders, but with no contribution from that
// provider.
type ResourceSetInputProvider struct {
	Name      string `json:"name"                yaml:"name"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`

	fluxopv1.ResourceSetInputProviderSpec `json:",inline" yaml:",inline"`

	Labels map[string]string `json:"-" yaml:"-"`
}

// Named identifies the ResourceSetInputProvider.
func (p *ResourceSetInputProvider) Named() NamedResource {
	return NamedResource{Kind: KindResourceSetInputProvider, Namespace: p.Namespace, Name: p.Name}
}

// NamespacedName is "<namespace>/<name>".
func (p *ResourceSetInputProvider) NamespacedName() string { return p.Namespace + "/" + p.Name }

// parseResourceSetInputProvider decodes a ResourceSetInputProvider CR.
func parseResourceSetInputProvider(doc map[string]any) (*ResourceSetInputProvider, error) {
	if err := checkAPIVersion(doc, FluxOperatorDomain); err != nil {
		return nil, err
	}
	var cr fluxopv1.ResourceSetInputProvider
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("ResourceSetInputProvider decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("ResourceSetInputProvider missing metadata.name")
	}
	return &ResourceSetInputProvider{
		Name:                         cr.Name,
		Namespace:                    cmp.Or(cr.Namespace, DefaultNamespace),
		ResourceSetInputProviderSpec: cr.Spec,
		Labels:                       cr.Labels,
	}, nil
}

// ExportedInputs returns the input sets this provider contributes to a
// referencing ResourceSet. Mirrors the upstream RSIP controller's
// "exported inputs" semantics for the Static case; dynamic types
// contribute zero sets here because flate can't query their remote
// APIs offline. Each returned set is a fresh map[string]any safe for
// the caller to mutate (e.g. to inject the provider block).
func (p *ResourceSetInputProvider) ExportedInputs() ([]map[string]any, error) {
	switch p.Type {
	case fluxopv1.InputProviderStatic, "":
		defaults := map[string]any{}
		for k, v := range p.DefaultValues {
			if v == nil {
				defaults[k] = nil
				continue
			}
			var raw any
			if err := json.Unmarshal(v.Raw, &raw); err != nil {
				return nil, fmt.Errorf("defaultValues[%s]: %w", k, err)
			}
			defaults[k] = raw
		}
		// Upstream injects a synthetic "id" derived from the RSIP UID.
		// flate has no UIDs; a deterministic hash of namespace/name is
		// stable across runs and still uniquely identifies the input set
		// when downstream templates reference inputs.id.
		if _, exists := defaults["id"]; !exists {
			defaults["id"] = p.derivedID()
		}
		return []map[string]any{defaults}, nil
	}
	return nil, nil
}

// derivedID is the placeholder for upstream's UID-derived id field —
// stable across flate runs because flate has no UIDs.
func (p *ResourceSetInputProvider) derivedID() string {
	sum := sha256.Sum256([]byte(p.Namespace + "/" + p.Name))
	return hex.EncodeToString(sum[:8])
}
