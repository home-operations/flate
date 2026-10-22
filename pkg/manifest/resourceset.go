package manifest

import (
	"cmp"
	"encoding/json"
	"fmt"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/kustomize/kyaml/yaml"
)

// ResourceSet is the flux-operator ResourceSet CRD
// (fluxcd.controlplane.io/v1). A ResourceSet templates a fixed set of
// resources across a matrix of input values — the controller renders
// spec.resources / spec.resourcesTemplate once per input set. Standalone rendering
// preserves document namespaces; controller emission defaults omitted namespaces
// on namespaced children to the ResourceSet namespace.
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

	// Suspend reports whether the reconciliation-disabled annotation pauses the set.
	Suspend bool `json:"-" yaml:"-"`
}

// Suspended reports whether reconciliation is disabled.
func (r *ResourceSet) Suspended() bool { return r.Suspend }

// Named identifies the ResourceSet.
func (r *ResourceSet) Named() NamedResource {
	return NamedResource{Group: kindTable[kindResourceSet].Group, Kind: KindResourceSet, Namespace: r.Namespace, Name: r.Name}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (r *ResourceSet) WithNamespace(namespace string) BaseManifest {
	out := *r
	out.Namespace = namespace
	return &out
}

// GetLabels returns the ResourceSet's metadata.labels.
func (r *ResourceSet) GetLabels() map[string]string { return r.Labels }

// parseResourceSet decodes a ResourceSet CR via the flux-operator
// typed schema (controlplane.io/v1).
func parseResourceSet(doc map[string]any) (*ResourceSet, error) {
	var cr fluxopv1.ResourceSet
	if err := decodeCR(doc, &cr, "ResourceSet", FluxOperatorDomain); err != nil {
		return nil, err
	}
	for i, dep := range cr.Spec.DependsOn {
		gv, err := schema.ParseGroupVersion(dep.APIVersion)
		if err != nil || gv.Version == "" {
			return nil, inputf("ResourceSet %w", field.Invalid(field.NewPath("spec", "dependsOn").Index(i).Child("apiVersion"), dep.APIVersion, "must have a valid version"))
		}
		scopes.ResolveScope(yaml.TypeMeta{APIVersion: dep.APIVersion, Kind: dep.Kind})
	}
	spec, _ := doc["spec"].(map[string]any)
	inputsFrom, _ := spec["inputsFrom"].([]any)
	for i, ref := range cr.Spec.InputsFrom {
		explicit := ref.APIVersion != ""
		if i < len(inputsFrom) {
			raw, _ := inputsFrom[i].(map[string]any)
			_, explicit = raw["apiVersion"]
		}
		if !explicit {
			continue
		}
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil || gv.Version == "" {
			return nil, inputf("ResourceSet %w", field.Invalid(field.NewPath("spec", "inputsFrom").Index(i).Child("apiVersion"), ref.APIVersion, "must have a valid version"))
		}
	}
	return &ResourceSet{
		Name:            cr.Name,
		Namespace:       cr.Namespace,
		ResourceSetSpec: cr.Spec,
		Labels:          cr.Labels,
		Suspend:         cr.IsDisabled(),
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
	Status                                fluxopv1.ResourceSetInputProviderStatus `json:"status,omitzero" yaml:"status,omitempty"`

	Labels map[string]string `json:"-" yaml:"-"`
}

// Named identifies the ResourceSetInputProvider.
func (p *ResourceSetInputProvider) Named() NamedResource {
	return NamedResource{Group: kindTable[kindResourceSetInputProvider].Group, Kind: KindResourceSetInputProvider, Namespace: p.Namespace, Name: p.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (p *ResourceSetInputProvider) Suspended() bool { return false }

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (p *ResourceSetInputProvider) WithNamespace(namespace string) BaseManifest {
	out := *p
	out.Namespace = namespace
	return &out
}

// GetLabels returns the ResourceSetInputProvider's metadata.labels.
func (p *ResourceSetInputProvider) GetLabels() map[string]string { return p.Labels }

// parseResourceSetInputProvider decodes a ResourceSetInputProvider CR.
func parseResourceSetInputProvider(doc map[string]any) (*ResourceSetInputProvider, error) {
	var cr fluxopv1.ResourceSetInputProvider
	if err := decodeCR(doc, &cr, "ResourceSetInputProvider", FluxOperatorDomain); err != nil {
		return nil, err
	}
	return &ResourceSetInputProvider{
		Name:                         cr.Name,
		Namespace:                    cr.Namespace,
		ResourceSetInputProviderSpec: cr.Spec,
		Status:                       cr.Status,
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
	if len(p.Status.ExportedInputs) > 0 {
		return decodeResourceSetInputs(p.Status.ExportedInputs)
	}
	switch p.Type {
	case fluxopv1.InputProviderStatic, "":
		exported, err := decodeResourceSetInputs([]fluxopv1.ResourceSetInput{p.DefaultValues})
		if err != nil {
			return nil, fmt.Errorf("defaultValues: %w", err)
		}
		defaults := exported[0]
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

func decodeResourceSetInputs(inputs []fluxopv1.ResourceSetInput) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(inputs))
	for i, in := range inputs {
		decoded := make(map[string]any, len(in))
		for k, v := range in {
			if v == nil {
				decoded[k] = nil
				continue
			}
			var raw any
			if err := json.Unmarshal(v.Raw, &raw); err != nil {
				return nil, fmt.Errorf("input[%d].%s: %w", i, k, err)
			}
			decoded[k] = raw
		}
		out = append(out, decoded)
	}
	return out, nil
}

// derivedID is the placeholder for upstream's UID-derived id field —
// stable across flate runs because flate has no UIDs.
func (p *ResourceSetInputProvider) derivedID() string {
	// First 8 bytes (16 hex chars) of the full digest — enough to
	// uniquely identify the input set within a render.
	return SHA256Hex([]byte(p.Namespace + "/" + p.Name))[:16]
}

func (r *ResourceSet) references(yield func(Reference) bool) {
	for i := range r.DependsOn {
		dep := &r.DependsOn[i]
		id := NamedResource{Group: apiGroup(dep.APIVersion), Kind: dep.Kind, Name: dep.Name}
		namespaced, prepared := scopes.LookupScope(yaml.TypeMeta{APIVersion: dep.APIVersion, Kind: dep.Kind})
		if !prepared {
			panic("manifest: ResourceSet dependency scope was not prepared")
		}
		if namespaced {
			id.Namespace = r.Namespace
			if dep.Namespace != "" {
				id.Namespace = dep.Namespace
			}
		}
		if !yield(Reference{
			NamedResource: id,
			Use:           "dependsOn", Position: i, ReadyExpr: referenceExpression(&dep.ReadyExpr), Gate: GateInputs,
		}) {
			return
		}
	}
	for i, ref := range r.InputsFrom {
		kind := cmp.Or(ref.Kind, KindResourceSetInputProvider)
		var gate GateStage
		if ref.Name != "" {
			gate = GateInputs
		}
		if !yield(Reference{
			NamedResource: Resolve(NamedResource{Group: referenceGroup(kind, ref.APIVersion), Kind: kind, Name: ref.Name}, "", r.Namespace),
			Use:           "inputsFrom", Position: i, Content: true, Gate: gate,
		}) {
			return
		}
	}
}

func (r *ResourceSet) kind() KindInfo { return kindTable[kindResourceSet].KindInfo }

func (p *ResourceSetInputProvider) kind() KindInfo {
	return kindTable[kindResourceSetInputProvider].KindInfo
}
