package manifest

import (
	"cmp"
	"encoding/json"
	"fmt"
	"hash/adler32"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
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

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (r *ResourceSet) ProjectionCompatible(BaseManifest) bool { return true }

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
			return nil, NewError(ErrInput, fmt.Errorf("ResourceSet %w", field.Invalid(field.NewPath("spec", "dependsOn").Index(i).Child("apiVersion"), dep.APIVersion, "must have a valid version")), "")
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
			return nil, NewError(ErrInput, fmt.Errorf("ResourceSet %w", field.Invalid(field.NewPath("spec", "inputsFrom").Index(i).Child("apiVersion"), ref.APIVersion, "must have a valid version")), "")
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
// Static inputs use defaultValues, and OCIArtifactTag inputs use registry tags.
// Other dynamic types require pre-baked status.exportedInputs.
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

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (p *ResourceSetInputProvider) ProjectionCompatible(BaseManifest) bool { return true }

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

// ProviderInputKind selects how a provider contributes ResourceSet inputs.
type ProviderInputKind uint8

const (
	// ProviderInputLiteral uses decoded status or Static inputs.
	ProviderInputLiteral ProviderInputKind = iota
	// ProviderInputRegistry acquires OCI artifact tags.
	ProviderInputRegistry
	// ProviderInputUnsupported contributes no offline inputs.
	ProviderInputUnsupported
)

// ProviderInputPlan describes one provider's literal, registry or unsupported inputs.
type ProviderInputPlan struct {
	Kind     ProviderInputKind
	Inputs   []map[string]any
	Registry *RegistryAccess
}

func (p *ResourceSetInputProvider) inputKind() ProviderInputKind {
	if len(p.Status.ExportedInputs) > 0 || p.Type == fluxopv1.InputProviderStatic || p.Type == "" {
		return ProviderInputLiteral
	}
	if p.Type == fluxopv1.InputProviderOCIArtifactTag {
		return ProviderInputRegistry
	}
	return ProviderInputUnsupported
}

// InputPlan classifies the provider and returns detached literal input maps.
func (p *ResourceSetInputProvider) InputPlan() (ProviderInputPlan, error) {
	plan := ProviderInputPlan{Kind: p.inputKind()}
	switch plan.Kind {
	case ProviderInputLiteral:
		if len(p.Status.ExportedInputs) > 0 {
			inputs, err := decodeResourceSetInputs(p.Status.ExportedInputs)
			plan.Inputs = inputs
			return plan, err
		}
		exported, err := decodeResourceSetInputs([]fluxopv1.ResourceSetInput{p.DefaultValues})
		if err != nil {
			return plan, fmt.Errorf("defaultValues: %w", err)
		}
		defaults := exported[0]
		if _, exists := defaults["id"]; !exists {
			defaults["id"] = p.derivedID()
		}
		plan.Inputs = exported
	case ProviderInputRegistry:
		plan.Registry = &RegistryAccess{
			Owner: DiagnosticFor(p.Named(), p), URL: p.URL, Namespace: p.Namespace,
			SecretRef: p.SecretRef, CertSecretRef: p.CertSecretRef, Insecure: p.Insecure,
			Timeout: operationTimeout(nil),
		}
	case ProviderInputUnsupported:
	}
	return plan, nil
}

// SelectTags applies flux-operator's filters, stable ordering and limit to registry tags.
func (p *ResourceSetInputProvider) SelectTags(tags []string) ([]string, error) {
	var include, exclude *regexp.Regexp
	var constraint *semver.Constraints
	if filter := p.Filter; filter != nil {
		for _, field := range []struct {
			name, pattern string
			target        **regexp.Regexp
		}{
			{"includeBranch", filter.IncludeBranch, &include},
			{"includeTag", filter.IncludeTag, &include},
			{"includeEnvironment", filter.IncludeEnvironment, &include},
			{"excludeBranch", filter.ExcludeBranch, &exclude},
			{"excludeTag", filter.ExcludeTag, &exclude},
			{"excludeEnvironment", filter.ExcludeEnvironment, &exclude},
		} {
			if field.pattern == "" {
				continue
			}
			compiled, err := regexp.Compile(field.pattern)
			if err != nil {
				return nil, NewError(ErrInput, fmt.Errorf("invalid %s regex: %w", field.name, err), "")
			}
			*field.target = compiled
		}
		if filter.Semver != "" {
			var err error
			constraint, err = semver.NewConstraint(filter.Semver)
			if err != nil {
				return nil, NewError(ErrInput, fmt.Errorf("invalid semver expression: %w", err), "")
			}
		}
	}
	type parsedTag struct {
		name    string
		version *semver.Version
	}
	selected := make([]parsedTag, 0, len(tags))
	for _, tag := range tags {
		if include != nil && !include.MatchString(tag) || exclude != nil && exclude.MatchString(tag) {
			continue
		}
		parsed := parsedTag{name: tag}
		if constraint != nil {
			// Flux ParseVersion requires three dot-separated components before SemVer parsing.
			if strings.Count(tag, ".") < 2 {
				continue
			}
			v, err := semver.NewVersion(tag)
			if err != nil || !constraint.Check(v) {
				continue
			}
			parsed.version = v
		}
		selected = append(selected, parsed)
	}
	if constraint != nil {
		slices.SortStableFunc(selected, func(a, b parsedTag) int { return b.version.Compare(a.version) })
	} else {
		slices.SortFunc(selected, func(a, b parsedTag) int { return cmp.Compare(b.name, a.name) })
	}
	limit := (&fluxopv1.ResourceSetInputProvider{Spec: p.ResourceSetInputProviderSpec}).GetFilterLimit()
	out := make([]string, min(limit, len(selected)))
	for i := range out {
		out[i] = selected[i].name
	}
	return out, nil
}

// TagInputs returns one resolved tag's generated fields merged with provider defaults.
func (p *ResourceSetInputProvider) TagInputs(tag, digest string) (fluxopv1.ResourceSetInput, error) {
	defaults, err := (&fluxopv1.ResourceSetInputProvider{Spec: p.ResourceSetInputProviderSpec}).GetDefaultInputs()
	if err != nil {
		return nil, NewError(ErrInput, fmt.Errorf("defaultValues: %w", err), "")
	}
	inputs, err := fluxopv1.NewResourceSetInput(defaults, map[string]any{
		"id": strconv.FormatUint(uint64(adler32.Checksum([]byte(tag))), 10), "tag": tag, "digest": digest,
	})
	if err != nil {
		return nil, NewError(ErrInput, fmt.Errorf("tag %q inputs: %w", tag, err), "")
	}
	return inputs, nil
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

func (p *ResourceSetInputProvider) derivedID() string {
	// flate has no UID, so Static IDs use the namespace and name.
	return SHA256Hex([]byte(p.Namespace + "/" + p.Name))[:16]
}

func (p *ResourceSetInputProvider) references(yield func(Reference) bool) {
	if p.inputKind() == ProviderInputRegistry {
		credentialReferences(yield, p.Namespace, p.SecretRef, p.CertSecretRef)
	}
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
			Use:           "inputsFrom", Position: i, Content: true, Gate: gate, Selector: ref.Selector, NamespaceDefault: true,
		}) {
			return
		}
	}
}

func (r *ResourceSet) kind() KindInfo { return kindTable[kindResourceSet].KindInfo }

func (p *ResourceSetInputProvider) kind() KindInfo {
	return kindTable[kindResourceSetInputProvider].KindInfo
}
