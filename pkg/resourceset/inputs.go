package resourceset

import (
	"fmt"
	"slices"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/resourcesetinput"
	"github.com/home-operations/flate/pkg/manifest"
)

// ProviderInputState distinguishes evaluated content from unavailable inputs.
type ProviderInputState uint8

const (
	// ProviderInputEvaluated includes a successful empty evaluation.
	ProviderInputEvaluated ProviderInputState = iota
	// ProviderInputUnsupported identifies a provider requiring an unsupported remote API.
	ProviderInputUnsupported
	// ProviderInputUnavailable identifies unavailable credential material.
	ProviderInputUnavailable
)

// ProviderInputs contains captured, evaluated inputs for one provider.
type ProviderInputs struct {
	// Declaration is the captured provider before evaluation and is excluded from content identity.
	Declaration *manifest.ResourceSetInputProvider `json:"-"`
	ID          manifest.NamedResource
	Type        string
	State       ProviderInputState
	Inputs      []map[string]any
}

func buildInputSets(rs *manifest.ResourceSet, providers []ProviderInputs) ([]map[string]any, error) {
	groups := make([]resourcesetinput.Group, 0, 1+len(providers))
	groups = append(groups, resourcesetinput.Inline(rs.Name, rs.Namespace, rs.Inputs))
	for _, provider := range providers {
		inputs := make([]map[string]any, 0, len(provider.Inputs))
		block := resourcesetinput.Provider(provider.ID.Kind, provider.ID.Name, provider.ID.Namespace)
		for _, input := range provider.Inputs {
			input = manifest.DeepCopyMap(input)
			input["provider"] = block
			inputs = append(inputs, input)
		}
		groups = append(groups, resourcesetinput.Group{Name: provider.ID.Name, Inputs: inputs})
	}
	return resourcesetinput.Combine(groups, rs.InputStrategy)
}

// CollectProviders resolves unique provider declarations in canonical order.
func CollectProviders(rs *manifest.ResourceSet, resolve ProviderResolver) ([]ProviderInputs, error) {
	if resolve == nil || len(rs.InputsFrom) == 0 {
		return nil, nil
	}
	providers := make(map[manifest.NamedResource]*manifest.ResourceSetInputProvider)
	for edge := range manifest.References(rs, manifest.ReferenceRelations) {
		if edge.Use != "inputsFrom" {
			continue
		}
		ref := rs.InputsFrom[edge.Position]
		matches, err := resolve(ref, edge.NamedResource)
		if err != nil {
			return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("inputsFrom %q: %w", ref.Name, err), "")
		}
		for _, p := range matches {
			if p == nil {
				continue
			}
			k := p.Named()
			if _, dup := providers[k]; !dup {
				providers[k] = p
			}
		}
	}
	content := make([]ProviderInputs, 0, len(providers))
	for _, provider := range providers {
		content = append(content, ProviderInputs{Declaration: provider, ID: provider.Named(), Type: provider.Type})
	}
	slices.SortFunc(content, func(a, b ProviderInputs) int { return a.ID.Compare(b.ID) })
	return content, nil
}

type providerReader interface {
	GetObject(manifest.NamedResource) manifest.BaseManifest
	ListObjects(string, string) []manifest.BaseManifest
}

// StoreResolver resolves named or selected providers from immutable query inputs.
// References MUST remain immutable, and the resolver MUST be used sequentially.
func StoreResolver(s providerReader) ProviderResolver {
	type compilation struct {
		matcher manifest.ReferenceMatcher
		err     error
	}
	type query struct {
		id       manifest.NamedResource
		selector *metav1.LabelSelector
	}
	var firstQuery query
	var firstCompilation compilation
	var selectors map[query]compilation
	return func(ref fluxopv1.InputProviderReference, id manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
		if id.Name != "" {
			obj, ok := s.GetObject(id).(*manifest.ResourceSetInputProvider)
			if !ok {
				return nil, nil
			}
			return []*manifest.ResourceSetInputProvider{obj}, nil
		}
		if ref.Selector == nil {
			return nil, nil
		}
		key := query{id: id, selector: ref.Selector}
		compiled := firstCompilation
		if key != firstQuery {
			var ok bool
			compiled, ok = selectors[key]
			if !ok {
				compiled.matcher, compiled.err = id.Matcher(ref.Selector)
				if firstQuery.selector == nil {
					firstQuery, firstCompilation = key, compiled
				} else {
					if selectors == nil {
						selectors = make(map[query]compilation)
					}
					selectors[key] = compiled
				}
			}
		}
		if compiled.err != nil {
			return nil, compiled.err
		}
		var out []*manifest.ResourceSetInputProvider
		for _, object := range s.ListObjects(id.Group, id.Kind) {
			p, ok := object.(*manifest.ResourceSetInputProvider)
			if ok && compiled.matcher.Matches(p.Named(), p.Labels) {
				out = append(out, p)
			}
		}
		return out, nil
	}
}
