package resourceset

import (
	"encoding/json"
	"fmt"
	"slices"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

type providerInputs struct {
	name   string
	inputs []map[string]any
}

func isPermute(rs *manifest.ResourceSet) bool {
	return rs.InputStrategy != nil && rs.InputStrategy.Name == fluxopv1.InputStrategyPermute
}

func buildInputSets(rs *manifest.ResourceSet, providers []ProviderInputs) ([]map[string]any, error) {
	groups := make([]providerInputs, 0, 1+len(providers))
	inlineProv := providerBlock(manifest.KindResourceSet, rs.Name, rs.Namespace)
	inline := make([]map[string]any, 0, len(rs.Inputs))
	for _, in := range rs.Inputs {
		decoded := decodeInputSet(in)
		decoded["provider"] = inlineProv
		inline = append(inline, decoded)
	}
	groups = append(groups, providerInputs{name: rs.Name, inputs: inline})
	for _, p := range providers {
		exported := make([]map[string]any, 0, len(p.Inputs))
		for _, input := range p.Inputs {
			exported = append(exported, manifest.DeepCopyMap(input))
		}
		provBlock := providerBlock(p.ID.Kind, p.ID.Name, p.ID.Namespace)
		for _, set := range exported {
			set["provider"] = provBlock
		}
		groups = append(groups, providerInputs{name: p.ID.Name, inputs: exported})
	}

	if isPermute(rs) {
		return permute(groups, rs.InputStrategy.IncludeEmptyProviders)
	}
	total := 0
	for _, g := range groups {
		total += len(g.inputs)
	}
	out := make([]map[string]any, 0, total)
	for _, g := range groups {
		out = append(out, g.inputs...)
	}
	return out, nil
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

func providerBlock(kind, name, namespace string) map[string]any {
	return map[string]any{
		"apiVersion": fluxopv1.GroupVersion.String(),
		"kind":       kind,
		"name":       name,
		"namespace":  namespace,
	}
}

func decodeInputSet(in fluxopv1.ResourceSetInput) map[string]any {
	decoded := make(map[string]any, len(in))
	for k, v := range in {
		if v == nil {
			decoded[k] = nil
			continue
		}
		var raw any
		if err := json.Unmarshal(v.Raw, &raw); err != nil {
			// Malformed entries are skipped silently — the parser
			// already accepted the document, and there's no good
			// signaling channel beyond a controller log line.
			continue
		}
		decoded[k] = raw
	}
	return decoded
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
