package resourceset

import (
	"encoding/json"
	"fmt"
	"slices"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/home-operations/flate/pkg/manifest"
)

type providerContent struct {
	ID     manifest.NamedResource
	Type   string
	Inputs []map[string]any
}

type providerInputs struct {
	name   string
	inputs []map[string]any
}

func isPermute(rs *manifest.ResourceSet) bool {
	return rs.InputStrategy != nil && rs.InputStrategy.Name == fluxopv1.InputStrategyPermute
}

func buildInputSets(rs *manifest.ResourceSet, providers []providerContent) ([]map[string]any, error) {
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
		exported := p.Inputs
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

func collectProviderInputs(rs *manifest.ResourceSet, resolve ProviderResolver) ([]providerContent, error) {
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
			return nil, fmt.Errorf("%w: inputsFrom %q: %w", manifest.ErrInput, ref.Name, err)
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
	var content []providerContent
	for id := range providers {
		content = append(content, providerContent{ID: id})
	}
	slices.SortFunc(content, func(a, b providerContent) int {
		return a.ID.Compare(b.ID)
	})
	for i := range content {
		p := providers[content[i].ID]
		inputs, err := p.ExportedInputs()
		if err != nil {
			return nil, fmt.Errorf("%w: ResourceSetInputProvider %s: %w", manifest.ErrInput, p.Named().NamespacedName(), err)
		}
		content[i].Type, content[i].Inputs = p.Type, inputs
	}
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
		selector labels.Selector
		err      error
	}
	var firstSelector *metav1.LabelSelector
	var firstCompilation compilation
	var selectors map[*metav1.LabelSelector]compilation
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
		compiled := firstCompilation
		if ref.Selector != firstSelector {
			var ok bool
			compiled, ok = selectors[ref.Selector]
			if !ok {
				compiled.selector, compiled.err = metav1.LabelSelectorAsSelector(ref.Selector)
				if firstSelector == nil {
					firstSelector, firstCompilation = ref.Selector, compiled
				} else {
					if selectors == nil {
						selectors = make(map[*metav1.LabelSelector]compilation)
					}
					selectors[ref.Selector] = compiled
				}
			}
		}
		if compiled.err != nil {
			return nil, compiled.err
		}
		var out []*manifest.ResourceSetInputProvider
		for _, object := range s.ListObjects(id.Group, id.Kind) {
			p, ok := object.(*manifest.ResourceSetInputProvider)
			if ok && p.Namespace == id.Namespace && compiled.selector.Matches(labels.Set(p.Labels)) {
				out = append(out, p)
			}
		}
		return out, nil
	}
}
