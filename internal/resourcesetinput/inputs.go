package resourcesetinput

import (
	"encoding/json"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
)

// Group contains input sets under one provider name.
type Group struct {
	Name   string
	Inputs []map[string]any
}

// Provider identifies an input provider in the template context.
func Provider(kind, name, namespace string) map[string]any {
	return map[string]any{
		"apiVersion": fluxopv1.GroupVersion.String(),
		"kind":       kind,
		"name":       name,
		"namespace":  namespace,
	}
}

// Inline decodes the ResourceSet's inline inputs with their provider context.
func Inline(name, namespace string, inputs []fluxopv1.ResourceSetInput) Group {
	group := Group{Name: name, Inputs: make([]map[string]any, 0, len(inputs))}
	provider := Provider("ResourceSet", name, namespace)
	for _, input := range inputs {
		decoded := make(map[string]any, len(input))
		for key, value := range input {
			if value == nil {
				decoded[key] = nil
				continue
			}
			var raw any
			if json.Unmarshal(value.Raw, &raw) != nil {
				continue
			}
			decoded[key] = raw
		}
		decoded["provider"] = provider
		group.Inputs = append(group.Inputs, decoded)
	}
	return group
}

// Combine applies the requested ResourceSet input composition strategy.
func Combine(groups []Group, strategy *fluxopv1.InputStrategySpec) ([]map[string]any, error) {
	if strategy != nil && strategy.Name == fluxopv1.InputStrategyPermute {
		return Permute(groups, strategy.IncludeEmptyProviders)
	}
	total := 0
	for _, group := range groups {
		total += len(group.Inputs)
	}
	inputs := make([]map[string]any, 0, total)
	for _, group := range groups {
		inputs = append(inputs, group.Inputs...)
	}
	return inputs, nil
}
