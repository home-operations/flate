package manifest

import (
	"slices"
	"strings"
)

// ContainsValuePlaceholder reports whether any string value in v contains a
// wipe placeholder. Keys do not affect Helm's schema-validation decision.
func ContainsValuePlaceholder(v any) bool {
	switch t := v.(type) {
	case string:
		return HasValuePlaceholder(t)
	case map[string]any:
		for _, vv := range t {
			if ContainsValuePlaceholder(vv) {
				return true
			}
		}
	case []any:
		if slices.ContainsFunc(t, ContainsValuePlaceholder) {
			return true
		}
	}
	return false
}

// HasValuePlaceholder reports whether s contains a wipe placeholder, including
// one embedded in a concatenated value such as "registry...PLACEHOLDER_DOMAIN..".
func HasValuePlaceholder(s string) bool {
	return strings.Contains(s, ValuePlaceholderPrefix)
}

// DeepCopyMap returns a deep copy of m suitable for in-place mutation
// without aliasing the source. Walks nested maps and slices; scalars
// are copied by value. Used by Kustomization.Clone / HelmRelease.Clone
// to isolate render-time mutations from the canonical store-owned
// state.
func DeepCopyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return DeepCopyMap(t)
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = deepCopyValue(vv)
		}
		return out
	}
	return v
}
