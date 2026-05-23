package manifest

import "strings"

// AnyStringLeaf reports whether any string leaf of v satisfies pred.
// Walks nested maps and slices once. Used to detect features in
// decoded YAML trees (`${VAR}` references for substitution gating,
// wipe-placeholders for schema-skip, etc.) without paying for a
// marshal round-trip.
func AnyStringLeaf(v any, pred func(string) bool) bool {
	switch t := v.(type) {
	case string:
		return pred(t)
	case map[string]any:
		for _, vv := range t {
			if AnyStringLeaf(vv, pred) {
				return true
			}
		}
	case []any:
		for _, vv := range t {
			if AnyStringLeaf(vv, pred) {
				return true
			}
		}
	}
	return false
}

// ContainsValuePlaceholder reports whether v contains a wipe-placeholder
// string leaf — i.e. flate fabricated this value during secret wiping
// rather than receiving it from the user.
func ContainsValuePlaceholder(v any) bool {
	return AnyStringLeaf(v, func(s string) bool {
		return strings.Contains(s, ValuePlaceholderPrefix)
	})
}

// IsValuePlaceholder reports whether s itself is or contains a wipe
// placeholder. Different from a HasPrefix check — a value like
// `registry...PLACEHOLDER_DOMAIN..` (envsubst concat) still trips this.
func IsValuePlaceholder(s string) bool {
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
