package manifest

import "strings"

// AnyStringLeaf reports whether any string leaf of v satisfies pred.
// Walks nested maps and slices once, inspecting only map values (not
// keys). Used to detect features in decoded YAML trees (wipe-
// placeholders for schema-skip, etc.) without paying for a marshal
// round-trip. For substitution gating, use AnyStringNode — Flux
// applies postBuild substitution to the serialized YAML text, where a
// map key is just another token, so a key-only `${VAR}` must not be
// missed.
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

// AnyStringNode reports whether any string node of v satisfies pred —
// like AnyStringLeaf, but the map case also tests the KEY, not just the
// value. This is the correct gate for postBuild substitution: Flux runs
// drone/envsubst over the rendered YAML *text*, so a `${VAR}` in key
// position (e.g. `${OP_VAULT}: 1`) is substituted exactly like one in
// value position. A values-only walk would skip a doc whose only
// reference sits in a key (with a non-string value), leaving the var
// literal and diverging from Flux.
func AnyStringNode(v any, pred func(string) bool) bool {
	switch t := v.(type) {
	case string:
		return pred(t)
	case map[string]any:
		for k, vv := range t {
			if pred(k) || AnyStringNode(vv, pred) {
				return true
			}
		}
	case []any:
		for _, vv := range t {
			if AnyStringNode(vv, pred) {
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
