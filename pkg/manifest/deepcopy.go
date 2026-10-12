package manifest

import (
	"fmt"
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

// FillEmptyValueLeaves returns a deep copy of m with every empty-string or null
// leaf replaced by a stable ..PLACEHOLDER_<key>.. token, and the sorted dotted
// paths it filled. m is not mutated; (nil, nil) for a nil/empty input.
//
// Used by the HelmRelease controller's best-effort rescue: a chart that
// requires a value the offline render left empty — a ${VAR} from a
// substituteFrom Secret flate couldn't read — then templates against the
// placeholder instead of failing schema/template validation. The placeholder
// is keyed on the field name (not the original var, which is gone by render
// time); the returned paths name the filled fields for an advisory. Non-empty
// leaves (including partial concatenations like ".cfargotunnel.com" and numeric
// or boolean values) are left untouched.
func FillEmptyValueLeaves(m map[string]any) (map[string]any, []string) {
	out := DeepCopyMap(m)
	if out == nil {
		return nil, nil
	}
	var paths []string
	for k, v := range out {
		out[k] = fillEmptyLeaf(v, k, k, &paths)
	}
	slices.Sort(paths)
	return out, paths
}

// fillEmptyLeaf walks node, replacing each empty-string / null leaf with
// ..PLACEHOLDER_<name>.. and appending its dotted path. name is the map key
// that named the leaf (slice elements inherit their slice's key); path is the
// full dotted location for the advisory.
func fillEmptyLeaf(node any, name, path string, paths *[]string) any {
	switch t := node.(type) {
	case map[string]any:
		for k, v := range t {
			t[k] = fillEmptyLeaf(v, k, path+"."+k, paths)
		}
		return t
	case []any:
		for i, v := range t {
			t[i] = fillEmptyLeaf(v, name, fmt.Sprintf("%s[%d]", path, i), paths)
		}
		return t
	}
	if node == nil || node == "" {
		*paths = append(*paths, path)
		return fmt.Sprintf(ValuePlaceholderTemplate, name)
	}
	return node
}
