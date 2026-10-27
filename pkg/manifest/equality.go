package manifest

import (
	"maps"
	"reflect"
	"slices"
)

// Equal compares manifest content without derived projection-cache state.
func Equal(a, b BaseManifest) bool {
	if a == b {
		return true
	}
	if r, ok := a.(*RawObject); ok {
		other, ok := b.(*RawObject)
		return ok && r.equal(other)
	}
	if k, ok := a.(*Kustomization); ok {
		other, ok := b.(*Kustomization)
		return ok && k.equal(other)
	}
	return reflect.DeepEqual(a, b)
}

// ValueEqual compares JSON-shaped values directly, preserving container nilness and scalar types.
// Other values retain reflect.DeepEqual semantics.
func ValueEqual(a, b any) bool {
	switch v := a.(type) {
	case map[string]any:
		other, ok := b.(map[string]any)
		return ok && (v == nil) == (other == nil) && maps.EqualFunc(v, other, ValueEqual)
	case []any:
		other, ok := b.([]any)
		return ok && (v == nil) == (other == nil) && slices.EqualFunc(v, other, ValueEqual)
	case nil, string, bool, int, int64, float64:
		return a == b
	default:
		return reflect.DeepEqual(a, b)
	}
}

// LabelsEqual compares labels with documents[0] for a and documents[1] for b.
// A typed left operand compares typed labels only, ignoring documents and excluded.
// Otherwise only nil documents permit raw-spec label inputs; nil and empty label maps are equal.
func LabelsEqual(a, b BaseManifest, documents [2]map[string]any, excluded func(string) bool) bool {
	type labeled interface{ GetLabels() map[string]string }
	if x, ok := a.(labeled); ok {
		y, ok := b.(labeled)
		return ok && maps.Equal(x.GetLabels(), y.GetLabels())
	}
	al, bl := labels(a, documents[0]), labels(b, documents[1])
	if excluded == nil {
		return maps.EqualFunc(al, bl, ValueEqual)
	}
	return MapEqualExcept(al, bl, excluded)
}

func labels(obj BaseManifest, input map[string]any) map[string]any {
	if raw, ok := obj.(*RawObject); ok && input == nil {
		input = raw.labelInput()
	}
	metadata, _ := input["metadata"].(map[string]any)
	labels, _ := metadata["labels"].(map[string]any)
	return labels
}

// MapEqualExcept compares non-excluded keys and values, treating nil and empty maps equally.
// The excluded predicate MUST be non-nil; values use ValueEqual.
func MapEqualExcept[V any](a, b map[string]V, excluded func(string) bool) bool {
	for key, value := range a {
		if excluded(key) {
			continue
		}
		other, ok := b[key]
		if !ok || !ValueEqual(value, other) {
			return false
		}
	}
	for key := range b {
		if !excluded(key) {
			if _, ok := a[key]; !ok {
				return false
			}
		}
	}
	return true
}
