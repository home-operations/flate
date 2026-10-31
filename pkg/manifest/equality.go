package manifest

import (
	"maps"
	"reflect"
	"slices"
	"strings"
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
	if h, ok := a.(*HelmRelease); ok {
		other, ok := b.(*HelmRelease)
		return ok && h.equal(other)
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

// OutputEqual compares authoritative desired documents after removing known reconciliation-only leaves.
// Unknown fields remain significant; supplied documents preserve fields outside typed specs.
func OutputEqual(a, b BaseManifest, documents [2]map[string]any) (bool, error) {
	if a == nil || b == nil || a.Named() != b.Named() {
		return a == b, nil
	}
	policy, ok := a.(interface{ outputFields() string })
	if !ok {
		return Equal(a, b) && ValueEqual(documents[0], documents[1]), nil
	}
	for side, object := range [2]BaseManifest{a, b} {
		doc := documents[side]
		if doc == nil {
			var err error
			doc, err = ObjectDocument(object.Named(), object)
			if err != nil {
				return false, err
			}
		}
		documents[side] = DeepCopyMap(doc)
		spec, _ := documents[side]["spec"].(map[string]any)
		for field := range strings.FieldsSeq(policy.outputFields()) {
			removeOutputField(spec, strings.Split(field, "."))
		}
	}
	return ValueEqual(documents[0], documents[1]), nil
}

func removeOutputField(value any, path []string) bool {
	switch value := value.(type) {
	case map[string]any:
		if len(path) == 1 {
			delete(value, path[0])
		} else if child, ok := value[path[0]]; ok && removeOutputField(child, path[1:]) {
			delete(value, path[0])
		}
		return len(value) == 0
	case []any:
		if len(path) == 1 && path[0] == "*" {
			return true
		}
		empty := true
		for _, child := range value {
			empty = removeOutputField(child, path) && empty
		}
		return empty
	}
	return false
}
