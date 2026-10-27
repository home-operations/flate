package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestValueEqual_Values(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		a, b  any
		equal bool
	}{
		{"nil", nil, nil, true},
		{"nil and map", nil, map[string]any{}, false},
		{"nil and slice", nil, []any{}, false},
		{"nil maps", map[string]any(nil), map[string]any(nil), true},
		{"nil and empty maps", map[string]any(nil), map[string]any{}, false},
		{"nil slices", []any(nil), []any(nil), true},
		{"nil and empty slices", []any(nil), []any{}, false},
		{"empty maps", map[string]any{}, map[string]any{}, true},
		{"empty slices", []any{}, []any{}, true},
		{"map and slice", map[string]any{}, []any{}, false},
		{"missing and nil", map[string]any{}, map[string]any{"key": nil}, false},
		{"different nil keys", map[string]any{"a": nil}, map[string]any{"b": nil}, false},
		{"present nil", map[string]any{"key": nil}, map[string]any{"key": nil}, true},
		{"nested equal", map[string]any{"items": []any{map[string]any{"key": "value"}, int64(1)}}, map[string]any{"items": []any{map[string]any{"key": "value"}, int64(1)}}, true},
		{"nested changed", map[string]any{"items": []any{map[string]any{"key": "value"}}}, map[string]any{"items": []any{map[string]any{"key": "changed"}}}, false},
		{"nested nil and empty", map[string]any{"items": []any(nil)}, map[string]any{"items": []any{}}, false},
		{"slice length", []any{nil}, []any{}, false},
		{"string", "value", "value", true},
		{"changed string", "value", "changed", false},
		{"bool", true, true, true},
		{"changed bool", true, false, false},
		{"int", 1, 1, true},
		{"changed int", 1, 2, false},
		{"int64", int64(1), int64(1), true},
		{"changed int64", int64(1), int64(2), false},
		{"float64", float64(1), float64(1), true},
		{"changed float64", float64(1), float64(2), false},
		{"integer types", 1, int64(1), false},
		{"number types", int64(1), float64(1), false},
		{"string and map", "value", map[string]any{}, false},
		{"fallback slice", []string{"value"}, []string{"value"}, true},
		{"fallback changed slice", []string{"value"}, []string{"other"}, false},
		{"fallback map", map[string]string{"key": "value"}, map[string]string{"key": "value"}, true},
		{"fallback scalar", uint64(1), uint64(1), true},
		{"fallback concrete types", uint64(1), int64(1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, ValueEqual(tc.a, tc.b), tc.equal)
			assert.Equal(t, ValueEqual(tc.b, tc.a), tc.equal)
		})
	}
}

func TestLabelsEqual_Representations(t *testing.T) {
	t.Parallel()
	document := func(labels any) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": labels}}
	}
	app := document(map[string]any{"app": "same"})
	other := document(map[string]any{"app": "other"})
	raw := &RawObject{Spec: app}
	empty := &RawObject{}
	typed := &Kustomization{Labels: map[string]string{"app": "same"}}
	excluded := func(key string) bool { return key == "owner" }
	for _, tc := range []struct {
		name             string
		a, b             BaseManifest
		documents        [2]map[string]any
		excluded         func(string) bool
		forward, reverse bool
	}{
		{name: "typed labels ignore documents", a: typed, b: typed.Clone(), documents: [2]map[string]any{app, other}, forward: true, reverse: true},
		{name: "typed labels ignore exclusions", a: &Kustomization{Labels: map[string]string{"owner": "a"}}, b: &Kustomization{Labels: map[string]string{"owner": "b"}}, excluded: excluded},
		{name: "typed nil and empty labels", a: &Kustomization{}, b: &Kustomization{Labels: map[string]string{}}, forward: true, reverse: true},
		{name: "typed difference despite equal documents", a: typed, b: &Kustomization{}, documents: [2]map[string]any{app, app}},
		{name: "raw inputs", a: raw, b: raw.Clone(), forward: true, reverse: true},
		{name: "raw input and document", a: raw, b: empty, documents: [2]map[string]any{nil, app}, forward: true, reverse: true},
		{name: "raw changed input", a: raw, b: &RawObject{Spec: other}},
		{name: "authoritative empty document", a: raw, b: empty, documents: [2]map[string]any{{}, nil}, forward: true, reverse: true},
		{name: "empty document suppresses raw input", a: raw, b: raw.Clone(), documents: [2]map[string]any{{}, nil}},
		{name: "document overrides raw input", a: raw, b: empty, documents: [2]map[string]any{other, other}, forward: true, reverse: true},
		{name: "documents", a: empty, b: empty, documents: [2]map[string]any{app, app}, forward: true, reverse: true},
		{name: "changed documents", a: empty, b: empty, documents: [2]map[string]any{app, other}},
		{name: "nil and empty labels", a: empty, b: empty, documents: [2]map[string]any{document(nil), document(map[string]any{})}, forward: true, reverse: true},
		{name: "missing metadata", a: empty, b: empty, documents: [2]map[string]any{{}, document(nil)}, forward: true, reverse: true},
		{name: "malformed metadata", a: empty, b: empty, documents: [2]map[string]any{{"metadata": "invalid"}, nil}, forward: true, reverse: true},
		{name: "malformed labels", a: empty, b: empty, documents: [2]map[string]any{document("invalid"), nil}, forward: true, reverse: true},
		{name: "typed metadata map ignored", a: empty, b: empty, documents: [2]map[string]any{{"metadata": map[string]string{"labels": "value"}}, nil}, forward: true, reverse: true},
		{name: "typed label map ignored", a: empty, b: empty, documents: [2]map[string]any{document(map[string]string{"app": "value"}), nil}, forward: true, reverse: true},
		{name: "spec is only a label input", a: &RawObject{Spec: map[string]any{"labels": map[string]any{"app": "same"}}}, b: empty, forward: true, reverse: true},
		{name: "mixed operands without documents", a: empty, b: typed, forward: true},
		{name: "mixed operands with documents", a: raw, b: typed, documents: [2]map[string]any{nil, app}, forward: true},
		{name: "mixed document difference", a: raw, b: typed, documents: [2]map[string]any{nil, other}},
		{name: "non-raw untyped document", a: &ConfigMap{}, b: empty, documents: [2]map[string]any{app, app}, forward: true, reverse: true},
		{name: "excluded label change", a: empty, b: empty, documents: [2]map[string]any{document(map[string]any{"owner": "a"}), document(map[string]any{"owner": "b"})}, excluded: excluded, forward: true, reverse: true},
		{name: "unexcluded label change", a: empty, b: empty, documents: [2]map[string]any{app, other}, excluded: excluded},
		{name: "nested label value", a: empty, b: empty, documents: [2]map[string]any{document(map[string]any{"app": []any{"same"}}), document(map[string]any{"app": []any{"other"}})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, LabelsEqual(tc.a, tc.b, tc.documents, tc.excluded), tc.forward)
			assert.Equal(t, LabelsEqual(tc.b, tc.a, [2]map[string]any{tc.documents[1], tc.documents[0]}, tc.excluded), tc.reverse)
		})
	}
	assert.Diff(t, raw.Spec, document(map[string]any{"app": "same"}))
}

func TestMapEqualExcept_StringValues(t *testing.T) {
	t.Parallel()
	excluded := func(key string) bool { return key == "owner" }
	for _, tc := range []struct {
		name  string
		a, b  map[string]string
		equal bool
	}{
		{"nil and empty", nil, map[string]string{}, true},
		{"equal", map[string]string{"app": "same"}, map[string]string{"app": "same"}, true},
		{"excluded change", map[string]string{"owner": "a"}, map[string]string{"owner": "b"}, true},
		{"excluded extra key", nil, map[string]string{"owner": "a"}, true},
		{"changed value", map[string]string{"app": "a"}, map[string]string{"app": "b"}, false},
		{"reverse-only key", nil, map[string]string{"app": ""}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, MapEqualExcept(tc.a, tc.b, excluded), tc.equal)
			assert.Equal(t, MapEqualExcept(tc.b, tc.a, excluded), tc.equal)
		})
	}
}

func TestMapEqualExcept_JSONValues(t *testing.T) {
	t.Parallel()
	excluded := func(key string) bool { return key == "owner" }
	for _, tc := range []struct {
		name  string
		a, b  map[string]any
		equal bool
	}{
		{"nil and empty", nil, map[string]any{}, true},
		{"equal nested", map[string]any{"app": []any{"same"}}, map[string]any{"app": []any{"same"}}, true},
		{"excluded change", map[string]any{"owner": "a"}, map[string]any{"owner": "b"}, true},
		{"excluded extra key", nil, map[string]any{"owner": "a"}, true},
		{"changed nested", map[string]any{"app": []any{"a"}}, map[string]any{"app": []any{"b"}}, false},
		{"reverse-only key", nil, map[string]any{"app": nil}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, MapEqualExcept(tc.a, tc.b, excluded), tc.equal)
			assert.Equal(t, MapEqualExcept(tc.b, tc.a, excluded), tc.equal)
		})
	}
}

func TestEqual_Allocations(t *testing.T) {
	document := func() map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": "same", "owner": "producer"}}, "items": []any{map[string]any{"value": int64(1)}}}
	}
	a, b := &RawObject{Kind: "Widget", Spec: document()}, &RawObject{Kind: "Widget", Spec: document()}
	documents := [2]map[string]any{document(), document()}
	typedA := &Kustomization{Labels: map[string]string{"app": "same"}}
	typedB := typedA.Clone()
	excluded := func(key string) bool { return key == "owner" }
	for _, tc := range []struct {
		name   string
		equal  func() bool
		allocs float64
	}{
		{"raw content", func() bool { return Equal(a, b) }, 0},
		{"values", func() bool { return ValueEqual(a.Spec, b.Spec) }, 0},
		{"raw labels", func() bool { return LabelsEqual(a, b, [2]map[string]any{}, nil) }, 0},
		{"document labels", func() bool { return LabelsEqual(a, b, documents, nil) }, 0},
		{"excluded labels", func() bool { return LabelsEqual(a, b, documents, excluded) }, 0},
		{"typed labels", func() bool { return LabelsEqual(typedA, typedB, documents, excluded) }, 0},
		{"filtered string maps", func() bool { return MapEqualExcept(typedA.Labels, typedB.Labels, excluded) }, 2},
		{"filtered JSON maps", func() bool { return MapEqualExcept(a.Spec, b.Spec, excluded) }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, func() {
				if !tc.equal() {
					t.Fatal("distinct equal inputs compared unequal")
				}
			})
			assert.Equal(t, allocs, tc.allocs)
		})
	}
}
