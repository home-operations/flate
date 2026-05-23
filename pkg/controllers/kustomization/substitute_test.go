package kustomization

import "testing"

// containsSubstitution gates whether substituteDoc pays for a full
// YAML marshal/unmarshal round-trip. Walks the decoded tree looking
// for any string leaf containing `${`. Test the obvious shapes.
func TestContainsSubstitution(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want bool
	}{
		{"nil", nil, false},
		{"empty map", map[string]any{}, false},
		{"plain scalars only", map[string]any{
			"a": "literal", "b": 3, "c": true,
		}, false},
		{"top-level scalar with ${", map[string]any{
			"a": "${VAR}",
		}, true},
		{"nested in list", map[string]any{
			"data": []any{"first", "${VAR}", "third"},
		}, true},
		{"deeply nested", map[string]any{
			"spec": map[string]any{
				"containers": []any{
					map[string]any{"image": "ghcr.io/x:${TAG}"},
				},
			},
		}, true},
		{"string with dollar but no brace", map[string]any{"a": "price$5"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := containsSubstitution(tc.in); got != tc.want {
				t.Errorf("containsSubstitution = %v, want %v", got, tc.want)
			}
		})
	}
}

// substituteDoc skips the marshal/unmarshal round-trip when no `${` is
// present anywhere — pure pass-through with no allocations from yaml.
func TestSubstituteDoc_NoSubstitutionShortCircuits(t *testing.T) {
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "cm"},
		"data":       map[string]any{"key": "literal"},
	}
	out, err := substituteDoc(doc, map[string]string{"VAR": "x"})
	if err != nil {
		t.Fatalf("substituteDoc: %v", err)
	}
	// Same pointer identity: short-circuit returned the input untouched.
	if &out == &doc {
		// Note: maps are reference types; &doc compares header addresses
		// which are stack-local. The real assertion is that the returned
		// map is the SAME map, which we verify via mutation visibility.
	}
	out["data"].(map[string]any)["new"] = "marker"
	if doc["data"].(map[string]any)["new"] != "marker" {
		t.Errorf("expected short-circuit to return the input map; got a copy")
	}
}

// substituteDoc round-trips through YAML for type-coercion when `${`
// is present — `replicas: ${N}` ends up as int after substitution.
func TestSubstituteDoc_PreservesYAMLTypeCoercion(t *testing.T) {
	doc := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"spec":       map[string]any{"replicas": "${REPLICAS}"},
	}
	out, err := substituteDoc(doc, map[string]string{"REPLICAS": "3"})
	if err != nil {
		t.Fatalf("substituteDoc: %v", err)
	}
	spec := out["spec"].(map[string]any)
	if rep, _ := spec["replicas"].(int); rep != 3 {
		t.Errorf("replicas = %v (%T), want int 3 (yaml round-trip should coerce)", spec["replicas"], spec["replicas"])
	}
}
