package kustomization

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

// containsSubstitution gates whether substituteDoc pays for a full
// YAML marshal/unmarshal round-trip. The check lives as a closure
// passed to manifest.AnyStringLeaf — pin its behavior here.
func TestContainsSubstitution(t *testing.T) {
	hasDollar := func(s string) bool { return strings.Contains(s, "${") }
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
			if got := manifest.AnyStringLeaf(tc.in, hasDollar); got != tc.want {
				t.Errorf("AnyStringLeaf(${) = %v, want %v", got, tc.want)
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
	// Short-circuit returned the input map untouched. Maps are
	// reference types so we can't compare pointer identity via &out;
	// the real assertion is mutation visibility: writing to out must
	// be visible on doc.
	out["data"].(map[string]any)["new"] = "marker"
	if doc["data"].(map[string]any)["new"] != "marker" {
		t.Errorf("expected short-circuit to return the input map; got a copy")
	}
}

// substituteDoc substitutes a `${VAR}` in MAP KEY position, matching
// Flux (which runs envsubst over the serialized YAML text). This is the
// ClusterSecretStore `vaults: { ${OP_VAULT}: 1 }` shape: the only `${`
// in the doc sits in a key and the value is a non-string, so the old
// values-only gate skipped the doc and left the key literal.
func TestSubstituteDoc_SubstitutesMapKey(t *testing.T) {
	doc := map[string]any{
		"apiVersion": "external-secrets.io/v1",
		"kind":       "ClusterSecretStore",
		"spec": map[string]any{
			"provider": map[string]any{
				"onepassword": map[string]any{
					"vaults": map[string]any{"${OP_VAULT}": 1},
				},
			},
		},
	}
	out, err := substituteDoc(doc, map[string]string{"OP_VAULT": "homelab"})
	if err != nil {
		t.Fatalf("substituteDoc: %v", err)
	}
	vaults := out["spec"].(map[string]any)["provider"].(map[string]any)["onepassword"].(map[string]any)["vaults"].(map[string]any)
	if _, stale := vaults["${OP_VAULT}"]; stale {
		t.Errorf("key left unsubstituted; got keys %v", vaults)
	}
	v, ok := vaults["homelab"]
	if !ok {
		t.Fatalf("substituted key missing; got keys %v", vaults)
	}
	// The marshal/unmarshal round-trip still coerces the value even when
	// the trigger was a key — `1` stays an int, not a string.
	if iv, _ := v.(int); iv != 1 {
		t.Errorf("value = %v (%T), want int 1", v, v)
	}
}

// An escaped `$${VAR}` key unescapes to a literal `${VAR}` key — it must
// not be excluded from the round-trip (the gate has to pass so envsubst
// can do the unescaping) and must not be expanded. Matches Flux.
func TestSubstituteDoc_EscapedDollarKey(t *testing.T) {
	doc := map[string]any{
		"data": map[string]any{"$${OP_VAULT}": 1},
	}
	out, err := substituteDoc(doc, map[string]string{"OP_VAULT": "homelab"})
	if err != nil {
		t.Fatalf("substituteDoc: %v", err)
	}
	data := out["data"].(map[string]any)
	if _, ok := data["${OP_VAULT}"]; !ok {
		t.Errorf("escaped key should unescape to literal ${OP_VAULT}; got %v", data)
	}
	if _, expanded := data["homelab"]; expanded {
		t.Errorf("escaped key must not expand; got %v", data)
	}
}

// When a key-position substitution makes two sibling keys collide, the
// re-decode hits a duplicate key. go.yaml.in/yaml/v4 fails loudly on
// that rather than silently last-wins, so substituteDoc surfaces it as
// a render error (the reconcile fails) instead of dropping data —
// documenting the behavior for this pathological misconfiguration.
func TestSubstituteDoc_KeyCollisionErrors(t *testing.T) {
	doc := map[string]any{
		"data": map[string]any{"${OP_VAULT}": 1, "homelab": 2},
	}
	out, err := substituteDoc(doc, map[string]string{"OP_VAULT": "homelab"})
	if err == nil {
		t.Fatalf("expected a duplicate-key error on collision; got out=%#v", out)
	}
	if out != nil {
		t.Errorf("expected nil doc on error; got %#v", out)
	}
	if !strings.Contains(err.Error(), "homelab") || !strings.Contains(err.Error(), "already defined") {
		t.Errorf("error should name the colliding key; got %v", err)
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
