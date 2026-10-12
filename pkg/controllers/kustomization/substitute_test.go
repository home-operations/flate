package kustomization

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/kustomize/api/resource"
	kyaml "sigs.k8s.io/kustomize/kyaml/yaml"
	"sigs.k8s.io/yaml"

	"github.com/google/go-cmp/cmp"
)

func TestSubstituteDoc_PrecheckEquivalence(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  map[string]any
	}{
		{"literal", map[string]any{"data": map[string]any{"key": "literal", "boolean": "yes", "date": "2001-01-01"}}},
		{"nested scalars", map[string]any{"spec": map[string]any{"values": []any{nil, true, 3, 1.5, int64(42)}}}},
		{"negative zero", map[string]any{"value": math.Copysign(0, -1)}},
		{"large signed integer", map[string]any{"value": int(9007199254740993)}},
		{"large unsigned integer", map[string]any{"value": uint64(18446744073709551615)}},
		{"timestamp", map[string]any{"value": time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{"bare dollars", map[string]any{"$PID": []any{"$", "echo $PID", "$- $9", "cost $5"}}},
		{"key reference", map[string]any{"${VAR}": 1}},
		{"value reference", map[string]any{"value": "${VAR}"}},
		{"default reference", map[string]any{"value": "${MISSING:=yes}"}},
		{"escape after bare dollar", map[string]any{"value": "$PID $$PID"}},
		{"nested escape", map[string]any{"spec": []any{map[string]any{"$$PID": "literal"}}}},
		{"infinity", map[string]any{"spec": []any{math.Inf(1)}}},
		{"NaN", map[string]any{"spec": map[string]any{"value": math.NaN()}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vars := map[string]string{"VAR": "example"}
			got, gotErr := substituteDoc(tc.doc, vars, true)
			raw, err := yaml.Marshal(tc.doc)
			if err != nil {
				if gotErr == nil {
					t.Fatalf("precheck error = nil, round-trip error = %v", err)
				}
				return
			}
			node, err := kyaml.Parse(string(raw))
			if err != nil {
				t.Fatal(err)
			}
			res, err := fluxkustomize.SubstituteVariables(t.Context(), nil,
				unstructured.Unstructured{Object: map[string]any{
					"spec": map[string]any{"postBuild": map[string]any{"substitute": map[string]any{"VAR": "example"}}},
				}}, &resource.Resource{RNode: *node},
				fluxkustomize.SubstituteWithDryRun(true), fluxkustomize.SubstituteWithAlways(true), fluxkustomize.SubstituteWithStrict(true))
			if (gotErr != nil) != (err != nil) {
				t.Fatalf("precheck error = %v, Flux error = %v", gotErr, err)
			}
			if err != nil {
				return
			}
			raw, err = res.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(string(raw), string(encoded)); diff != "" {
				t.Errorf("precheck equivalence (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSubstituteDoc_DoubleDollarEscape(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  map[string]any
		want map[string]any
	}{
		{"value", map[string]any{"data": map[string]any{"script": "echo $$PID"}},
			map[string]any{"data": map[string]any{"script": "echo $PID"}}},
		{"key", map[string]any{"data": map[string]any{"$$PID": "literal"}},
			map[string]any{"data": map[string]any{"$PID": "literal"}}},
		{"nested list", map[string]any{"spec": map[string]any{"args": []any{"literal", "echo $$PID"}}},
			map[string]any{"spec": map[string]any{"args": []any{"literal", "echo $PID"}}}},
		{"nested map in list", map[string]any{"spec": map[string]any{"containers": []any{
			map[string]any{"command": "echo $$PID"},
		}}}, map[string]any{"spec": map[string]any{"containers": []any{
			map[string]any{"command": "echo $PID"},
		}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := substituteDoc(tc.doc, map[string]string{"PID": "expanded"}, true)
			if err != nil {
				t.Fatalf("substituteDoc: %v", err)
			}
			if diff := cmp.Diff(tc.want, out); diff != "" {
				t.Errorf("substitution (-want +got):\n%s", diff)
			}
		})
	}
}

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
	out, err := substituteDoc(doc, map[string]string{"OP_VAULT": "homelab"}, false)
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

	if iv, _ := v.(float64); iv != 1 {
		t.Errorf("value = %v (%T), want float64 1", v, v)
	}
}

func TestSubstituteDoc_KeyCollisionLastWins(t *testing.T) {
	doc := map[string]any{"${OP_VAULT}": 1, "homelab": 2}
	out, err := substituteDoc(doc, map[string]string{"OP_VAULT": "homelab"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(map[string]any{"homelab": float64(2)}, out); diff != "" {
		t.Errorf("substitution (-want +got):\n%s", diff)
	}
}

// Escaped references must remain literal even when their variables are defined.
func TestSubstituteDoc_EscapedDollarKey(t *testing.T) {
	doc := map[string]any{
		"data": map[string]any{"$${OP_VAULT}": 1},
	}
	out, err := substituteDoc(doc, map[string]string{"OP_VAULT": "homelab"}, false)
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

func TestSubstituteDoc_PreservesYAMLTypeCoercion(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  any
	}{
		{name: "integer", value: "3", want: float64(3)},
		{name: "date", value: "2001-01-01", want: "2001-01-01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := map[string]any{"spec": map[string]any{"value": "${VALUE}"}}
			out, err := substituteDoc(doc, map[string]string{"VALUE": tc.value}, false)
			if err != nil {
				t.Fatalf("substituteDoc: %v", err)
			}
			if diff := cmp.Diff(tc.want, out["spec"].(map[string]any)["value"]); diff != "" {
				t.Errorf("coercion (-want +got):\n%s", diff)
			}
		})
	}
}
