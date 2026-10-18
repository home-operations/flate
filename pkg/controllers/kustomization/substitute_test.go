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
	t.Parallel()
	for _, tc := range []struct {
		name   string
		doc    map[string]any
		vars   map[string]string
		strict bool
		want   map[string]any
	}{
		{"literal", map[string]any{"data": map[string]any{"key": "literal", "boolean": "yes", "date": "2001-01-01"}}, map[string]string{"VAR": "example"}, true, nil},
		{"nested scalars", map[string]any{"spec": map[string]any{"values": []any{nil, true, 3, 1.5, int64(42)}}}, map[string]string{"VAR": "example"}, true, nil},
		{"negative zero", map[string]any{"value": math.Copysign(0, -1)}, map[string]string{"VAR": "example"}, true, nil},
		{"large signed integer", map[string]any{"value": int(9007199254740993)}, map[string]string{"VAR": "example"}, true, nil},
		{"large unsigned integer", map[string]any{"value": uint64(18446744073709551615)}, map[string]string{"VAR": "example"}, true, nil},
		{"timestamp", map[string]any{"value": time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)}, map[string]string{"VAR": "example"}, true, nil},
		{"bare dollars", map[string]any{"$PID": []any{"$", "echo $PID", "$- $9", "cost $5"}}, map[string]string{"VAR": "example"}, true, nil},
		{"key reference", map[string]any{"${VAR}": 1}, map[string]string{"VAR": "example"}, true, nil},
		{"value reference", map[string]any{"value": "${VAR}"}, map[string]string{"VAR": "example"}, true, nil},
		{"default reference", map[string]any{"value": "${MISSING:=yes}"}, map[string]string{"VAR": "example"}, true, nil},
		{"escape after bare dollar", map[string]any{"value": "$PID $$PID"}, map[string]string{"VAR": "example"}, true, nil},
		{"nested escape", map[string]any{"spec": []any{map[string]any{"$$PID": "literal"}}}, map[string]string{"VAR": "example"}, true, nil},
		{"infinity", map[string]any{"spec": []any{math.Inf(1)}}, map[string]string{"VAR": "example"}, true, nil},
		{"NaN", map[string]any{"spec": map[string]any{"value": math.NaN()}}, map[string]string{"VAR": "example"}, true, nil},
		{"double dollar value", map[string]any{"data": map[string]any{"script": "echo $$PID"}},
			map[string]string{"PID": "expanded"}, true, map[string]any{"data": map[string]any{"script": "echo $PID"}}},
		{"double dollar key", map[string]any{"data": map[string]any{"$$PID": "literal"}},
			map[string]string{"PID": "expanded"}, true, map[string]any{"data": map[string]any{"$PID": "literal"}}},
		{"double dollar nested list", map[string]any{"spec": map[string]any{"args": []any{"literal", "echo $$PID"}}},
			map[string]string{"PID": "expanded"}, true, map[string]any{"spec": map[string]any{"args": []any{"literal", "echo $PID"}}}},
		{"double dollar nested map in list", map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"command": "echo $$PID"}}}},
			map[string]string{"PID": "expanded"}, true, map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"command": "echo $PID"}}}}},
		{"substituted map key", map[string]any{
			"apiVersion": "external-secrets.io/v1", "kind": "ClusterSecretStore",
			"spec": map[string]any{"provider": map[string]any{"onepassword": map[string]any{"vaults": map[string]any{"${OP_VAULT}": 1}}}},
		}, map[string]string{"OP_VAULT": "homelab"}, false, map[string]any{
			"apiVersion": "external-secrets.io/v1", "kind": "ClusterSecretStore",
			"spec": map[string]any{"provider": map[string]any{"onepassword": map[string]any{"vaults": map[string]any{"homelab": float64(1)}}}},
		}},
		{"key collision last wins", map[string]any{"${OP_VAULT}": 1, "homelab": 2},
			map[string]string{"OP_VAULT": "homelab"}, false, map[string]any{"homelab": float64(2)}},
		{"escaped dollar key", map[string]any{"data": map[string]any{"$${OP_VAULT}": 1}},
			map[string]string{"OP_VAULT": "homelab"}, false, map[string]any{"data": map[string]any{"${OP_VAULT}": float64(1)}}},
		{"integer coercion", map[string]any{"spec": map[string]any{"value": "${VALUE}"}},
			map[string]string{"VALUE": "3"}, false, map[string]any{"spec": map[string]any{"value": float64(3)}}},
		{"date coercion", map[string]any{"spec": map[string]any{"value": "${VALUE}"}},
			map[string]string{"VALUE": "2001-01-01"}, false, map[string]any{"spec": map[string]any{"value": "2001-01-01"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, gotErr := substituteDoc(tc.doc, tc.vars, tc.strict)
			if tc.want != nil {
				if gotErr != nil {
					t.Fatalf("substituteDoc: %v", gotErr)
				}
				if diff := cmp.Diff(tc.want, got); diff != "" {
					t.Errorf("substitution (-want +got):\n%s", diff)
				}
			}
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
			vars := make(map[string]any, len(tc.vars))
			for key, value := range tc.vars {
				vars[key] = value
			}
			res, err := fluxkustomize.SubstituteVariables(t.Context(), nil,
				unstructured.Unstructured{Object: map[string]any{
					"spec": map[string]any{"postBuild": map[string]any{"substitute": vars}},
				}}, &resource.Resource{RNode: *node},
				fluxkustomize.SubstituteWithDryRun(true), fluxkustomize.SubstituteWithAlways(true), fluxkustomize.SubstituteWithStrict(tc.strict))
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
