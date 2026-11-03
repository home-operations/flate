package kustomize

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/fluxcd/pkg/envsubst"
	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	yamlv3 "go.yaml.in/yaml/v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/kustomize/api/resource"
	kyaml "sigs.k8s.io/kustomize/kyaml/yaml"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"

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
		{"supplied default", map[string]any{"value": "${ENV:=prod}"}, map[string]string{"ENV": "stage"}, true, map[string]any{"value": "stage"}},
		{"nested default", map[string]any{"value": "${ENV:=${OTHER:=prod}}"}, map[string]string{"OTHER": "stage"}, true, map[string]any{"value": "stage"}},
		{"escaped default", map[string]any{"value": "$${ENV:=prod}"}, map[string]string{"ENV": "stage"}, true, map[string]any{"value": "${ENV:=prod}"}},
		{"colon question upstream control", map[string]any{"value": "${MISSING:?required}"}, nil, true, map[string]any{"value": "required"}},
		{"eager strict operand", map[string]any{"value": "${SET:=${MISSING}}"}, map[string]string{"SET": "present"}, true, nil},
		{
			"replacement is not recursive",
			map[string]any{"value": "${SET}"},
			map[string]string{"SET": "${MISSING}"},
			true,
			map[string]any{"value": "${MISSING}"},
		},
		{"strict missing bare", map[string]any{"value": "${MISSING}"}, nil, true, nil},
		{"default reference", map[string]any{"value": "${MISSING:=yes}"}, map[string]string{"VAR": "example"}, true, nil},
		{"escape after bare dollar", map[string]any{"value": "$PID $$PID"}, map[string]string{"VAR": "example"}, true, nil},
		{"nested escape", map[string]any{"spec": []any{map[string]any{"$$PID": "literal"}}}, map[string]string{"VAR": "example"}, true, nil},
		{"infinity", map[string]any{"spec": []any{math.Inf(1)}}, map[string]string{"VAR": "example"}, true, nil},
		{"NaN", map[string]any{"spec": map[string]any{"value": math.NaN()}}, map[string]string{"VAR": "example"}, true, nil},
		{
			"double dollar value",
			map[string]any{"data": map[string]any{"script": "echo $$PID"}},
			map[string]string{"PID": "expanded"},
			true,
			map[string]any{"data": map[string]any{"script": "echo $PID"}},
		},
		{
			"double dollar key",
			map[string]any{"data": map[string]any{"$$PID": "literal"}},
			map[string]string{"PID": "expanded"},
			true,
			map[string]any{"data": map[string]any{"$PID": "literal"}},
		},
		{
			"double dollar nested list",
			map[string]any{"spec": map[string]any{"args": []any{"literal", "echo $$PID"}}},
			map[string]string{"PID": "expanded"},
			true,
			map[string]any{"spec": map[string]any{"args": []any{"literal", "echo $PID"}}},
		},
		{
			"double dollar nested map in list",
			map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"command": "echo $$PID"}}}},
			map[string]string{"PID": "expanded"},
			true,
			map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"command": "echo $PID"}}}},
		},
		{"substituted map key", map[string]any{
			"apiVersion": "external-secrets.io/v1", "kind": "ClusterSecretStore",
			"spec": map[string]any{"provider": map[string]any{"onepassword": map[string]any{"vaults": map[string]any{"${OP_VAULT}": 1}}}},
		}, map[string]string{"OP_VAULT": "homelab"}, false, map[string]any{
			"apiVersion": "external-secrets.io/v1", "kind": "ClusterSecretStore",
			"spec": map[string]any{"provider": map[string]any{"onepassword": map[string]any{"vaults": map[string]any{"homelab": float64(1)}}}},
		}},
		{
			"key collision last wins",
			map[string]any{"${OP_VAULT}": 1, "homelab": 2},
			map[string]string{"OP_VAULT": "homelab"},
			false,
			map[string]any{"homelab": float64(2)},
		},
		{
			"escaped dollar key",
			map[string]any{"data": map[string]any{"$${OP_VAULT}": 1}},
			map[string]string{"OP_VAULT": "homelab"},
			false,
			map[string]any{"data": map[string]any{"${OP_VAULT}": float64(1)}},
		},
		{
			"integer coercion",
			map[string]any{"spec": map[string]any{"value": "${VALUE}"}},
			map[string]string{"VALUE": "3"},
			false,
			map[string]any{"spec": map[string]any{"value": float64(3)}},
		},
		{
			"date coercion",
			map[string]any{"spec": map[string]any{"value": "${VALUE}"}},
			map[string]string{"VALUE": "2001-01-01"},
			false,
			map[string]any{"spec": map[string]any{"value": "2001-01-01"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSubstituteDocPrecheckEquivalenceCase(t, &tc)
		},
		)
	}
}

func TestSubstitutionNames_EvaluatorReads(t *testing.T) {
	t.Parallel()
	for _, expression := range []string{
		"literal", "${USED}", "${MISSING:=fallback}", "${USED:=${OTHER:=fallback}}",
		"${SET:=${MISSING}}", "${#USED}", "${USED#${PREFIX}}", "${USED%${SUFFIX}}",
		"${USED/${SEARCH}/${REPLACEMENT}}", "${USED:${START}:${LENGTH}}", "$${ESCAPED}", "${USED^^}",
	} {
		t.Run(expression, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"${KEY}": expression}
			raw, err := yamlv3.Marshal(doc)
			{
				assert.NoError(t, err)
			}
			want := make(map[string]struct{})
			_, err = envsubst.Eval(string(raw), func(name string) (string, bool) {
				want[name] = struct{}{}
				return "1", true
			})
			{
				assert.NoError(t, err)
			}
			{
				diff := cmp.Diff(
					want,
					values.SubstitutionNames([]manifest.DocOccurrence{{Document: doc}}, true),
				)
				assert.ErrorIf(t, diff != "", "names (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSubstitutionNames_ExcludedAndUnproved(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		doc            map[string]any
		active, proven bool
	}{
		{name: "inactive", doc: map[string]any{"value": "${ARR[@]}"}, proven: true},
		{
			name: "disabled",
			doc: map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"kustomize.toolkit.fluxcd.io/substitute": "disabled"}},
				"value":    "${ARR[@]}",
			},
			active: true,
			proven: true,
		},
		{name: "invalid expression", doc: map[string]any{"value": "${ARR[@]}"}, active: true},
		{name: "no round trip", doc: map[string]any{"value": "literal"}, active: true, proven: true},
		{name: "scalar round trip", doc: map[string]any{"value": math.Copysign(0, -1)}, active: true, proven: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			names := values.SubstitutionNames([]manifest.DocOccurrence{{Document: tc.doc}}, tc.active)
			assert.FatalIf(
				t,
				(names != nil) != tc.proven || len(names) != 0,
				"names = %v, proven = %t, want %t",
				names,
				names != nil,
				tc.proven,
			)
		})
	}
}

func TestRenderRequestContent_UnprovedAlways(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		postBuild *kustomizev1.PostBuild
		invalid   bool
	}{
		{name: "nil PostBuild"},
		{name: "Always empty", postBuild: &kustomizev1.PostBuild{SubstituteStrategy: kustomizev1.SubstituteStrategyAlways}, invalid: true},
		{
			name: "Always whole bag",
			postBuild: &kustomizev1.PostBuild{
				SubstituteStrategy: kustomizev1.SubstituteStrategyAlways,
				Substitute:         map[string]string{"OTHER": "first"},
			},
			invalid: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderRequestContentUnprovedAlwaysCase(t, &tc)
		},
		)
	}
}

func testSubstituteDocPrecheckEquivalenceCase(
	t *testing.T,
	testCaseInput *struct {
		name   string
		doc    map[string]any
		vars   map[string]string
		strict bool
		want   map[string]any
	},
) {
	t.Helper()

	got, gotErr := substituteDoc(testCaseInput.doc, testCaseInput.vars, testCaseInput.strict)
	if testCaseInput.want != nil {
		{
			assert.FatalIf(t, gotErr != nil, "substituteDoc: %v", gotErr)
		}
		{
			diff := cmp.Diff(testCaseInput.want, got)
			assert.ErrorIf(t, diff != "", "substitution (-want +got):\n%s", diff)
		}
	}
	raw, err := yaml.Marshal(testCaseInput.doc)
	if err != nil {
		{
			assert.FatalIf(t, gotErr == nil, "precheck error = nil, round-trip error = %v", err)
		}
		return
	}
	node, err := kyaml.Parse(string(raw))
	{
		assert.NoError(t, err)
	}
	vars := make(map[string]any, len(testCaseInput.vars))
	for key, value := range testCaseInput.vars {
		vars[key] = value
	}
	res, err := fluxkustomize.SubstituteVariables(
		t.Context(),
		nil,
		unstructured.Unstructured{Object: map[string]any{
			"spec": map[string]any{"postBuild": map[string]any{"substitute": vars}},
		}},
		&resource.Resource{RNode: *node},
		fluxkustomize.SubstituteWithDryRun(
			true,
		),
		fluxkustomize.SubstituteWithAlways(true),
		fluxkustomize.SubstituteWithStrict(testCaseInput.strict),
	)
	{
		assert.FatalIf(t, (gotErr != nil) != (err != nil), "precheck error = %v, Flux error = %v", gotErr, err)
	}
	if err != nil {
		return
	}
	raw, err = res.MarshalJSON()
	{
		assert.NoError(t, err)
	}
	encoded, err := json.Marshal(got)
	{
		assert.NoError(t, err)
	}
	{
		diff := cmp.Diff(string(raw), string(encoded))
		assert.ErrorIf(t, diff != "", "precheck equivalence (-want +got):\n%s", diff)
	}
}

func testRenderRequestContentUnprovedAlwaysCase(
	t *testing.T,
	testCaseInput *struct {
		name      string
		postBuild *kustomizev1.PostBuild
		invalid   bool
	},
) {
	t.Helper()

	ks := &manifest.Kustomization{Name: "app", Namespace: "apps", PostBuild: testCaseInput.postBuild}
	docs := []manifest.DocOccurrence{
		{
			Document: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "result"},
				"data":       map[string]any{"value": "${ARR[@]}"},
			},
		},
	}
	req := RenderRequest{Spec: ks, Source: &store.SourceArtifact{}}
	content := req.Content(docs)
	before := manifest.Fingerprint(content)
	if testCaseInput.postBuild != nil && len(testCaseInput.postBuild.Substitute) != 0 {
		next := ks.Clone()
		next.PostBuild.Substitute["OTHER"] = "second"
		req.Spec = next
		{
			assert.FatalIf(
				t,
				manifest.Fingerprint(req.Content(docs)) == before,
				"%s",
				"unproved syntax omitted the conservative whole bag",
			)
		}
	}
	err := Shape(ks, docs, false)
	assert.FatalIf(
		t,
		errors.Is(err, manifest.ErrInvalidSubstituteReference) != testCaseInput.invalid,
		"Shape error = %v, want invalid substitution %t",
		err,
		testCaseInput.invalid,
	)
	{
		assert.FatalIf(t, manifest.Fingerprint(content) != before, "%s", "Shape mutated the captured content")
	}
}
