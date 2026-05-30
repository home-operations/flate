package helm

import (
	"testing"

	"helm.sh/helm/v4/pkg/chart/common"
	"helm.sh/helm/v4/pkg/chart/common/util"
	chart "helm.sh/helm/v4/pkg/chart/v2"
)

// TestValidateChartSchema_MatchesHelm pins that flate's cached schema
// validation (schema.go) is byte-for-byte equivalent to helm's own
// per-render path: same pass/fail, same surfaced error message. This is
// the behavior-preservation contract behind compiling each chart's schema
// once instead of per render.
func TestValidateChartSchema_MatchesHelm(t *testing.T) {
	schema := []byte(`{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type": "object",
		"properties": {"replicas": {"type": "integer", "minimum": 1}},
		"required": ["replicas"]
	}`)
	mkChart := func() *chart.Chart {
		return &chart.Chart{
			Metadata: &chart.Metadata{Name: "demo", Version: "0.1.0"},
			Schema:   schema,
		}
	}

	cases := []struct {
		name string
		vals map[string]any
	}{
		{"valid", map[string]any{"replicas": int64(2)}},
		{"missing_required", map[string]any{}},
		{"wrong_type", map[string]any{"replicas": "two"}},
		{"below_minimum", map[string]any{"replicas": int64(0)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{}
			flateErr := c.validateChartSchema(mkChart(), tc.vals)
			_, helmErr := util.ToRenderValuesWithSchemaValidation(
				mkChart(), tc.vals,
				common.ReleaseOptions{Name: "r", Namespace: "d", IsInstall: true},
				nil, false,
			)

			if (flateErr == nil) != (helmErr == nil) {
				t.Fatalf("nil-ness mismatch: flate=%v helm=%v", flateErr, helmErr)
			}
			if flateErr != nil && flateErr.Error() != helmErr.Error() {
				t.Errorf("error message mismatch:\n flate: %q\n helm:  %q", flateErr.Error(), helmErr.Error())
			}
		})
	}
}

// TestCompileSchema_CachesSingleFlight pins that a schema compiles once and
// is reused: the same bytes return the same *jsonschema.Schema pointer.
func TestCompileSchema_CachesSingleFlight(t *testing.T) {
	c := &Client{}
	schema := []byte(`{"type":"object"}`)
	a, err := c.compileSchema(schema)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	b, err := c.compileSchema([]byte(`{"type":"object"}`))
	if err != nil {
		t.Fatalf("compile (2): %v", err)
	}
	if a != b {
		t.Errorf("expected cached compile to return the same *jsonschema.Schema, got distinct pointers")
	}
}
