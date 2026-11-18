package values

import (
	"math"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSubstitutionNames_UnknownNode(t *testing.T) {
	t.Parallel()
	assert.FatalIf(
		t,
		collectSubstitutionNames(nil, make(map[string]struct{})),
		"%s",
		"unknown node proved no variable dependencies",
	)
}

func TestSubstitutionNames_ExcludedAndUnproved(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		doc            map[string]any
		active, proven bool
		prepared       bool
		needs          bool
		names          []string
	}{
		{name: "inactive", doc: map[string]any{"value": "${ARR[@]}"}, proven: true, needs: true},
		{
			name: "disabled",
			doc: map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"kustomize.toolkit.fluxcd.io/substitute": "disabled"}},
				"value":    "${ARR[@]}",
			},
			active: true,
			proven: true,
			needs:  true,
		},
		{name: "invalid expression", doc: map[string]any{"value": "${ARR[@]}"}, active: true, prepared: true, needs: true},
		{name: "no round trip", doc: map[string]any{"value": "literal"}, active: true, proven: true},
		{name: "scalar round trip", doc: map[string]any{"value": math.Copysign(0, -1)}, active: true, proven: true, prepared: true, needs: true},
		{
			name: "opaque bare dollar",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]"},
			},
			active: true, proven: true,
		},
		{
			name: "opaque trailing dollar",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:token$,iv:y,tag:z,type:str]"},
			},
			active: true, proven: true,
		},
		{
			name: "opaque dollar with int64 framing",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data":  map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]"},
				"value": int64(9007199254740993),
			},
			active: true, proven: true, prepared: true,
		},
		{
			name: "opaque dollar with timestamp framing",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data":  map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]"},
				"value": time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			active: true, proven: true, prepared: true,
		},
		{
			name:   "custom representation without syntax",
			doc:    map[string]any{"value": countedSubstitutionYAML{count: new(int)}},
			active: true, proven: true,
		},
		{
			name: "opaque dollar with custom representation",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data":  map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]"},
				"value": countedSubstitutionYAML{count: new(int)},
			},
			active: true, proven: true, prepared: true, names: []string{"VALUE"},
		},
		{
			name:   "required after inspection",
			doc:    map[string]any{"value": []any{int64(42), "$$PID"}},
			active: true, proven: true, prepared: true, needs: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := SubstitutionDocument(manifest.DocOccurrence{Document: tc.doc}, tc.active)
			assert.NoError(t, err)
			assert.Equal(t, raw != nil, tc.prepared)
			assert.Equal(t, needsSubstitution(tc.doc), tc.needs)
			names := SubstitutionNames(raw)
			assert.FatalIf(
				t,
				(names != nil) != tc.proven || len(names) != len(tc.names),
				"names = %v, proven = %t, want %t",
				names,
				names != nil,
				tc.proven,
			)
			for _, name := range tc.names {
				_, present := names[name]
				assert.Equal(t, present, true)
			}
		})
	}
}

func TestNeedsSubstitution_BinaryFraming(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		doc  map[string]any
	}{
		{
			name: "binary opaque dollar",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:\xff$TOKEN,iv:y,tag:z,type:str]"},
			},
		},
		{
			name: "binary value with opaque dollar",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]", "RAW": "\xff"},
			},
		},
		{
			name: "binary key normalization",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]"},
				"\xff": "binary",
			},
		},
		{
			name: "binary without projected syntax",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap", "data": map[string]any{"RAW": "\xff"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, needsSubstitution(tc.doc), false)
		})
	}
}

type countedSubstitutionYAML struct{ count *int }

func (c countedSubstitutionYAML) MarshalYAML() (any, error) {
	*c.count++
	return "${VALUE}", nil
}
