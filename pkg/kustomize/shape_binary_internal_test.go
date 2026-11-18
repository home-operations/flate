package kustomize

import (
	"encoding/json"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

func TestShape_BinarySubstitutionFraming(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		input    string
		decoded  map[string]any
		shaped   map[string]any
		prepared bool
		output   string
	}{
		{
			name:  "binary opaque dollar",
			input: "apiVersion: v1\nkind: ConfigMap\ndata:\n  TOKEN: !!binary RU5DW0FFUzI1Nl9HQ00sZGF0YTr/JFRPS0VOLGl2OnksdGFnOnosdHlwZTpzdHJd\n",
			decoded: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:\xff$TOKEN,iv:y,tag:z,type:str]"},
			},
			shaped: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:�$$TOKEN,iv:y,tag:z,type:str]"},
			},
			prepared: true,
			output:   `{"apiVersion":"v1","data":{"TOKEN":"ENC[AES256_GCM,data:�$$TOKEN,iv:y,tag:z,type:str]"},"kind":"ConfigMap"}`,
		},
		{
			name:  "binary value with opaque dollar",
			input: "apiVersion: v1\nkind: ConfigMap\ndata:\n  TOKEN: ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]\n  RAW: !!binary /w==\n",
			decoded: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]", "RAW": "\xff"},
			},
			shaped: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]", "RAW": "�"},
			},
			prepared: true,
			output:   `{"apiVersion":"v1","data":{"RAW":"�","TOKEN":"ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]"},"kind":"ConfigMap"}`,
		},
		{
			name:  "binary key normalization",
			input: "apiVersion: v1\nkind: ConfigMap\ndata:\n  TOKEN: ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]\n!!binary /w==: binary\n",
			decoded: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]"},
				"\xff": "binary",
			},
			shaped: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"data": map[string]any{"TOKEN": "ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]"},
				"�":    "binary",
			},
			prepared: true,
			output:   `{"apiVersion":"v1","data":{"TOKEN":"ENC[AES256_GCM,data:$TOKEN,iv:y,tag:z,type:str]"},"kind":"ConfigMap","�":"binary"}`,
		},
		{
			name:  "binary without projected syntax",
			input: "apiVersion: v1\nkind: ConfigMap\ndata:\n  RAW: !!binary /w==\n",
			decoded: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap", "data": map[string]any{"RAW": "\xff"},
			},
			output: `{"apiVersion":"v1","data":{"RAW":"�"},"kind":"ConfigMap"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			docs, err := manifest.DecodeDocs(strings.NewReader(tc.input), manifest.RequiredDocuments)
			assert.NoError(t, err)
			assert.FatalIf(t, len(docs) != 1, "decoded %d documents", len(docs))
			assert.NoError(t, docs[0].Err)
			assert.Diff(t, docs[0].Document, tc.decoded)
			data, err := values.SubstitutionDocument(docs[0], true)
			assert.NoError(t, err)
			assert.Equal(t, data != nil, tc.prepared)
			names := values.SubstitutionNames(data)
			assert.Equal(t, names != nil, true)
			assert.Equal(t, len(names), 0)
			assert.Diff(t, docs[0].Document, tc.decoded)
			ks := &manifest.Kustomization{PostBuild: &kustomizev1.PostBuild{
				Substitute: map[string]string{"OTHER": "readable"},
			}}
			shape := PrepareShape(&RenderRequest{Spec: ks}, docs)
			assert.NoError(t, shape.Apply(t.Context(), ShapeOptions{Strict: true}))
			want := tc.shaped
			if want == nil {
				want = tc.decoded
			}
			assert.Diff(t, docs[0].Document, want)
			encoded, err := json.Marshal(docs[0].Document)
			assert.NoError(t, err)
			assert.Equal(t, string(encoded), tc.output)
			assert.Equal(t, docs[0].Ordinal, 0)
			assert.Equal(t, len(docs[0].ConsumedUnreadable), 0)
		})
	}
}
