package kustomize

import (
	"encoding/base64"
	"testing"

	"github.com/home-operations/flate/internal/engine/values"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestShape_SanitizesBeforeSubstitution(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"inactive", "active", "disabled", "strict"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ks := &manifest.Kustomization{}
			if mode != "inactive" {
				ks.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VAR": "readable"}}
			}
			doc := map[string]any{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata":   map[string]any{"name": "input"},
				"stringData": map[string]any{
					"KEY":        "ENC[AES256_GCM,data:${VAR},iv:y,tag:z,type:str]",
					"UNKNOWN":    "ENC[AES256_GCM,data:${UNDEFINED},iv:y,tag:z,type:str]",
					"BARE":       "ENC[AES256_GCM,data:$PID,iv:y,tag:z,type:str]",
					"TRAILING":   "ENC[AES256_GCM,data:token$,iv:y,tag:z,type:str]",
					"ESCAPED":    "ENC[AES256_GCM,data:$$PID,iv:y,tag:z,type:str]",
					"PLAIN":      "plain",
					"EXPRESSION": "${VAR}",
				},
				"data": map[string]any{
					"ENCODED": base64.StdEncoding.EncodeToString(
						[]byte("ENC[AES256_GCM,data:${UNDEFINED},iv:y,tag:z,type:str]"),
					),
				},
				"sops": map[string]any{"mac": "encrypted"},
			}
			if mode == "disabled" {
				doc["metadata"].(map[string]any)["annotations"] = map[string]any{
					manifest.SubstituteAnnotationKey: "disabled",
				}
			}
			docs := []manifest.DocOccurrence{{Document: doc}}
			if err := PrepareShape(
				&RenderRequest{Spec: ks},
				docs,
			).Apply(t.Context(), ShapeOptions{Strict: mode == "strict"}); err != nil {
				t.Fatal(err)
			}
			wantExpression := "${VAR}"
			if mode == "active" || mode == "strict" {
				wantExpression = "readable"
			}
			if diff := cmp.Diff(map[string]any{
				"KEY":      "ENC[AES256_GCM,data:${VAR},iv:y,tag:z,type:str]",
				"UNKNOWN":  "ENC[AES256_GCM,data:${UNDEFINED},iv:y,tag:z,type:str]",
				"BARE":     "ENC[AES256_GCM,data:$PID,iv:y,tag:z,type:str]",
				"TRAILING": "ENC[AES256_GCM,data:token$,iv:y,tag:z,type:str]",
				"ESCAPED":  "ENC[AES256_GCM,data:$$PID,iv:y,tag:z,type:str]",
				"PLAIN":    "plain", "EXPRESSION": wantExpression,
			}, docs[0].Document["stringData"]); diff != "" {
				t.Fatalf("shaped data (-want +got): %s", diff)
			}
			assert.Diff(t, docs[0].Document["data"], doc["data"])
			raw, err := values.SubstitutionDocument(docs[0], mode != "inactive")
			assert.NoError(t, err)
			names := values.SubstitutionNames(raw)
			assert.Equal(t, len(names), 0)
			assert.FatalIf(t, docs[0].Document["sops"] != nil || len(docs[0].Warnings) != 0,
				"shaped occurrence=%+v", docs[0])
		})
	}
}
