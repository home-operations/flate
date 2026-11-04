package kustomize

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestShape_SanitizesBeforeSubstitution(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"inactive", "active", "disabled"} {
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
					"KEY":   "ENC[AES256_GCM,data:${VAR},iv:y,tag:z,type:str]",
					"PLAIN": "plain",
				},
				"sops": map[string]any{"mac": "encrypted"},
			}
			if mode == "disabled" {
				doc["metadata"].(map[string]any)["annotations"] = map[string]any{
					manifest.SubstituteAnnotationKey: "disabled",
				}
			}
			docs := []manifest.DocOccurrence{{Document: doc}}
			if err := Shape(t.Context(), ks, docs, ShapeOptions{}); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(map[string]any{"PLAIN": "plain"}, docs[0].Document["stringData"]); diff != "" {
				t.Fatalf("shaped data (-want +got): %s", diff)
			}
			assert.FatalIf(t, docs[0].Document["sops"] != nil || len(docs[0].Warnings) != 1,
				"shaped occurrence=%+v", docs[0])
		})
	}
}
