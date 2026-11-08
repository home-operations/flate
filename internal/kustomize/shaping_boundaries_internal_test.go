package kustomize

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestShaping_ReuseBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		before, after bool
		first, second string
		want          string
	}{
		{"changed_output", true, true, "${LEFT}", "${RIGHT}", "right"},
		{"activation", false, true, "${RIGHT}", "${RIGHT}", "right"},
		{"deactivation", true, false, "${RIGHT}", "${RIGHT}", "${RIGHT}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			documents := func(value string) []manifest.DocOccurrence {
				return []manifest.DocOccurrence{{Document: map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{"name": "input"},
					"data":     map[string]any{"value": value},
				}}}
			}
			spec := func(active bool) *manifest.Kustomization {
				ks := &manifest.Kustomization{Name: "app", Namespace: "apps"}
				if active {
					ks.PostBuild = &kustomizev1.PostBuild{
						Substitute: map[string]string{"LEFT": "left", "RIGHT": "right"},
					}
				}
				return ks
			}
			var preparation Preparation
			t.Cleanup(preparation.Retire)
			first := RenderRequest{Spec: spec(tc.before), Preparation: &preparation}
			PrepareShape(&first, documents(tc.first))
			second := RenderRequest{Spec: spec(tc.after), Preparation: &preparation}
			docs := documents(tc.second)
			shaping := PrepareShape(&second, docs)
			fresh := RenderRequest{Spec: second.Spec}
			assert.Equal(
				t,
				manifest.Fingerprint(shaping.Content()),
				manifest.Fingerprint(PrepareShape(&fresh, documents(tc.second)).Content()),
			)
			assert.NoError(t, shaping.Apply(t.Context(), ShapeOptions{Strict: true}))
			assert.Diff(t, docs[0].Document["data"], map[string]any{"value": tc.want})
		})
	}
}
