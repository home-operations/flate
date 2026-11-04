package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestHasSubstituteDisabled(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		doc  map[string]any
		want bool
	}{
		{
			name: "annotation set to disabled",
			doc: map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]any{
						"kustomize.toolkit.fluxcd.io/substitute": "disabled",
					},
				},
			},
			want: true,
		},
		{
			name: "label set to disabled",
			doc: map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{
						"kustomize.toolkit.fluxcd.io/substitute": "disabled",
					},
				},
			},
			want: true,
		},
		{
			name: "annotation set to anything else",
			doc: map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]any{
						"kustomize.toolkit.fluxcd.io/substitute": "enabled",
					},
				},
			},
			want: false,
		},
		{
			name: "no metadata",
			doc:  map[string]any{"kind": "ConfigMap"},
			want: false,
		},
		{
			name: "metadata without labels or annotations",
			doc: map[string]any{
				"metadata": map[string]any{"name": "x"},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			{
				got := HasSubstituteDisabled(tc.doc)
				assert.ErrorIf(t, got != tc.want, "HasSubstituteDisabled = %v, want %v", got, tc.want)
			}
		})
	}
}
