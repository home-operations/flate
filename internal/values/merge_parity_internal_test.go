package values

import (
	"maps"
	"testing"

	"helm.sh/helm/v4/pkg/chart/v2/loader"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDeepMergeInto_MatchesHelmMerge(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		base, override map[string]any
	}{
		{"nil-inputs", nil, nil},
		{"nil-base", nil, map[string]any{"m": map[string]any{"x": 1.0}}},
		{"empty-override", map[string]any{"m": map[string]any{"x": 1.0}}, map[string]any{}},
		{"disjoint", map[string]any{"a": 1.0}, map[string]any{"b": 2.0}},
		{"scalar-over-scalar", map[string]any{"a": 1.0}, map[string]any{"a": 2.0}},
		{
			"nested-collision",
			map[string]any{"m": map[string]any{"x": 1.0, "y": 2.0}},
			map[string]any{"m": map[string]any{"y": 9.0, "z": 3.0}},
		},
		{
			"deep-collision",
			map[string]any{"m": map[string]any{"n": map[string]any{"x": 1.0}}},
			map[string]any{"m": map[string]any{"n": map[string]any{"y": 2.0}}},
		},
		{
			"list-map-replace",
			map[string]any{"l": []any{map[string]any{"x": 1.0}}},
			map[string]any{"l": []any{map[string]any{"y": 2.0}}},
		},
		{"list-replace", map[string]any{"l": []any{1.0, 2.0}}, map[string]any{"l": []any{3.0}}},
		{"scalar-over-map", map[string]any{"k": map[string]any{"x": 1.0}}, map[string]any{"k": "scalar"}},
		{"map-over-scalar", map[string]any{"k": "scalar"}, map[string]any{"k": map[string]any{"x": 1.0}}},
		{"nil-override", map[string]any{"k": map[string]any{"x": 1.0}}, map[string]any{"k": nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			baseBefore := manifest.DeepCopyMap(tc.base)
			overrideBefore := manifest.DeepCopyMap(tc.override)
			want := loader.MergeMaps(tc.base, tc.override)
			got := DeepMergeInto(maps.Clone(tc.base), tc.override)
			assert.Diff(t, got, want)
			assert.Diff(t, tc.base, baseBefore)
			assert.Diff(t, tc.override, overrideBefore)
		})
	}
}
