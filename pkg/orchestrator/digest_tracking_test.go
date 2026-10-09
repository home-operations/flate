package orchestrator

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func digestTrackingValues(patches any) map[string]any {
	return map[string]any{"instance": map[string]any{"kustomize": map[string]any{"patches": patches}}}
}

func TestDisablesChartDigestTracking_Values(t *testing.T) {
	gate := map[string]any{"patch": "--feature-gates=DisableChartDigestTracking=true"}
	targeted := map[string]any{"patch": gate["patch"], "target": map[string]any{"name": "helm-controller"}}
	falseGate := map[string]any{"patch": "DisableChartDigestTracking=false"}
	for _, tc := range []struct {
		name   string
		values map[string]any
		want   bool
	}{
		{name: "empty"},
		{name: "unrelated", values: map[string]any{"other": true}},
		{name: "malformed instance", values: map[string]any{"instance": []any{}}},
		{name: "missing kustomize", values: map[string]any{"instance": map[string]any{}}},
		{name: "malformed kustomize", values: map[string]any{"instance": map[string]any{"kustomize": "patches"}}},
		{name: "missing patches", values: map[string]any{"instance": map[string]any{"kustomize": map[string]any{}}}},
		{name: "malformed patches", values: digestTrackingValues("DisableChartDigestTracking=true")},
		{name: "malformed entries", values: digestTrackingValues([]any{nil, "patch", map[string]any{}, map[string]any{"patch": true}})},
		{name: "unrelated patch", values: digestTrackingValues([]any{map[string]any{"patch": "--feature-gates=Other=true"}})},
		{name: "absent target", values: digestTrackingValues([]any{gate}), want: true},
		{name: "exact target", values: digestTrackingValues([]any{targeted}), want: true},
		{name: "other controller", values: digestTrackingValues([]any{map[string]any{"patch": gate["patch"], "target": map[string]any{"name": "source-controller"}}})},
		{name: "regex target", values: digestTrackingValues([]any{map[string]any{"patch": gate["patch"], "target": map[string]any{"name": "helm-.*"}}})},
		{name: "kind-only target", values: digestTrackingValues([]any{map[string]any{"patch": gate["patch"], "target": map[string]any{"kind": "Deployment"}}})},
		{name: "malformed target name", values: digestTrackingValues([]any{map[string]any{"patch": gate["patch"], "target": map[string]any{"name": []any{"helm-controller"}}}})},
		{name: "malformed target", values: digestTrackingValues([]any{map[string]any{"patch": gate["patch"], "target": "helm-controller"}})},
		{name: "null target", values: digestTrackingValues([]any{map[string]any{"patch": gate["patch"], "target": nil}})},
		{name: "false only", values: digestTrackingValues([]any{falseGate})},
		{name: "true then false", values: digestTrackingValues([]any{targeted, falseGate}), want: true},
		{name: "false then true", values: digestTrackingValues([]any{falseGate, targeted}), want: true},
		{name: "comment text", values: digestTrackingValues([]any{map[string]any{"patch": "# DisableChartDigestTracking=true"}}), want: true},
		{name: "combined text", values: digestTrackingValues([]any{map[string]any{"patch": "DisableChartDigestTracking=false,DisableChartDigestTracking=true"}}), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hr := &manifest.HelmRelease{Values: tc.values}
			original := hr.Clone()
			assert.Equal(t, disablesChartDigestTracking(hr), tc.want)
			assert.Diff(t, hr, original)
			assert.Equal(t, testing.AllocsPerRun(100, func() { disablesChartDigestTracking(hr) }), 0.0)
		})
	}
	assert.Equal(t, disablesChartDigestTracking(nil), false)
}

func TestDisablesChartDigestTracking_InputOrder(t *testing.T) {
	gate := &manifest.HelmRelease{Values: digestTrackingValues([]any{map[string]any{"patch": "DisableChartDigestTracking=true"}})}
	releases := []*manifest.HelmRelease{nil, {}, gate, {Values: digestTrackingValues([]any{map[string]any{"patch": "DisableChartDigestTracking=false"}})}}
	for range 2 {
		disabled := false
		for _, hr := range releases {
			disabled = disabled || disablesChartDigestTracking(hr)
		}
		assert.Equal(t, disabled, true)
		slices.Reverse(releases)
	}
}
