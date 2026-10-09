package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkDisablesChartDigestTracking_Values(b *testing.B) {
	for _, tc := range []struct {
		name string
		hr   *manifest.HelmRelease
	}{
		{name: "no instance", hr: &manifest.HelmRelease{Values: map[string]any{"other": true}}},
		{name: "no kustomize", hr: &manifest.HelmRelease{Values: map[string]any{"instance": map[string]any{}}}},
		{name: "malformed instance", hr: &manifest.HelmRelease{Values: map[string]any{"instance": []any{}}}},
		{name: "malformed kustomize", hr: &manifest.HelmRelease{Values: map[string]any{"instance": map[string]any{"kustomize": "patches"}}}},
		{name: "empty", hr: &manifest.HelmRelease{}},
		{name: "gate", hr: &manifest.HelmRelease{Values: digestTrackingValues([]any{
			map[string]any{"patch": "--feature-gates=DisableChartDigestTracking=true", "target": map[string]any{"name": "helm-controller"}},
		})}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				disablesChartDigestTracking(tc.hr)
			}
		})
	}
}
