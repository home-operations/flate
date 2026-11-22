package helm_test

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_GeneratedChartConcreteReads(t *testing.T) {
	for _, tc := range []struct {
		scenario string
		name     string
		reached  bool
	}{
		{"helm-dependencies", "changed-only-emitted-helmrelease-chart-templated", true},
		{"external-artifact-reach", "chart-library-only", true},
		{"external-artifact-reach", "chart-root-values", true},
		{"external-artifact-reach", "chart-generated-source", true},
		{"external-artifact-reach", "chart-unrelated", false},
		{"external-artifact-reach", "chart-unrelated-in-generated-source", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := harness.Open(t, tc.scenario, tc.name)
			for _, result := range fixture.RunWithWork(t) {
				work := result.ReconcileWork(t)
				maps.DeleteFunc(work, func(key [3]string, _ [2]int) bool { return key[2] != "helm" })
				want := make(map[[3]string][2]int)
				if tc.reached {
					const owner = "HelmRelease.helm.toolkit.fluxcd.io/flux-system/app"
					want[[3]string{"0", owner, "helm"}] = [2]int{1, 0}
					want[[3]string{"1", owner, "helm"}] = [2]int{1, 0}
				}
				if !maps.Equal(work, want) {
					t.Errorf("%s work got %v, want %v", result.Step, work, want)
				}
			}
		})
	}
}
