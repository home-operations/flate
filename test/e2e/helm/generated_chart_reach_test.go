package helm_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_GeneratedChartConcreteReads(t *testing.T) {
	for _, tc := range []struct {
		scenario string
		name     string
		diffs    []string
		reached  bool
	}{
		{
			"helm-dependencies", "changed-only-emitted-helmrelease-chart-templated",
			[]string{"generated-values-diff", "generated-library-diff"},
			true,
		},
		{
			"helm-dependencies", "changed-only-mounted-helmrelease-chart-templated-values",
			[]string{"mounted-generated-values-diff"},
			true,
		},
		{
			"helm-dependencies", "changed-only-mounted-helmrelease-chart-templated-template",
			[]string{"mounted-generated-template-diff"},
			true,
		},
		{
			"helm-dependencies", "changed-only-mounted-helmrelease-chart-templated-library",
			[]string{"mounted-generated-library-diff"},
			true,
		},
		{
			"helm-dependencies", "changed-only-mounted-helmrelease-chart-templated-unrelated",
			[]string{"mounted-generated-unrelated-diff"},
			false,
		},
		{"external-artifact-reach", "chart-library-only", []string{"diff"}, true},
		{"external-artifact-reach", "chart-root-values", []string{"diff"}, true},
		{"external-artifact-reach", "chart-generated-source", []string{"diff"}, true},
		{"external-artifact-reach", "chart-unrelated", []string{"diff"}, false},
		{"external-artifact-reach", "chart-unrelated-in-generated-source", []string{"diff"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := harness.Open(t, tc.scenario, tc.name)
			results := fixture.RunWithWork(t)
			want := make(map[[3]string][2]int)
			if tc.reached {
				const owner = "HelmRelease.helm.toolkit.fluxcd.io/flux-system/app"
				want[[3]string{"0", owner, "helm"}] = [2]int{1, 0}
				want[[3]string{"1", owner, "helm"}] = [2]int{1, 0}
			}
			for _, step := range tc.diffs {
				i := slices.IndexFunc(results, func(result harness.Result) bool { return result.Step == step })
				if i < 0 {
					t.Fatalf("missing diff step %q", step)
				}
				work := results[i].ReconcileWork(t)
				maps.DeleteFunc(work, func(key [3]string, _ [2]int) bool { return key[2] != "helm" })
				if !maps.Equal(work, want) {
					t.Errorf("%s work got %v, want %v", step, work, want)
				}
			}
		})
	}
}
