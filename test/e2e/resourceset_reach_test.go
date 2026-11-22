package e2e_test

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_ResourceSetChildReach(t *testing.T) {
	for _, tc := range []struct {
		name    string
		readers [][2]string
	}{
		{
			name: "resourceset-reach",
			readers: [][2]string{
				{"Kustomization.kustomize.toolkit.fluxcd.io/default/alpha", "kustomize"},
				{"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/default-apps", "kustomize"},
				{"ResourceSet.fluxcd.controlplane.io/flux-system/cluster-apps", "evaluation"},
			},
		},
		{
			name: "resourceset-provider-reach",
			readers: [][2]string{
				{"Kustomization.kustomize.toolkit.fluxcd.io/default/child", "kustomize"},
				{"ResourceSet.fluxcd.controlplane.io/flux-system/factory", "evaluation"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := harness.Open(t, "external-artifact-reach", tc.name)
			work := fixture.RunWithWork(t)[0].ReconcileWork(t)
			maps.DeleteFunc(work, func(key [3]string, _ [2]int) bool {
				return key[2] != "kustomize" && key[2] != "evaluation"
			})
			want := make(map[[3]string][2]int)
			for _, side := range []string{"0", "1"} {
				for _, reader := range tc.readers {
					want[[3]string{side, reader[0], reader[1]}] = [2]int{1, 0}
				}
			}
			if !maps.Equal(work, want) {
				t.Errorf("work got %v, want %v", work, want)
			}
		})
	}
}
