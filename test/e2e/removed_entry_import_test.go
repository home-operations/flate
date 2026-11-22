package e2e_test

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_RemovedEntryImport(t *testing.T) {
	fixture := harness.Open(t, "external-artifact-reach", "removed-import")
	debug := fixture.RunWithWork(t)
	work := debug[len(debug)-1].ReconcileWork(t)
	maps.DeleteFunc(work, func(key [3]string, _ [2]int) bool { return key[2] != "kustomize" && key[2] != "helm" })
	const kind = "Kustomization.kustomize.toolkit.fluxcd.io/"
	want := map[[3]string][2]int{
		{"0", kind + "app/app", "kustomize"}:                    {1, 0},
		{"0", kind + "clusters/local/flux", "kustomize"}:        {1, 0},
		{"0", kind + "flux-system/cluster-config", "kustomize"}: {1, 0},
		{"1", kind + "clusters/local/flux", "kustomize"}:        {1, 0},
		{"1", kind + "flux-system/cluster-config", "kustomize"}: {1, 0},
	}
	if !maps.Equal(work, want) {
		t.Errorf("work got %v, want %v", work, want)
	}
}
