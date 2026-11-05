package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_OrphanParentSource(t *testing.T) {
	for _, source := range []string{"local", "external"} {
		t.Run(source, func(t *testing.T) {
			testScopeCase(t, "scope-orphans", "parent-"+source)
		})
	}
}

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_OrphanedKustomizationNotDiscovered(t *testing.T) {
	testScopeCase(t, "scope-orphans", "undiscovered-kustomization")
}
