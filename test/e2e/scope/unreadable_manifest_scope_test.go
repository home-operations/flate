package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_UnreadableManifestScope(t *testing.T) {
	for _, scenario := range []string{"owned-independent-sibling", "unowned-nested-census"} {
		t.Run(scenario, func(t *testing.T) {
			testScopeCase(t, "scope-unreadable", scenario)
		})
	}
}
