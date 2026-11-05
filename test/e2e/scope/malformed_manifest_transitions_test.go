package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_Diff_MalformedManifestTransitions(t *testing.T) {
	for _, scenario := range []string{"malformed-to-valid", "valid-to-malformed"} {
		t.Run(scenario, func(t *testing.T) {
			testScopeCase(t, "scope-malformed-transitions", scenario)
		})
	}
}
