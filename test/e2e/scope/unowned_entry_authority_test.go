package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_UnownedMalformedEntryAuthority(t *testing.T) {
	for _, scenario := range []string{"malformed", "unreadable"} {
		t.Run(scenario, func(t *testing.T) {
			testScopeCase(t, "scope-unowned", scenario)
		})
	}
}
