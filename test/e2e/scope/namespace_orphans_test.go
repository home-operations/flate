package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_NamespaceOrphans(t *testing.T) {
	for _, scenario := range []string{"sibling", "explicit", "standalone", "shared", "terminal-replay"} {
		t.Run(scenario, func(t *testing.T) {
			testScopeCase(t, "scope-orphans", "namespace-"+scenario)
		})
	}
}

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_NamespaceOrphans_FailingOwner(t *testing.T) {
	for _, concurrency := range []string{"2", "4"} {
		t.Run("concurrency"+concurrency, func(t *testing.T) {
			testScopeCase(t, "scope-orphans", "namespace-failing-owner-"+concurrency)
		})
	}
}
