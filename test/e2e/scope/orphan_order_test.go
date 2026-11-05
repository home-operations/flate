package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_ExcludedSiblingsDeterministic(t *testing.T) {
	testScopeCase(t, "scope-orphans", "excluded-siblings")
}
