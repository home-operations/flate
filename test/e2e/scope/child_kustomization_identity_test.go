package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_ChildKustomizationIdentity(t *testing.T) {
	for _, scenario := range []string{"bare", "target", "authored"} {
		t.Run(scenario, func(t *testing.T) {
			testScopeCase(t, "scope-namespaces", "child-"+scenario)
		})
	}
	for _, scenario := range []string{
		"insert01-remove0", "insert01-remove1", "insert10-remove0", "insert10-remove1",
	} {
		t.Run("authored/"+scenario, func(t *testing.T) {
			testScopeCase(t, "scope-namespaces", "child-authored-"+scenario)
		})
	}
}
