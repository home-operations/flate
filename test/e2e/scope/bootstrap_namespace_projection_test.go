package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_BootstrapSiblingNamespaceProjection(t *testing.T) {
	testScopeCase(t, "scope-namespaces", "bootstrap-sibling-projection")
}
