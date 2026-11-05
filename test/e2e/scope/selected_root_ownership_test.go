package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_SelectedKustomizeRoot(t *testing.T) {
	testScopeCase(t, "entry-membership", "selected-root")
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_SelectedKustomizeRootNestedOwner(t *testing.T) {
	testScopeCase(t, "entry-membership", "selected-root-nested-owner")
}
