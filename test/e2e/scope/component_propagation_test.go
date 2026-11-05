package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ComponentChangePropagatesToAllConsumers(t *testing.T) {
	testScopeCase(t, "component-reach", "component-change-propagates-to-all-consumers")
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_NonSharedChangeDoesNotPropagate(t *testing.T) {
	testScopeCase(t, "component-reach", "non-shared-change-does-not-propagate")
}
