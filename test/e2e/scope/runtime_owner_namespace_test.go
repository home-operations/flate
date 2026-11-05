package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_RuntimeOwnerNamespace(t *testing.T) {
	for _, namespace := range []string{"omitted", "explicit"} {
		t.Run(namespace, func(t *testing.T) {
			testScopeCase(t, "scope-namespaces", "runtime-"+namespace)
		})
	}
}

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_RuntimeOwnerNamespace_ChangedOnly(t *testing.T) {
	for _, transition := range []string{"chart-value", "authored-namespace"} {
		t.Run(transition, func(t *testing.T) {
			for _, direction := range []string{"forward", "reverse"} {
				t.Run(direction, func(t *testing.T) {
					testScopeCase(t, "scope-namespaces", "runtime-"+transition+"-"+direction)
				})
			}
		})
	}
}
