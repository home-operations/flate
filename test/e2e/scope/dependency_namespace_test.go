package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_DependencyNamespaces(t *testing.T) {
	for _, name := range []string{"corrected", "missing", "escaped"} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "dependency-namespaces", name)
		})
	}
}
