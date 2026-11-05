package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_NamespaceDiff(t *testing.T) {
	for _, name := range []string{"changed", "added", "removed", "renamed", "empty"} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "namespace-scope", "namespace-diff-"+name)
		})
	}
}
