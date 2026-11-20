package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_CustomBootstrapSource(t *testing.T) {
	for _, name := range []string{"narrow", "root", "canonical", "diff"} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "custom-bootstrap-source", name)
		})
	}
}
