package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ReachPrecision(t *testing.T) {
	for _, name := range []string{"consumed-variable", "unused-variable", "metadata-only"} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "reach-precision", name)
		})
	}
}
