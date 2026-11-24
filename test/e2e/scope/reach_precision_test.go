package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ReachPrecision(t *testing.T) {
	testScopeCases(t, "reach-precision-scope", []string{"consumed-variable", "unused-variable", "metadata-only"})
}
