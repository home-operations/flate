package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ReadinessParity(t *testing.T) {
	for _, name := range []string{
		"source-republication",
		"kustomization-rename",
		"helmrelease-rename",
		"transitive-failure",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "readiness-parity", name)
		})
	}
}
