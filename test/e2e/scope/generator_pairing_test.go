package scope_test

import "testing"

//nolint:paralleltest // The CLI harness changes process-global state.
func TestE2E_Diff_GeneratorPairing(t *testing.T) {
	for _, name := range []string{"bare-configmap", "bare-secret", "flux"} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "diff-generator-pairing", name)
		})
	}
}
