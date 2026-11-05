package substitution_test

import "testing"

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SOPSUnreadableActiveSubstitution(t *testing.T) {
	runSubstitutionCases(t, "substitute-sops",
		"sops-unreadable-active-substitution",
	)
}
