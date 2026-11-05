package substitution_test

import "testing"

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SOPSUnreadableEmptyVariableSet(t *testing.T) {
	runSubstitutionCases(t, "substitute-sops",
		"sops-unreadable-empty-variable-set-default-inactive",
		"sops-unreadable-empty-variable-set-default-skipped",
		"sops-unreadable-empty-variable-set-active-inline",
		"sops-unreadable-empty-variable-set-active-readable",
		"sops-unreadable-empty-variable-set-always-empty",
		"sops-unreadable-empty-variable-set-strict-active",
		"sops-unreadable-empty-variable-set-resource-disabled",
		"sops-unreadable-empty-variable-set-dummy-supply",
		"sops-unreadable-empty-variable-set-mixed-emitted",
		"sops-unreadable-empty-variable-set-mixed-skipped",
		"sops-unreadable-empty-variable-set-no-postbuild",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SOPSUnreadableDiffBothDirections(t *testing.T) {
	runSubstitutionCases(t, "substitute-sops",
		"sops-unreadable-diff-both-directions-ciphertext",
		"sops-unreadable-diff-both-directions-plaintext",
		"sops-unreadable-diff-both-directions-metadata",
	)
}
