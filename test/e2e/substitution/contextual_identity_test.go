package substitution_test

import "testing"

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstitutedChildIdentity(t *testing.T) {
	runSubstitutionCases(
		t,
		"substitution-selection",
		"identity-inline-forward",
		"identity-inline-reverse",
		"identity-generated-forward",
		"identity-generated-reverse",
		"identity-shared-forward",
		"identity-shared-reverse",
		"identity-unknown-forward",
		"identity-unknown-reverse",
	)
}
