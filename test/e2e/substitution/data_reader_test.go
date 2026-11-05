package substitution_test

import "testing"

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DataReader_SuppliedSSHOnBothDiffSides(t *testing.T) {
	runSubstitutionCases(t, "supplied-inputs",
		"data-reader--suppliedssh-on-both-diff-sides",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DataReader_GeneratorEmbeddedCiphertext(t *testing.T) {
	runSubstitutionCases(t, "substitute-sops",
		"data-reader--generator-embedded-ciphertext-secretgenerator",
		"data-reader--generator-embedded-ciphertext-configmapgenerator",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DataReader_StringDataPrecedence(t *testing.T) {
	runSubstitutionCases(t, "supplied-inputs",
		"data-reader--string-data-precedence",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DataReader_SuppliedSourceProducesCredential(t *testing.T) {
	runSubstitutionCases(t, "supplied-inputs",
		"data-reader--supplied-source-produces-credential",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DataReader_ClusterAdvisoriesOnBothBaseSides(t *testing.T) {
	runSubstitutionCases(t, "supplied-inputs",
		"data-reader--cluster-advisories-on-both-base-sides",
	)
}
