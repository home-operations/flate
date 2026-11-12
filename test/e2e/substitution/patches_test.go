package substitution_test

import "testing"

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_ParentPatchesPropagateToChildren(t *testing.T) {
	runSubstitutionCases(t, "substitute-values",
		"parent-patches-propagate-to-children",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstituteDisabledAnnotation(t *testing.T) {
	runSubstitutionCases(t, "substitute-values",
		"substitute-disabled-annotation",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_BootstrapGeneratorEnvAndFileSubstitution(t *testing.T) {
	runSubstitutionCases(t, "substitute-producers",
		"bootstrap-generator-env-and-file-substitution",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstitutionPrecedence(t *testing.T) {
	runSubstitutionCases(t, "substitute-values",
		"substitution-precedence-build",
		"substitution-precedence-test",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_UnappliedPackagePatches(t *testing.T) {
	runSubstitutionCases(t, "substitute-values",
		"unapplied-package-patches-unapplied",
		"unapplied-package-patches-applied",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstitutionRequiredMissing(t *testing.T) {
	runSubstitutionCases(t, "substitute-values",
		"required-missing-secret",
		"required-missing-configmap",
	)
}
