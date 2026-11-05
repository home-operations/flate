package scope_test

import "testing"

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_DiffAutoBaseline_Base(t *testing.T) {
	testScopeCase(t, "scope-selection-baseline-explicit-base", "baseline-explicit-base")
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_DiffSelfRefBaselineRenders(t *testing.T) {
	testScopeCase(t, "scope-selection-baseline-self-ref", "baseline-self-ref")
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_DiffAutoBaseline_CommittedMembership(t *testing.T) {
	for _, name := range []string{
		"committed-membership-delete",
		"committed-membership-rename",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-selection-"+name, name)
		})
	}
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_DiffImagesRequiresBaselineWhenNoGit(t *testing.T) {
	testScopeCase(t, "scope-selection-images-no-git-baseline", "images-no-git-baseline")
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_GetKS_YAMLExposesProjectedFields(t *testing.T) {
	testScopeCase(t, "scope-selection-get-projected-kustomization", "get-projected-kustomization")
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_MissingLocalInputDiagnostics(t *testing.T) {
	for _, name := range []string{
		"missing-local-resource",
		"missing-local-chart",
		"auxiliary-supplied",
		"auxiliary-allow-configmap",
		"auxiliary-allow-secret",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-selection-"+name, name)
		})
	}
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_Diff_SameDirectoryFailure(t *testing.T) {
	testScopeCase(t, "scope-selection-same-directory-failure", "same-directory-failure")
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_Diff_OwnershipCycle(t *testing.T) {
	testScopeCase(t, "scope-selection-ownership-cycle", "ownership-cycle")
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_BootstrapErrorSurfacesNotMasked(t *testing.T) {
	testScopeCase(t, "scope-selection-bootstrap-template-error", "bootstrap-template-error")
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_ExternalSourcedWidePathDoesNotBlockCluster(t *testing.T) {
	testScopeCase(t, "scope-selection-external-wide-source", "external-wide-source")
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_SecretMaterialPolicy(t *testing.T) {
	for _, name := range []string{
		"secret-material-username-required",
		"secret-material-username-allowed",
		"secret-material-password-required",
		"secret-material-password-allowed",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-selection-"+name, name)
		})
	}
}
