package substitution_test

import "testing"

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DependsOnDefaults_SuppliedValues(t *testing.T) {
	runSubstitutionCases(t, "dependency-readiness",
		"depends-on-defaults--supplied-values-prefix-config",
		"depends-on-defaults--supplied-values-prefix-dev-config",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DependsOnDefaults_ChangedOnly(t *testing.T) {
	runSubstitutionCases(t, "dependency-readiness",
		"depends-on-defaults--changed-only-build",
		"depends-on-defaults--changed-only-diff",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DependsOnDefaults_EmittedChild(t *testing.T) {
	runSubstitutionCases(t, "dependency-readiness",
		"depends-on-defaults--emitted-child-default-only",
		"depends-on-defaults--emitted-child-discovered-supplier",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DependsOnDefaults_EmptyNameFails(t *testing.T) {
	runSubstitutionCases(t, "dependency-readiness",
		"depends-on-defaults--empty-name-fails",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_ReadyExpr_Kustomization(t *testing.T) {
	runSubstitutionCases(t, "dependency-readiness",
		"ready-expr--kustomization",
		"ready-expr--kustomization-build",
		"ready-expr--kustomization-diff",
		"ready-expr--kustomization-test",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_ReadyExpr_EmittedIndexed(t *testing.T) {
	runSubstitutionCases(t, "dependency-readiness",
		"ready-expr--emitted-indexed-false",
		"ready-expr--emitted-indexed-true",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DependsOnProducedSource(t *testing.T) {
	runSubstitutionCases(t, "dependency-readiness",
		"depends-on-produced-source-interval",
		"depends-on-produced-source-interval-build",
		"depends-on-produced-source-interval-diff",
		"depends-on-produced-source-substitution",
		"depends-on-produced-source-substitution-build",
		"depends-on-produced-source-substitution-diff",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DependsOnProducedTarget(t *testing.T) {
	runSubstitutionCases(t, "dependency-readiness",
		"depends-on-produced-target-interval",
		"depends-on-produced-target-interval-build",
		"depends-on-produced-target-interval-diff",
		"depends-on-produced-target-substitution",
		"depends-on-produced-target-substitution-build",
		"depends-on-produced-target-substitution-diff",
	)
}
