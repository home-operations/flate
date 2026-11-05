package crds_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_MissingCRDs_StrictIssue(t *testing.T) {
	runCRDScenario(t, "crd-admission", "missing-crds-strict-issue")
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ResourceSet_RuntimeClassDependency(t *testing.T) {
	runCRDScenario(t, "crd-admission", "resource-set-runtime-class-dependency")
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_MissingCRDs_Policy(t *testing.T) {
	for _, name := range []string{
		"reference-crd", "reference-crd-opt-in", "strict-absent", "allowed-absent",
		"supplied-crd", "supplied-crd-opt-in", "strict-absent-explicit-crd", "allowed-absent-explicit-crd",
		"missing-configmap", "missing-secret", "missing-widget", "supplied-configmap",
	} {
		t.Run(name, func(t *testing.T) {
			runCRDScenario(t, "crd-admission", "policy-"+name)
		})
	}
	t.Run("selection", func(t *testing.T) {
		for _, name := range []string{"strict-absent", "allowed-absent", "present", "foreign-group"} {
			t.Run(name, func(t *testing.T) {
				runCRDScenario(t, "crd-admission", "selection-"+name)
			})
		}
	})
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_MissingCRDs_Environment(t *testing.T) {
	runCRDScenario(t, "crd-admission", "missing-crds-environment")
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_MissingCRDs_DeterministicWarnings(t *testing.T) {
	runCRDScenario(t, "crd-admission", "missing-crds-deterministic-warnings")
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_MissingCRDs_DiffSides(t *testing.T) {
	runCRDScenario(t, "crd-admission", "missing-crds-diff-sides")
}
