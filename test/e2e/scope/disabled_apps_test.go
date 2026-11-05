package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_DisabledAppAdmission(t *testing.T) {
	for _, name := range []string{
		"disabled-nested-enabled",
		"disabled-nested-absent",
		"disabled-nested-commented",
		"disabled-invalid-api-version",
		"disabled-invalid-yaml-syntax",
		"disabled-shared-active-target",
		"disabled-cross-package-inclusion",
		"disabled-single-root",
		"disabled-single-scoped",
		"disabled-multi-root",
		"disabled-multi-scoped",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-membership", name)
		})
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_DisabledAppDiffSides(t *testing.T) {
	for _, name := range []string{
		"disabled-diff-nested-remove",
		"disabled-diff-nested-restore",
		"disabled-diff-nested-payload",
		"disabled-diff-remove",
		"disabled-diff-restore",
		"membership-namespace-payload",
		"membership-namespace-namespace-forward",
		"membership-namespace-namespace-reverse",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-membership", name)
		})
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_SelectedEntrySelfOwnership(t *testing.T) {
	for _, name := range []string{
		"entry-self-ownership-selected-package",
		"entry-self-ownership-plain-entry-ancestor",
		"entry-self-ownership-disabled-foreign-owner",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-membership", name)
		})
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_EntryPrerequisiteDiffSides(t *testing.T) {
	for _, name := range []string{
		"entry-prerequisite-forward",
		"entry-prerequisite-reverse",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-packages-"+name, name)
		})
	}
}
