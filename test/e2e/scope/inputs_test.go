package scope_test

import "testing"

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_DiffKustomizeEscapedInputs(t *testing.T) {
	for _, name := range []string{
		"escaped-implicit-shallow",
		"escaped-implicit-deep",
		"escaped-mixed",
		"escaped-explicit",
		"escaped-patch-only",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-inputs-"+name, name)
		})
	}
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_DiffKustomizeInputLifecycle(t *testing.T) {
	for _, name := range []string{
		"input-lifecycle-add",
		"input-lifecycle-delete",
		"input-lifecycle-dangling",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-inputs-"+name, name)
		})
	}
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_DiffSelectedKustomizeInputs(t *testing.T) {
	for _, name := range []string{
		"selected-input-builder",
		"selected-input-patch",
		"selected-input-unrelated",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-inputs-"+name, name)
		})
	}
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_DiffLocalChartInputs(t *testing.T) {
	for _, name := range []string{
		"chart-input-template-committed",
		"chart-input-template-directories",
		"chart-input-template-added-directories",
		"chart-input-template-added-committed",
		"chart-input-template-deleted-directories",
		"chart-input-template-deleted-committed",
		"chart-input-default-values",
		"chart-input-packaged-subchart",
		"chart-input-root-chart",
		"chart-input-policy-admits-template",
		"chart-input-policy-removes-template",
		"chart-input-policy-created",
		"chart-input-policy-deleted",
		"chart-input-excluded-payload",
		"chart-input-unchanged",
		"chart-input-remote-chart-excludes-local-payload",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-inputs-"+name, name)
		})
	}
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_ChangedOnlyLocalChartInputs(t *testing.T) {
	for _, name := range []string{
		"chart-changed-directories",
		"chart-changed-directories-invalid",
		"chart-changed-committed",
		"chart-changed-committed-invalid",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-inputs-"+name, name)
		})
	}
}
