package scope_test

import "testing"

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_CrossSourceOptionalInputRemoval(t *testing.T) {
	testScopeCase(t, "scope-inputs-cross-source-optional-removal", "cross-source-optional-removal")
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_OptionalInputChangedOnly(t *testing.T) {
	for _, name := range []string{
		"optional-changed-directory",
		"optional-changed-committed",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-inputs-"+name, name)
		})
	}
}

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_OptionalInputDiffSides(t *testing.T) {
	for _, name := range []string{
		"optional-sides-appear",
		"optional-sides-remove",
		"optional-sides-projected-appear",
		"optional-sides-projected-remove",
		"optional-sides-required-removal",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-inputs-"+name, name)
		})
	}
}
