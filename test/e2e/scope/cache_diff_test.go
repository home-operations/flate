package scope_test

import "testing"

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_Diff_InTreeCache(t *testing.T) {
	for _, name := range []string{
		"cache-implicit-unchanged-external",
		"cache-implicit-changed-external",
		"cache-implicit-suspended-current-external",
		"cache-implicit-suspended-orig-external",
		"cache-self-unchanged-external",
		"cache-self-changed-external",
		"cache-self-changed-relative",
		"cache-self-changed-absolute",
		"cache-self-suspended-current-external",
		"cache-self-suspended-orig-external",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-selection-"+name, name)
		})
	}
}
