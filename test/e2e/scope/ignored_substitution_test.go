package scope_test

import "testing"

//nolint:paralleltest // The shared CLI harness changes process-wide state.
func TestE2E_Diff_IgnoredRequiredSubstitution(t *testing.T) {
	for _, name := range []string{
		"ignored-required-all",
		"ignored-required-all-reverse",
		"ignored-required-named",
		"ignored-required-named-reverse",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "scope-inputs-"+name, name)
		})
	}
}
