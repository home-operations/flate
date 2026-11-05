package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_SelectedKustomizeClassification(t *testing.T) {
	for _, name := range []string{
		"classification-yaml",
		"classification-yml",
		"classification-capitalized",
		"classification-plain",
		"classification-malformed",
		"classification-multiple",
		"classification-component",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-membership", name)
		})
	}
}
