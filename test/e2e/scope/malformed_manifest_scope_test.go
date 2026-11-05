package scope_test

import "testing"

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestE2E_MalformedManifestScope(t *testing.T) {
	for _, scenario := range []string{
		"owned-graph", "shared-graph", "owned-ad-hoc", "owned-ad-hoc-wide",
		"suspended-owner", "changed-unselected-owner", "generated-owner",
		"suspended-generated-owner", "external-source-owner", "unowned-graph", "unowned-ad-hoc",
	} {
		t.Run(scenario, func(t *testing.T) {
			testScopeCase(t, "scope-malformed", scenario)
		})
	}
}
