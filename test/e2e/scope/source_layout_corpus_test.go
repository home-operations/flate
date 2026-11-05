package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_SourceLayoutCorpus(t *testing.T) {
	for _, tc := range []struct{ scenario, name string }{
		{"entry-membership-layout", "pinned-disabled-declaration"},
		{"orphan-membership", "pinned-namespaced-static-root"},
		{"namespace-scope-layout", "pinned-application-namespaces"},
		{"input-boundary", "pinned-runner-escaped-input"},
		{"component-reach-layout", "pinned-backup-component"},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			testScopeCase(t, tc.scenario, tc.name)
		})
	}
}
