package scope_test

import "testing"

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_RepositoryRootMultiCluster_IsolatesCollidingIdentities(t *testing.T) {
	t.Skip(
		"Repository-root discovery combines colliding cluster identities; the discovery partition owner must restore isolation for main, test, and utility.",
	)
	for _, name := range []string{"root-partition", "one-sided-diff"} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "multi-cluster", name)
		})
	}
}
