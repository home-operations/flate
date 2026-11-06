package scope_test

import "testing"

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_RepositoryRootMultiCluster_IsolatesCollidingIdentities(t *testing.T) {
	for _, name := range []string{"root-partition", "one-sided-diff"} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "multi-cluster", name)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging and environment.
func TestE2E_ClusterPartitionRoot(t *testing.T) {
	for _, name := range []string{"root", "explicit", "singleton", "renamed"} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "cluster-partition", name)
		})
	}
}
