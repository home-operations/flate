package scope_test

import "testing"

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_RepositoryRootMultiCluster_IsolatesCollidingIdentities(t *testing.T) {
	testScopeCases(t, "multi-cluster", []string{
		"root-partition", "one-sided-diff", "shared-instance", "shared-instance-refs", "shared-classic",
		"classic-native", "classic-generated-values", "instance-subpackage", "instance-subpackage-values",
		"authored-parent", "shared-member", "chart-sync",
	})
}

//nolint:paralleltest // CLI execution changes process-wide logging and environment.
func TestE2E_ClusterPartitionRoot(t *testing.T) {
	testScopeCases(t, "cluster-partition", []string{"root", "explicit", "singleton", "renamed"})
}
