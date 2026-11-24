package scope_test

import "testing"

//nolint:paralleltest // The CLI harness changes process-global state.
func TestE2E_DiscoveryRemoteSourceWalk(t *testing.T) {
	testScopeCases(t, "discovery-remote-source-walk", []string{
		"local-control", "root", "subpath", "neighbor", "shared", "fetched", "fetched-root",
		"both-changed", "reference-added", "reference-removed",
	})
}
