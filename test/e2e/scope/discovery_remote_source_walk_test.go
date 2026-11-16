package scope_test

import "testing"

//nolint:paralleltest // The CLI harness changes process-global state.
func TestE2E_DiscoveryRemoteSourceWalk(t *testing.T) {
	for _, name := range []string{
		"local-control", "root", "subpath", "neighbor", "shared", "fetched", "fetched-root",
		"both-changed", "reference-added", "reference-removed",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "discovery-remote-source-walk", name)
		})
	}
}
