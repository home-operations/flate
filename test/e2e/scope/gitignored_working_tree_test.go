package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_GitIgnoredWorkingTree(t *testing.T) {
	testScopeCase(t, "gitignored-working-tree", "ignored-working-tree")
}
