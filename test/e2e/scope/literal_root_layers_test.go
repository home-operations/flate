package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_RootLayerDeclarations(t *testing.T) {
	testScopeCase(t, "entry-packages-root-layer-declarations", "root-layer-declarations")
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_LiteralRootOverlayCollision(t *testing.T) {
	testScopeCase(t, "entry-packages-literal-root-overlay-collision", "literal-root-overlay-collision")
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_LiteralRootFailureNamespace(t *testing.T) {
	for _, name := range []string{
		"literal-root-failure-build-all",
		"literal-root-failure-build-ks",
		"literal-root-failure-test-all",
		"literal-root-failure-test-ks",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-packages-"+name, name)
		})
	}
}
