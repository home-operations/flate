package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_EntryPackageJSON(t *testing.T) {
	for _, name := range []string{
		"entry-package-json-build",
		"entry-package-json-diff",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-packages-"+name, name)
		})
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_EntryPackageFailure(t *testing.T) {
	for _, name := range []string{
		"entry-package-failure-test",
		"entry-package-failure-diff",
		"entry-package-failure-diff-json",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-packages-"+name, name)
		})
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_DiscoveryPackageEntries(t *testing.T) {
	for _, name := range []string{
		"discovery-package-one",
		"discovery-package-two",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "entry-packages-"+name, name)
		})
	}
}
