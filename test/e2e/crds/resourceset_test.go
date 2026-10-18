// Package crds tests resource expansion and CRD availability.
package crds

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_ResourceSetExpandsIntoChildKustomizations(t *testing.T) {
	// The resourceset/ fixture has one ResourceSet that templates a
	// Namespace + a Kustomization per tenant (frontend, backend). After
	// loading, flate should expose the rendered child Kustomizations
	// via `get ks` alongside the static parent — confirming the
	// load-time expansion pass plumbed each rendered doc through the
	// store.
	out := harness.RunCLIOutputOnly(t, "get", "ks", "--path", harness.TestdataPath(t, "resourceset"))
	for _, want := range []string{
		"cluster-tenants", // parent KS from cluster/flux-system.yaml
		"apps-frontend",   // emitted by ResourceSet for tenant=frontend
		"apps-backend",    // emitted by ResourceSet for tenant=backend
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in ks listing:\n%s", want, out)
		}
	}
}
