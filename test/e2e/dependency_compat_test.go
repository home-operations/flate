package e2e

import (
	"github.com/home-operations/flate/internal/testutil"
	"testing"
)

func TestE2E_DependencyNamespace_UnchangedOutput(t *testing.T) {
	dir := missingCRDRepo(t, `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: compatible, namespace: flux-system}
spec:
 dependsOn:
 - {apiVersion: v1, kind: ConfigMap, name: settings}
 resources:
 - apiVersion: example.com/v1
   kind: Widget
   metadata: {name: compatible, namespace: flux-system}
   spec: {value: unchanged}
`)
	testutil.WriteFile(t, dir, "apps/settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {value: unchanged}\n")
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n- settings.yaml\n")
	out, errOut := requireCLIOK(t, "build", "all", "--path", dir, "--cache-dir", t.TempDir(), "--concurrency", "2")
	if errOut != "" {
		t.Fatalf("unexpected diagnostics: %s", errOut)
	}
	t.Logf("compatibility output: %q", out)
}
