package orchestrator

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOrchestrator_UnreferencedBaseKSNotAdmitted(t *testing.T) {
	dir := t.TempDir()

	testutil.WriteFile(t, dir, "cluster/flux-system.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system", "  path: ./apps/test\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n"))
	testutil.WriteFile(t, dir, "apps/test/kustomization.yaml", "resources:\n  - ./app-a\n  - ./app-b\n")
	testutil.WriteFile(t, dir, "apps/test/app-a/kustomization.yaml", "resources:\n  - ../../base/app-a\n")
	testutil.WriteFile(t, dir, "apps/test/app-b/kustomization.yaml", "resources:\n  - ../../base/app-b\n")

	for _, app := range []string{"app-a", "app-b"} {
		testutil.WriteFile(t, dir, "apps/base/"+app+"/kustomization.yaml", "resources:\n  - cm.yaml\n")
		testutil.WriteFile(t, dir, "apps/base/"+app+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: "+app+"-cm\n  namespace: flux-system\n")
	}
	testutil.WriteFile(t, dir, "apps/base/app-a/ks.yaml", testutil.KustomizationYAML("app-a", "flux-system", "  path: ./apps/${CLUSTER_ENV}/app-a\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n"))
	testutil.WriteFile(t, dir, "apps/base/app-b/ks.yaml", testutil.KustomizationYAML("app-b", "flux-system", "  path: ./apps/${CLUSTER_ENV}/app-b\n  dependsOn:\n    - name: app-a\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n"))

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true, Concurrency: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render returned a failure error for unreferenced base KSes: %v", err)
	}

	for _, name := range []string{"app-a", "app-b"} {
		id := testutil.KustomizationID("flux-system", name)
		if info, failed := res.Failed[id]; failed {
			t.Errorf("%s failed: %s", name, info.String())
		}
		if _, orphaned := res.Orphans[id]; orphaned || o.store.GetObject(id) != nil {
			t.Errorf("disabled Kustomization admitted: %s", id)
		}
	}
	if len(res.Warnings) != 0 {
		t.Errorf("disabled Kustomizations produced warnings: %+v", res.Warnings)
	}
	if o.store.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "app-a-cm"}) == nil {
		t.Error("app-a-cm ConfigMap missing; the referenced base resource should still render")
	}
}
