package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOrchestrator_UnreferencedBaseKSNotAdmitted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [cluster/flux-system.yaml]\n")

	testutil.WriteFile(
		t,
		dir,
		"cluster/flux-system.yaml",
		testutil.KustomizationYAML(
			"cluster-apps",
			"flux-system",
			"  path: ./apps/test\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/test/kustomization.yaml", "resources:\n  - ./app-a\n  - ./app-b\n")
	testutil.WriteFile(t, dir, "apps/test/app-a/kustomization.yaml", "resources:\n  - ../../base/app-a\n")
	testutil.WriteFile(t, dir, "apps/test/app-b/kustomization.yaml", "resources:\n  - ../../base/app-b\n")

	for _, app := range []string{"app-a", "app-b"} {
		testutil.WriteFile(t, dir, "apps/base/"+app+"/kustomization.yaml", "resources:\n  - cm.yaml\n")
		testutil.WriteFile(
			t,
			dir,
			"apps/base/"+app+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: "+app+"-cm\n  namespace: flux-system\n",
		)
	}
	testutil.WriteFile(
		t,
		dir,
		"apps/base/app-a/ks.yaml",
		testutil.KustomizationYAML(
			"app-a",
			"flux-system",
			"  path: ./apps/${CLUSTER_ENV}/app-a\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n",
		),
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/base/app-b/ks.yaml",
		testutil.KustomizationYAML(
			"app-b",
			"flux-system",
			"  path: ./apps/${CLUSTER_ENV}/app-b\n  dependsOn:\n    - name: app-a\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n",
		),
	)

	renderedSingleton1, err := renderTreesForTest(t.Context(), t,
		[]Tree{{Path: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{Concurrency: 2},
	)
	assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
	o := renderedSingleton1[0].Orchestrator
	res := renderedSingleton1[0].Result
	assert.FatalIf(t, err != nil, "Render returned a failure error for unreferenced base KSes: %v", err)

	for _, name := range []string{"app-a", "app-b"} {
		id := testutil.KustomizationID("flux-system", name)
		if info, failed := res.Failed[id]; failed {
			t.Errorf("%s failed: %s", name, info.String())
		}
		{
			_, orphaned := res.Orphans[id]
			assert.ErrorIf(t, orphaned || o.store.GetObject(id) != nil, "disabled Kustomization admitted: %s", id)
		}
	}
	assert.ErrorIf(t, len(res.Warnings) != 0,
		"disabled Kustomizations produced warnings: %+v", res.Warnings)

	assert.ErrorIf(t, o.store.GetObject(
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "app-a-cm"},
	) == nil, "%v", "app-a-cm ConfigMap missing; the referenced base resource should still render")
}
