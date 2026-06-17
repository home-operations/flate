package orchestrator

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// TestOrchestrator_UnreferencedBaseKSOrphaned covers the committed form of the
// #777 repro: apps/base/app-X/ks.yaml exists on disk but the base's
// kustomization.yaml references only cm.yaml, so cluster-apps (and real Flux)
// emit just the ConfigMaps and never apply the app KSes. flate's file walk still
// loads the stray ks.yaml files; pre-fix it reconciled them standalone and
// hard-failed the ${CLUSTER_ENV} path.
//
// Since real Flux would never apply an unreferenced base manifest, flate demotes
// it to an orphan warning instead of a failure — both the no-dependsOn root and
// its cascade dependent. The workload ConfigMaps still render.
func TestOrchestrator_UnreferencedBaseKSOrphaned(t *testing.T) {
	dir := t.TempDir()

	testutil.WriteFile(t, dir, "cluster/flux-system.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-apps
  namespace: flux-system
spec:
  path: ./apps/test
  sourceRef:
    kind: GitRepository
    name: flux-system
`)
	testutil.WriteFile(t, dir, "apps/test/kustomization.yaml", "resources:\n  - ./app-a\n  - ./app-b\n")
	testutil.WriteFile(t, dir, "apps/test/app-a/kustomization.yaml", "resources:\n  - ../../base/app-a\n")
	testutil.WriteFile(t, dir, "apps/test/app-b/kustomization.yaml", "resources:\n  - ../../base/app-b\n")

	// Base kustomizations reference ONLY cm.yaml — ks.yaml is a stray file the
	// base excludes, so nothing applies the app KSes.
	for _, app := range []string{"app-a", "app-b"} {
		testutil.WriteFile(t, dir, "apps/base/"+app+"/kustomization.yaml", "resources:\n  - cm.yaml\n")
		testutil.WriteFile(t, dir, "apps/base/"+app+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: "+app+"-cm\n  namespace: flux-system\n")
	}
	testutil.WriteFile(t, dir, "apps/base/app-a/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: app-a
  namespace: flux-system
spec:
  path: ./apps/${CLUSTER_ENV}/app-a
  sourceRef:
    kind: GitRepository
    name: flux-system
`)
	testutil.WriteFile(t, dir, "apps/base/app-b/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: app-b
  namespace: flux-system
spec:
  path: ./apps/${CLUSTER_ENV}/app-b
  dependsOn:
    - name: app-a
  sourceRef:
    kind: GitRepository
    name: flux-system
`)

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render returned a failure error for unreferenced base KSes: %v", err)
	}

	for _, name := range []string{"app-a", "app-b"} {
		id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
		if info, failed := res.Failed[id]; failed {
			t.Errorf("%s hard-failed, want orphaned: %s", name, info.Message)
		}
		if _, orphaned := res.Orphans[id]; !orphaned {
			t.Errorf("%s not orphaned; want it demoted (unreferenced base manifest)", name)
		}
	}
	// The referenced workload ConfigMap is still applied.
	if o.store.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "app-a-cm"}) == nil {
		t.Error("app-a-cm ConfigMap missing; the referenced base resource should still render")
	}
}
