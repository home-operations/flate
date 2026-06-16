package orchestrator

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// TestOrchestrator_CrossTreeBaseSubstitution reproduces issue #777: Flux
// Kustomization manifests stored as kustomize bases (apps/base/app-X/ks.yaml,
// spec.path ./apps/${CLUSTER_ENV}/app-X) that a parent KS (cluster-apps,
// spec.path ./apps/test) pulls in cross-tree via overlays and applies with
// postBuild.substituteFrom + a patch injecting substituteFrom into every child
// KS. Real Flux only ever applies cluster-apps's substituted emission of each
// app KS; the raw base file is never reconciled directly.
//
// Pre-fix, flate's file walk loaded each base KS as a standalone object whose
// source file sits under no KS spec.path — so it had no parent gate and a root
// (no-dependsOn) app KS reconciled the raw, UNSUBSTITUTED copy immediately,
// failing "path is not a directory: ./apps/${CLUSTER_ENV}/app-a" before
// cluster-apps emitted the substituted version. The app KS spec.path also
// re-includes its own base, so its self-emission stripped the parent-injected
// postBuild and flip-flopped back to the unsubstituted path.
//
// Post-fix: discovery gates each base KS on its emitting parent (cluster-apps),
// so it reconciles only after the substituted emission lands instead of racing
// it; and emit no longer lets a KS's self-emission clobber its own store object
// (which would strip the postBuild). Every app KS resolves ${CLUSTER_ENV} →
// test deterministically, for both the no-dependsOn root and the dependent.
func TestOrchestrator_CrossTreeBaseSubstitution(t *testing.T) {
	dir := t.TempDir()

	// Entry KSes: cluster-settings (provides the substitution ConfigMap) and
	// cluster-apps (fans out ./apps/test with postBuild substitution + a patch
	// injecting substituteFrom into every emitted child Kustomization).
	testutil.WriteFile(t, dir, "cluster/flux-system.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-settings
  namespace: flux-system
spec:
  path: ./settings
  sourceRef:
    kind: GitRepository
    name: flux-system
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-apps
  namespace: flux-system
spec:
  path: ./apps/test
  dependsOn:
    - name: cluster-settings
  sourceRef:
    kind: GitRepository
    name: flux-system
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
  patches:
    - patch: |-
        apiVersion: kustomize.toolkit.fluxcd.io/v1
        kind: Kustomization
        metadata:
          name: not-used
        spec:
          postBuild:
            substituteFrom:
              - kind: ConfigMap
                name: cluster-settings
      target:
        group: kustomize.toolkit.fluxcd.io
        kind: Kustomization
`)

	testutil.WriteFile(t, dir, "settings/kustomization.yaml", "resources:\n  - ./cluster-settings.yaml\n")
	testutil.WriteFile(t, dir, "settings/cluster-settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  CLUSTER_ENV: "test"
`)

	// Overlay: each app pulls its cross-tree base. The base's kustomization
	// references ks.yaml (the Flux KS) AND cm.yaml (the workload), so
	// cluster-apps's render emits the app KSes and app-X's own render
	// re-includes its base (the self-reference that drove the flip-flop).
	testutil.WriteFile(t, dir, "apps/test/kustomization.yaml", "resources:\n  - ./app-a\n  - ./app-b\n")
	testutil.WriteFile(t, dir, "apps/test/app-a/kustomization.yaml", "resources:\n  - ../../base/app-a\n")
	testutil.WriteFile(t, dir, "apps/test/app-b/kustomization.yaml", "resources:\n  - ../../base/app-b\n")

	testutil.WriteFile(t, dir, "apps/base/app-a/kustomization.yaml", "resources:\n  - ks.yaml\n  - cm.yaml\n")
	// app-a is a ROOT — no dependsOn — so it fires as soon as its source is
	// ready, the case that consistently lost the race pre-fix.
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
	testutil.WriteFile(t, dir, "apps/base/app-a/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: app-a-cm
  namespace: flux-system
`)

	testutil.WriteFile(t, dir, "apps/base/app-b/kustomization.yaml", "resources:\n  - ks.yaml\n  - cm.yaml\n")
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
	testutil.WriteFile(t, dir, "apps/base/app-b/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: app-b-cm
  namespace: flux-system
`)

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	for _, name := range []string{"app-a", "app-b"} {
		id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
		ks, ok := o.store.GetObject(id).(*manifest.Kustomization)
		if !ok {
			t.Fatalf("%s missing from store", name)
		}
		if want := "./apps/test/" + name; ks.Path != want {
			t.Errorf("%s spec.path = %q, want %q (${CLUSTER_ENV} should resolve to test)", name, ks.Path, want)
		}
		if info, failed := res.Failed[id]; failed {
			t.Errorf("%s failed: %s", name, info.Message)
		}
	}
}
