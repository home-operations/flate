package change_test

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestIntegration_KeepCascadeIsBlockedByAncestorOnlyEmitter(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", "resources: [media/ks.yaml, db/ks.yaml]\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/media/kustomization.yaml", "resources: [plex/ks.yaml]\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/db/kustomization.yaml", "resources: [postgres/ks.yaml]\n")

	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes/flux/cluster.yaml"), "---\n"+
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  url: https://example.test/cluster.git
`+"---\n"+
		testutil.KustomizationYAML("cluster-apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`))

	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes/apps/media/ks.yaml"), testutil.KustomizationYAML("media-apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps/media
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`))
	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes/apps/db/ks.yaml"), testutil.KustomizationYAML("db-apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps/db
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`))

	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes/apps/media/plex/ks.yaml"), testutil.KustomizationYAML("plex", "media", `  interval: 10m
  path: ./kubernetes/apps/media/plex/app
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`))
	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes/apps/db/postgres/ks.yaml"), testutil.KustomizationYAML("postgres", "db", `  interval: 10m
  path: ./kubernetes/apps/db/postgres/app
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`))

	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes/apps/media/plex/app/kustomization.yaml"),
		"resources: [./cm.yaml]\n")
	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes/apps/db/postgres/app/kustomization.yaml"),
		"resources: [./cm.yaml]\n")

	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes/apps/media/plex/app/cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: plex-config
  namespace: media
data:
  changed: "yes"
`)
	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes/apps/db/postgres/app/cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-config
  namespace: db
data:
  unchanged: "true"
`)

	o, err := orchestrator.New(t.Context(), orchestrator.Config{
		Path:        dir,
		WipeSecrets: true,
		ExternalChanges: change.NewSet([]string{
			"kubernetes/apps/media/plex/app/cm.yaml",
		}),
	})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}

	if _, err := o.Render(t.Context()); err != nil {
		t.Fatalf("Render: %v", err)
	}

	plex := testutil.KustomizationID("media", "plex")
	postgres := testutil.KustomizationID("db", "postgres")
	dbApps := testutil.KustomizationID("flux-system", "db-apps")

	requireReconciled := func(id manifest.NamedResource) {
		t.Helper()
		info, ok := o.Store().GetStatus(id)
		if !ok {
			t.Errorf("%s: no status reported (expected reconciled)", id)
			return
		}
		if info.Outcome() == store.OutcomeUnchanged {
			t.Errorf("%s: expected reconciled, got OutcomeUnchanged — keep-cascade fix did not pull this into keep", id)
		}
	}
	requireUnchanged := func(id manifest.NamedResource) {
		t.Helper()
		info, ok := o.Store().GetStatus(id)
		if !ok {
			t.Errorf("%s: no status reported (expected OutcomeUnchanged)", id)
			return
		}
		if info.Outcome() != store.OutcomeUnchanged {
			t.Errorf("%s: keep-cascade leaked; expected OutcomeUnchanged, got Status=%s Message=%q",
				id, info.Status, info.Message)
		}
	}

	requireReconciled(plex)
	requireUnchanged(dbApps)
	requireUnchanged(postgres)
}

// The depwait long-wait + render-budget-cap behavior (#310/#313/#311)
// is covered at the Waiter level by pkg/depwait's
// TestWaiter_RenderOnlyDepWaitsBeyondGrace and friends — the unit
// suite exercises every code path the orchestrator wiring reaches
// via depwait.Waiter.Existence. An orchestrator-level duplicate
// would require a slow fake fetcher to recreate the timing window
// in real-time, slowing CI without catching new bugs.
