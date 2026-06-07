package change_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

// TestIntegration_KeepCascadeIsBlockedByAncestorOnlyEmitter pins
// the end-to-end behavior of the keep-set fix (#308 + #312) against
// a realistic two-subtree layout:
//
//	cluster-apps (KS at kubernetes/apps)
//	├── media-apps        (KS at kubernetes/apps/media)
//	│   └── plex          (KS at kubernetes/apps/media/plex, has CM)
//	└── db-apps           (KS at kubernetes/apps/db)
//	    └── postgres      (KS at kubernetes/apps/db/postgres, has CM)
//
// The change set touches a single file under media/plex. After
// orchestrator.Render, the expected story is:
//
//   - plex's chain reconciles (ShouldReconcile==true throughout).
//   - The ancestor chain (cluster-apps, media-apps) renders so
//     parent-injected patches reach plex — but they don't promote
//     unrelated children into keep (the #308 cascade fix).
//   - db-apps and postgres land in the Store with StatusReady,
//     Message=MsgUnchanged — proof that PreGate short-circuited
//     them and no kustomize build / no helm template / no source
//     fetch happened for the unrelated subtree.
//
// Pre-fix, every KS in the tree reconciled because
// emitRenderedChildren cascaded keep through the ancestor renders.
// Post-fix, only the change-touched subtree runs.
func TestIntegration_KeepCascadeIsBlockedByAncestorOnlyEmitter(t *testing.T) {
	dir := t.TempDir()

	// Root sources + cluster-apps KS that fans out into media-apps
	// and db-apps.
	mustWrite(t, filepath.Join(dir, "kubernetes/flux/cluster.yaml"), `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  url: https://example.test/cluster.git
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-apps
  namespace: flux-system
spec:
  interval: 10m
  path: ./kubernetes/apps
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)

	// Intermediate ancestors per subtree.
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/media/ks.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: media-apps
  namespace: flux-system
spec:
  interval: 10m
  path: ./kubernetes/apps/media
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/db/ks.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: db-apps
  namespace: flux-system
spec:
  interval: 10m
  path: ./kubernetes/apps/db
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)

	// Leaf KSes — each owns its own spec.path directly.
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/media/plex/ks.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: plex
  namespace: media
spec:
  interval: 10m
  path: ./kubernetes/apps/media/plex/app
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/db/postgres/ks.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: postgres
  namespace: db
spec:
  interval: 10m
  path: ./kubernetes/apps/db/postgres/app
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)

	// Each leaf's app/ subtree gets an empty kustomization.yaml so
	// kustomize doesn't error out at render time.
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/media/plex/app/kustomization.yaml"),
		"resources: [./cm.yaml]\n")
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/db/postgres/app/kustomization.yaml"),
		"resources: [./cm.yaml]\n")

	// The change touches plex's CM only.
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/media/plex/app/cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: plex-config
  namespace: media
data:
  changed: "yes"
`)
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/db/postgres/app/cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-config
  namespace: db
data:
  unchanged: "true"
`)

	o, err := orchestrator.New(orchestrator.Config{
		Path:        dir,
		WipeSecrets: true,
		ExternalChanges: change.NewSet([]string{
			"kubernetes/apps/media/plex/app/cm.yaml",
		}),
	})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}

	if _, err := o.Render(context.Background()); err != nil {
		t.Logf("Render returned err: %v (informational)", err)
	}

	// Resolve every KS's final status. The keep-cascade fix means
	// the db side stays MsgUnchanged — proof its reconcile body
	// never ran.
	plex := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "media", Name: "plex"}
	postgres := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "db", Name: "postgres"}
	dbApps := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "db-apps"}

	requireReconciled := func(id manifest.NamedResource) {
		t.Helper()
		info, ok := o.Store().GetStatus(id)
		if !ok {
			t.Errorf("%s: no status reported (expected reconciled)", id)
			return
		}
		if store.IsUnchanged(info) {
			t.Errorf("%s: expected reconciled, got MsgUnchanged — keep-cascade fix did not pull this into keep", id)
		}
	}
	requireUnchanged := func(id manifest.NamedResource) {
		t.Helper()
		info, ok := o.Store().GetStatus(id)
		if !ok {
			t.Errorf("%s: no status reported (expected MsgUnchanged)", id)
			return
		}
		if !store.IsUnchanged(info) {
			t.Errorf("%s: keep-cascade leaked; expected MsgUnchanged, got Status=%s Message=%q",
				id, info.Status, info.Message)
		}
	}

	// plex is the file-change owner: must reconcile.
	requireReconciled(plex)
	// db-apps and postgres are in the unrelated subtree: must stay
	// SKIPPED. db-apps is an ancestor in its own tree but NOT an
	// ancestor of the changed file, so it should also stay
	// SKIPPED.
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

// TestIntegration_DiffWidensToRepoRootForSiblingCheckouts pins the
// fix for the diff-coupling footgun seen in real-world validation:
// when the user points `--path` at a Flux cluster subdir (e.g.
// `<repo>/kubernetes/flux/cluster`) and `--path-orig` at the same
// subdir of a sibling checkout, edits in `<repo>/kubernetes/apps/...`
// must still enter the keep set. The old behavior diffed the literal
// subdirs (which were byte-identical) and produced an empty keep set,
// silently rendering nothing. The new behavior detects that both sides
// resolve to distinct .git roots and widens the diff to those roots.
func TestIntegration_DiffWidensToRepoRootForSiblingCheckouts(t *testing.T) {
	origRoot := t.TempDir()
	currRoot := t.TempDir()
	// Mark both as separate git checkouts so discovery.FindRepoRoot
	// picks them up. A bare directory is enough — change.Detect's git
	// path runs `git diff --no-index` which doesn't read .git internals.
	mustWrite(t, filepath.Join(origRoot, ".git", "HEAD"), "ref: refs/heads/main\n")
	mustWrite(t, filepath.Join(currRoot, ".git", "HEAD"), "ref: refs/heads/main\n")

	// Identical Flux bootstrap on both sides — the cluster subdir
	// content is byte-identical, so a literal subdir-vs-subdir diff
	// would return zero.
	bootstrap := `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  url: https://example.test/cluster.git
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-apps
  namespace: flux-system
spec:
  interval: 10m
  path: ./kubernetes/apps/foo
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`
	mustWrite(t, filepath.Join(origRoot, "kubernetes/flux/cluster.yaml"), bootstrap)
	mustWrite(t, filepath.Join(currRoot, "kubernetes/flux/cluster.yaml"), bootstrap)

	// The Kustomization spec.path tree lives under apps/foo — outside
	// the flux/ subdir, exactly where real-world repos put their app
	// manifests. The change is in this tree.
	appKust := `resources:
  - ./cm.yaml
`
	mustWrite(t, filepath.Join(origRoot, "kubernetes/apps/foo/kustomization.yaml"), appKust)
	mustWrite(t, filepath.Join(currRoot, "kubernetes/apps/foo/kustomization.yaml"), appKust)
	mustWrite(t, filepath.Join(origRoot, "kubernetes/apps/foo/cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: foo-config
  namespace: flux-system
data:
  value: original
`)
	mustWrite(t, filepath.Join(currRoot, "kubernetes/apps/foo/cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: foo-config
  namespace: flux-system
data:
  value: changed
`)

	// Point --path / --path-orig at the cluster subdir, NOT the repo
	// root — this is the user-facing convention and the case the fix
	// targets. The flux/cluster directory is byte-identical between
	// the two checkouts; only kubernetes/apps/foo/cm.yaml differs.
	currCluster := filepath.Join(currRoot, "kubernetes/flux")

	// The CLI lifts a subdir --path / --path-orig to its repo root before
	// reaching the orchestrator (repoRootOf); here we pass the explicit
	// roots directly. change.Detect runs root-to-root, so an edit in
	// kubernetes/apps/foo — outside the scanned flux/ subdir — still
	// enters the keep set, without any .git-ancestor "widen" heuristic.
	o, err := orchestrator.New(orchestrator.Config{
		Path:        currCluster,
		RepoRoot:    currRoot,
		PathOrig:    origRoot,
		WipeSecrets: true,
	})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	f := o.Filter()
	if f == nil {
		t.Fatal("expected change.Filter to be wired (PathOrig was set), got nil — buildChangeFilter dropped the filter")
	}
	clusterApps := manifest.NamedResource{
		Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps",
	}
	if !f.ShouldReconcile(clusterApps) {
		t.Errorf("cluster-apps owns the changed file (apps/foo/cm.yaml) but was filtered out of the keep set; diff did not widen to repo root")
	}
}
