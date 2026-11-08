package loader

import (
	"context"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// TestFilter_AddExtendsKeepSetAtRuntime pins issue #204: a parent
// KS in the keep set may render and emit a child KS that wasn't
// discoverable at filter-build time (kustomize component +
// replacement patterns generate per-app Kustomizations on the fly).
// The KS controller calls admissionFixture.AddEmitted(parent, new(child)) before
// AddObject so the listener's PreGate filter check sees the
// extended keep set. This test uses addUngated directly (skipping
// the primacy gate) to seed the runtime keep without simulating
// a parent render; the gated path is covered by the AddEmitted
// tests below.
//
// Without this, the kept parent reconciles but every render-emitted
// child gets marked Ready "unchanged" by PreGate, never reconciles,
// and the child's own render output is silently absent from the
// diff (the user's repo: 37 of 40 OCIRepository digest changes
// missing because their `components/ks/app` pattern emits the leaf
// KS via replacements).
func TestFilter_AddExtendsKeepSetAtRuntime(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			parent := testutil.KustomizationID("flux-system", "database")
			child := &manifest.Kustomization{Name: "leaf-app", Namespace: "database"}
			sibling := testutil.KustomizationID("database", "sibling")
			foreign := testutil.KustomizationID("other", child.Name)
			objects := filtertest.MapLister{
				parent: &manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace},
			}
			f := newAdmission(
				t,
				admissionConfig{
					changes: change.NewSet([]string{"parent.yaml"}),
					index: new(filterFixture{
						files:   map[manifest.NamedResource]string{parent: "parent.yaml", sibling: "sibling.yaml"},
						objects: objects,
					}).index(),
					inventory: objects,
					root:      "",
				},
			)
			assert.Equal(t, f.Reconcile(parent), true)
			tasks := task.NewBounded(workers)
			captured, published := make(chan bool, workers), make(chan struct{})
			observations := make(chan bool, workers)
			for range workers {
				tasks.Go(t.Context(), "selection-reader", func(context.Context) {
					captured <- f.Reconcile(child.Named())
					<-published
					observations <- f.Reconcile(child.Named()) && !f.Reconcile(sibling) && !f.Reconcile(foreign)
				})
			}
			for range workers {
				assert.Equal(t, <-captured, false)
			}
			added := f.AddEmitted(parent, new(store.Child{Obj: child}), f.objs)
			assert.Diff(t, added, []manifest.NamedResource{child.Named()})
			close(published)
			tasks.BlockTillDone()
			for range workers {
				assert.Equal(t, <-observations, true)
			}
			assert.Equal(t, tasks.Failures(), int64(0))
		})
	}
}

// TestFilter_AddOnDisabledFilterIsNoOp protects against accidentally
// activating the keep-set semantics when the filter is disabled
// (no --path-orig). A disabled filter must continue to return true
// for everything regardless of Add() calls.
func TestFilter_AddOnDisabledFilterIsNoOp(t *testing.T) {
	t.Parallel()
	var f admissionFixture
	id := testutil.KustomizationID("ns", "x")
	f.addUngated(id)
	other := testutil.HelmReleaseID("ns", "y")
	assert.ErrorIf(t, !f.Reconcile(other), "disabled filter must still keep everything after Add()")
}

// TestFilter_AddEmittedRejectsAncestorOnlyEmitter pins the
// keep-cascade fix: when a parent KS is in the keep set only as an
// ancestor (kept so its patches/substituteFrom render before the
// leaf descendants per #58), its file-loaded emitted children must
// NOT auto-join keep. Without this gate, a one-file change in a
// deeply-nested leaf cascades the entire tree into keep — every
// ancestor renders, emits every file-loaded child, AddEmitted
// previously called Add unconditionally, every emitted child then
// emits its own children, and the cluster is back to a full reconcile.
//
// The change: AddEmitted no-ops when the emitter is in keep only
// as an ancestor. Render-only children (no sourceFile entry) of an
// ancestor parent are also gated out — if the ancestor's own render
// is unchanged from baseline, anything it emits at render time is
// unchanged too, and there's no diff to capture.
func TestFilter_AddEmittedRejectsAncestorOnlyEmitter(t *testing.T) {
	t.Parallel()
	// reloader's OCIRepository file is the only file change. The
	// leaf KS (reloader/app) owns that file → marked primary. The
	// "cluster-apps" ancestor whose spec.path covers everything is
	// kept (so its patches apply) but is NOT primary.
	leafKS := testutil.KustomizationID("kube-system", "reloader-app")
	clusterApps := testutil.KustomizationID("flux-system", "cluster-apps")
	siblingApp := testutil.KustomizationID("media", "plex-app")

	leafKSObj := &manifest.Kustomization{
		Name: "reloader-app", Namespace: "kube-system",
		Path: "kubernetes/apps/kube-system/reloader/app",
	}
	clusterAppsObj := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "kubernetes/apps",
	}
	siblingObj := &manifest.Kustomization{
		Name: "plex-app", Namespace: "media",
		Path: "kubernetes/apps/media/plex/app",
	}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"kubernetes/apps/kube-system/reloader/app/ocirepository.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				leafKS:      "kubernetes/apps/kube-system/reloader/app/ks.yaml",
				clusterApps: "kubernetes/flux/cluster-apps.yaml",
				siblingApp:  "kubernetes/apps/media/plex/ks.yaml",
			}, root: "", objects: filtertest.MapLister{leafKS: leafKSObj, clusterApps: clusterAppsObj, siblingApp: siblingObj}}).index(),
			inventory: filtertest.MapLister{leafKS: leafKSObj, clusterApps: clusterAppsObj, siblingApp: siblingObj},
			root:      "",
		},
	)

	if !f.Reconcile(leafKS) {
		t.Fatalf("leaf KS (owner of changed file) must be in keep; keep=%v", f.KeepNames())
	}
	if !f.Reconcile(clusterApps) {
		t.Fatalf("cluster-apps ancestor must be in keep (so its patches render); keep=%v", f.KeepNames())
	}
	if f.Reconcile(siblingApp) {
		t.Fatalf(
			"precondition: sibling under same ancestor must NOT be in keep at resolve time; keep=%v",
			f.KeepNames(),
		)
	}

	// Simulate cluster-apps rendering and emitting the file-loaded
	// sibling. AddEmitted must NOT keep-add it.
	f.AddEmitted(clusterApps, new(store.Child{Obj: refObj(siblingApp)}), f.objs)
	if f.Reconcile(siblingApp) {
		t.Errorf(
			"AddEmitted(ancestor, sibling) over-extended keep set; sibling should remain SKIPPED; keep=%v",
			f.KeepNames(),
		)
	}

	// Same path but with a render-only child (no sourceFiles entry):
	// still rejected because the emitter (ancestor) is not primary
	// and an unchanged emitter wouldn't produce a different child.
	renderOnly := testutil.KustomizationID("rendered", "from-replacement")
	f.AddEmitted(clusterApps, new(store.Child{Obj: refObj(renderOnly)}), f.objs)
	if f.Reconcile(renderOnly) {
		t.Errorf("AddEmitted(ancestor, render-only) should reject when ancestor is not primary; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddEmittedNoCascadeAcrossDeepAncestorChain pins the
// 3+ level ancestor case from the home-ops layout: a single file
// change deep under cluster-apps → kube-system → reloader →
// reloader-app should land ONLY reloader-app and the ancestor
// chain in keep, NOT every other ancestor's siblings (media-apps,
// network-apps, etc.). Each ancestor renders so its patches
// propagate (#58) but its AddEmitted calls for unrelated children
// must skip because the ancestor itself is not primary.
//
// The fix's correctness rests on this chain depth — the single-
// ancestor test alone wouldn't catch a future change that
// accidentally promoted an ancestor to primary mid-cascade.
func TestFilter_AddEmittedNoCascadeAcrossDeepAncestorChain(t *testing.T) {
	t.Parallel()
	clusterApps := testutil.KustomizationID("flux-system", "cluster-apps")
	kubeSystem := testutil.KustomizationID("flux-system", "kube-system")
	reloader := testutil.KustomizationID("kube-system", "reloader")
	reloaderApp := testutil.KustomizationID("kube-system", "reloader-app")

	// Unrelated siblings at each ancestor level — none should
	// land in keep regardless of which level emits them.
	mediaSibling := testutil.KustomizationID("flux-system", "media-apps")
	spegelSibling := testutil.KustomizationID("kube-system", "spegel")
	reloaderSibling := testutil.KustomizationID("kube-system", "descheduler-app")

	objs := filtertest.MapLister{
		clusterApps: &manifest.Kustomization{
			Name: clusterApps.Name, Namespace: clusterApps.Namespace,
			Path: "kubernetes/apps",
		},
		kubeSystem: &manifest.Kustomization{
			Name: kubeSystem.Name, Namespace: kubeSystem.Namespace,
			Path: "kubernetes/apps/kube-system",
		},
		reloader: &manifest.Kustomization{
			Name: reloader.Name, Namespace: reloader.Namespace,
			Path: "kubernetes/apps/kube-system/reloader",
		},
		reloaderApp: &manifest.Kustomization{
			Name: reloaderApp.Name, Namespace: reloaderApp.Namespace,
			Path: "kubernetes/apps/kube-system/reloader/app",
		},
		mediaSibling: &manifest.Kustomization{
			Name:      mediaSibling.Name,
			Namespace: mediaSibling.Namespace,
			Path:      "kubernetes/apps/media",
		},
		spegelSibling: &manifest.Kustomization{
			Name:      spegelSibling.Name,
			Namespace: spegelSibling.Namespace,
			Path:      "kubernetes/apps/kube-system/spegel",
		},
		reloaderSibling: &manifest.Kustomization{
			Name:      reloaderSibling.Name,
			Namespace: reloaderSibling.Namespace,
			Path:      "kubernetes/apps/kube-system/descheduler/app",
		},
	}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"kubernetes/apps/kube-system/reloader/app/ocirepository.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				clusterApps:     "kubernetes/flux/cluster-apps.yaml",
				kubeSystem:      "kubernetes/flux/kube-system.yaml",
				reloader:        "kubernetes/apps/kube-system/reloader/ks.yaml",
				reloaderApp:     "kubernetes/apps/kube-system/reloader/app/ks.yaml",
				mediaSibling:    "kubernetes/flux/media-apps.yaml",
				spegelSibling:   "kubernetes/apps/kube-system/spegel/ks.yaml",
				reloaderSibling: "kubernetes/apps/kube-system/descheduler/app/ks.yaml",
			}, root: "", objects: objs}).index(),
			inventory: objs,
			root:      "",
		},
	)

	// Precondition: the leaf and its ancestor chain are in keep.
	for _, id := range []manifest.NamedResource{reloaderApp, reloader, kubeSystem, clusterApps} {
		if !f.Reconcile(id) {
			t.Fatalf("expected %s in keep at resolve time; keep=%v", id, f.KeepNames())
		}
	}
	// And the unrelated siblings are NOT.
	for _, id := range []manifest.NamedResource{mediaSibling, spegelSibling, reloaderSibling} {
		if f.Reconcile(id) {
			t.Fatalf("precondition: %s must NOT be in keep before emissions; keep=%v", id, f.KeepNames())
		}
	}

	// Simulate cascade: each ancestor renders and emits all of
	// its children — including the unrelated siblings. Walking
	// the chain top-down so we cover the worst case where a
	// primary could theoretically leak through an upper ancestor.
	f.AddEmitted(clusterApps, new(store.Child{Obj: refObj(kubeSystem)}), f.objs)
	f.AddEmitted(clusterApps, new(store.Child{Obj: refObj(mediaSibling)}), f.objs)
	f.AddEmitted(kubeSystem, new(store.Child{Obj: refObj(reloader)}), f.objs)
	f.AddEmitted(kubeSystem, new(store.Child{Obj: refObj(spegelSibling)}), f.objs)
	f.AddEmitted(reloader, new(store.Child{Obj: refObj(reloaderApp)}), f.objs)
	f.AddEmitted(reloader, new(store.Child{Obj: refObj(reloaderSibling)}), f.objs)

	// Each ancestor (clusterApps, kubeSystem, reloader) is in
	// keep only as ancestor-only — none of their AddEmitted calls
	// should promote a file-loaded sibling into keep.
	for _, id := range []manifest.NamedResource{mediaSibling, spegelSibling, reloaderSibling} {
		if f.Reconcile(id) {
			t.Errorf("ancestor-only cascade leaked: %s joined keep via AddEmitted; keep=%v", id, f.KeepNames())
		}
	}
	// Sanity: the ancestor chain itself is still kept.
	for _, id := range []manifest.NamedResource{reloaderApp, reloader, kubeSystem, clusterApps} {
		if !f.Reconcile(id) {
			t.Errorf("ancestor chain entry %s dropped after AddEmitted churn; keep=%v", id, f.KeepNames())
		}
	}
}

// TestFilter_AddEmittedFromPrimaryParent verifies the #204 path
// still works: a primary parent (its own file changed, or it owns
// the changed file) emitting any child — render-only or file-loaded
// — keep-adds the child. This is the original AddEmitted purpose
// and the bug-cascade prevention should not regress it.
func TestFilter_AddEmittedFromPrimaryParent(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("flux-system", "database")
	child := testutil.KustomizationID("database", "leaf-app")
	objects := filtertest.MapLister{parent: &manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace}}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"apps/database/parent.yaml"}),
			index: new(filterFixture{
				files:   map[manifest.NamedResource]string{parent: "apps/database/parent.yaml"},
				root:    "",
				objects: objects,
			}).index(),
			inventory: objects,
			root:      "",
		},
	)
	if !f.Reconcile(parent) {
		t.Fatalf("parent must be primary from direct file change; keep=%v", f.KeepNames())
	}

	f.AddEmitted(parent, new(store.Child{Obj: refObj(child)}), f.objs)
	if !f.Reconcile(child) {
		t.Errorf("AddEmitted(primary parent, child) should keep-add child; keep=%v", f.KeepNames())
	}
}
