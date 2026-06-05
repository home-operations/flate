package change

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFilter_DisabledKeepsEverything(t *testing.T) {
	var f Filter
	id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "x"}
	if !f.ShouldReconcile(id) {
		t.Fatal("disabled filter must keep everything")
	}
}

func TestFilter_ResolveDirectMatch(t *testing.T) {
	hr := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "x"}
	f := NewFilter(
		NewSet([]string{"apps/x/helmrelease.yaml"}),
		map[manifest.NamedResource]string{hr: "apps/x/helmrelease.yaml"},
		"",
		testutil.EmptyLister(),
	)
	if !f.ShouldReconcile(hr) {
		t.Fatalf("expected direct-match keep; keep=%v", f.KeepNames())
	}
}

func TestFilter_SharedComponentPropagatesToAllConsumers(t *testing.T) {
	plex := &manifest.Kustomization{
		Name: "plex", Namespace: "media",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path:       "apps/media/plex/app",
			Components: []string{"../../../../components/volsync"},
		},
	}
	atuin := &manifest.Kustomization{
		Name: "atuin", Namespace: "default",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path:       "apps/default/atuin/app",
			Components: []string{"../../../../components/volsync"},
		},
	}
	hrPlex := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "media", Name: "plex"}
	hrAtuin := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "atuin"}
	ksPlex, ksAtuin := plex.Named(), atuin.Named()

	f := NewFilter(
		NewSet([]string{"components/volsync/pvc.yaml"}),
		map[manifest.NamedResource]string{
			ksPlex:  "apps/media/plex/app/ks.yaml",
			hrPlex:  "apps/media/plex/app/helmrelease.yaml",
			ksAtuin: "apps/default/atuin/app/ks.yaml",
			hrAtuin: "apps/default/atuin/app/helmrelease.yaml",
		},
		"",
		testutil.MapLister{ksPlex: plex, ksAtuin: atuin},
	)

	for _, id := range []manifest.NamedResource{ksPlex, ksAtuin, hrPlex, hrAtuin} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
}

func TestFilter_AncestorKSAlsoKept(t *testing.T) {
	// A meta-KS at apps/ and a specific KS at apps/media/plex/app —
	// the leaf plex is the deepest owner of the changed file, but the
	// meta-KS's render (parent-injected spec.patches and
	// postBuild.substituteFrom) mutates plex's spec. Both must be
	// kept under changed-only mode so the leaf renders against the
	// post-mutation spec. See #58.
	meta := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "apps"},
	}
	plex := &manifest.Kustomization{
		Name: "plex", Namespace: "media",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "apps/media/plex/app"},
	}
	hrPlex := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "media", Name: "plex"}
	metaID, plexID := meta.Named(), plex.Named()

	f := NewFilter(
		NewSet([]string{"apps/media/plex/app/helmrelease.yaml"}),
		map[manifest.NamedResource]string{
			metaID: "flux/cluster/ks.yaml",
			plexID: "apps/media/plex/app/ks.yaml",
			hrPlex: "apps/media/plex/app/helmrelease.yaml",
		},
		"",
		testutil.MapLister{metaID: meta, plexID: plex},
	)

	for _, id := range []manifest.NamedResource{plexID, hrPlex, metaID} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
}

// TestFilter_StructuralParentOfOwnerKSAlsoKept covers the home-ops
// cross-tree pattern (joryirving-style): a leaf Flux KS in apps/base/
// is registered by a parent KS rendering apps/main/. The leaf's
// source file lives under the parent's spec.path, so the parent has
// to reconcile too — otherwise the namespace-scoped sources it emits
// (e.g. components/namespace producing one OCIRepository per tenant
// ns) never land in the store and the leaf can't resolve its chart
// ref. The changed file itself stays in apps/base/, which the parent
// does NOT cover via spec.path, so ancestorsOf(file) wouldn't catch
// the parent — only ownersOf(leaf-KS source file) does.
func TestFilter_StructuralParentOfOwnerKSAlsoKept(t *testing.T) {
	parent := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "apps/main"},
	}
	leaf := &manifest.Kustomization{
		Name: "actual", Namespace: "self-hosted",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "apps/base/self-hosted/actual"},
	}
	hrActual := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "self-hosted", Name: "actual"}
	parentID, leafID := parent.Named(), leaf.Named()

	f := NewFilter(
		NewSet([]string{"apps/base/self-hosted/actual/helmrelease.yaml"}),
		map[manifest.NamedResource]string{
			parentID: "clusters/main/apps.yaml",
			leafID:   "apps/main/self-hosted/actual.yaml",
			hrActual: "apps/base/self-hosted/actual/helmrelease.yaml",
		},
		"",
		testutil.MapLister{parentID: parent, leafID: leaf},
	)

	for _, id := range []manifest.NamedResource{hrActual, leafID, parentID} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
}

func TestFilter_AncestorKSDoesNotPullInUnrelatedSiblings(t *testing.T) {
	// Two leaf KSes under the same meta-KS. Only plex changes. plex
	// + meta are kept; atuin (an unrelated sibling under the meta-KS)
	// must NOT be pulled in — keeping the meta-KS reconcile-eligible
	// does not widen sibling-resource coverage.
	meta := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "apps"},
	}
	plex := &manifest.Kustomization{
		Name: "plex", Namespace: "media",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "apps/media/plex/app"},
	}
	atuin := &manifest.Kustomization{
		Name: "atuin", Namespace: "default",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "apps/default/atuin/app"},
	}
	metaID, plexID, atuinID := meta.Named(), plex.Named(), atuin.Named()
	hrAtuin := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "atuin"}

	f := NewFilter(
		NewSet([]string{"apps/media/plex/app/helmrelease.yaml"}),
		map[manifest.NamedResource]string{
			metaID:  "flux/cluster/ks.yaml",
			plexID:  "apps/media/plex/app/ks.yaml",
			atuinID: "apps/default/atuin/app/ks.yaml",
			hrAtuin: "apps/default/atuin/app/helmrelease.yaml",
		},
		"",
		testutil.MapLister{metaID: meta, plexID: plex, atuinID: atuin},
	)

	if !f.ShouldReconcile(metaID) {
		t.Errorf("meta KS must be kept (ancestor of changed file): %v", f.KeepNames())
	}
	if !f.ShouldReconcile(plexID) {
		t.Errorf("plex KS must be kept (owns changed file): %v", f.KeepNames())
	}
	if f.ShouldReconcile(atuinID) {
		t.Errorf("unrelated sibling atuin must not be pulled in: %v", f.KeepNames())
	}
	if f.ShouldReconcile(hrAtuin) {
		t.Errorf("unrelated sibling atuin HR must not be pulled in: %v", f.KeepNames())
	}
}

func TestFilter_KeepNamespaces(t *testing.T) {
	// Drive keep via NewFilter rather than mutating the struct.
	ksA := &manifest.Kustomization{Name: "x", Namespace: "ns-a"}
	ksB := &manifest.Kustomization{Name: "y", Namespace: "ns-b"}
	ksCluster := &manifest.Kustomization{Name: "cluster-scope", Namespace: ""}
	a, b, c := ksA.Named(), ksB.Named(), ksCluster.Named()
	f := NewFilter(
		NewSet([]string{"a.yaml", "b.yaml", "c.yaml"}),
		map[manifest.NamedResource]string{a: "a.yaml", b: "b.yaml", c: "c.yaml"},
		"",
		testutil.MapLister{a: ksA, b: ksB, c: ksCluster},
	)
	ns := f.KeepNamespaces()
	got := make([]string, 0, len(ns))
	for k := range ns {
		got = append(got, k)
	}
	slices.Sort(got)
	want := []string{"ns-a", "ns-b"}
	if !slices.Equal(got, want) {
		t.Errorf("KeepNamespaces=%v want %v", got, want)
	}
}

func TestFilter_TransitiveDepsHelmRelease(t *testing.T) {
	hr := &manifest.HelmRelease{
		Name: "plex", Namespace: "media",
		Chart: manifest.HelmChart{
			RepoKind: manifest.KindOCIRepository, RepoName: "app-template", RepoNamespace: "flux-system",
		},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{
				{Kind: manifest.KindConfigMap, Name: "plex-values"},
			},
		},
	}
	hrID := hr.Named()
	repoID := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "app-template"}
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "media", Name: "plex-values"}

	f := NewFilter(
		NewSet([]string{"hr.yaml"}),
		map[manifest.NamedResource]string{hrID: "hr.yaml"},
		"",
		testutil.MapLister{hrID: hr},
	)

	if !f.ShouldReconcile(repoID) {
		t.Errorf("chart source not pulled in by HR; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(cmID) {
		t.Errorf("valuesFrom ref not pulled in by HR; keep=%v", f.KeepNames())
	}
}

func TestFilter_TransitiveDepsKustomization(t *testing.T) {
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		SourceKind: manifest.KindGitRepository, SourceName: "flux-system", SourceNamespace: "flux-system",
		DependsOn: []manifest.DependencyRef{{
			NamedResource: manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "repositories"},
		}},
	}
	ksID := ks.Named()
	gitID := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "flux-system"}
	depID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "repositories"}

	f := NewFilter(
		NewSet([]string{"ks.yaml"}),
		map[manifest.NamedResource]string{ksID: "ks.yaml"},
		"",
		testutil.MapLister{ksID: ks},
	)

	if !f.ShouldReconcile(gitID) {
		t.Errorf("sourceRef not pulled in by KS; keep=%v", f.KeepNames())
	}
	// dependsOn is reconcile-ordering only; unchanged ancestors must
	// stay OUT of the keep set so offline diffs don't render unrelated
	// trees.
	if f.ShouldReconcile(depID) {
		t.Errorf("dependsOn leaked into keep set; keep=%v", f.KeepNames())
	}
}

func TestFilter_DependsOnNotFollowed(t *testing.T) {
	// dependsOn is reconcile-ordering only. A change to `a` must not
	// drag `b` into the keep set just because `a` depends on `b`.
	a := &manifest.Kustomization{
		Name: "a", Namespace: "flux-system",
		SourceKind: manifest.KindGitRepository, SourceName: "src", SourceNamespace: "flux-system",
		DependsOn: []manifest.DependencyRef{{
			NamedResource: manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "b"},
		}},
	}
	b := &manifest.Kustomization{Name: "b", Namespace: "flux-system"}
	aID, bID := a.Named(), b.Named()

	f := NewFilter(
		NewSet([]string{"a.yaml"}),
		map[manifest.NamedResource]string{aID: "a.yaml"},
		"",
		testutil.MapLister{aID: a, bID: b},
	)

	if !f.ShouldReconcile(aID) {
		t.Fatalf("a should be kept; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(bID) {
		t.Errorf("dependsOn dragged b into keep set; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddExtendsKeepSetAtRuntime pins issue #204: a parent
// KS in the keep set may render and emit a child KS that wasn't
// discoverable at filter-build time (kustomize component +
// replacement patterns generate per-app Kustomizations on the fly).
// The KS controller calls Filter.AddEmitted(parent, child) before
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
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "database"}
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "database", Name: "leaf-app"}

	f := NewFilter(
		NewSet([]string{"apps/database/parent.yaml"}),
		map[manifest.NamedResource]string{parent: "apps/database/parent.yaml"},
		"",
		testutil.EmptyLister(),
	)
	if !f.ShouldReconcile(parent) {
		t.Fatalf("parent must be in keep set from file change; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(child) {
		t.Fatalf("precondition: child must NOT be in keep set before Add; keep=%v", f.KeepNames())
	}

	f.addUngated(child)
	if !f.ShouldReconcile(child) {
		t.Errorf("Add(child) should extend keep set; ShouldReconcile(child)=false; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddOnDisabledFilterIsNoOp protects against accidentally
// activating the keep-set semantics when the filter is disabled
// (no --path-orig). A disabled filter must continue to return true
// for everything regardless of Add() calls.
func TestFilter_AddOnDisabledFilterIsNoOp(t *testing.T) {
	var f Filter
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "x"}
	f.addUngated(id)
	other := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "y"}
	if !f.ShouldReconcile(other) {
		t.Errorf("disabled filter must still keep everything after Add()")
	}
}

func TestFilter_ShouldReconcileEmptyNamespaceFallback(t *testing.T) {
	hrLoaded := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "", Name: "x"}
	hrLookup := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "media", Name: "x"}
	f := NewFilter(
		NewSet([]string{"f"}),
		map[manifest.NamedResource]string{hrLoaded: "f"},
		"",
		testutil.EmptyLister(),
	)
	// keep contains hrLoaded (namespace=""); a lookup with namespace=media
	// must still hit via the (Kind, Name) fallback index.
	if !f.ShouldReconcile(hrLookup) {
		t.Fatalf("(Kind, Name) fallback didn't match; keep=%v", f.KeepNames())
	}
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
	// reloader's OCIRepository file is the only file change. The
	// leaf KS (reloader/app) owns that file → marked primary. The
	// "cluster-apps" ancestor whose spec.path covers everything is
	// kept (so its patches apply) but is NOT primary.
	leafKS := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "kube-system", Name: "reloader-app"}
	clusterApps := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
	siblingApp := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "media", Name: "plex-app"}

	leafKSObj := &manifest.Kustomization{Name: "reloader-app", Namespace: "kube-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/apps/kube-system/reloader/app"}}
	clusterAppsObj := &manifest.Kustomization{Name: "cluster-apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/apps"}}
	siblingObj := &manifest.Kustomization{Name: "plex-app", Namespace: "media",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/apps/media/plex/app"}}

	f := NewFilter(
		NewSet([]string{"kubernetes/apps/kube-system/reloader/app/ocirepository.yaml"}),
		map[manifest.NamedResource]string{
			leafKS:      "kubernetes/apps/kube-system/reloader/app/ks.yaml",
			clusterApps: "kubernetes/flux/cluster-apps.yaml",
			siblingApp:  "kubernetes/apps/media/plex/ks.yaml",
		},
		"",
		testutil.MapLister{leafKS: leafKSObj, clusterApps: clusterAppsObj, siblingApp: siblingObj},
	)

	if !f.ShouldReconcile(leafKS) {
		t.Fatalf("leaf KS (owner of changed file) must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(clusterApps) {
		t.Fatalf("cluster-apps ancestor must be in keep (so its patches render); keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(siblingApp) {
		t.Fatalf("precondition: sibling under same ancestor must NOT be in keep at resolve time; keep=%v", f.KeepNames())
	}

	// Simulate cluster-apps rendering and emitting the file-loaded
	// sibling. AddEmitted must NOT keep-add it.
	f.AddEmitted(clusterApps, siblingApp)
	if f.ShouldReconcile(siblingApp) {
		t.Errorf("AddEmitted(ancestor, sibling) over-extended keep set; sibling should remain SKIPPED; keep=%v", f.KeepNames())
	}

	// Same path but with a render-only child (no sourceFiles entry):
	// still rejected because the emitter (ancestor) is not primary
	// and an unchanged emitter wouldn't produce a different child.
	renderOnly := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "rendered", Name: "from-replacement"}
	f.AddEmitted(clusterApps, renderOnly)
	if f.ShouldReconcile(renderOnly) {
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
	clusterApps := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
	kubeSystem := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "kube-system"}
	reloader := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "kube-system", Name: "reloader"}
	reloaderApp := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "kube-system", Name: "reloader-app"}

	// Unrelated siblings at each ancestor level — none should
	// land in keep regardless of which level emits them.
	mediaSibling := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "media-apps"}
	spegelSibling := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "kube-system", Name: "spegel"}
	reloaderSibling := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "kube-system", Name: "descheduler-app"}

	objs := testutil.MapLister{
		clusterApps: &manifest.Kustomization{Name: clusterApps.Name, Namespace: clusterApps.Namespace,
			KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/apps"}},
		kubeSystem: &manifest.Kustomization{Name: kubeSystem.Name, Namespace: kubeSystem.Namespace,
			KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/apps/kube-system"}},
		reloader: &manifest.Kustomization{Name: reloader.Name, Namespace: reloader.Namespace,
			KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/apps/kube-system/reloader"}},
		reloaderApp: &manifest.Kustomization{Name: reloaderApp.Name, Namespace: reloaderApp.Namespace,
			KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/apps/kube-system/reloader/app"}},
		mediaSibling:    &manifest.Kustomization{Name: mediaSibling.Name, Namespace: mediaSibling.Namespace},
		spegelSibling:   &manifest.Kustomization{Name: spegelSibling.Name, Namespace: spegelSibling.Namespace},
		reloaderSibling: &manifest.Kustomization{Name: reloaderSibling.Name, Namespace: reloaderSibling.Namespace},
	}

	f := NewFilter(
		NewSet([]string{"kubernetes/apps/kube-system/reloader/app/ocirepository.yaml"}),
		map[manifest.NamedResource]string{
			clusterApps:     "kubernetes/flux/cluster-apps.yaml",
			kubeSystem:      "kubernetes/flux/kube-system.yaml",
			reloader:        "kubernetes/apps/kube-system/reloader/ks.yaml",
			reloaderApp:     "kubernetes/apps/kube-system/reloader/app/ks.yaml",
			mediaSibling:    "kubernetes/flux/media-apps.yaml",
			spegelSibling:   "kubernetes/apps/kube-system/spegel/ks.yaml",
			reloaderSibling: "kubernetes/apps/kube-system/descheduler/app/ks.yaml",
		},
		"",
		objs,
	)

	// Precondition: the leaf and its ancestor chain are in keep.
	for _, id := range []manifest.NamedResource{reloaderApp, reloader, kubeSystem, clusterApps} {
		if !f.ShouldReconcile(id) {
			t.Fatalf("expected %s in keep at resolve time; keep=%v", id, f.KeepNames())
		}
	}
	// And the unrelated siblings are NOT.
	for _, id := range []manifest.NamedResource{mediaSibling, spegelSibling, reloaderSibling} {
		if f.ShouldReconcile(id) {
			t.Fatalf("precondition: %s must NOT be in keep before emissions; keep=%v", id, f.KeepNames())
		}
	}

	// Simulate cascade: each ancestor renders and emits all of
	// its children — including the unrelated siblings. Walking
	// the chain top-down so we cover the worst case where a
	// primary could theoretically leak through an upper ancestor.
	f.AddEmitted(clusterApps, kubeSystem)
	f.AddEmitted(clusterApps, mediaSibling)
	f.AddEmitted(kubeSystem, reloader)
	f.AddEmitted(kubeSystem, spegelSibling)
	f.AddEmitted(reloader, reloaderApp)
	f.AddEmitted(reloader, reloaderSibling)

	// Each ancestor (clusterApps, kubeSystem, reloader) is in
	// keep only as ancestor-only — none of their AddEmitted calls
	// should promote a file-loaded sibling into keep.
	for _, id := range []manifest.NamedResource{mediaSibling, spegelSibling, reloaderSibling} {
		if f.ShouldReconcile(id) {
			t.Errorf("ancestor-only cascade leaked: %s joined keep via AddEmitted; keep=%v", id, f.KeepNames())
		}
	}
	// Sanity: the ancestor chain itself is still kept.
	for _, id := range []manifest.NamedResource{reloaderApp, reloader, kubeSystem, clusterApps} {
		if !f.ShouldReconcile(id) {
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
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "database"}
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "database", Name: "leaf-app"}

	f := NewFilter(
		NewSet([]string{"apps/database/parent.yaml"}),
		map[manifest.NamedResource]string{parent: "apps/database/parent.yaml"},
		"",
		testutil.EmptyLister(),
	)
	if !f.ShouldReconcile(parent) {
		t.Fatalf("parent must be primary from direct file change; keep=%v", f.KeepNames())
	}

	f.AddEmitted(parent, child)
	if !f.ShouldReconcile(child) {
		t.Errorf("AddEmitted(primary parent, child) should keep-add child; keep=%v", f.KeepNames())
	}
}

// TestFilter_KeepByNameDoesNotLeakAcrossNamespaces pins the
// asymmetric-fallback contract: a fully-namespaced keep entry must
// NOT match a fully-namespaced lookup that happens to share
// (Kind, Name). Without this, a kept
// Kustomization/cluster-infra/external-secrets would silently scope-in
// an unrelated Kustomization/database/external-secrets (and likewise
// for the runtime AddEmitted / Add paths), broadening changed-only
// mode in ways the user can't see. The empty-namespace bridge in
// TestFilter_ShouldReconcileEmptyNamespaceFallback is preserved
// because it indexes only entries whose Namespace is empty.
func TestFilter_KeepByNameDoesNotLeakAcrossNamespaces(t *testing.T) {
	kept := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "cluster-infra", Name: "external-secrets"}
	collider := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "database", Name: "external-secrets"}
	f := NewFilter(
		NewSet([]string{"apps/cluster-infra/external-secrets/ks.yaml"}),
		map[manifest.NamedResource]string{kept: "apps/cluster-infra/external-secrets/ks.yaml"},
		"",
		testutil.EmptyLister(),
	)
	if !f.ShouldReconcile(kept) {
		t.Fatalf("precondition: cluster-infra/external-secrets must be in keep; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(collider) {
		t.Errorf("keepByName leaked across namespaces: database/external-secrets matched on name alone; keep=%v", f.KeepNames())
	}

	// Same invariant via the runtime Add path (issue #204 surface).
	f.addUngated(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "kube-system", Name: "cert-manager"})
	if f.ShouldReconcile(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "monitoring-system", Name: "cert-manager"}) {
		t.Errorf("Add() leaked across namespaces: monitoring-system/cert-manager matched on name alone; keep=%v", f.KeepNames())
	}
}

// TestFilter_TransitiveDepsHelmReleaseViaHelmChartCRD pins the BFS
// through a chartRef→HelmChart CRD: the HelmChart's own sourceRef
// must land in keep so changed-only mode doesn't PreGate-skip the
// backing artifact. Pre-fix, transitiveDeps had no KindHelmChart
// branch — the HelmChart joined keep but its source artifact did
// not, and render failed "artifact not found."
func TestFilter_TransitiveDepsHelmReleaseViaHelmChartCRD(t *testing.T) {
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "apps",
		Chart: manifest.HelmChart{
			RepoKind: manifest.KindHelmChart, RepoName: "demo-chart", RepoNamespace: "apps",
		},
	}
	hc := &manifest.HelmChartSource{
		Name: "demo-chart", Namespace: "apps",
		HelmChartSpec: sourcev1.HelmChartSpec{
			SourceRef: sourcev1.LocalHelmChartSourceReference{
				Kind: manifest.KindOCIRepository, Name: "backing-oci",
			},
		},
	}
	hrID := hr.Named()
	hcID := hc.Named()
	ociID := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "backing-oci"}

	f := NewFilter(
		NewSet([]string{"hr.yaml"}),
		map[manifest.NamedResource]string{hrID: "hr.yaml"},
		"",
		testutil.MapLister{hrID: hr, hcID: hc},
	)

	if !f.ShouldReconcile(hcID) {
		t.Errorf("chartRef-HelmChart not pulled in by HR; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(ociID) {
		t.Errorf("HelmChart's sourceRef OCIRepository not pulled in; keep=%v", f.KeepNames())
	}
}

// TestFilter_ReverseEdgeCentralizedOCIRepository pins the reverse-edge
// fix: when a centralized OCIRepository (its own Kustomization tree,
// separate from the consuming HelmReleases) has its spec.ref.tag
// bumped, the HelmRelease that chartRefs it must re-render even though
// the HR's own files didn't change. Pre-fix the filter walked only
// consumer→source edges, so the changed OCIRepository never reached its
// consumer and `diff hr` printed nothing.
//
// Mirrors github.com/Boemeltrein/TalosCluster: OCIRepository under
// repositories/oci owned by a `repositories` KS, HelmRelease under
// clusters/.../envoy-gateway/app owned by a different KS. The HR is NOT
// in the store at resolve() time (render-driven discovery); the reverse
// edge is driven by the discovery-supplied consumerRefs instead.
func TestFilter_ReverseEdgeCentralizedOCIRepository(t *testing.T) {
	const (
		ociFile = "repositories/oci/envoy-gateway.yaml"
		hrFile  = "clusters/main/kubernetes/networking/envoy-gateway/app/helm-release.yaml"
		sibFile = "clusters/main/kubernetes/networking/envoy-gateway/app/other-hr.yaml"
	)
	oci := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "envoy-gateway"}
	hr := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "envoy-gateway", Name: "envoy-gateway"}
	// Sibling HR under the SAME owner KS that consumes a DIFFERENT source
	// — it must stay out of keep (the owner KS is ancestor-only, so it
	// can't cascade unrelated siblings in).
	sibling := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "envoy-gateway", Name: "other"}
	otherOCI := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "something-else"}
	reposKS := &manifest.Kustomization{Name: "repositories", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "repositories"}}
	appKS := &manifest.Kustomization{Name: "envoy-gateway", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "clusters/main/kubernetes/networking/envoy-gateway/app"}}

	f := NewFilterWithCache(
		NewSet([]string{ociFile}),
		map[manifest.NamedResource]string{
			oci:             ociFile,
			hr:              hrFile,
			sibling:         sibFile,
			reposKS.Named(): "repositories/flux-entry.yaml",
			appKS.Named():   "clusters/main/kubernetes/networking/envoy-gateway/ks.yaml",
		},
		"",
		testutil.MapLister{reposKS.Named(): reposKS, appKS.Named(): appKS},
		nil,
		map[manifest.NamedResource][]manifest.NamedResource{
			hr:      {oci},
			sibling: {otherOCI},
		},
	)

	if !f.ShouldReconcile(oci) {
		t.Fatalf("changed OCIRepository must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(hr) {
		t.Fatalf("reverse edge: HR consuming the changed OCIRepository must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(appKS.Named()) {
		t.Errorf("HR owner KS must render (ancestor) to emit the HR; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(sibling) {
		t.Errorf("sibling HR consuming an unchanged source must NOT be pulled in; keep=%v", f.KeepNames())
	}
	// Namespace scoping (scopedNamespaces → KeepNamespaces) gates diff
	// output: without the HR's namespace the command would scope it out
	// even if ShouldReconcile passed.
	if ns := f.KeepNamespaces(); ns != nil {
		if _, ok := ns["envoy-gateway"]; !ok {
			t.Errorf("HR namespace must be in keep-namespaces for diff scoping; got %v", ns)
		}
	} else {
		t.Errorf("KeepNamespaces returned nil; expected envoy-gateway in scope")
	}
}

// TestFilter_ReverseEdgeNoCascadeFromChangedConsumer pins the guard on
// the reverse edge: it fires ONLY for a source whose OWN file changed,
// never for a source merely referenced by a changed consumer. Two
// HelmReleases share one OCIRepository; editing hrA's file must keep
// hrA but must NOT drag hrB in via the shared source — otherwise a
// single app edit would reverse-cascade every sibling sharing a common
// app-template source.
func TestFilter_ReverseEdgeNoCascadeFromChangedConsumer(t *testing.T) {
	shared := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "app-template"}
	aID := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "a"}
	bID := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "b"}

	f := NewFilterWithCache(
		NewSet([]string{"apps/a/hr.yaml"}),
		map[manifest.NamedResource]string{
			aID:    "apps/a/hr.yaml",
			bID:    "apps/b/hr.yaml",
			shared: "repositories/oci/app-template.yaml",
		},
		"",
		testutil.EmptyLister(),
		nil,
		map[manifest.NamedResource][]manifest.NamedResource{
			aID: {shared},
			bID: {shared},
		},
	)

	if !f.ShouldReconcile(aID) {
		t.Fatalf("changed HR a must be in keep; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(bID) {
		t.Errorf("unchanged sibling HR b reverse-cascaded via shared source; keep=%v", f.KeepNames())
	}
}

// TestFilter_ForwardEdgeChangedHRKeepsChartRefSource is the forward
// counterpart to the reverse edge: when a HelmRelease's OWN file changes, its
// chartRef/sourceRef source must enter keep so the source controller fetches
// the chart. HelmReleases are render-driven (absent from the store at
// resolve()), so transitiveDeps' store lookup returns nil; the keep walk
// relies on the discovery-supplied consumerRefs for the forward edge too.
// Regression: a one-file HR edit left its OCIRepository unfetched and render
// failed "artifact not available" (surfaced after #566 removed the anonymous
// helm-registry fallback that had masked it). Mirrors k8s-gitops media/plex.
func TestFilter_ForwardEdgeChangedHRKeepsChartRefSource(t *testing.T) {
	const (
		hrFile  = "kubernetes/apps/media/plex/app/helmrelease.yaml"
		sibFile = "kubernetes/apps/media/other/app/helmrelease.yaml"
		ociFile = "kubernetes/flux/repositories/app-template.yaml"
	)
	// Render-driven HRs are seeded with an empty namespace (discovery records
	// them pre-inheritance); the shared chartRef OCIRepository lives elsewhere.
	hr := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "plex"}
	sibling := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "other"}
	oci := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "app-template"}

	f := NewFilterWithCache(
		NewSet([]string{hrFile}), // only plex's file changed
		map[manifest.NamedResource]string{
			hr:      hrFile,
			sibling: sibFile,
			oci:     ociFile,
		},
		"",
		testutil.EmptyLister(), // HR + OCIRepository absent from the store at resolve()
		nil,
		map[manifest.NamedResource][]manifest.NamedResource{
			hr:      {oci},
			sibling: {oci}, // shares the same chart source
		},
	)

	if !f.ShouldReconcile(hr) {
		t.Fatalf("changed HelmRelease must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(oci) {
		t.Fatalf("forward edge: the changed HR's chartRef OCIRepository must be in keep so its chart is fetched; keep=%v", f.KeepNames())
	}
	// Keeping the shared source via plex's forward edge must NOT reverse-
	// cascade to a sibling HR that merely shares it (its file is unchanged).
	if f.ShouldReconcile(sibling) {
		t.Errorf("sibling HR sharing the chart source must NOT be pulled in; keep=%v", f.KeepNames())
	}
}

// TestFilter_SubstituteFromConfigMapKeepsProducerKustomization pins
// issue #418: a kept Kustomization with a non-Optional
// postBuild.substituteFrom ConfigMap must pull the producer KS that
// renders that CM into keep, even when the producer's own files
// didn't change. Without this, changed-only mode fails reconcile
// with "ConfigMap/flux-system/cluster-settings: dependency not found"
// because the producer never runs and the CM never materializes.
//
// The producer joins keep as ancestor-only — NOT primary — so its
// render output for unrelated siblings can't cascade into changed-
// only scope (the #204 keep-cascade rationale).
func TestFilter_SubstituteFromConfigMapKeepsProducerKustomization(t *testing.T) {
	clusterAppsObj := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "kubernetes/apps",
		},
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
		},
	}
	clusterVarsObj := &manifest.Kustomization{
		Name: "cluster-vars", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "kubernetes/flux/vars",
			// The CM lives in a shared Component referenced by
			// cluster-vars — ownersOf returns cluster-vars for any
			// file under the resolved component path.
			Components: []string{"../../components/cluster-settings"},
		},
	}
	ntfyObj := &manifest.Kustomization{
		Name: "ntfy", Namespace: "communication",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "kubernetes/apps/communication/ntfy/app",
		},
	}
	hrObj := &manifest.HelmRelease{Name: "ntfy", Namespace: "communication"}
	// rawSettings is the DiscoveryOnly-indexed pre-render form:
	// Namespace="" because kustomize's namespace directive hasn't
	// run yet at file-walk time.
	rawSettings := &manifest.ConfigMap{Name: "cluster-settings", Namespace: ""}
	rawSettingsID := rawSettings.Named()

	clusterApps := clusterAppsObj.Named()
	clusterVars := clusterVarsObj.Named()
	ntfy := ntfyObj.Named()
	hr := hrObj.Named()
	// renderedSettings is the form the consumer KS waits on once
	// kustomize's namespace directive has stamped flux-system onto
	// the rendered CM.
	renderedSettings := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}

	f := NewFilter(
		NewSet([]string{"kubernetes/apps/communication/ntfy/app/helmrelease.yaml"}),
		map[manifest.NamedResource]string{
			clusterApps:   "kubernetes/flux/cluster-apps.yaml",
			clusterVars:   "kubernetes/flux/cluster-vars.yaml",
			ntfy:          "kubernetes/apps/communication/ntfy/ks.yaml",
			hr:            "kubernetes/apps/communication/ntfy/app/helmrelease.yaml",
			rawSettingsID: "kubernetes/components/cluster-settings/cluster-settings.yaml",
		},
		"",
		testutil.MapLister{
			clusterApps:   clusterAppsObj,
			clusterVars:   clusterVarsObj,
			ntfy:          ntfyObj,
			hr:            hrObj,
			rawSettingsID: rawSettings,
		},
	)

	// The leaf HR's file changed, so ntfy + the cluster-apps
	// ancestor are pulled in; the rendered ConfigMap is a
	// substituteFrom dep of cluster-apps; and cluster-vars is the
	// producer of that ConfigMap.
	for _, id := range []manifest.NamedResource{clusterApps, ntfy, hr, renderedSettings, clusterVars} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
	// The producer is ancestor-only: AddEmitted from cluster-vars
	// to a sentinel child must reject (producer is NOT primary).
	sentinel := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "unrelated", Name: "sentinel"}
	f.AddEmitted(clusterVars, sentinel)
	if f.ShouldReconcile(sentinel) {
		t.Errorf("producer cluster-vars must be ancestor-only (NOT primary) — AddEmitted leaked sentinel into keep; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddEmittedKeepsSubstituteFromProducerDependencyOnly
// covers the runtime AddEmitted path: a primary parent KS emits a
// child KS at render time, and that child has a substituteFrom CM
// produced by an unchanged third KS. The producer must land in keep
// (so the CM materializes) but stay dependency-only — its render
// output for unrelated siblings is unchanged, so promoting it would
// re-introduce the #204 cascade. See #418.
func TestFilter_AddEmittedKeepsSubstituteFromProducerDependencyOnly(t *testing.T) {
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "parent-apps"}
	childObj := &manifest.Kustomization{
		Name: "child-app", Namespace: "apps",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "kubernetes/apps/child",
		},
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "shared-settings"},
		},
	}
	child := childObj.Named()
	producerObj := &manifest.Kustomization{
		Name: "settings-producer", Namespace: "apps",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "kubernetes/apps/settings",
		},
	}
	producer := producerObj.Named()
	// CM with namespace="" mirrors the DiscoveryOnly pre-render form.
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "", Name: "shared-settings"}
	cmObj := &manifest.ConfigMap{Name: cmID.Name, Namespace: cmID.Namespace}

	f := NewFilter(
		NewSet([]string{"kubernetes/parent/parent.yaml"}),
		map[manifest.NamedResource]string{
			parent:   "kubernetes/parent/parent.yaml",
			producer: "kubernetes/apps/settings/ks.yaml",
			cmID:     "kubernetes/apps/settings/cm.yaml",
		},
		"",
		testutil.MapLister{producer: producerObj, child: childObj, cmID: cmObj},
	)

	if !f.ShouldReconcile(parent) {
		t.Fatalf("parent must be primary from direct file change; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(producer) {
		t.Fatalf("precondition: producer must NOT be in keep before AddEmitted runs; keep=%v", f.KeepNames())
	}

	// Primary parent emits child at render time. AddEmitted walks
	// child's transitiveDeps, hits the substituteFrom CM, and pulls
	// the producer in dependency-only.
	f.AddEmitted(parent, child)

	if !f.ShouldReconcile(child) {
		t.Errorf("child must be in keep after AddEmitted from primary parent; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(producer) {
		t.Errorf("producer of child's substituteFrom CM must be in keep; keep=%v", f.KeepNames())
	}
	// Producer is dependency-only: AddEmitted from producer to a
	// sentinel child must reject because producer is not primary.
	sentinel := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "unrelated", Name: "sentinel"}
	f.AddEmitted(producer, sentinel)
	if f.ShouldReconcile(sentinel) {
		t.Errorf("producer must be dependency-only (NOT primary) — AddEmitted leaked sentinel into keep; keep=%v", f.KeepNames())
	}
}

// TestFilter_AddEmittedFiresOnAddForSubstituteFromProducer pins that
// a runtime-added producer KS is delivered to OnAdd. The orchestrator
// wires OnAdd → Store.Refire for KindKustomization (#418); without
// the OnAdd dispatch, an unchanged producer joining keep at runtime
// would stay PreGate-skipped and its CM would never materialize.
func TestFilter_AddEmittedFiresOnAddForSubstituteFromProducer(t *testing.T) {
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "parent-apps"}
	childObj := &manifest.Kustomization{
		Name: "child-app", Namespace: "apps",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/apps/child"},
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "shared-settings"},
		},
	}
	child := childObj.Named()
	producerObj := &manifest.Kustomization{
		Name: "settings-producer", Namespace: "apps",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/apps/settings"},
	}
	producer := producerObj.Named()
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "", Name: "shared-settings"}
	cmObj := &manifest.ConfigMap{Name: cmID.Name, Namespace: cmID.Namespace}

	f := NewFilter(
		NewSet([]string{"kubernetes/parent/parent.yaml"}),
		map[manifest.NamedResource]string{
			parent:   "kubernetes/parent/parent.yaml",
			producer: "kubernetes/apps/settings/ks.yaml",
			cmID:     "kubernetes/apps/settings/cm.yaml",
		},
		"",
		testutil.MapLister{producer: producerObj, child: childObj, cmID: cmObj},
	)

	var added []manifest.NamedResource
	f.OnAdd = func(id manifest.NamedResource) {
		added = append(added, id)
	}

	f.AddEmitted(parent, child)

	if !slices.Contains(added, producer) {
		t.Errorf("OnAdd must fire for the runtime-added producer KS; added=%v", added)
	}
}

// TestFilter_ChainedSubstituteFromProducers covers the chained
// producer case: KS A has substituteFrom CM-a (produced by KS B);
// KS B has substituteFrom CM-b (produced by KS C). C must land in
// keep as ancestor — without the chain walk, an unchanged C would
// silently be skipped and B's render would fail. See #418.
func TestFilter_ChainedSubstituteFromProducers(t *testing.T) {
	aObj := &manifest.Kustomization{
		Name: "consumer", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/consumer"},
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cm-a"},
		},
	}
	bObj := &manifest.Kustomization{
		Name: "producer-b", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/b"},
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cm-b"},
		},
	}
	cObj := &manifest.Kustomization{
		Name: "producer-c", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/c"},
	}
	a, b, c := aObj.Named(), bObj.Named(), cObj.Named()

	cmA := &manifest.ConfigMap{Name: "cm-a", Namespace: ""}
	cmAID := cmA.Named()
	cmB := &manifest.ConfigMap{Name: "cm-b", Namespace: ""}
	cmBID := cmB.Named()

	f := NewFilter(
		NewSet([]string{"kubernetes/consumer/file.yaml"}),
		map[manifest.NamedResource]string{
			a:     "kubernetes/consumer/ks.yaml",
			b:     "kubernetes/b/ks.yaml",
			c:     "kubernetes/c/ks.yaml",
			cmAID: "kubernetes/b/cm-a.yaml",
			cmBID: "kubernetes/c/cm-b.yaml",
		},
		"",
		testutil.MapLister{a: aObj, b: bObj, c: cObj, cmAID: cmA, cmBID: cmB},
	)

	if !f.ShouldReconcile(a) {
		t.Fatalf("consumer A must be in keep (file change); keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(b) {
		t.Errorf("producer B (renders cm-a) must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(c) {
		t.Errorf("chained producer C (renders cm-b consumed by B) must be in keep; keep=%v", f.KeepNames())
	}
}

// TestFilter_SelfProducerSkippedInResolve pins the bjw-s
// self-substitute pattern: a KS may produce its own
// postBuild.substituteFrom CM. The BFS must NOT enqueue the KS as
// its own ancestor (would infinite-loop or self-edge). See #418.
func TestFilter_SelfProducerSkippedInResolve(t *testing.T) {
	selfObj := &manifest.Kustomization{
		Name: "self", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "kubernetes/self"},
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "self-settings"},
		},
	}
	self := selfObj.Named()
	// CM lives at spec.path of self — so ownersOf returns self.
	cmObj := &manifest.ConfigMap{Name: "self-settings", Namespace: ""}
	cmID := cmObj.Named()

	f := NewFilter(
		NewSet([]string{"kubernetes/self/changed.yaml"}),
		map[manifest.NamedResource]string{
			self: "kubernetes/self/ks.yaml",
			cmID: "kubernetes/self/cm.yaml",
		},
		"",
		testutil.MapLister{self: selfObj, cmID: cmObj},
	)

	if !f.ShouldReconcile(self) {
		t.Errorf("self KS must be in keep; keep=%v", f.KeepNames())
	}
	// Sanity: no infinite loop happened (we made it here). Also
	// confirm self only appears once in the producer index — not as
	// its own ancestor via enqueueAncestor.
	prods := f.ProducersFor(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "self-settings"})
	if !slices.Contains(prods, self) {
		t.Errorf("producers index should still contain self for cm-by-name lookup; got=%v", prods)
	}
}

// TestFilter_ProducerByNameDoesNotLeakAcrossNamespaces pins the
// asymmetric producer-byName rule, mirroring
// TestFilter_KeepByNameDoesNotLeakAcrossNamespaces. A producer KS
// renders a CM in namespace ns1; an unrelated producer in ns2
// renders a same-named CM in ns2. A ProducersFor lookup on
// CM/ns1/shared MUST return only the ns1 producer — never the ns2
// one. See #418.
func TestFilter_ProducerByNameDoesNotLeakAcrossNamespaces(t *testing.T) {
	// Both producer KSes spec a CM named "shared". The CMs land at
	// distinct on-disk paths, so ownersOf attributes each producer
	// only to its own CM file.
	aObj := &manifest.Kustomization{
		Name: "producer-a", Namespace: "ns1",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "p1"},
	}
	bObj := &manifest.Kustomization{
		Name: "producer-b", Namespace: "ns2",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "p2"},
	}
	a, b := aObj.Named(), bObj.Named()

	// The DiscoveryOnly-indexed CMs are in their respective on-disk
	// paths but have explicit namespace already (so they land in
	// producersByID only — NOT producersByName).
	cmNS1 := &manifest.ConfigMap{Name: "shared", Namespace: "ns1"}
	cmNS1ID := cmNS1.Named()
	cmNS2 := &manifest.ConfigMap{Name: "shared", Namespace: "ns2"}
	cmNS2ID := cmNS2.Named()

	f := NewFilter(
		NewSet([]string{"unrelated.yaml"}),
		map[manifest.NamedResource]string{
			a:       "p1/ks.yaml",
			b:       "p2/ks.yaml",
			cmNS1ID: "p1/cm.yaml",
			cmNS2ID: "p2/cm.yaml",
		},
		"",
		testutil.MapLister{a: aObj, b: bObj, cmNS1ID: cmNS1, cmNS2ID: cmNS2},
	)

	gotNS1 := f.ProducersFor(cmNS1ID)
	if !slices.Equal(gotNS1, []manifest.NamedResource{a}) {
		t.Errorf("ProducersFor(CM/ns1/shared) leaked across namespaces: got %v, want [%v]", gotNS1, a)
	}
	gotNS2 := f.ProducersFor(cmNS2ID)
	if !slices.Equal(gotNS2, []manifest.NamedResource{b}) {
		t.Errorf("ProducersFor(CM/ns2/shared) leaked across namespaces: got %v, want [%v]", gotNS2, b)
	}
}
