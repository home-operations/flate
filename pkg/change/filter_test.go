package change

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// emptyLister is enough for filter resolution tests where transitiveDeps
// would otherwise fail to find the parent KS in a real store.
type emptyLister struct{}

func (emptyLister) GetObject(manifest.NamedResource) manifest.BaseManifest { return nil }
func (emptyLister) ListObjects(string) []manifest.BaseManifest             { return nil }

// mapLister returns canned objects from a map for transitive-deps testing.
type mapLister map[manifest.NamedResource]manifest.BaseManifest

func (m mapLister) GetObject(id manifest.NamedResource) manifest.BaseManifest { return m[id] }
func (m mapLister) ListObjects(kind string) []manifest.BaseManifest {
	out := make([]manifest.BaseManifest, 0, len(m))
	for id, obj := range m {
		if id.Kind == kind {
			out = append(out, obj)
		}
	}
	return out
}

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
		emptyLister{},
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
		mapLister{ksPlex: plex, ksAtuin: atuin},
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
		mapLister{metaID: meta, plexID: plex},
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
		mapLister{parentID: parent, leafID: leaf},
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
		mapLister{metaID: meta, plexID: plex, atuinID: atuin},
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
		mapLister{a: ksA, b: ksB, c: ksCluster},
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
		mapLister{hrID: hr},
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
		mapLister{ksID: ks},
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
		mapLister{aID: a, bID: b},
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
// extended keep set. This test uses Filter.Add directly (skipping
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
		emptyLister{},
	)
	if !f.ShouldReconcile(parent) {
		t.Fatalf("parent must be in keep set from file change; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(child) {
		t.Fatalf("precondition: child must NOT be in keep set before Add; keep=%v", f.KeepNames())
	}

	f.Add(child)
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
	f.Add(id)
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
		emptyLister{},
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
		mapLister{leafKS: leafKSObj, clusterApps: clusterAppsObj, siblingApp: siblingObj},
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

	objs := mapLister{
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
		emptyLister{},
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
		emptyLister{},
	)
	if !f.ShouldReconcile(kept) {
		t.Fatalf("precondition: cluster-infra/external-secrets must be in keep; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(collider) {
		t.Errorf("keepByName leaked across namespaces: database/external-secrets matched on name alone; keep=%v", f.KeepNames())
	}

	// Same invariant via the runtime Add path (issue #204 surface).
	f.Add(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "kube-system", Name: "cert-manager"})
	if f.ShouldReconcile(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "monitoring-system", Name: "cert-manager"}) {
		t.Errorf("Add() leaked across namespaces: monitoring-system/cert-manager matched on name alone; keep=%v", f.KeepNames())
	}
}
