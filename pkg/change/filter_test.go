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
