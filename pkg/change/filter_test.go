package change

import (
	"slices"
	"testing"

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
		Path:       "apps/media/plex/app",
		Components: []string{"../../../../components/volsync"},
	}
	atuin := &manifest.Kustomization{
		Name: "atuin", Namespace: "default",
		Path:       "apps/default/atuin/app",
		Components: []string{"../../../../components/volsync"},
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

func TestFilter_LongestPrefixOwnerWins(t *testing.T) {
	// A meta-KS at apps/ and a specific KS at apps/media/plex/app —
	// changes inside plex must belong to plex, not the meta-KS.
	meta := &manifest.Kustomization{Name: "cluster-apps", Namespace: "flux-system", Path: "apps"}
	plex := &manifest.Kustomization{Name: "plex", Namespace: "media", Path: "apps/media/plex/app"}
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

	if !f.ShouldReconcile(plexID) || !f.ShouldReconcile(hrPlex) {
		t.Errorf("plex tree should be kept: %v", f.KeepNames())
	}
	if f.ShouldReconcile(metaID) {
		t.Errorf("meta KS leaked into keep set despite a deeper owner: %v", f.KeepNames())
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
		ValuesFrom: []manifest.ValuesReference{
			{Kind: manifest.KindConfigMap, Name: "plex-values"},
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
		DependsOn: []string{"flux-system/repositories"},
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
		DependsOn: []string{"flux-system/b"},
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
