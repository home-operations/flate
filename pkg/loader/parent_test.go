package loader

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestBuildParentIndex_CrossTreeBasePattern(t *testing.T) {
	// cluster-apps is the root with spec.path=./kubernetes/apps/main.
	// karma lives at apps/main/observability/karma.yaml — under
	// cluster-apps's spec.path — so cluster-apps is its parent. karma's
	// own spec.path crosses over to apps/base/ but that's irrelevant
	// for THIS index (which is about source-file-vs-spec.path).
	s := store.New()
	clusterApps := &manifest.Kustomization{
		Name:      "cluster-apps",
		Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "./kubernetes/apps/main",
		},
	}
	karma := &manifest.Kustomization{
		Name:      "karma",
		Namespace: "observability",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "./kubernetes/apps/base/observability/karma",
		},
	}
	s.AddObject(clusterApps)
	s.AddObject(karma)

	sourceFiles := map[manifest.NamedResource]string{
		clusterApps.Named(): "kubernetes/clusters/main/apps.yaml",
		karma.Named():       "kubernetes/apps/main/observability/karma.yaml",
	}
	parents := BuildParentIndexForKindWithCache(s, "", sourceFiles, manifest.KindKustomization, nil)

	if got, want := parents[karma.Named()], clusterApps.Named(); got != want {
		t.Errorf("karma.parent = %+v; want %+v", got, want)
	}
	if _, ok := parents[clusterApps.Named()]; ok {
		t.Errorf("cluster-apps should be parentless (root)")
	}
}

func TestBuildParentIndex_DeepestPrefixWins(t *testing.T) {
	// Outer spec.path is a strict prefix of inner spec.path; both
	// contain the grandchild's source file. The inner KS should win as
	// the structural parent.
	s := store.New()
	outer := &manifest.Kustomization{
		Name:              "outer",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	inner := &manifest.Kustomization{
		Name:              "inner",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps/media"},
	}
	grandchild := &manifest.Kustomization{
		Name:              "plex",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps/media/plex/app"},
	}
	s.AddObject(outer)
	s.AddObject(inner)
	s.AddObject(grandchild)

	sourceFiles := map[manifest.NamedResource]string{
		outer.Named():      "clusters/main/apps.yaml",
		inner.Named():      "apps/media/kustomization.yaml",
		grandchild.Named(): "apps/media/plex/ks.yaml",
	}
	parents := BuildParentIndexForKindWithCache(s, "", sourceFiles, manifest.KindKustomization, nil)

	if got, want := parents[grandchild.Named()], inner.Named(); got != want {
		t.Errorf("grandchild.parent = %+v; want %+v (deepest prefix)", got, want)
	}
	if got, want := parents[inner.Named()], outer.Named(); got != want {
		t.Errorf("inner.parent = %+v; want %+v", got, want)
	}
}

func TestBuildParentIndex_NoSelfMatch(t *testing.T) {
	// A KS whose own source file lives under its spec.path must NOT
	// match itself as parent. Edge case for in-place trees.
	s := store.New()
	ks := &manifest.Kustomization{
		Name:              "self",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/self/ks.yaml",
	}
	parents := BuildParentIndexForKindWithCache(s, "", sourceFiles, manifest.KindKustomization, nil)
	if _, ok := parents[ks.Named()]; ok {
		t.Errorf("KS must not be its own parent: %v", parents)
	}
}

func TestBuildParentIndex_PeersSharingSamePathHaveNoParent(t *testing.T) {
	// Two Kustomizations defined in the same file and pointing at the
	// SAME spec.path (a dual git-source redundancy pattern) are peers,
	// not parent/child — neither renders the other. A mutual parent edge
	// would deadlock the pair through collectDeps (each waits on the
	// other to reach Ready, then both time out).
	s := store.New()
	config := &manifest.Kustomization{
		Name:              "0-config",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./clusters/main/flux"},
	}
	softServe := &manifest.Kustomization{
		Name:              "0-soft-serve",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./clusters/main/flux"},
	}
	s.AddObject(config)
	s.AddObject(softServe)
	sourceFiles := map[manifest.NamedResource]string{
		config.Named():    "clusters/main/flux/flux-repo.yaml",
		softServe.Named(): "clusters/main/flux/flux-repo.yaml",
	}
	parents := BuildParentIndexForKindWithCache(s, "", sourceFiles, manifest.KindKustomization, nil)
	if p, ok := parents[config.Named()]; ok {
		t.Errorf("same-spec.path peer must not be a parent: 0-config.parent = %+v", p)
	}
	if p, ok := parents[softServe.Named()]; ok {
		t.Errorf("same-spec.path peer must not be a parent: 0-soft-serve.parent = %+v", p)
	}
}

func TestBuildParentIndex_RendererOfDefinitionFileStillParents(t *testing.T) {
	// A KS whose definition file lives under another KS's spec.path — but
	// whose OWN spec.path is a different directory — must still be
	// attributed to that renderer. The peer-exclusion keys on the child's
	// own claimed prefix (./apps), not on its source-file directory, so
	// this strict-ancestor edge must survive.
	s := store.New()
	root := &manifest.Kustomization{
		Name:              "0-config",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./clusters/main/flux"},
	}
	app := &manifest.Kustomization{
		Name:              "app",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	s.AddObject(root)
	s.AddObject(app)
	sourceFiles := map[manifest.NamedResource]string{
		root.Named(): "clusters/main/flux/flux-repo.yaml",
		// app is defined in the dir root renders, but claims ./apps.
		app.Named(): "clusters/main/flux/app-ks.yaml",
	}
	parents := BuildParentIndexForKindWithCache(s, "", sourceFiles, manifest.KindKustomization, nil)
	if got, want := parents[app.Named()], root.Named(); got != want {
		t.Errorf("app.parent = %+v; want %+v (the KS that renders app's definition dir)", got, want)
	}
}

func TestBuildParentIndex_NoSourceFileSkipped(t *testing.T) {
	// A KS without a recorded source file (e.g. lifted purely from a
	// parent's render output, no annotation propagated to the orchestrator)
	// has no detectable file — skip rather than blow up.
	s := store.New()
	parent := &manifest.Kustomization{
		Name:              "parent",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	orphan := &manifest.Kustomization{
		Name:              "orphan",
		Namespace:         "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps/orphan/app"},
	}
	s.AddObject(parent)
	s.AddObject(orphan)
	sourceFiles := map[manifest.NamedResource]string{
		parent.Named(): "clusters/main/apps.yaml",
		// orphan deliberately absent.
	}
	parents := BuildParentIndexForKindWithCache(s, "", sourceFiles, manifest.KindKustomization, nil)
	if _, ok := parents[orphan.Named()]; ok {
		t.Errorf("KS without source file must not appear in parent index: %v", parents)
	}
}

// TestKSPathPrefixes_SortsLongestFirst pins the contract documented
// on KSPathPrefixesWithCache: prefixes come back sorted by length descending
// so the first HasPrefix match on a given file is the deepest
// (most-specific) structural parent. Both BuildParentIndex and the
// orchestrator's detectOrphans rely on this for correctness.
func TestKSPathPrefixes_SortsLongestFirst(t *testing.T) {
	s := store.New()
	root := &manifest.Kustomization{
		Name: "root", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	mid := &manifest.Kustomization{
		Name: "mid", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps/team-a"},
	}
	leaf := &manifest.Kustomization{
		Name: "leaf", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps/team-a/web"},
	}
	s.AddObject(root)
	s.AddObject(mid)
	s.AddObject(leaf)

	prefixes := KSPathPrefixesWithCache(s, "", nil)
	if len(prefixes) != 3 {
		t.Fatalf("expected 3 prefixes, got %d", len(prefixes))
	}
	// Longest first: leaf > mid > root.
	if got := []string{prefixes[0].ID.Name, prefixes[1].ID.Name, prefixes[2].ID.Name}; got[0] != "leaf" || got[1] != "mid" || got[2] != "root" {
		t.Errorf("expected leaf/mid/root by descending prefix length, got %v", got)
	}
}

// TestKSPathPrefixes_SkipsEmptyPath confirms the "ks.Path == ”"
// guard: a Kustomization without a spec.path (chart-of-charts style,
// or chained-via-sourceRef-only) doesn't contribute a prefix that
// would silently swallow files at the repo root.
func TestKSPathPrefixes_SkipsEmptyPath(t *testing.T) {
	s := store.New()
	with := &manifest.Kustomization{
		Name: "with", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	without := &manifest.Kustomization{Name: "without", Namespace: "flux-system"}
	s.AddObject(with)
	s.AddObject(without)

	prefixes := KSPathPrefixesWithCache(s, "", nil)
	if len(prefixes) != 1 || prefixes[0].ID.Name != "with" {
		t.Errorf("expected only 'with' in prefixes; got %+v", prefixes)
	}
}

// TestLongestParent_SkipsSelf locks the self-exclusion contract:
// a KS sitting on its own spec.path (rare but possible — a KS whose
// definition file lives at the same prefix it renders) must not be
// returned as its own parent.
func TestLongestParent_SkipsSelf(t *testing.T) {
	prefixes := []KSPathPrefix{
		{ID: manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "self"}, Prefix: "apps/team-a/"},
	}
	self := prefixes[0].ID
	if _, ok := LongestParent(prefixes, "apps/team-a/ks.yaml", self); ok {
		t.Errorf("LongestParent must skip self matches")
	}
}

// TestKSPathPrefixes_IncludesSpecComponents pins the components fold:
// a KS that lists a relative spec.components path should claim that
// component subtree in the prefix set, so a child file living under
// the component dir attributes back to the parent. Without this,
// discovery's parent index treats the child as orphan and the
// change-mode index's already-correct attribution diverges — exactly
// the false-positive the audit's 9.1 finding called out.
func TestKSPathPrefixes_IncludesSpecComponents(t *testing.T) {
	s := store.New()
	parent := &manifest.Kustomization{
		Name: "parent", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path:       "./apps/team-a",
			Components: []string{"../shared/observability"},
		},
	}
	s.AddObject(parent)

	prefixes := KSPathPrefixesWithCache(s, "", nil)
	// Expect TWO entries for the parent: its spec.path and the
	// resolved component dir.
	var sawPath, sawComponent bool
	for _, p := range prefixes {
		if p.ID != parent.Named() {
			continue
		}
		switch p.Prefix {
		case "apps/team-a/":
			sawPath = true
		case "apps/shared/observability/":
			sawComponent = true
		}
	}
	if !sawPath {
		t.Errorf("expected prefix for spec.path; got %+v", prefixes)
	}
	if !sawComponent {
		t.Errorf("expected prefix for resolved spec.components entry; got %+v", prefixes)
	}

	// Integration: LongestParent over a file under the component dir
	// must attribute to the parent KS.
	got, ok := LongestParent(prefixes, "apps/shared/observability/karma.yaml", manifest.NamedResource{})
	if !ok {
		t.Fatalf("LongestParent missed component-dir child — components fold regression")
	}
	if got.Name != "parent" {
		t.Errorf("LongestParent = %q, want 'parent' (component-dir child should attribute to its containing KS)", got.Name)
	}
}

// TestLongestParent_DeepestMatchWins exercises the typical case:
// a file under apps/team-a/web/ should attribute to the deepest
// covering KS, not the shallower one — which is what
// KSPathPrefixesWithCache's descending-length sort enables. Pins the
// integration of the two helpers.
func TestLongestParent_DeepestMatchWins(t *testing.T) {
	s := store.New()
	root := &manifest.Kustomization{
		Name: "root", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	leaf := &manifest.Kustomization{
		Name: "leaf", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps/team-a/web"},
	}
	s.AddObject(root)
	s.AddObject(leaf)
	prefixes := KSPathPrefixesWithCache(s, "", nil)
	got, ok := LongestParent(prefixes, "apps/team-a/web/deploy.yaml", manifest.NamedResource{})
	if !ok {
		t.Fatalf("expected a parent match")
	}
	if got.Name != "leaf" {
		t.Errorf("expected deepest parent 'leaf', got %q", got.Name)
	}
}
