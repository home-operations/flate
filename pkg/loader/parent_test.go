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
	parents := BuildParentIndexForKind(s, sourceFiles, manifest.KindKustomization)

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
	parents := BuildParentIndexForKind(s, sourceFiles, manifest.KindKustomization)

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
	parents := BuildParentIndexForKind(s, sourceFiles, manifest.KindKustomization)
	if _, ok := parents[ks.Named()]; ok {
		t.Errorf("KS must not be its own parent: %v", parents)
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
	parents := BuildParentIndexForKind(s, sourceFiles, manifest.KindKustomization)
	if _, ok := parents[orphan.Named()]; ok {
		t.Errorf("KS without source file must not appear in parent index: %v", parents)
	}
}

// TestKSPathPrefixes_SortsLongestFirst pins the contract documented
// on KSPathPrefixes: prefixes come back sorted by length descending
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

	prefixes := KSPathPrefixes(s)
	if len(prefixes) != 3 {
		t.Fatalf("expected 3 prefixes, got %d", len(prefixes))
	}
	// Longest first: leaf > mid > root.
	if got := []string{prefixes[0].ID.Name, prefixes[1].ID.Name, prefixes[2].ID.Name}; got[0] != "leaf" || got[1] != "mid" || got[2] != "root" {
		t.Errorf("expected leaf/mid/root by descending prefix length, got %v", got)
	}
}

// TestKSPathPrefixes_SkipsEmptyPath confirms the "ks.Path == ''"
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

	prefixes := KSPathPrefixes(s)
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

// TestLongestParent_DeepestMatchWins exercises the typical case:
// a file under apps/team-a/web/ should attribute to the deepest
// covering KS, not the shallower one — which is what
// KSPathPrefixes's descending-length sort enables. Pins the
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
	prefixes := KSPathPrefixes(s)
	got, ok := LongestParent(prefixes, "apps/team-a/web/deploy.yaml", manifest.NamedResource{})
	if !ok {
		t.Fatalf("expected a parent match")
	}
	if got.Name != "leaf" {
		t.Errorf("expected deepest parent 'leaf', got %q", got.Name)
	}
}
