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
	parents := BuildParentIndex(s, sourceFiles)

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
	parents := BuildParentIndex(s, sourceFiles)

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
	parents := BuildParentIndex(s, sourceFiles)
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
	parents := BuildParentIndex(s, sourceFiles)
	if _, ok := parents[orphan.Named()]; ok {
		t.Errorf("KS without source file must not appear in parent index: %v", parents)
	}
}
