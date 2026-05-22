package loader

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

var writeFile = testutil.WriteFile

func TestApplyNamespaceInheritance_FluxTargetNamespaceWins(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "apps/plex/kustomization.yaml", "namespace: should-be-overridden\n")

	s := store.New()
	parent := &manifest.Kustomization{
		Name:      "plex",
		Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path:            "apps/plex",
			TargetNamespace: "media",
		},
	}
	hr := &manifest.HelmRelease{
		Name:      "plex",
		Namespace: "", // inherits
	}
	s.AddObject(parent)
	s.AddObject(hr)

	sourceFiles := map[manifest.NamedResource]string{
		parent.Named(): "apps/plex/ks.yaml",
		hr.Named():     "apps/plex/helmrelease.yaml",
	}
	ApplyNamespaceInheritance(s, sourceFiles, root)

	// HR's namespace should now reflect the Flux KS targetNamespace,
	// not the kustomize-level "should-be-overridden" directive.
	if got := s.GetObject(manifest.NamedResource{
		Kind: manifest.KindHelmRelease, Namespace: "media", Name: "plex",
	}); got == nil {
		t.Fatalf("expected HR to be reindexed at media/plex; sources=%v", sourceFiles)
	}
	// sourceFiles must reflect the renamed id.
	want := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "media", Name: "plex"}
	if _, ok := sourceFiles[want]; !ok {
		t.Errorf("sourceFiles not rewritten for new id; got keys=%v", sourceFiles)
	}
}

func TestApplyNamespaceInheritance_KustomizeDirectiveFallback(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "apps/atuin/kustomization.yaml", "namespace: default\n")

	// No Flux KS in the store, just an HR — so kustomize.yaml's
	// `namespace:` directive is the only namespace source.
	s := store.New()
	hr := &manifest.HelmRelease{Name: "atuin", Namespace: ""}
	s.AddObject(hr)

	sourceFiles := map[manifest.NamedResource]string{
		hr.Named(): "apps/atuin/helmrelease.yaml",
	}
	ApplyNamespaceInheritance(s, sourceFiles, root)

	if got := s.GetObject(manifest.NamedResource{
		Kind: manifest.KindHelmRelease, Namespace: "default", Name: "atuin",
	}); got == nil {
		t.Fatalf("expected HR to be reindexed at default/atuin")
	}
}

func TestApplyNamespaceInheritance_DeepestPrefixWins(t *testing.T) {
	root := t.TempDir()
	// Outer directive says "outer", inner says "inner" — inner is deeper
	// so should win.
	writeFile(t, root, "apps/kustomization.yaml", "namespace: outer\n")
	writeFile(t, root, "apps/media/kustomization.yaml", "namespace: inner\n")

	s := store.New()
	hr := &manifest.HelmRelease{Name: "plex", Namespace: ""}
	s.AddObject(hr)
	sourceFiles := map[manifest.NamedResource]string{
		hr.Named(): "apps/media/plex/helmrelease.yaml",
	}
	ApplyNamespaceInheritance(s, sourceFiles, root)

	if got := s.GetObject(manifest.NamedResource{
		Kind: manifest.KindHelmRelease, Namespace: "inner", Name: "plex",
	}); got == nil {
		t.Fatalf("deepest prefix didn't win; sourceFiles=%v", sourceFiles)
	}
}

func TestApplyNamespaceInheritance_NoSourceFilesNoop(t *testing.T) {
	s := store.New()
	hr := &manifest.HelmRelease{Name: "x", Namespace: ""}
	s.AddObject(hr)
	// Empty sourceFiles must not crash and must not rewrite anything.
	ApplyNamespaceInheritance(s, map[manifest.NamedResource]string{}, "")

	if got := s.GetObject(manifest.NamedResource{
		Kind: manifest.KindHelmRelease, Name: "x",
	}); got == nil {
		t.Fatalf("HR with empty namespace lost")
	}
}

func TestReadKustomizeNamespace_AnchoredByRepoRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "deep/sub/kustomization.yaml", "namespace: from-disk\n")

	got := readKustomizeNamespace(root, "deep/sub")
	if got != "from-disk" {
		t.Errorf("readKustomizeNamespace=%q want %q", got, "from-disk")
	}

	// Bogus dir returns empty without erroring.
	if got := readKustomizeNamespace(root, "no/such/dir"); got != "" {
		t.Errorf("missing kustomization should return empty, got %q", got)
	}
}

func TestApplyNamespaceInheritance_HRChartRepoNamespaceTracksHR(t *testing.T) {
	// When HR.Chart.RepoNamespace is empty, it implicitly tracks the
	// HR's own namespace. After inheritance fills the HR namespace in,
	// the chart's RepoNamespace should follow.
	root := t.TempDir()
	writeFile(t, root, "apps/plex/kustomization.yaml", "namespace: media\n")

	s := store.New()
	hr := &manifest.HelmRelease{
		Name:      "plex",
		Namespace: "",
		Chart: manifest.HelmChart{
			RepoKind: manifest.KindOCIRepository, RepoName: "app-template",
			// RepoNamespace empty — tracks HR namespace
		},
	}
	s.AddObject(hr)
	sourceFiles := map[manifest.NamedResource]string{
		hr.Named(): "apps/plex/helmrelease.yaml",
	}
	ApplyNamespaceInheritance(s, sourceFiles, root)

	updated := s.GetObject(manifest.NamedResource{
		Kind: manifest.KindHelmRelease, Namespace: "media", Name: "plex",
	}).(*manifest.HelmRelease)
	if updated.Chart.RepoNamespace != "media" {
		t.Errorf("Chart.RepoNamespace=%q want media", updated.Chart.RepoNamespace)
	}
}
