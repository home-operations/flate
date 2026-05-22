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

// TestApplyNamespaceInheritance_CrossTreeBasePattern covers the
// multi-cluster shared-base layout (e.g. joryirving/home-ops): the
// parent kustomization.yaml under `main/<ns>/` carries the `namespace:`
// directive that — at parent-render time — patches the Flux KS itself
// to namespace=<ns> and (via a replacements: block) injects
// spec.targetNamespace=<ns>. The Flux KS's spec.path then points at a
// directory under `base/` that has no local namespace directive.
//
// Pre-fix behavior: inheritance only saw the KS's empty
// targetNamespace, so resources under the cross-tree base/ path stayed
// at namespace="" and later failed source-ref resolution.
//
// Post-fix: the KS's effective namespace (derived from the
// kustomization.yaml directive on its own file) propagates to
// resources under spec.path even when both live in different subtrees.
func TestApplyNamespaceInheritance_CrossTreeBasePattern(t *testing.T) {
	root := t.TempDir()
	// Parent kustomize file lives under main/games and carries the
	// `namespace: games` directive that real Flux's replacements block
	// would later turn into spec.targetNamespace=games.
	writeFile(t, root, "apps/main/games/kustomization.yaml", "namespace: games\n")

	s := store.New()
	// Flux KS lives at apps/main/games/romm.yaml but spec.path crosses
	// over to apps/base/games/romm. Neither metadata.namespace nor
	// spec.targetNamespace is set in the source YAML.
	ks := &manifest.Kustomization{
		Name: "romm",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path: "./apps/base/games/romm",
		},
	}
	hr := &manifest.HelmRelease{Name: "romm"}
	s.AddObject(ks)
	s.AddObject(hr)

	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/main/games/romm.yaml",
		hr.Named(): "apps/base/games/romm/helmrelease.yaml",
	}
	ApplyNamespaceInheritance(s, sourceFiles, root)

	// Both the KS and the HR should now be namespace=games — the KS
	// from the local kustomize.yaml directive, the HR from the KS's
	// effective namespace projected onto its spec.path.
	want := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "games", Name: "romm"}
	if got := s.GetObject(want); got == nil {
		t.Fatalf("expected cross-tree HR at games/romm; sourceFiles=%v", sourceFiles)
	}
	if got := s.GetObject(manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "romm"}); got != nil {
		t.Errorf("empty-namespace HR should have been removed")
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
