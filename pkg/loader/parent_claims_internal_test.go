package loader

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestBuildParentIndex_NoSourceFileSkipped(t *testing.T) {
	t.Parallel()
	// A KS without a recorded source file (e.g. lifted purely from a
	// parent's render output, no annotation propagated to the orchestrator)
	// has no detectable file — skip rather than blow up.
	s := store.New()
	parent := &manifest.Kustomization{
		Name:      "parent",
		Namespace: "flux-system",
		Path:      "./apps",
	}
	orphan := &manifest.Kustomization{
		Name:      "orphan",
		Namespace: "flux-system",
		Path:      "./apps/orphan/app",
	}
	s.AddObject(parent)
	s.AddObject(orphan)
	sourceFiles := map[manifest.NamedResource]string{
		parent.Named(): "clusters/main/apps.yaml",
		// orphan deliberately absent.
	}
	parents := parentsForTest(t, loaderFixture(s, sourceFiles), "", manifest.KindKustomization)
	{
		_, ok := parents[orphan.Named()]
		assert.ErrorIf(t, ok, "KS without source file must not appear in parent index: %v", parents)
	}
}

// TestClaims_SortsLongestFirst pins the contract documented
// on Claims: prefixes come back sorted by length descending
// so the first HasPrefix match on a given file is the deepest
// (most-specific) structural parent. Both BuildParentIndex and the
// orchestrator's detectOrphans rely on this for correctness.
func TestClaims_SortsLongestFirst(t *testing.T) {
	t.Parallel()
	s := store.New()
	root := &manifest.Kustomization{
		Name: "root", Namespace: "flux-system",
		Path: "./apps",
	}
	mid := &manifest.Kustomization{
		Name: "mid", Namespace: "flux-system",
		Path: "./apps/team-a",
	}
	leaf := &manifest.Kustomization{
		Name: "leaf", Namespace: "flux-system",
		Path: "./apps/team-a/web",
	}
	s.AddObject(root)
	s.AddObject(mid)
	s.AddObject(leaf)

	prefixes := Claims(
		s,
		"",
		s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization),
	)
	{
		assert.FatalIf(t, len(prefixes) != 3, "expected 3 prefixes, got %d", len(prefixes))
	}
	// Longest first: leaf > mid > root.
	{
		got := []string{
			prefixes[0].ID.Name,
			prefixes[1].ID.Name,
			prefixes[2].ID.Name,
		}
		assert.ErrorIf(t, got[0] != "leaf" || got[1] != "mid" ||
			got[2] != "root", "expected leaf/mid/root by descending prefix length, got %v", got)
	}
}

func TestClaims_EmptyPathClaimsRoot(t *testing.T) {
	t.Parallel()
	s := store.New()
	with := &manifest.Kustomization{
		Name: "with", Namespace: "flux-system",
		Path: "./apps",
	}
	without := &manifest.Kustomization{
		Name:      "without",
		Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "flux-system",
			Namespace: "flux-system",
		},
	}
	s.AddObject(with)
	s.AddObject(without)

	prefixes := Claims(
		s,
		"",
		s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization),
	)
	{
		assert.FatalIf(t, len(prefixes) != 2 || prefixes[0].ID != with.Named() || prefixes[1].ID != without.Named() ||
			prefixes[1].Prefix != "", "expected apps and source-root claims; got %+v", prefixes)
	}
	if parent, ok := manifest.StrictParent(
		prefixes,
		"root.yaml",
		manifest.NamedResource{},
		nil,
	); !ok ||
		parent != without.Named() {
		t.Fatalf("root.yaml parent = %v, %t; want %v", parent, ok, without.Named())
	}
}

func TestClaims_EmptyExternalPath(t *testing.T) {
	t.Parallel()
	s := store.New()
	repo := &manifest.GitRepository{Name: "external", Namespace: "flux-system"}
	external := &manifest.Kustomization{
		Name:      "external",
		Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      repo.Name,
			Namespace: repo.Namespace,
		},
	}
	local := &manifest.Kustomization{Name: "local", Namespace: "flux-system", Path: "./apps"}
	s.AddObject(repo)
	s.AddObject(external)
	s.AddObject(local)
	prefixes := loaderFixture(s, nil).Claims(t.TempDir())
	{
		assert.FatalIf(
			t,
			len(prefixes) != 1 || prefixes[0].ID != local.Named(),
			"external source root must not claim local files: %+v",
			prefixes,
		)
	}
	{
		parent, ok := manifest.StrictParent(
			Claims(s, "", s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)),
			"bad.yaml",
			manifest.NamedResource{},
			nil,
		)
		assert.FatalIf(t, !ok ||
			parent != external.Named(), "final reporting must retain external source-root ownership: parent=%v, found=%t", parent, ok)
	}
}

// TestStrictParent_SkipsSelf locks the self-exclusion contract:
// a KS sitting on its own spec.path (rare but possible — a KS whose
// definition file lives at the same prefix it renders) must not be
// returned as its own parent.
func TestStrictParent_SkipsSelf(t *testing.T) {
	t.Parallel()
	prefixes := []manifest.KSClaim{
		{ID: testutil.KustomizationID("flux-system", "self"), Prefix: "apps/team-a/"},
	}
	self := prefixes[0].ID
	{
		_, ok := manifest.StrictParent(prefixes, "apps/team-a/ks.yaml", self, nil)
		assert.ErrorIf(t, ok, "StrictParent must skip self matches")
	}
}

func TestClaims_IncludesSpecComponents(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/team-a/kustomization.yaml", "components: [../shared/local]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/shared/observability/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/shared/local/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n",
	)
	s := store.New()
	admitForTest(s, root)
	parent := &manifest.Kustomization{
		Name: "parent", Namespace: "flux-system",
		Path:       "./apps/team-a",
		Components: []string{"../shared/observability"},
	}
	s.AddObject(parent)

	prefixes := Claims(
		s,
		root,
		s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization),
	)
	assert.Diff(t, prefixes, []manifest.KSClaim{
		{ID: parent.Named(), Prefix: "apps/shared/observability/"},
		{ID: parent.Named(), Prefix: "apps/shared/local/"},
		{ID: parent.Named(), Prefix: "apps/team-a/"},
	})

	got, ok := manifest.StrictParent(prefixes, "apps/shared/observability/karma.yaml", manifest.NamedResource{}, nil)
	{
		assert.FatalIf(t, !ok, "StrictParent missed component-dir child — components fold regression")
	}
	{
		assert.ErrorIf(
			t,
			got.Name != "parent",
			"StrictParent = %q, want 'parent' (component-dir child should attribute to its containing KS)",
			got.Name,
		)
	}
}

// TestStrictParent_DeepestMatchWins exercises the typical case:
// a file under apps/team-a/web/ should attribute to the deepest
// covering KS, not the shallower one — which is what
// the descending-length sort in Claims enables. Pins the
// integration of the two helpers.
func TestStrictParent_DeepestMatchWins(t *testing.T) {
	t.Parallel()
	s := store.New()
	root := &manifest.Kustomization{
		Name: "root", Namespace: "flux-system",
		Path: "./apps",
	}
	leaf := &manifest.Kustomization{
		Name: "leaf", Namespace: "flux-system",
		Path: "./apps/team-a/web",
	}
	s.AddObject(root)
	s.AddObject(leaf)
	prefixes := Claims(
		s,
		"",
		s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization),
	)
	got, ok := manifest.StrictParent(prefixes, "apps/team-a/web/deploy.yaml", manifest.NamedResource{}, nil)
	{
		assert.FatalIf(t, !ok, "expected a parent match")
	}
	{
		assert.ErrorIf(t, got.Name != "leaf", "expected deepest parent 'leaf', got %q", got.Name)
	}
}

// TestStrictParent_AbsoluteSpecPath is the #920 regression: a Kustomization
// whose spec.path carries a leading slash (accepted by Flux, which resolves it
// against the artifact root) must still be found as the parent of the files
// under that directory. Before the fix its claim prefix kept the slash, so no
// repo-relative source file ever matched and every child was orphaned.
func TestStrictParent_AbsoluteSpecPath(t *testing.T) {
	t.Parallel()
	s := store.New()
	ks := &manifest.Kustomization{
		Name: "flux-instance", Namespace: "flux-system",
		Path: "/kubernetes/apps/flux-system/flux-operator/instance",
	}
	s.AddObject(ks)

	prefixes := Claims(
		s,
		"",
		s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization),
	)
	hr := testutil.HelmReleaseID("", "flux-instance")
	parent, ok := manifest.StrictParent(
		prefixes,
		"kubernetes/apps/flux-system/flux-operator/instance/helm-release.yaml",
		hr,
		nil,
	)
	{
		assert.FatalIf(
			t,
			!ok,
			"expected the absolute spec.path KS to parent its helm-release.yaml; prefixes=%v",
			prefixes,
		)
	}
	if parent != ks.Named() {
		t.Errorf("parent = %v, want %v", parent, ks.Named())
	}
}
