package loader

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

func TestFinalizeIndex_ParentSegments(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, base      string
		link, mounted, pinned bool
	}{
		{name: "plain", path: "./apps/../base", base: "base"},
		{name: "trailing parent", path: "./apps/base/..", base: "apps"},
		{name: "symlink", path: "./alias/../base", base: "deep/base", link: true},
		{name: "clamped", path: "./apps/../../base", base: "base"},
		{name: "source root", path: "./apps/../.."},
		{name: "source view", path: "./alias/../base", base: "deep/base", link: true, mounted: true},
		{name: "committed source view", path: "./alias/../base", base: "deep/base", link: true, mounted: true, pinned: true},
		{name: "committed parent segments", path: "./apps/../base", base: "base", mounted: true, pinned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, dir := range []string{"apps/base", "deep/inner"} {
				if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			prefix := tc.base
			if prefix != "" {
				prefix += "/"
			}
			testutil.WriteFile(t, root, prefix+"kustomization.yaml", "namespace: apps\nresources: [hr.yaml, cm.yaml]\nconfigMapGenerator:\n- name: generated\n  literals: [VALUE=fixture]\n")
			testutil.WriteFile(t, root, prefix+"hr.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: child, namespace: apps}\n")
			testutil.WriteFile(t, root, prefix+"cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data, namespace: apps}\n")
			st := store.New()
			if !tc.mounted || tc.pinned {
				admitForTest(st, root)
			}
			ks := &manifest.Kustomization{Name: "parent", Namespace: "flux-system", Path: tc.path}
			if tc.pinned {
				ks.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "pinned", Namespace: "flux-system"}
			}
			st.AddObject(ks)
			child := testutil.HelmReleaseID("apps", "child")
			data := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "data"}
			generated := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "generated"}
			l := loaderFixture(st, map[manifest.NamedResource]string{child: prefix + "hr.yaml", data: prefix + "cm.yaml"})
			if tc.mounted {
				b := tree.NewBuilder(os.DirFS(root))
				if err := b.AddSymlink("alias", "deep/inner"); err != nil {
					t.Fatal(err)
				}
				input, err := b.Build()
				if err != nil {
					t.Fatal(err)
				}
				if tc.pinned {
					src := &manifest.GitRepository{Name: ks.SourceRef.Name, Namespace: ks.SourceRef.Namespace}
					st.AddObject(src)
					st.SetArtifact(src.Named(), &store.SourceArtifact{LocalRoot: root, LocalPath: filepath.Join(root, "snapshot"), Key: "pinned", FS: input})
				} else {
					l.documents.views = map[string]fs.FS{root: input}
				}
			} else if tc.link {
				if err := os.Symlink("deep/inner", filepath.Join(root, "alias")); err != nil {
					t.Fatal(err)
				}
			}
			l.SourceRoot = root
			l.FinalizeIndex(root)
			assert.Equal(t, l.Index.Records[child].Parent, ks.Named())
			owners, _ := l.Index.OwnersOfFile(prefix + "hr.yaml")
			assert.Diff(t, owners, []manifest.NamedResource{ks.Named()})
			assert.Diff(t, testutil.ProductionOwners(l.Index.Records[data]), []manifest.NamedResource{ks.Named()})
			assert.Diff(t, testutil.Owners(l.Store.Productions(data, l.Index)), []manifest.NamedResource{ks.Named()})
			assert.Diff(t, testutil.ProductionOwners(l.Index.Records[generated]), []manifest.NamedResource{ks.Named()})
			sourceRoot, _, _ := SourceView(st, ks.SourceID(), root)
			assert.Equal(t, l.dirCoveredByOtherKS(filepath.Join(sourceRoot, tc.base)), tc.base != "")
			if tc.pinned {
				assert.Equal(t, l.dirCoveredByOtherKS(filepath.Join(root, tc.base)), false)
			}
			assert.Equal(t, ks.Path, tc.path)
		})
	}
}

func parentsForTest(t *testing.T, l *Loader, repoRoot, kind string) map[manifest.NamedResource]manifest.NamedResource {
	t.Helper()
	l.FinalizeIndex(repoRoot)
	out := map[manifest.NamedResource]manifest.NamedResource{}
	for id, r := range l.Index.Records {
		if id.Kind == kind && r.Parent != (manifest.NamedResource{}) {
			out[id] = r.Parent
		}
	}
	return out
}

func TestBuildParentIndex_IndexedChildrenWithoutStoreObjects(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		group, kind string
		parent      bool
	}{
		{manifest.FluxKustomizeDomain, manifest.KindKustomization, true},
		{manifest.HelmReleaseDomain, manifest.KindHelmRelease, true},
		{manifest.FluxOperatorDomain, manifest.KindResourceSet, true},
		{manifest.SourceDomain, manifest.KindGitRepository, false},
		{manifest.SourceDomain, manifest.KindOCIRepository, false},
		{manifest.SourceDomain, manifest.KindHelmRepository, false},
		{manifest.SourceDomain, manifest.KindHelmChart, false},
		{manifest.SourceDomain, manifest.KindBucket, false},
		{manifest.SourceDomain, manifest.KindExternalArtifact, false},
		{manifest.FluxOperatorDomain, manifest.KindResourceSetInputProvider, false},
		{"", manifest.KindConfigMap, false},
		{"", manifest.KindSecret, false},
	} {
		group, kind, eligible := tc.group, tc.kind, tc.parent
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			outer := &manifest.Kustomization{Name: "outer", Namespace: "flux-system", Path: "./apps"}
			inner := &manifest.Kustomization{Name: "inner", Namespace: "flux-system", Path: "./apps/team"}
			s.AddObject(outer)
			s.AddObject(inner)
			cases := []struct {
				name, namespace, file string
				want                  manifest.NamedResource
			}{
				{"deepest", "team", "apps/team/release.yaml", inner.Named()},
				{"outer", "other", "apps/other/release.yaml", outer.Named()},
				{"standalone", "team", "release.yaml", manifest.NamedResource{}},
				{"empty", "team", "", manifest.NamedResource{}},
			}
			files := map[manifest.NamedResource]string{}
			for _, tc := range cases {
				files[manifest.NamedResource{Group: group, Kind: kind, Namespace: tc.namespace, Name: tc.name}] = tc.file
			}
			files[manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "team", Name: "data"}] = "apps/team/data.yaml"
			parents := parentsForTest(t, loaderFixture(s, files), "", kind)
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					id := manifest.NamedResource{Group: group, Kind: kind, Namespace: tc.namespace, Name: tc.name}
					want := tc.want
					if !eligible {
						want = manifest.NamedResource{}
					}
					if s.GetObject(id) != nil {
						t.Fatal("indexed child must stay absent from store")
					}
					if got := parents[id]; got != want {
						t.Errorf("parent[%s] = %v, want %v", id, got, want)
					}
				})
			}
			want := 0
			if eligible {
				want = 2
			}
			if len(parents) != want {
				t.Errorf("parents = %v; indexed children must stay absent from store", parents)
			}
		})
	}
}

// TestClaims_DropsExternalSourced pins issue #752
// (lunevans/talos-cluster): a Kustomization sourced from a genuine external repo
// (an in-tree GitRepository with no working-tree artifact) must not claim
// local-tree path prefixes, even with a wide spec.path like ./kubernetes —
// otherwise it becomes the false structural parent of the whole cluster and its
// skip cascades. A bootstrap-sourced KS and an aliased (working-tree) source
// keep their claims.
func TestClaims_DropsExternalSourced(t *testing.T) {
	t.Parallel()
	repoRoot := "/repo" // no on-disk component reads will resolve; that's fine
	s := store.New()

	// Bootstrap-sourced root (sourceRef == BootstrapSourceID) — kept.
	bootstrap := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system"},
		Path:      "./kubernetes/apps",
	}
	// Self/aliased source: an in-tree GitRepository with a working-tree artifact
	// (URL-matched by discovery) — kept.
	aliasedKS := &manifest.Kustomization{
		Name: "self-apps", Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "home-kubernetes", Namespace: "flux-system"},
		Path:      "./kubernetes/self",
	}
	// External source: an in-tree GitRepository with NO working-tree artifact,
	// claiming the WIDE ./kubernetes — must be dropped.
	external := &manifest.Kustomization{
		Name: "side-sync", Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "side-repo", Namespace: "flux-system"},
		Path:      "./kubernetes",
	}
	s.AddObject(bootstrap)
	s.AddObject(aliasedKS)
	s.AddObject(external)
	s.AddObject(&manifest.GitRepository{Name: "home-kubernetes", Namespace: "flux-system"})
	s.AddObject(&manifest.GitRepository{Name: "side-repo", Namespace: "flux-system"})
	// home-kubernetes is aliased to the working tree (LocalPath == repoRoot).
	s.SetArtifact(
		manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "home-kubernetes"},
		&store.SourceArtifact{Kind: manifest.KindGitRepository, URL: "file://" + repoRoot, LocalPath: repoRoot, FS: fstest.MapFS{}},
	)

	prefixes := loaderFixture(s, nil).Claims(repoRoot)
	seen := map[manifest.NamedResource]bool{}
	for _, p := range prefixes {
		seen[p.ID] = true
	}
	if seen[external.Named()] {
		t.Errorf("external-sourced KS claim must be dropped from the prefix index")
	}
	if !seen[bootstrap.Named()] || !seen[aliasedKS.Named()] {
		t.Errorf("local-sourced KS claims must be retained; seen=%v", seen)
	}
	if parent, ok := manifest.StrictParent(Claims(s, repoRoot), "kubernetes/other/bad.yaml", manifest.NamedResource{}, nil); !ok || parent != external.Named() {
		t.Fatalf("final reporting must retain external-source ownership: parent=%v, found=%t", parent, ok)
	}
}

func TestSourceView_LocalRoot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, group string
		external    bool
	}{
		{manifest.KindGitRepository, manifest.SourceDomain, true},
		{manifest.KindOCIRepository, manifest.SourceDomain, true},
		{manifest.KindBucket, manifest.SourceDomain, true},
		{manifest.KindHelmRepository, manifest.SourceDomain, false},
		{manifest.KindExternalArtifact, manifest.SourceDomain, false},
		{manifest.KindGitRepository, "wrong.group", false},
	} {
		t.Run("absent/"+tc.group+"/"+tc.kind, func(t *testing.T) {
			t.Parallel()
			ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", SourceRef: kustomizev1.CrossNamespaceSourceReference{APIVersion: tc.group + "/v1", Kind: tc.kind, Name: "entry"}}
			root, pinned, external := SourceView(store.New(), ks.SourceID(), "/repo")
			if root != "/repo" || pinned || external != tc.external {
				t.Fatalf("absent source: %q, %t, %t", root, pinned, external)
			}
		})
	}
	for _, tc := range []struct {
		name, repoRoot, localRoot string
		external, unavailable     bool
	}{
		{name: "own-repository", repoRoot: "/repo", localRoot: "/repo"},
		{name: "another-repository", repoRoot: "/repo", localRoot: "/other", external: true},
		{name: "unset-local-root", repoRoot: "/repo", external: true},
		{name: "unset-roots", external: true},
		{name: "unavailable-source", repoRoot: "/repo", localRoot: "/repo", external: true, unavailable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			repo := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
			ks := &manifest.Kustomization{
				Name: "apps", Namespace: repo.Namespace, Path: "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: repo.Name, Namespace: repo.Namespace},
			}
			st.AddObject(repo)
			st.AddObject(ks)
			artifact := &store.SourceArtifact{
				Kind: manifest.KindGitRepository, LocalPath: "/cache", LocalRoot: tc.localRoot, Key: "pinned", FS: fstest.MapFS{},
			}
			if tc.unavailable {
				artifact.FS = nil
			}
			st.SetArtifact(repo.Named(), artifact)
			_, _, external := SourceView(st, ks.SourceID(), tc.repoRoot)
			if external != tc.external {
				t.Fatalf("external = %v, want %v", external, tc.external)
			}
		})
	}
}

func TestBuildParentIndex_CrossTreeBasePattern(t *testing.T) {
	t.Parallel()
	// cluster-apps is the root with spec.path=./kubernetes/apps/main.
	// karma lives at apps/main/observability/karma.yaml — under
	// cluster-apps's spec.path — so cluster-apps is its parent. karma's
	// own spec.path crosses over to apps/base/ but that's irrelevant
	// for THIS index (which is about source-file-vs-spec.path).
	s := store.New()
	clusterApps := &manifest.Kustomization{
		Name:      "cluster-apps",
		Namespace: "flux-system",
		Path:      "./kubernetes/apps/main",
	}
	karma := &manifest.Kustomization{
		Name:      "karma",
		Namespace: "observability",
		Path:      "./kubernetes/apps/base/observability/karma",
	}
	s.AddObject(clusterApps)
	s.AddObject(karma)

	sourceFiles := map[manifest.NamedResource]string{
		clusterApps.Named(): "kubernetes/clusters/main/apps.yaml",
		karma.Named():       "kubernetes/apps/main/observability/karma.yaml",
	}
	parents := parentsForTest(t, loaderFixture(s, sourceFiles), "", manifest.KindKustomization)

	if got, want := parents[karma.Named()], clusterApps.Named(); got != want {
		t.Errorf("karma.parent = %+v; want %+v", got, want)
	}
	if _, ok := parents[clusterApps.Named()]; ok {
		t.Errorf("cluster-apps should be parentless (root)")
	}
}

func TestBuildParentIndex_DeepestPrefixWins(t *testing.T) {
	t.Parallel()
	// Outer spec.path is a strict prefix of inner spec.path; both
	// contain the grandchild's source file. The inner KS should win as
	// the structural parent.
	s := store.New()
	outer := &manifest.Kustomization{
		Name:      "outer",
		Namespace: "flux-system",
		Path:      "./apps",
	}
	inner := &manifest.Kustomization{
		Name:      "inner",
		Namespace: "flux-system",
		Path:      "./apps/media",
	}
	grandchild := &manifest.Kustomization{
		Name:      "plex",
		Namespace: "flux-system",
		Path:      "./apps/media/plex/app",
	}
	s.AddObject(outer)
	s.AddObject(inner)
	s.AddObject(grandchild)

	sourceFiles := map[manifest.NamedResource]string{
		outer.Named():      "clusters/main/apps.yaml",
		inner.Named():      "apps/media/kustomization.yaml",
		grandchild.Named(): "apps/media/plex/ks.yaml",
	}
	parents := parentsForTest(t, loaderFixture(s, sourceFiles), "", manifest.KindKustomization)

	if got, want := parents[grandchild.Named()], inner.Named(); got != want {
		t.Errorf("grandchild.parent = %+v; want %+v (deepest prefix)", got, want)
	}
	if got, want := parents[inner.Named()], outer.Named(); got != want {
		t.Errorf("inner.parent = %+v; want %+v", got, want)
	}
}

func TestBuildParentIndex_NoSelfMatch(t *testing.T) {
	t.Parallel()
	// A KS whose own source file lives under its spec.path must NOT
	// match itself as parent. Edge case for in-place trees.
	s := store.New()
	ks := &manifest.Kustomization{
		Name:      "self",
		Namespace: "flux-system",
		Path:      "./apps",
	}
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/self/ks.yaml",
	}
	parents := parentsForTest(t, loaderFixture(s, sourceFiles), "", manifest.KindKustomization)
	if _, ok := parents[ks.Named()]; ok {
		t.Errorf("KS must not be its own parent: %v", parents)
	}
}

func TestBuildParentIndex_PeersSharingSamePathHaveNoParent(t *testing.T) {
	t.Parallel()
	// Two Kustomizations defined in the same file and pointing at the
	// SAME spec.path (a dual git-source redundancy pattern) are peers,
	// not parent/child — neither renders the other. A mutual parent edge
	// would deadlock the pair through collectDeps (each waits on the
	// other to reach Ready, then both time out).
	s := store.New()
	config := &manifest.Kustomization{
		Name:      "0-config",
		Namespace: "flux-system",
		Path:      "./clusters/main/flux",
	}
	softServe := &manifest.Kustomization{
		Name:      "0-soft-serve",
		Namespace: "flux-system",
		Path:      "./clusters/main/flux",
	}
	s.AddObject(config)
	s.AddObject(softServe)
	sourceFiles := map[manifest.NamedResource]string{
		config.Named():    "clusters/main/flux/flux-repo.yaml",
		softServe.Named(): "clusters/main/flux/flux-repo.yaml",
	}
	parents := parentsForTest(t, loaderFixture(s, sourceFiles), "", manifest.KindKustomization)
	if p, ok := parents[config.Named()]; ok {
		t.Errorf("same-spec.path peer must not be a parent: 0-config.parent = %+v", p)
	}
	if p, ok := parents[softServe.Named()]; ok {
		t.Errorf("same-spec.path peer must not be a parent: 0-soft-serve.parent = %+v", p)
	}
}

func TestBuildParentIndex_RendererOfDefinitionFileStillParents(t *testing.T) {
	t.Parallel()
	// A KS whose definition file lives under another KS's spec.path — but
	// whose OWN spec.path is a different directory — must still be
	// attributed to that renderer. The peer-exclusion keys on the child's
	// own claimed prefix (./apps), not on its source-file directory, so
	// this strict-ancestor edge must survive.
	s := store.New()
	root := &manifest.Kustomization{
		Name:      "0-config",
		Namespace: "flux-system",
		Path:      "./clusters/main/flux",
	}
	app := &manifest.Kustomization{
		Name:      "app",
		Namespace: "flux-system",
		Path:      "./apps",
	}
	s.AddObject(root)
	s.AddObject(app)
	sourceFiles := map[manifest.NamedResource]string{
		root.Named(): "clusters/main/flux/flux-repo.yaml",
		// app is defined in the dir root renders, but claims ./apps.
		app.Named(): "clusters/main/flux/app-ks.yaml",
	}
	parents := parentsForTest(t, loaderFixture(s, sourceFiles), "", manifest.KindKustomization)
	if got, want := parents[app.Named()], root.Named(); got != want {
		t.Errorf("app.parent = %+v; want %+v (the KS that renders app's definition dir)", got, want)
	}
}

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
	if _, ok := parents[orphan.Named()]; ok {
		t.Errorf("KS without source file must not appear in parent index: %v", parents)
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

	prefixes := Claims(s, "")
	if len(prefixes) != 3 {
		t.Fatalf("expected 3 prefixes, got %d", len(prefixes))
	}
	// Longest first: leaf > mid > root.
	if got := []string{prefixes[0].ID.Name, prefixes[1].ID.Name, prefixes[2].ID.Name}; got[0] != "leaf" || got[1] != "mid" || got[2] != "root" {
		t.Errorf("expected leaf/mid/root by descending prefix length, got %v", got)
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
		Name: "without", Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system"},
	}
	s.AddObject(with)
	s.AddObject(without)

	prefixes := Claims(s, "")
	if len(prefixes) != 2 || prefixes[0].ID != with.Named() || prefixes[1].ID != without.Named() || prefixes[1].Prefix != "" {
		t.Fatalf("expected apps and source-root claims; got %+v", prefixes)
	}
	if parent, ok := manifest.StrictParent(prefixes, "root.yaml", manifest.NamedResource{}, nil); !ok || parent != without.Named() {
		t.Fatalf("root.yaml parent = %v, %t; want %v", parent, ok, without.Named())
	}
}

func TestClaims_EmptyExternalPath(t *testing.T) {
	t.Parallel()
	s := store.New()
	repo := &manifest.GitRepository{Name: "external", Namespace: "flux-system"}
	external := &manifest.Kustomization{
		Name: "external", Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: repo.Name, Namespace: repo.Namespace},
	}
	local := &manifest.Kustomization{Name: "local", Namespace: "flux-system", Path: "./apps"}
	s.AddObject(repo)
	s.AddObject(external)
	s.AddObject(local)
	prefixes := loaderFixture(s, nil).Claims(t.TempDir())
	if len(prefixes) != 1 || prefixes[0].ID != local.Named() {
		t.Fatalf("external source root must not claim local files: %+v", prefixes)
	}
	if parent, ok := manifest.StrictParent(Claims(s, ""), "bad.yaml", manifest.NamedResource{}, nil); !ok || parent != external.Named() {
		t.Fatalf("final reporting must retain external source-root ownership: parent=%v, found=%t", parent, ok)
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
	if _, ok := manifest.StrictParent(prefixes, "apps/team-a/ks.yaml", self, nil); ok {
		t.Errorf("StrictParent must skip self matches")
	}
}

func TestClaims_IncludesSpecComponents(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/team-a/kustomization.yaml", "components: [../shared/local]\n")
	testutil.WriteFile(t, root, "apps/shared/observability/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n")
	testutil.WriteFile(t, root, "apps/shared/local/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n")
	s := store.New()
	admitForTest(s, root)
	parent := &manifest.Kustomization{
		Name: "parent", Namespace: "flux-system",
		Path:       "./apps/team-a",
		Components: []string{"../shared/observability"},
	}
	s.AddObject(parent)

	prefixes := Claims(s, root)
	assert.Diff(t, prefixes, []manifest.KSClaim{
		{ID: parent.Named(), Prefix: "apps/shared/observability/"},
		{ID: parent.Named(), Prefix: "apps/shared/local/"},
		{ID: parent.Named(), Prefix: "apps/team-a/"},
	})

	got, ok := manifest.StrictParent(prefixes, "apps/shared/observability/karma.yaml", manifest.NamedResource{}, nil)
	if !ok {
		t.Fatalf("StrictParent missed component-dir child — components fold regression")
	}
	if got.Name != "parent" {
		t.Errorf("StrictParent = %q, want 'parent' (component-dir child should attribute to its containing KS)", got.Name)
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
	prefixes := Claims(s, "")
	got, ok := manifest.StrictParent(prefixes, "apps/team-a/web/deploy.yaml", manifest.NamedResource{}, nil)
	if !ok {
		t.Fatalf("expected a parent match")
	}
	if got.Name != "leaf" {
		t.Errorf("expected deepest parent 'leaf', got %q", got.Name)
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

	prefixes := Claims(s, "")
	hr := testutil.HelmReleaseID("", "flux-instance")
	parent, ok := manifest.StrictParent(prefixes, "kubernetes/apps/flux-system/flux-operator/instance/helm-release.yaml", hr, nil)
	if !ok {
		t.Fatalf("expected the absolute spec.path KS to parent its helm-release.yaml; prefixes=%v", prefixes)
	}
	if parent != ks.Named() {
		t.Errorf("parent = %v, want %v", parent, ks.Named())
	}
}
