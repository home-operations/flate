package loader

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

func TestClaims_RefreshDoesNotPublish(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	admitForTest(st, root)
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [owner.yaml]\n")
	testutil.WriteFile(t, root, "owner.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./leaf\n"))
	l := New(st)
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	id := testutil.KustomizationID("apps", "owner")
	l.Claims(root)
	{
		assert.FatalIf(t, st.GetObject(id) != nil, "%s", "claims published an indexed owner")
	}
	l.Admit(root)
	owner := st.GetObject(id)
	{
		assert.FatalIf(t, owner == nil, "%s", "admission did not publish the owner")
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: []\n")},
	}})
	l.Claims(root)
	{
		assert.FatalIf(t, st.GetObject(id) != owner, "%s", "claims retired an owner after reader replacement")
	}
	l.Admit(root)
	{
		assert.FatalIf(t, st.GetObject(id) != nil, "%s", "admission retained the replaced reader's owner")
	}
}

func TestClaims_ReadinessViews(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	var reads atomic.Int64
	input := &store.SourceArtifact{LocalPath: root, FS: parsedReadFS{FS: fstest.MapFS{
		"apps/kustomization.yaml": {Data: []byte("resources: []\n")},
	}, reads: map[string]*atomic.Int64{"apps/kustomization.yaml": &reads}}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	ks := &manifest.Kustomization{
		Name:      "owner",
		Namespace: "apps",
		Path:      "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "source"},
	}
	st.AddObject(ks)
	l := New(st)
	l.SourceRoot = root
	{
		claims := l.Claims(root)
		assert.FatalIf(t, len(claims) != 0, "unresolved source claimed bootstrap files: %v", claims)
	}
	st.SetArtifact(ks.SourceID(), input)
	l.refreshSources()
	l.refreshClaims(root, l.kustomizations())
	assert.Equal(t, reads.Load(), int64(0))
	claims := l.Claims(root)
	assert.Diff(t, claims, []manifest.KSClaim{{ID: ks.Named(), Prefix: "apps/"}})
	assert.Equal(t, reads.Load(), int64(1))
	derived := &l.scopes.inputs[0].ownerClaims.claims[0]
	claims[0].Prefix = "caller/"
	assert.Diff(t, l.Claims(root), []manifest.KSClaim{{ID: ks.Named(), Prefix: "apps/"}})
	assert.Equal(t, reads.Load(), int64(1))
	{
		assert.FatalIf(
			t,
			&l.scopes.inputs[0].ownerClaims.claims[0] != derived,
			"%s",
			"unchanged owner claims were derived again",
		)
	}
	{
		assert.FatalIf(t, len(l.Index.Claims) != 0, "%s", "unready local source established admission coverage")
	}
	earlier := *ks
	earlier.Name = "earlier"
	st.AddObject(&earlier)
	claims = l.Claims(root)
	claims[0].Prefix = "caller/"
	assert.Diff(
		t,
		l.Claims(root),
		[]manifest.KSClaim{{ID: earlier.Named(), Prefix: "apps/"}, {ID: ks.Named(), Prefix: "apps/"}},
	)
	{
		assert.FatalIf(
			t,
			&l.scopes.inputs[1].ownerClaims.claims[0] != derived,
			"%s",
			"owner insertion discarded unchanged claim derivation",
		)
	}
	_, err := l.Load(t.Context(), filepath.Join(root, "apps"))
	assert.NoError(t, err)
	l.Claims(root)
	assert.Diff(t, l.Index.Claims["apps/"], []manifest.NamedResource{earlier.Named(), ks.Named()})
	{
		assert.FatalIf(
			t,
			st.GetObject(ks.Named()) != ks,
			"%s",
			"claim refresh replaced the owner's immutable declaration",
		)
	}
	readsBefore, sourcesBefore := reads.Load(), len(l.scopes.sources)
	later := *ks
	later.Name, later.SourceRef.Name = "z-later", "other"
	st.SetArtifact(later.SourceID(), input)
	st.AddObject(&later)
	assert.Diff(
		t,
		l.Claims(root),
		[]manifest.KSClaim{
			{ID: earlier.Named(), Prefix: "apps/"},
			{ID: ks.Named(), Prefix: "apps/"},
			{ID: later.Named(), Prefix: "apps/"},
		},
	)
	assert.Equal(t, len(l.scopes.sources), sourcesBefore+1)
	assert.Equal(t, reads.Load(), readsBefore)
	{
		assert.FatalIf(
			t,
			&l.scopes.inputs[1].ownerClaims.claims[0] != derived,
			"%s",
			"source storage growth discarded unchanged claim derivation",
		)
	}
	st.SetArtifact(later.SourceID(), nil)
	assert.Diff(
		t,
		l.Claims(root),
		[]manifest.KSClaim{{ID: earlier.Named(), Prefix: "apps/"}, {ID: ks.Named(), Prefix: "apps/"}},
	)
	assert.Equal(t, reads.Load(), readsBefore)
	{
		assert.FatalIf(
			t,
			&l.scopes.inputs[1].ownerClaims.claims[0] != derived,
			"%s",
			"source withdrawal discarded unrelated claim derivation",
		)
	}
}

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
			testFinalizeIndexParentSegmentsCase(t, &tc)
		},
		)
	}
}

func parentsForTest(t *testing.T, l *Loader, repoRoot, kind string) map[manifest.NamedResource]manifest.NamedResource {
	t.Helper()
	{
		err := l.FinalizeIndex(t.Context(), repoRoot, true)
		assert.NoError(t, err)
	}
	out := map[manifest.NamedResource]manifest.NamedResource{}
	for id := range l.Index.Records {
		r := l.Index.Records[id]
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
			testBuildParentIndexIndexedChildrenWithoutStoreObjectsCase(
				t,
				&testBuildParentIndexIndexedChildrenWithoutStoreObjectsCaseInputs{
					group:    &group,
					kind:     &kind,
					eligible: &eligible,
				},
			)
		},
		)
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
		Name:      "cluster-apps",
		Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "flux-system",
			Namespace: "flux-system",
		},
		Path: "./kubernetes/apps",
	}
	// Self/aliased source: an in-tree GitRepository with a working-tree artifact
	// (URL-matched by discovery) — kept.
	aliasedKS := &manifest.Kustomization{
		Name:      "self-apps",
		Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "home-kubernetes",
			Namespace: "flux-system",
		},
		Path: "./kubernetes/self",
	}
	// External source: an in-tree GitRepository with NO working-tree artifact,
	// claiming the WIDE ./kubernetes — must be dropped.
	external := &manifest.Kustomization{
		Name:      "side-sync",
		Namespace: "flux-system",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "side-repo",
			Namespace: "flux-system",
		},
		Path: "./kubernetes",
	}
	s.AddObject(bootstrap)
	s.AddObject(aliasedKS)
	s.AddObject(external)
	s.AddObject(&manifest.GitRepository{Name: "home-kubernetes", Namespace: "flux-system"})
	s.AddObject(&manifest.GitRepository{Name: "side-repo", Namespace: "flux-system"})
	// home-kubernetes is aliased to the working tree (LocalPath == repoRoot).
	s.SetArtifact(
		manifest.NamedResource{
			Group:     "source.toolkit.fluxcd.io",
			Kind:      manifest.KindGitRepository,
			Namespace: "flux-system",
			Name:      "home-kubernetes",
		},
		&store.SourceArtifact{
			Kind:      manifest.KindGitRepository,
			URL:       "file://" + repoRoot,
			LocalPath: repoRoot,
			FS:        fstest.MapFS{},
		},
	)

	prefixes := loaderFixture(s, nil).Claims(repoRoot)
	seen := map[manifest.NamedResource]bool{}
	for _, p := range prefixes {
		seen[p.ID] = true
	}
	{
		assert.ErrorIf(t, seen[external.Named()], "external-sourced KS claim must be dropped from the prefix index")
	}
	{
		assert.ErrorIf(
			t,
			!seen[bootstrap.Named()] || !seen[aliasedKS.Named()],
			"local-sourced KS claims must be retained; seen=%v",
			seen,
		)
	}
	{
		parent, ok := manifest.StrictParent(
			Claims(
				s,
				repoRoot,
				s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization),
			),
			"kubernetes/other/bad.yaml",
			manifest.NamedResource{},
			nil,
		)
		assert.FatalIf(t, !ok ||
			parent != external.Named(), "final reporting must retain external-source ownership: parent=%v, found=%t", parent, ok)
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
			ks := &manifest.Kustomization{
				Name:      "apps",
				Namespace: "flux-system",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					APIVersion: tc.group + "/v1",
					Kind:       tc.kind,
					Name:       "entry",
				},
			}
			root, pinned, external := SourceView(store.New(), ks.SourceID(), "/repo")
			{
				assert.FatalIf(
					t,
					root != "/repo" || pinned || external != tc.external,
					"absent source: %q, %t, %t",
					root,
					pinned,
					external,
				)
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
				Name:      "apps",
				Namespace: repo.Namespace,
				Path:      "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind:      manifest.KindGitRepository,
					Name:      repo.Name,
					Namespace: repo.Namespace,
				},
			}
			st.AddObject(repo)
			st.AddObject(ks)
			artifact := &store.SourceArtifact{
				Kind:      manifest.KindGitRepository,
				LocalPath: "/cache",
				LocalRoot: tc.localRoot,
				Key:       "pinned",
				Content:   "pinned",
				FS:        fstest.MapFS{},
			}
			if tc.unavailable {
				artifact.FS = nil
			}
			st.SetArtifact(repo.Named(), artifact)
			_, _, external := SourceView(st, ks.SourceID(), tc.repoRoot)
			{
				assert.FatalIf(t, external != tc.external, "external = %v, want %v", external, tc.external)
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

	{
		got, want := parents[karma.Named()], clusterApps.Named()
		assert.ErrorIf(t, got != want, "karma.parent = %+v; want %+v", got, want)
	}
	{
		_, ok := parents[clusterApps.Named()]
		assert.ErrorIf(t, ok, "cluster-apps should be parentless (root)")
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

	{
		got, want := parents[grandchild.Named()], inner.Named()
		assert.ErrorIf(t, got != want, "grandchild.parent = %+v; want %+v (deepest prefix)", got, want)
	}
	{
		got, want := parents[inner.Named()], outer.Named()
		assert.ErrorIf(t, got != want, "inner.parent = %+v; want %+v", got, want)
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
	{
		_, ok := parents[ks.Named()]
		assert.ErrorIf(t, ok, "KS must not be its own parent: %v", parents)
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
	{
		p, ok := parents[config.Named()]
		assert.ErrorIf(t, ok, "same-spec.path peer must not be a parent: 0-config.parent = %+v", p)
	}
	{
		p, ok := parents[softServe.Named()]
		assert.ErrorIf(t, ok, "same-spec.path peer must not be a parent: 0-soft-serve.parent = %+v", p)
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
	{
		got, want := parents[app.Named()], root.Named()
		assert.ErrorIf(
			t,
			got != want,
			"app.parent = %+v; want %+v (the KS that renders app's definition dir)",
			got,
			want,
		)
	}
}

func testFinalizeIndexParentSegmentsCase(t *testing.T, testCaseInput *struct {
	name    string
	path    string
	base    string
	link    bool
	mounted bool
	pinned  bool
},
) {
	t.Helper()

	root := t.TempDir()
	for _, dir := range []string{"apps/base", "deep/inner"} {
		{
			err := os.MkdirAll(filepath.Join(root, dir), 0o755)
			assert.NoError(t, err)
		}
	}
	prefix := testCaseInput.base
	if prefix != "" {
		prefix += "/"
	}
	testutil.WriteFile(
		t,
		root,
		prefix+"kustomization.yaml",
		"namespace: apps\nresources: [hr.yaml, cm.yaml]\nconfigMapGenerator:\n- name: generated\n  literals: [VALUE=fixture]\n",
	)
	testutil.WriteFile(
		t,
		root,
		prefix+"hr.yaml",
		"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: child, namespace: apps}\n",
	)
	testutil.WriteFile(
		t,
		root,
		prefix+"cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data, namespace: apps}\n",
	)
	st := store.New()
	if !testCaseInput.mounted || testCaseInput.pinned {
		admitForTest(st, root)
	}
	ks := &manifest.Kustomization{Name: "parent", Namespace: "flux-system", Path: testCaseInput.path}
	if testCaseInput.pinned {
		ks.SourceRef = kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "pinned",
			Namespace: "flux-system",
		}
	}
	st.AddObject(ks)
	child := testutil.HelmReleaseID("apps", "child")
	data := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "data"}
	generated := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "generated"}
	l := loaderFixture(
		st,
		map[manifest.NamedResource]string{child: prefix + "hr.yaml", data: prefix + "cm.yaml"},
	)
	if testCaseInput.mounted {
		b := tree.NewBuilder(os.DirFS(root))
		{
			err := b.AddSymlink("alias", "deep/inner")
			assert.NoError(t, err)
		}
		input, err := b.Build()
		{
			assert.NoError(t, err)
		}
		if testCaseInput.pinned {
			src := &manifest.GitRepository{Name: ks.SourceRef.Name, Namespace: ks.SourceRef.Namespace}
			st.AddObject(src)
			st.SetArtifact(
				src.Named(),
				&store.SourceArtifact{
					LocalRoot: root,
					LocalPath: filepath.Join(root, "snapshot"),
					Key:       "pinned",
					Content:   "pinned",
					FS:        input,
				},
			)
		} else {
			l.documents.views = map[string]fs.FS{root: input}
		}
	} else if testCaseInput.link {
		{
			err := os.Symlink("deep/inner", filepath.Join(root, "alias"))
			assert.NoError(t, err)
		}
	}
	l.SourceRoot = root
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	assert.Equal(t, l.Index.Records[child].Parent, ks.Named())
	owners, _ := l.Index.OwnersOfFile(prefix + "hr.yaml")
	assert.Diff(t, owners, []manifest.NamedResource{ks.Named()})
	assert.Diff(t, testutil.Owners(l.Index.Records[data].Productions), []manifest.NamedResource{ks.Named()})
	assert.Diff(t, testutil.Owners(l.Store.Productions(data, l.Index)), []manifest.NamedResource{ks.Named()})
	assert.Diff(t, testutil.Owners(l.Index.Records[generated].Productions), []manifest.NamedResource{ks.Named()})

	assert.Equal(t, ks.Path, testCaseInput.path)
}

type testBuildParentIndexIndexedChildrenWithoutStoreObjectsCaseInputs struct {
	group    *string
	kind     *string
	eligible *bool
}

func testBuildParentIndexIndexedChildrenWithoutStoreObjectsCase(
	t *testing.T,
	testCaseInput *testBuildParentIndexIndexedChildrenWithoutStoreObjectsCaseInputs,
) {
	t.Helper()

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
		files[manifest.NamedResource{Group: (*testCaseInput.group), Kind: (*testCaseInput.kind), Namespace: tc.namespace, Name: tc.name}] = tc.file
	}
	files[manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "team", Name: "data"}] = "apps/team/data.yaml"
	parents := parentsForTest(t, loaderFixture(s, files), "", (*testCaseInput.kind))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := manifest.NamedResource{
				Group:     (*testCaseInput.group),
				Kind:      (*testCaseInput.kind),
				Namespace: tc.namespace,
				Name:      tc.name,
			}
			want := tc.want
			if !(*testCaseInput.eligible) {
				want = manifest.NamedResource{}
			}
			{
				assert.FatalIf(t, s.GetObject(id) != nil, "%s", "indexed child must stay absent from store")
			}
			{
				got := parents[id]
				assert.ErrorIf(t, got != want, "parent[%s] = %v, want %v", id, got, want)
			}
		})
	}
	want := 0
	if *testCaseInput.eligible {
		want = 2
	}
	{
		assert.ErrorIf(
			t,
			len(parents) != want,
			"parents = %v; indexed children must stay absent from store",
			parents,
		)
	}
}
