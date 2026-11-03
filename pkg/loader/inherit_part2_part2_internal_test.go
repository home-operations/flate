package loader

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func testAdmitGraphProjectionEffectiveNamesIncludeNamespaceTransformation(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	for _, side := range []string{"one", "two"} {
		testutil.WriteFile(
			t,
			root,
			side+"/kustomization.yaml",
			"namespace: "+side+"\nnamePrefix: pre-\nnameSuffix: -post\nresources: [objects.yaml]\n",
		)
		testutil.WriteFile(
			t,
			root,
			side+"/objects.yaml",
			"apiVersion: v1\nkind: Namespace\nmetadata: {name: _}\n---\napiVersion: foreign.example/v2\nkind: Namespace\nmetadata: {name: _}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: values}\n",
		)
	}
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	for _, side := range []string{"one", "two"} {
		occurrences, err := l.Occurrences(t.Context(), filepath.Join(root, side), "")
		assert.NoError(t, err)
		ids := make([]manifest.NamedResource, 0, len(occurrences))
		for _, o := range occurrences {
			ids = append(ids, o.Identity)
			{
				assert.FatalIf(t, o.object.Named() != o.Authored, "%s", "projection mutated an authored object")
			}
		}
		assert.Diff(
			t,
			ids,
			[]manifest.NamedResource{
				{Kind: "Namespace", Name: side},
				{Group: "foreign.example", Kind: "Namespace", Namespace: side, Name: "_"},
				{Kind: manifest.KindConfigMap, Namespace: side, Name: "pre-values-post"},
			},
		)
	}
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	for id := range l.Index.Records {
		assert.FatalIf(t, id.Kind == "Namespace", "inventory-only resource entered the production index: %s", id)
	}
}

func testAdmitGraphProjectionBootstrapDocumentOwners(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(
		t,
		root,
		"app/siblings.yaml",
		testutil.KustomizationYAML(
			"excluded",
			"apps",
			"  path: ./leaf\n",
		)+"---\n"+testutil.KustomizationYAML(
			"self",
			"apps",
			"  path: ./app\n",
		),
	)
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	for _, name := range []string{"self", "excluded"} {
		id := testutil.KustomizationID("apps", name)
		assert.FatalIf(t, l.Store.GetObject(id) != nil ||
			l.AdmitsRoot(
				l.IndexedObject(id, l.Index.Records[id]).Obj,
				filepath.Join(root, "app/siblings.yaml"),
			), "omitted declaration became runnable: %s", id)
		{
			_, ok := l.Index.Lookup(id)
			assert.FatalIf(t, !ok, "lost inactive evidence: %s", id)
		}
	}
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	{
		assert.FatalIf(t, l.AdmitsRoot(
			l.IndexedObject(
				testutil.KustomizationID("apps", "self"),
				l.Index.Records[testutil.KustomizationID("apps", "self")],
			).Obj,
			filepath.Join(root, "app/siblings.yaml"),
		), "%s", "finalization lost denial")
	}
}

func testAdmitGraphProjectionInactiveUnresolvedSource(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(
		t,
		root,
		"apps/disabled.yaml",
		testutil.KustomizationYAML(
			"disabled",
			"apps",
			"  path: ./target\n  sourceRef: {kind: GitRepository, name: missing}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"target/child.yaml",
		testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"),
	)
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	child := testutil.KustomizationID("apps", "child")
	{
		assert.FatalIf(t, l.Store.GetObject(child) != nil ||
			l.AdmitsRoot(
				l.IndexedObject(child, l.Index.Records[child]).Obj,
				filepath.Join(root, "target/child.yaml"),
			), "%s", "inactive unresolved owner enabled builder-free descendant")
	}
}

func testAdmitGraphProjectionPlainEntryUnderInactiveClaim(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "direct.yaml", testutil.KustomizationYAML("direct", "apps", "  path: ./leaf\n"))
	testutil.WriteFile(t, root, "disabled/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(
		t,
		root,
		"disabled/owner.yaml",
		testutil.KustomizationYAML("disabled", "apps", "  path: .\n"),
	)
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	id := testutil.KustomizationID("apps", "direct")
	{
		assert.FatalIf(t, l.Store.GetObject(id) == nil ||
			!l.AdmitsRoot(
				l.IndexedObject(id, l.Index.Records[id]).Obj,
				filepath.Join(root, "direct.yaml"),
			), "%s", "inactive broad claim disabled the plain entry's direct manifest")
	}
}

func testAdmitGraphProjectionInactiveAliasResourceOutsideClaim(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(
		t,
		root,
		"flux/disabled.yaml",
		testutil.KustomizationYAML(
			"disabled",
			"apps",
			"  path: ./target\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "target/kustomization.yaml", "resources: [../outside/source.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"outside/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: inactive-source, namespace: apps}\nspec: {url: https://fixture.invalid/unused}\n",
	)
	st := store.New()
	l := New(st)
	l.SourceRoot = root
	admitForTest(st, root)
	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	st.AddObject(source)
	mount := filepath.Join(t.TempDir(), "alias")
	st.SetArtifact(
		source.Named(),
		&store.SourceArtifact{LocalPath: mount, LocalRoot: root, InputAlias: true, FS: os.DirFS(root)},
	)
	for _, dir := range []string{root, filepath.Join(mount, "target")} {
		_, err := l.Load(t.Context(), dir)
		assert.NoError(t, err)
	}
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "inactive-source",
	}
	{
		assert.FatalIf(t, st.GetObject(id) != nil ||
			l.AdmitsRoot(
				l.IndexedObject(id, l.Index.Records[id]).Obj,
				filepath.Join(root, "outside/source.yaml"),
			), "%s", "inactive alias enabled its physical resource outside the path claim")
	}
	for child := range l.Sources(t.Context()) {
		{
			assert.FatalIf(
				t,
				child.Obj.Named() == id,
				"%s",
				"inactive alias exposed its physical source for acquisition",
			)
		}
	}
}

func testAdmitGraphProjectionCase3(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	root := t.TempDir()
	files := map[string]string{
		"apps/kustomization.yaml": "resources: []\n",
		"apps/disabled.yaml": testutil.KustomizationYAML(
			"disabled",
			"apps",
			"  path: ./target\n  components: [../component]\n",
		),
		"target/kustomization.yaml":    "resources: [nested.yaml, source.yaml]\nconfigMapGenerator:\n- name: inactive-generated\n  literals: [VALUE=unused]\n",
		"target/nested.yaml":           testutil.KustomizationYAML("nested", "apps", "  path: ./nested\n"),
		"target/source.yaml":           "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: inactive-source, namespace: apps}\nspec: {url: oci://fixture.invalid/unused}\n",
		"nested/kustomization.yaml":    "resources: [cycle.yaml]\n",
		"nested/cycle.yaml":            testutil.KustomizationYAML("cycle", "apps", "  path: ./target\n"),
		"component/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [data.yaml]\n",
		"component/data.yaml":          "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: inactive-component, namespace: apps}\n",
	}
	if *testCaseInput {
		files["independent/kustomization.yaml"] = "resources: [../apps/disabled.yaml]\n"
	}
	for file, body := range files {
		testutil.WriteFile(t, root, file, body)
	}
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	before := l.Kustomizations()
	l.Admit(root)
	{
		assert.FatalIf(
			t,
			!slices.EqualFunc(before, l.Kustomizations(), func(a, b store.Child) bool { return a.Obj == b.Obj }),
			"%s",
			"unchanged revision replaced declarations",
		)
	}
	for _, ks := range before {
		if !(*testCaseInput) {
			t.Fatalf("inactive cycle enabled %s", ks.Obj.Named())
		}
	}
	{
		roots := l.StandaloneRoots(root)
		assert.FatalIf(
			t,
			!(*testCaseInput) && len(roots) != 1,
			"inactive targets became standalone: %v",
			roots,
		)
	}
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	id := testutil.KustomizationID("apps", "disabled")
	file := filepath.Join(root, "apps/disabled.yaml")
	if l.AdmitsRoot(l.IndexedObject(id, l.Index.Records[id]).Obj, file) != (*testCaseInput) {
		t.Fatalf(
			"finalized admission=%t want %t",
			l.AdmitsRoot(l.IndexedObject(id, l.Index.Records[id]).Obj, file), (*testCaseInput),
		)
	}
	{
		_, indexed := l.Index.Lookup(id)
		assert.FatalIf(t, !indexed, "%s", "inactive owner evidence was discarded")
	}
	if !(*testCaseInput) {
		for obj := range l.Sources(t.Context()) {
			t.Fatalf("inactive source escaped evidence boundary: %s", obj.Obj.Named())
		}
		for _, obj := range l.Store.ListObjects("", manifest.KindConfigMap) {
			if obj.Named().Name == "inactive-generated" || obj.Named().Name == "inactive-component" {
				t.Fatalf("inactive data materialized: %s", obj.Named())
			}
		}
	} else {
		{
			assert.FatalIf(t, len(l.Kustomizations()) != 3, "%s", "independent edge did not enable nested cycle")
		}
		{
			_, ok := lookupDataForTest[*manifest.ConfigMap](
				t,
				l.Store,
				manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "inactive-generated"},
			)
			assert.FatalIf(t, !ok, "%s", "independent edge did not enable generator")
		}
	}
}

func testAdmitGraphProjectionCase4(t *testing.T, testCaseInput *struct {
	name   string
	hrDir  string
	pinned bool
	files  map[string]string
	scans  []string
	want   []string
	wantKS []string
	alias  bool
},
) {
	t.Helper()

	root := t.TempDir()
	for file, body := range testCaseInput.files {
		testutil.WriteFile(t, root, file, body)
	}
	hrDir := "apps"
	if _, ok := testCaseInput.files["base/kustomization.yaml"]; ok {
		hrDir = "base"
	}
	if testCaseInput.hrDir != "" {
		hrDir = testCaseInput.hrDir
	}
	hrDoc := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app}
spec:
  chartRef: {kind: OCIRepository, name: chart}
  valuesFrom: [{kind: ConfigMap, name: values}, {kind: Secret, name: optional, optional: true}]
`
	testutil.WriteFile(t, root, hrDir+"/hr.yaml", hrDoc)
	l := loaderFixture(store.New(), nil)
	l.SourceRoot = root
	admitForTest(l.Store, root)
	if testCaseInput.alias {
		location, err := gitrepo.Locate(t.Context(), root, root)
		assert.NoError(t, err)
		input, err := gittree.AdmitDirectory(t.Context(), location)
		assert.NoError(t, err)
		l.Store.SetArtifact(manifest.BootstrapSourceID, input)
		view, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Source, Current: input.FS},
		)
		assert.NoError(t, err)
		id := manifest.NamedResource{
			Group:     "source.toolkit.fluxcd.io",
			Kind:      manifest.KindGitRepository,
			Namespace: "flux-system",
			Name:      "cluster",
		}
		l.Store.AddObject(
			&manifest.GitRepository{Name: id.Name, Namespace: id.Namespace, URL: "file://" + root},
		)
		l.Store.SetArtifact(
			id,
			&store.SourceArtifact{
				Kind:       id.Kind,
				LocalRoot:  root,
				LocalPath:  filepath.Join(filepath.Dir(root), ".flate-sources", "alias", id.Namespace, id.Name),
				InputAlias: true,
				FS:         view,
			},
		)
	}
	if testCaseInput.name == "self_claim_transformer_fallback" {
		for _, namespace := range []string{"flux-system", "myns"} {
			source := &manifest.GitRepository{Name: "cluster", Namespace: namespace}
			l.Store.AddObject(source)
			l.Store.SetArtifact(
				source.Named(),
				&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)},
			)
		}
	}
	if testCaseInput.pinned {
		artifact := t.TempDir()
		testutil.WriteFile(t, artifact, "apps/kustomization.yaml", "resources: [hr.yaml]\n")
		testutil.WriteFile(t, artifact, "apps/hr.yaml", hrDoc)
		l.Store.SetArtifact(
			manifest.NamedResource{
				Group:     "source.toolkit.fluxcd.io",
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      "cluster",
			},
			&store.SourceArtifact{
				LocalRoot: root,
				Key:       "pinned",
				Content:   "pinned",
				FS:        os.DirFS(artifact),
				LocalPath: artifact,
			},
		)
		l.documents = readerForTest(root)
		l.documents.views[artifact] = os.DirFS(artifact)
		l.documents.read(filepath.Join(artifact, "apps"))
	}
	for _, scan := range testCaseInput.scans {
		{
			_, err := l.Load(t.Context(), filepath.Join(root, scan))
			assert.NoError(t, err)
		}
		if testCaseInput.name == "named_ready" && scan == "flux" {
			l.Admit(root)
			if _, ok := l.Index.Lookup(testutil.HelmReleaseID("", "app")); !ok {
				t.Fatalf("unresolved source records before loading its path: %+v", l.Index.Records)
			}
			source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
			l.Store.AddObject(source)
			l.Store.SetArtifact(
				source.Named(),
				&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)},
			)
		}
	}
	l.Admit(root)
	if testCaseInput.pinned || testCaseInput.alias {
		ks := l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)[0]
		artifact := l.Store.GetArtifact(ks.SourceID(), store.ArtifactContent).(*store.SourceArtifact)
		{
			_, err := loadForTest(t.Context(), t, l, filepath.Join(artifact.LocalPath, "apps"))
			assert.NoError(t, err)
		}
	}
	var original, snapshot *manifest.Kustomization
	var raw manifest.DiscoveryRecord
	if len(testCaseInput.wantKS) != 0 {
		id := testutil.KustomizationID("flux-system", "myapp")
		var r manifest.DiscoveryRecord
		for _, record := range l.Index.Files() {
			if record.Authored == id {
				r = record
				break
			}
		}
		authored := l.authoredObject(r.Path, r.Authored)
		{
			assert.FatalIf(t, authored.Doc == nil || manifest.DocIdentity(authored.Doc) != r.Authored, "projection lost authored document: %v", authored.Doc)
		}
		original, _ = authored.Obj.(*manifest.Kustomization)
		{
			assert.FatalIf(t, original == nil, "%s", "authored Kustomization missing")
		}
		snapshot = original.Clone()
		raw = r
	}
	testAdmitGraphProjectionCase4Completion(t, &testAdmitGraphProjectionCase4CompletionState{testCaseInput: &testCaseInput, root: &root, l: &l, original: &original, snapshot: &snapshot, raw: &raw})
}

type testAdmitGraphProjectionCase4CompletionState struct {
	testCaseInput **struct {
		name   string
		hrDir  string
		pinned bool
		files  map[string]string
		scans  []string
		want   []string
		wantKS []string
		alias  bool
	}
	root     *string
	l        **Loader
	original **manifest.Kustomization
	snapshot **manifest.Kustomization
	raw      *manifest.DiscoveryRecord
}

func testAdmitGraphProjectionCase4Completion(t *testing.T, scope *testAdmitGraphProjectionCase4CompletionState) {
	t.Helper()
	(*scope.l).Admit((*scope.root))
	if (*scope.original) != nil {
		{
			diff := cmp.Diff(
				(*scope.original),
				(*scope.snapshot),
				cmp.AllowUnexported(manifest.Kustomization{}),
				cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
			)
			assert.FatalIf(t, diff != "", "authored object mutated (-want +got): %s", diff)
		}
		{
			diff := cmp.Diff(
				(*scope.l).authoredObject((*scope.raw).Path, (*scope.raw).Authored).Obj,
				(*scope.snapshot),
				cmp.AllowUnexported(manifest.Kustomization{}),
				cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
			)
			assert.FatalIf(t, diff != "", "authored object mutated (-want +got): %s", diff)
		}
		assert.Diff(t, (*scope.l).recordNamespaces((*scope.raw), "myns"), (*scope.testCaseInput).wantKS)
		got, want := map[manifest.NamedResource][]manifest.NamedResource{}, map[manifest.NamedResource][]manifest.NamedResource{}
		for id, r := range (*scope.l).Index.Files() {
			if r.Path == (*scope.raw).Path {
				assert.Equal(t, r.Authored, (*scope.raw).Authored)
				got[id] = r.Refs
			}
		}
		for _, ns := range (*scope.testCaseInput).wantKS {
			id := testutil.KustomizationID(ns, "myapp")
			want[id] = []manifest.NamedResource{
				{
					Group:     "source.toolkit.fluxcd.io",
					Kind:      manifest.KindGitRepository,
					Namespace: ns,
					Name:      "cluster",
				},
			}
		}
		assert.Diff(t, got, want)
	}
	if (*scope.testCaseInput).name == "self_claim" {
		id := testutil.KustomizationID("", "owner")
		{
			_, ok := (*scope.l).Index.Lookup(id)
			assert.FatalIf(
				t,
				!ok || (*scope.l).Store.GetObject(id) != nil,
				"%s",
				"namespace-less owner must remain indexed without independent publication",
			)
		}
		{
			err := (*scope.l).FinalizeIndex(t.Context(), (*scope.root), true)
			assert.NoError(t, err)
		}
		id.Namespace = "media"
		{
			_, ok := (*scope.l).Index.Lookup(id)
			assert.FatalIf(t, ok, "%s", "own coverage invented a second Kustomization identity")
		}
	}
	if (*scope.testCaseInput).pinned {
		{
			err := (*scope.l).FinalizeIndex(t.Context(), (*scope.root), true)
			assert.NoError(t, err)
		}
		r, _ := (*scope.l).Index.Lookup(testutil.HelmReleaseID("apps", "app"))
		readers := (*scope.l).Index.Readers[r.File]
		{
			assert.FatalIf(t, len(readers) != 1 || readers[0].Reader != testutil.KustomizationID("flux-system", "pinned"), "committed source file %q readers = %v", r.File, readers)
		}
	}
	if (*scope.testCaseInput).name == "cross_tree" {
		ks, ok := (*scope.l).Store.Get[*manifest.Kustomization](testutil.KustomizationID("games", "games"))
		{
			assert.FatalIf(t, !ok, "%s", "projected Kustomization missing")
		}
		var deps []manifest.NamedResource
		for ref := range manifest.References(ks, manifest.ReferenceRelations) {
			if ref.Use == "dependsOn" {
				deps = append(deps, ref.NamedResource)
			}
		}
		assert.FatalIf(
			t,
			len(deps) != 1 || deps[0].Namespace != "games",
			"projected dependsOn namespace: %+v",
			deps,
		)
	}
	for _, ns := range (*scope.testCaseInput).want {
		id := testutil.HelmReleaseID(ns, "app")
		r, ok := (*scope.l).Index.Lookup(id)
		assert.FatalIf(t, !ok || len(r.Refs) != 3, "%s record=%+v present=%t", id, r, ok)
		for _, ref := range r.Refs {
			assert.FatalIf(t, ref.Namespace != ns, "%s ref=%s", id, ref)
		}
		assert.FatalIf(t, (*scope.l).Store.GetObject(id) != nil, "%s admitted before promotion", id)
	}
	{
		_, ok := (*scope.l).Index.Lookup(
			testutil.HelmReleaseID("", "app"),
		)
		assert.FatalIf(t, ok &&
			!slices.Contains((*scope.testCaseInput).want, ""), "%s", "stale raw consumer record")
	}
	{
		_, ok := (*scope.l).Index.Lookup(testutil.HelmReleaseID("flux-system", "app"))
		assert.FatalIf(t, ok, "%s", "ancestor fallback retained a stale consumer identity")
	}
	{
		_, ok := (*scope.l).Index.Lookup(testutil.HelmReleaseID("inner", "app"))
		assert.FatalIf(t, ok, "%s", "inner base namespace must not override its overlay")
	}
	if (*scope.testCaseInput).name == "inactive_namespace" {
		{
			_, ok := (*scope.l).Index.Lookup(testutil.HelmReleaseID("inactive", "app"))
			assert.FatalIf(t, ok, "%s", "inactive owner projected a second consumer namespace")
		}
	}
}
