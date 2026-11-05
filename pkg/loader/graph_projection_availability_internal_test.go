package loader

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	child := testutil.KustomizationID("apps", "child")
	assert.FatalIf(t, l.Store.GetObject(child) != nil ||
		l.AdmitsRoot(
			l.IndexedObject(child, l.Index.Records[child]).Obj,
			filepath.Join(root, "target", "child.yaml"),
		), "%s", "inactive unresolved owner enabled builder-free descendant")
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
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	id := testutil.KustomizationID("apps", "direct")
	assert.FatalIf(t, l.Store.GetObject(id) == nil ||
		!l.AdmitsRoot(
			l.IndexedObject(id, l.Index.Records[id]).Obj,
			filepath.Join(root, "direct.yaml"),
		), "%s", "inactive broad claim disabled the plain entry's direct manifest")
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
		_, err := l.Load(t.Context(), dir, manifest.NamedResource{}, nil)
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
	assert.FatalIf(t, st.GetObject(id) != nil ||
		l.AdmitsRoot(
			l.IndexedObject(id, l.Index.Records[id]).Obj,
			filepath.Join(root, "outside", "source.yaml"),
		), "%s", "inactive alias enabled its physical resource outside the path claim")
	for child := range l.Sources(t.Context()) {
		assert.FatalIf(
			t,
			child.Obj.Named() == id,
			"%s",
			"inactive alias exposed its physical source for acquisition",
		)
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
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	before := l.Kustomizations()
	l.Admit(root)
	assert.FatalIf(
		t,
		!slices.EqualFunc(before, l.Kustomizations(), func(a, b store.Child) bool { return a.Obj == b.Obj }),
		"%s",
		"unchanged revision replaced declarations",
	)
	for i := range before {
		ks := &before[i]
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
	file := filepath.Join(root, "apps", "disabled.yaml")
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
		assert.FatalIf(t, len(l.Kustomizations()) != 3, "%s", "independent edge did not enable nested cycle")
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

func testAdmitGraphProjectionSubtreeAvailability(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"parent",
			"flux-system",
			"  {path: ./apps, targetNamespace: media, sourceRef: {kind: OCIRepository, name: source}}\n",
		),
	)
	testutil.WriteFile(t, root, "kubernetes/apps/kustomization.yaml", "resources: [child.yaml, explicit.yaml]\n")
	for _, name := range []string{"child", "explicit"} {
		namespace := ""
		if name == "explicit" {
			namespace = "control"
		}
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/"+name+".yaml",
			testutil.KustomizationYAML(
				name,
				namespace,
				"  {path: ./output, sourceRef: {kind: OCIRepository, name: source, namespace: flux-system}}\n",
			),
		)
	}
	st := store.New()
	l := loaderFixture(st, nil)
	l.SourceRoot = root
	admitForTest(st, root)
	for _, dir := range []string{"flux", "kubernetes/apps"} {
		{
			_, err := loadForTest(t.Context(), t, l, filepath.Join(root, dir))
			assert.NoError(t, err)
		}
	}
	owner, _ := st.Get[*manifest.Kustomization](testutil.KustomizationID("flux-system", "parent"))
	assert.FatalIf(t, owner == nil, "%s", "parent missing")
	l.Admit(root)
	{
		external := SourceView(st, owner.SourceID(), root).External
		assert.FatalIf(t, !external, "%s", "unavailable source borrowed bootstrap coordinates")
	}
	for _, id := range []manifest.NamedResource{
		testutil.KustomizationID("", "child"),
		testutil.KustomizationID("control", "explicit"),
	} {
		{
			_, ok := l.Index.Lookup(id)
			assert.FatalIf(t, !ok, "unavailable source changed raw identity %s", id)
		}
	}
	input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	view, err := fs.Sub(input.FS, "kubernetes")
	assert.NoError(t, err)
	source, err := owner.SourceID().BindLocal(root)
	assert.NoError(t, err)
	st.AddObject(source)
	mount := filepath.Join(filepath.Dir(root), ".flate-sources", "subtree", "flux-system", "source")
	st.SetArtifact(
		source.Named(),
		&store.SourceArtifact{
			Kind:       manifest.KindOCIRepository,
			LocalRoot:  filepath.Join(root, "kubernetes"),
			LocalPath:  mount,
			InputAlias: true,
			FS:         view,
		},
	)
	{
		location2 := SourceView(st, owner.SourceID(), root)
		got, external := location2.Path, location2.External
		assert.FatalIf(t, external || got != mount, "available source = %s, external=%t", got, external)
	}
	{
		_, err := l.Load(t.Context(), filepath.Join(mount, "apps"), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
	}
	raw := map[string]manifest.DiscoveryRecord{}
	for _, name := range []string{"child", "explicit"} {
		id := testutil.KustomizationID("", name)
		if name == "explicit" {
			id.Namespace = "control"
		}
		record, ok := l.Index.Lookup(id)
		file, err := filepath.Rel(root, filepath.Join(mount, "apps", name+".yaml"))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			!ok || record.File != filepath.ToSlash(file) || record.Authored != id,
			"raw %s record = %+v",
			id,
			record,
		)
		raw[name] = record
	}
	l.Admit(root)
	for _, name := range []string{"child", "explicit"} {
		id := testutil.KustomizationID("media", name)
		record, ok := l.Index.Lookup(id)
		if !ok || candidateForTest(t, l, id) == nil || st.GetObject(id) == nil || record.File != raw[name].File ||
			record.Path != filepath.Join(mount, "apps", name+".yaml") ||
			record.Path != raw[name].Path {
			t.Fatalf("projected %s record=%+v, raw=%+v", id, record, raw[name])
		}
	}
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	for _, name := range []string{"child", "explicit"} {
		id := testutil.KustomizationID("media", name)
		record, ok := l.Index.Lookup(id)
		assert.FatalIf(t, !ok || record.Parent != owner.Named() || record.File != "kubernetes/apps/"+name+".yaml" ||
			record.Path != raw[name].Path, "finalized %s record=%+v", id, record)
		{
			readers := slices.Collect(
				l.Index.ReadersOfFile(record.File),
			)
			assert.FatalIf(t, !slices.ContainsFunc(
				readers,
				func(r manifest.FileReader) bool { return r.Reader == owner.Named() },
			), "subtree readers = %v", readers)
		}
	}
}

func testAdmitGraphProjectionStaleDeclaration(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: immich\nresources: [ks.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/ks.yaml",
		testutil.KustomizationYAML("immich-db", "pg", "  {path: ./output, targetNamespace: pg}\n"),
	)
	l := loaderFixture(store.New(), nil)
	l.SourceRoot = root
	{
		_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "apps"))
		assert.NoError(t, err)
	}
	l.Admit(root)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	canonical := testutil.KustomizationID("immich", "immich-db")
	ks, ok := l.Store.Get[*manifest.Kustomization](canonical)
	assert.FatalIf(t, !ok || ks.TargetNamespace != "pg", "canonical declaration = %+v, want targetNamespace pg", ks)
	{
		_, ok := l.Index.Lookup(canonical)
		assert.FatalIf(t, !ok, "%s", "canonical declaration missing from finalized index")
	}
	stale := testutil.KustomizationID("pg", "immich-db")
	_, indexed := l.Index.Lookup(stale)
	assert.FatalIf(
		t,
		l.Store.GetObject(stale) != nil || indexed,
		"%s",
		"stale pg/immich-db declaration survived projection",
	)
}
