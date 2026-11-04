package loader

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoader_MalformedManifestFails(t *testing.T) {
	t.Parallel()
	const broken = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: blackbox-exporter
  namespace: flux-system
spec:
  path: ./apps/blackbox-exporter
  sourceRef:
    kind: GitRepository
    name: flux-system
brokenPointer: [ unclosed
`
	cases := []struct {
		name, body string
		tolerated  bool
	}{
		{name: "invalid YAML", body: broken},
		{name: "missing required version", body: "dependsOn: [{kind: Kustomization, name: target}]"},
		{name: "slash", body: "dependsOn: [{apiVersion: '/', kind: Kustomization, name: target}]"},
		{name: "empty version", body: "dependsOn: [{apiVersion: 'example.test/', kind: Kustomization, name: target}]"},
		{
			name: "multi slash",
			body: "dependsOn: [{apiVersion: 'example.test/v1/extra', kind: Kustomization, name: target}]",
		},
		{name: "optional empty version", body: "inputsFrom: [{apiVersion: '', name: target}]"},
		{name: "optional multi slash", body: "inputsFrom: [{apiVersion: 'example.test/v1/extra', name: target}]"},
		{
			name: "typed empty version",
			body: "apiVersion: helm.toolkit.fluxcd.io/\nkind: HelmRelease\nmetadata: {name: invalid}\n",
		},
		{name: "raw empty version", body: "apiVersion: example.test/\nkind: Foreign\nmetadata: {name: invalid}\n"},
		{
			name: "raw parse error",
			body: "apiVersion: example.test/v1/extra\nkind: Foreign\nmetadata: {name: invalid}\n",
		},
		{name: "unrelated configuration", body: "kind: Configuration\nsettings: {enabled: true}\n", tolerated: true},
		{
			name:      "unrelated HelmRelease",
			body:      "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: invalid}\nspec: {chart: {spec: {}}}\n",
			tolerated: true,
		},
		{
			name:      "unrelated ResourceSet",
			body:      "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {}\nspec: {}\n",
			tolerated: true,
		},
	}
	for _, tc := range cases {
		for _, graph := range []bool{false, true} {
			t.Run(tc.name+"/graph="+strconv.FormatBool(graph), func(t *testing.T) {
				t.Parallel()
				testLoaderMalformedManifestFailsCase(
					t,
					&testLoaderMalformedManifestFailsCaseInputs{tc: &tc, graph: &graph},
				)
			},
			)
		}
	}
}

func TestLoader_IndependentRoots(t *testing.T) {
	t.Parallel()
	for _, first := range []string{"plain", "builder"} {
		t.Run(first+" first", func(t *testing.T) {
			t.Parallel()
			testLoaderIndependentRootsCase(t, &first)
		},
		)
	}

	t.Run(
		"retained native views",
		func(t *testing.T) { t.Parallel(); testLoaderIndependentRootsRetainedNativeViews(t) },
	)
	t.Run("admitted membership", func(t *testing.T) { t.Parallel(); testLoaderIndependentRootsAdmittedMembership(t) })
	for _, tc := range []struct {
		name    string
		bounded bool
		nested  bool
	}{
		{name: "independent"},
		{name: "bounded", bounded: true},
		{name: "nested", nested: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testLoaderIndependentRootsCase2(t, &tc)
		},
		)
	}
}

type unreadableListingFS struct{ fs.FS }

type unreadableInputFS struct {
	fs.FS

	denied string
}

func (f unreadableInputFS) Open(name string) (fs.File, error) {
	if name == f.denied {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}
	return f.FS.Open(name)
}

func (f unreadableInputFS) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(f.FS, name)
}

func (unreadableListingFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrPermission}
}

func TestLoader_ComponentParseFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		selection manifest.Selection
	}{
		{name: "full"},
		{name: "named", selection: manifest.Selection{Roots: []manifest.NamedResource{{Kind: manifest.KindKustomization, Name: "selected"}}}},
		{name: "changed", selection: manifest.Selection{Changed: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"component/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [bad.yaml]\n",
			)
			testutil.WriteFile(
				t,
				root,
				"component/bad.yaml",
				"apiVersion: v1\nkind: ConfigMap\ndata: {value: invalid}\n",
			)
			st := store.New()
			admitForTest(st, root)
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = tc.selection
			l.Options.Filter = change.NewFilter()
			count, err := l.Load(t.Context(), filepath.Join(root, "component"))
			{
				assert.FatalIf(t, err != nil || count != 0, "Load = (%d, %v), want retained parse failure", count, err)
			}
			failed := l.documents.cache[filepath.Join(root, "component/bad.yaml")].objects
			{
				assert.FatalIf(
					t,
					len(failed) != 1 || failed[0].outcome.Err == nil || failed[0].fact != nil,
					"component parse failure not retained: %+v",
					failed,
				)
			}
			{
				assert.FatalIf(
					t,
					len(st.ListObjects("", manifest.KindConfigMap)) != 0 || len(st.Warnings()) != 0,
					"%s",
					"failed component document escaped census",
				)
			}
		})
	}
}

func TestLoader_WarningConditions(t *testing.T) {
	t.Parallel()
	for _, site := range []string{"kustomization", "sibling", "component", "readdir"} {
		t.Run(site, func(t *testing.T) {
			t.Parallel()
			testLoaderWarningConditionsCase(t, &site)
		},
		)
	}
	t.Run("failed nested resource preserves parent count", func(t *testing.T) {
		t.Parallel()
		testLoaderWarningConditionsFailedNestedResourcePreservesParentCount(t)
	},
	)
}

func TestLoader_PreferExistingIndexedObject(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "first/kustomization.yaml", "resources: [ks.yaml]\n")
	for _, dir := range []string{"first", "second"} {
		testutil.WriteFile(t, root, dir+"/ks.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./"+dir+"\n"))
	}
	l := New(store.New())
	l.SourceRoot, l.PreferExisting = root, true
	for _, dir := range []string{"first", "second"} {
		_, err := l.Load(t.Context(), filepath.Join(root, dir))
		assert.NoError(t, err)
	}
	l.Admit(root)
	id := testutil.KustomizationID("apps", "child")
	record, _ := l.Index.Lookup(id)
	if record.Path != filepath.Join(root, "first/ks.yaml") || len(l.Kustomizations()) != 1 ||
		l.Kustomizations()[0].Obj.(*manifest.Kustomization).Path != "./first" {
		t.Fatalf("first indexed object was replaced: %+v, %+v", record, l.Kustomizations())
	}
}

func TestLoader_InvalidBuilderStopsRawDescent(t *testing.T) {
	t.Parallel()
	for _, entry := range []string{"graph", "plain"} {
		t.Run(entry, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			if entry == "graph" {
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: [invalid]\n")
			}
			testutil.WriteFile(t, root, "invalid/kustomization.yaml", "resources: [\n")
			testutil.WriteFile(
				t,
				root,
				"invalid/ks.yaml",
				testutil.KustomizationYAML("raw", "apps", "  path: ./leaf\n"),
			)
			l := New(store.New())
			_, err := l.Load(t.Context(), root)
			assert.NoError(t, err)
			l.Admit(root)
			{
				_, ok := l.Index.Lookup(testutil.KustomizationID("apps", "raw"))
				assert.FatalIf(t, ok, "%s", "invalid builder admitted raw resource")
			}
		})
	}
}

func TestLoader_ProjectedEvidence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "namespace: apps\nresources: [objects.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"objects.yaml",
		testutil.KustomizationYAML(
			"child",
			"",
			"  path: ./leaf\n",
		)+"---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: source}\nspec: {url: https://fixture.invalid/source}\n",
	)
	l := New(store.New())
	l.SourceRoot = root
	l.Store.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."})
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	for _, cached := range []bool{true, false} {
		if !cached {
			l.candidates = nil
		}
		sources := 0
		for child := range l.Sources(t.Context()) {
			source := child.Obj
			sources++
			assert.Equal(t, source.Named().Namespace, "apps")
		}
		assert.Equal(t, sources, 1)
		owners := l.Kustomizations()
		assert.Equal(t, len(owners), 2)
		for _, owner := range owners {
			assert.Equal(t, owner.Obj.Named().Namespace, "apps")
		}
	}
	for _, obj := range l.documents.cache[filepath.Join(root, "objects.yaml")].objects {
		assert.Equal(t, obj.Obj.Named().Namespace, "")
	}
}

type testLoaderMalformedManifestFailsCaseInputs struct {
	tc *struct {
		name      string
		body      string
		tolerated bool
	}
	graph *bool
}

func testLoaderMalformedManifestFailsCase(t *testing.T, testCaseInput *testLoaderMalformedManifestFailsCaseInputs) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "app")
	body := testCaseInput.tc.body
	if strings.HasPrefix(body, "dependsOn:") || strings.HasPrefix(body, "inputsFrom:") {
		body = "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: invalid}\nspec:\n  " + body + "\n"
	}
	if testCaseInput.tc.tolerated {
		body += "---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy}\n"
	}
	testutil.WriteFile(t, dir, "blackbox-exporter.yaml", body)
	if *testCaseInput.graph {
		testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [blackbox-exporter.yaml]\n")
	}
	l := New(store.New())
	l.SourceRoot = filepath.Dir(dir)
	{
		_, err := loadForTest(t.Context(), t, l, dir)
		assert.FatalIf(t, err != nil, "indexed discovery aborted before ownership was known: %v", err)
	}
	first := retainedErrorsForTest(l)["app/blackbox-exporter.yaml"]
	if testCaseInput.tc.tolerated {
		{
			_, indexed := l.Index.Lookup(
				manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "healthy"},
			)
			assert.FatalIf(t, !indexed, "unrelated document poisoned its healthy sibling: error=%v", first)
		}
	} else {
		assert.FatalIf(
			t,
			!errors.Is(first, manifest.ErrInput),
			"source-qualified decode error = %v, want manifest.ErrInput",
			first,
		)
	}
	{
		_, err := loadForTest(t.Context(), t, l, filepath.Dir(dir))
		assert.NoError(t, err)
	}
	{
		got := retainedErrorsForTest(l)["app/blackbox-exporter.yaml"]
		assert.FatalIf(
			t,
			!errors.Is(got, first),
			"second walk replaced the first decode failure: first=%+v got=%+v",
			first,
			got,
		)
	}
}

func testLoaderIndependentRootsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "plain/empty.txt", "")
	testutil.WriteFile(t, root, "builder/kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"builder/ks.yaml",
		testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"),
	)
	l := New(store.New())
	l.SourceRoot = root
	l.Store.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./builder"})
	second := "builder"
	if (*testCaseInput) == "builder" {
		second = "plain"
	}
	for _, dir := range []string{(*testCaseInput), second} {
		_, err := l.Load(t.Context(), filepath.Join(root, dir))
		assert.NoError(t, err)
	}
	l.Admit(root)
	child := testutil.KustomizationID("apps", "child")
	{
		_, ok := l.Index.Lookup(child)
		assert.FatalIf(t, !ok, "%s", "loaded child evidence missing")
	}
	{
		assert.FatalIf(
			t,
			(l.Store.GetObject(child) != nil) != ((*testCaseInput) == "plain"),
			"%s",
			"later load changed first entry admission policy",
		)
	}
	independent := New(store.New())
	independent.SourceRoot = root
	independent.Store.AddObject(l.Store.GetObject(testutil.KustomizationID("apps", "owner")))
	_, err := independent.Load(t.Context(), filepath.Join(root, "builder"))
	assert.NoError(t, err)
	independent.Admit(root)
	{
		assert.FatalIf(
			t,
			independent.Store.GetObject(child) != nil,
			"%s",
			"independent loader inherited first entry policy",
		)
	}
}

func testLoaderIndependentRootsRetainedNativeViews(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	for _, name := range []string{"first", "second"} {
		testutil.WriteFile(t, root, name+"/ks.yaml", testutil.KustomizationYAML(name, "apps", "  {path: ./apps}\n"))
	}
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	artifact, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	s := store.New()
	s.SetArtifact(manifest.BootstrapSourceID, artifact)
	l := New(s)
	n, err := loadForTest(t.Context(), t, l, filepath.Join(root, "first"))
	assert.NoError(t, err)
	assert.Equal(t, n, 1)
	view := l.documents.views[root]
	observed, err := fs.Stat(view, "first")
	assert.NoError(t, err)
	{
		assert.FatalIf(t, view == artifact.FS, "%s", "loader did not retain a private operation view")
	}
	_, err = fs.ReadDir(view, "second")
	assert.NoError(t, err)
	_, err = fs.Stat(view, "second/late.yaml")
	{
		assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "uncreated file error = %v, want not exist", err)
	}
	testutil.WriteFile(
		t,
		root,
		"second/late.yaml",
		testutil.KustomizationYAML("late", "apps", "  {path: ./apps}\n"),
	)
	n, err = loadForTest(t.Context(), t, l, filepath.Join(root, "second"))
	assert.NoError(t, err)
	{
		assert.FatalIf(
			t,
			n != 1,
			"second graph loaded %d objects, want one from retained directory observations",
			n,
		)
	}
	{
		assert.FatalIf(t, l.documents.views[root] != view, "%s", "unchanged artifact replaced the private view")
	}
	retained, err := fs.Stat(l.documents.views[root], "first")
	assert.NoError(t, err)
	{
		assert.FatalIf(t, retained != observed, "%s", "load discarded retained directory observations")
	}
	for _, name := range []string{"first", "second"} {
		id := testutil.KustomizationID("apps", name)
		record, indexed := l.Index.Lookup(id)
		{
			assert.FatalIf(
				t,
				!indexed || record.Path != filepath.Join(root, name, "ks.yaml") || s.GetObject(id) == nil,
				"%s graph was not loaded and indexed: %+v",
				name,
				record,
			)
		}
	}
	{
		_, statErr := fs.Stat(l.documents.views[root], "second/late.yaml")
		assert.FatalIf(t, !errors.Is(statErr, fs.ErrNotExist), "load discarded the retained miss: %v", statErr)
	}
	other := New(s)
	_, err = other.Load(t.Context(), filepath.Join(root, "second"))
	assert.NoError(t, err)
	{
		assert.FatalIf(
			t,
			other.documents.views[root] == view || other.documents.views[root] == artifact.FS,
			"%s",
			"loaders shared their operation views",
		)
	}
	{
		_, err := fs.Stat(other.documents.views[root], "second/late.yaml")
		assert.FatalIf(t, err != nil, "second loader inherited a private miss: %v", err)
	}
	{
		assert.FatalIf(
			t,
			s.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != artifact,
			"%s",
			"loader replaced the source artifact",
		)
	}
	{
		_, err := fs.Stat(artifact.FS, "second/late.yaml")
		assert.FatalIf(t, err != nil, "loader mutated the artifact's observations: %v", err)
	}
}

func testLoaderIndependentRootsAdmittedMembership(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "first.yaml", testutil.KustomizationYAML("first", "apps", "  {path: ./apps}\n"))
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	admitted, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	input, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Source, Current: admitted.FS},
	)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "second.yaml", testutil.KustomizationYAML("second", "apps", "  {path: ./apps}\n"))
	s := store.New()
	s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(s)
	n, err := loadForTest(t.Context(), t, l, root)
	assert.NoError(t, err)
	assert.Equal(t, n, 1)
	objects := s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
	{
		assert.FatalIf(
			t,
			len(objects) != 1 || objects[0].Name != "first",
			"admitted objects = %v, want first only",
			objects,
		)
	}
}

func testLoaderIndependentRootsCase2(t *testing.T, testCaseInput *struct {
	name    string
	bounded bool
	nested  bool
},
) {
	t.Helper()

	roots := []string{t.TempDir(), t.TempDir()}
	if testCaseInput.nested {
		roots[0] = filepath.Join(roots[1], "child")
	}
	ids := []manifest.NamedResource{
		testutil.KustomizationID("apps", "first"),
		testutil.KustomizationID("apps", "second"),
	}
	for i, root := range roots {
		testutil.WriteFile(
			t,
			root,
			"ks.yaml",
			testutil.KustomizationYAML(ids[i].Name, ids[i].Namespace, "  {path: ./apps}\n"),
		)
	}
	l := New(store.New())
	if testCaseInput.bounded {
		l.SourceRoot = roots[0]
	}
	{
		_, err := loadForTest(t.Context(), t, l, roots[0])
		assert.NoError(t, err)
	}
	{
		assert.FatalIf(t, l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != nil ||
			l.Store.GetObject(manifest.BootstrapSourceID) != nil, "%s", "standalone load published bootstrap identity")
	}
	testLoaderIndependentRootsCase2Completion(
		t,
		&testLoaderIndependentRootsCase2CompletionState{testCaseInput: &testCaseInput, roots: &roots, ids: &ids, l: &l},
	)
}

func testLoaderWarningConditionsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
	bad := "apiVersion: v1\nkind: ConfigMap\nbroken: [ unclosed\n"
	switch *testCaseInput {
	case "sibling":
		testutil.WriteFile(t, root, "bad.yaml", bad)
	case "component":
		testutil.WriteFile(
			t,
			root,
			"kustomization.yaml",
			"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [bad.yaml]\n",
		)
		testutil.WriteFile(t, root, "bad.yaml", bad)
	}
	st := store.New()
	l := New(st)
	l.documents = readerForTest(root)
	if (*testCaseInput) == "kustomization" {
		l.documents.read(root)
		l.documents.views[root] = unreadableInputFS{FS: os.DirFS(root), denied: "kustomization.yaml"}
	}
	if (*testCaseInput) == "readdir" {
		l.documents.views[root] = unreadableListingFS{FS: os.DirFS(root)}
	}
	w := traversal{
		documents:   l.documents,
		policy:      traversalAdmission,
		loader:      l,
		directories: map[string]struct{}{},
	}
	w.walk(t.Context(), graphVisit{root: root, file: root}, nil)
	assert.NoError(t, w.err)
	warnings := st.Warnings()
	if (*testCaseInput) == "sibling" || (*testCaseInput) == "component" ||
		(*testCaseInput) == "kustomization" {
		{
			assert.FatalIf(t, len(warnings) != 0, "census failure produced warnings: %v", warnings)
		}
		file := "bad.yaml"
		if (*testCaseInput) == "kustomization" {
			file = "kustomization.yaml"
		}
		{
			doc := l.documents.cache[filepath.Join(root, file)]
			assert.FatalIf(t, len(doc.objects) != 1 ||
				doc.objects[0].outcome.Err == nil, "%s", "census failure was not retained")
		}
		return
	}
	{
		assert.FatalIf(
			t,
			len(warnings) != 1 || warnings[0].Category != manifest.WarnInputReadFailed || warnings[0].Count != 1 ||
				warnings[0].Resource != (manifest.NamedResource{}) ||
				!strings.HasPrefix(warnings[0].Detail[0], root) ||
				len(warnings[0].Detail) != 2,
			"%s warning = %+v", (*testCaseInput), warnings,
		)
	}
}

func testLoaderWarningConditionsFailedNestedResourcePreservesParentCount(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [nested]\n")
	testutil.WriteFile(t, root, "nested/kustomization.yaml", "resources: [first.yaml, blocked.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"nested/first.yaml",
		testutil.KustomizationYAML("first", "apps", "  path: ./workload\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"nested/blocked.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: blocked}\n",
	)
	l := New(store.New())
	l.SourceRoot = root
	l.documents = readerForTest(root)
	l.documents.views[root] = unreadableInputFS{FS: os.DirFS(root), denied: "nested/blocked.yaml"}
	count, err := l.Load(t.Context(), root)
	{
		assert.FatalIf(
			t,
			count != 1 || err != nil,
			"Load = (%d, %v), want preserved predecessor and completed census",
			count,
			err,
		)
	}
	failed := l.documents.cache[filepath.Join(root, "nested/blocked.yaml")].objects
	{
		assert.FatalIf(
			t,
			len(failed) != 1 || !errors.Is(failed[0].outcome.Err, fs.ErrPermission),
			"nested failure not retained: %+v",
			failed,
		)
	}
	{
		_, found := l.Index.Lookup(testutil.KustomizationID("apps", "first"))
		assert.FatalIf(t, !found, "%s", "failed successor removed previously read evidence")
	}
	{
		_, scanned := l.scopes.scanned[root]
		assert.FatalIf(t, !scanned, "%s", "completed census was not marked scanned")
	}
}

type testLoaderIndependentRootsCase2CompletionState struct {
	testCaseInput **struct {
		name    string
		bounded bool
		nested  bool
	}
	roots *[]string
	ids   *[]manifest.NamedResource
	l     **Loader
}

func testLoaderIndependentRootsCase2Completion(t *testing.T, scope *testLoaderIndependentRootsCase2CompletionState) {
	t.Helper()
	view := (*scope.l).documents.views[(*scope.roots)[0]]
	observed, err := fs.Stat(view, ".")
	assert.NoError(t, err)
	for _, root := range []string{(*scope.roots)[1], (*scope.roots)[0]} {
		_, err := loadForTest(t.Context(), t, (*scope.l), root)
		if (*scope.testCaseInput).bounded && root == (*scope.roots)[1] {
			{
				assert.FatalIf(
					t,
					!errors.Is(err, manifest.ErrInput),
					"outside SourceRoot error = %v, want unavailable source",
					err,
				)
			}
			{
				assert.FatalIf(
					t,
					(*scope.l).documents.views[(*scope.roots)[0]] != view,
					"%s",
					"outside SourceRoot load replaced the mounted view",
				)
			}
			{
				_, indexed := (*scope.l).Index.Lookup(
					(*scope.ids)[1],
				)
				assert.FatalIf(
					t,
					indexed || (*scope.l).Store.GetObject((*scope.ids)[1]) != nil || len((*scope.l).Index.Records) != 1 ||
						len(
							retainedErrorsForTest((*scope.l)),
						) != 0,
					"%s",
					"unrelated root supplied objects or evidence outside SourceRoot",
				)
			}
		} else {
			assert.NoError(t, err)
		}
		retained, err := fs.Stat((*scope.l).documents.views[(*scope.roots)[0]], ".")
		assert.NoError(t, err)
		{
			assert.FatalIf(t, retained != observed, "%s", "standalone binding discarded operation observations")
		}
		{
			assert.FatalIf(t, (*scope.l).Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != nil ||
				(*scope.l).Store.GetObject(
					manifest.BootstrapSourceID,
				) != nil, "%s", "independent load published bootstrap identity")
		}
		if (*scope.testCaseInput).nested {
			input, file := new((*scope.l).documents).input(filepath.Join((*scope.roots)[0], "ks.yaml"))
			{
				assert.FatalIf(
					t,
					input != view || file != "ks.yaml",
					"child source binding = (%v, %q), want original view and ks.yaml",
					input,
					file,
				)
			}
		}
		for i, id := range *scope.ids {
			if (*scope.testCaseInput).bounded && i == 1 {
				continue
			}
			r, indexed := (*scope.l).Index.Lookup(id)
			path := filepath.Join((*scope.roots)[i], "ks.yaml")
			file := filepath.ToSlash(path)
			if (*scope.testCaseInput).bounded {
				file = "ks.yaml"
			}
			{
				assert.FatalIf(
					t,
					!indexed || r.Path != path || r.File != file || (*scope.l).Store.GetObject(id) == nil,
					"%s record = %+v, present = %t; want path %s, file %s and stored object",
					id,
					r,
					indexed,
					path,
					file,
				)
			}
		}
	}
}
