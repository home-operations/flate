package loader

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
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
			assert.FatalIf(t, err != nil || count != 0, "Load = (%d, %v), want retained parse failure", count, err)
			failed := l.documents.cache[filepath.Join(root, "component", "bad.yaml")].objects
			assert.FatalIf(
				t,
				len(failed) != 1 || failed[0].outcome.Err == nil || failed[0].fact != nil,
				"component parse failure not retained: %+v",
				failed,
			)
			assert.FatalIf(
				t,
				len(st.ListObjects("", manifest.KindConfigMap)) != 0 || len(st.Warnings()) != 0,
				"%s",
				"failed component document escaped census",
			)
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
		assert.FatalIf(t, len(warnings) != 0, "census failure produced warnings: %v", warnings)
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
	assert.FatalIf(
		t,
		len(warnings) != 1 || warnings[0].Category != manifest.WarnInputReadFailed || warnings[0].Count != 1 ||
			warnings[0].Resource != (manifest.NamedResource{}) ||
			!strings.HasPrefix(warnings[0].Detail[0], root) ||
			len(warnings[0].Detail) != 2,
		"%s warning = %+v", (*testCaseInput), warnings,
	)
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
	assert.FatalIf(
		t,
		count != 1 || err != nil,
		"Load = (%d, %v), want preserved predecessor and completed census",
		count,
		err,
	)
	failed := l.documents.cache[filepath.Join(root, "nested", "blocked.yaml")].objects
	assert.FatalIf(
		t,
		len(failed) != 1 || !errors.Is(failed[0].outcome.Err, fs.ErrPermission),
		"nested failure not retained: %+v",
		failed,
	)
	{
		_, found := l.Index.Lookup(testutil.KustomizationID("apps", "first"))
		assert.FatalIf(t, !found, "%s", "failed successor removed previously read evidence")
	}
	{
		_, scanned := l.scopes.scanned[root]
		assert.FatalIf(t, !scanned, "%s", "completed census was not marked scanned")
	}
}
