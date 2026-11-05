package loader

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestProductionInput_PinnedAncestor(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	pinned := testutil.KustomizationID("apps", "pinned")
	nested := testutil.KustomizationID("apps", "nested")
	l := New(store.New())
	l.SourceRoot = root
	l.Index.Claims = map[string][]manifest.NamedResource{
		"apps/":        {pinned},
		"apps/nested/": {nested},
	}
	coordinate := "apps/nested/child.yaml"
	ownership := l.Index.OwnersOfFile(coordinate)
	assert.Equal(t, slices.Contains(ownership.Owners, nested), true)
	assert.Equal(t, slices.Contains(ownership.Ancestors, pinned), true)
	for _, tc := range []struct {
		name  string
		owner manifest.NamedResource
		want  bool
	}{
		{name: "working tree"},
		{name: "pinned source", owner: pinned, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			visit := graphVisit{file: filepath.Join(root, filepath.FromSlash(coordinate)), owner: tc.owner}
			assert.Equal(
				t,
				l.recordProductionInput(&visit, root, map[manifest.NamedResource]bool{pinned: true}, nil),
				tc.want,
			)
		})
	}
}

func TestSources_BuiltEffectiveDeclaration(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [repo.yaml]\n")
	testutil.WriteFile(t, root, "parent/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: repo, namespace: apps}
spec: {url: https://example.invalid/raw}
`)
	st := store.New()
	admitForTest(st, root)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./parent"}
	st.AddObject(parent)
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection.Changed = true
	packageRoot := filepath.Join(root, "parent")
	_, err := l.Load(t.Context(), packageRoot, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	var raw manifest.BaseManifest
	for child, sourceErr := range l.Sources(t.Context()) {
		assert.NoError(t, sourceErr)
		raw = child.Obj
	}
	assert.FatalIf(t, raw == nil, "fixture did not retain source")
	children := builtObjectsForTest(t, `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: repo, namespace: apps}
spec: {url: https://example.invalid/effective}
`)
	support := new(store.ReadSet)
	support.Init(st, parent.Named(), nil)
	support.GetDeclaration(parent.Named())
	support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	l.RegisterBuilt(
		BuiltInput{Root: packageRoot, Owner: parent.Named(), Source: manifest.BootstrapSourceID, Support: support},
		children,
	)
	defer l.Retire()
	id := children[0].Obj.Named()
	assert.Equal(t, l.IndexedObject(id, l.Index.Records[id]).Obj, raw)
	var sources []store.Child
	for child, sourceErr := range l.Sources(t.Context()) {
		assert.NoError(t, sourceErr)
		sources = append(sources, child)
	}
	assert.Equal(t, len(sources), 1)
	assert.Equal(t, sources[0].Obj, children[0].Obj)
}

func TestRootAdmission_ReaderOwnership(t *testing.T) {
	t.Parallel()
	owner := testutil.KustomizationID("apps", "owner")
	id := testutil.KustomizationID("apps", "child")
	for _, tc := range []struct {
		name     string
		reader   manifest.FileReader
		admitted bool
	}{
		{name: "render owner", reader: manifest.FileReader{Reader: owner}},
		{name: "materializer", reader: manifest.FileReader{Reader: owner, Materialize: true}, admitted: true},
		{name: "exact source", reader: manifest.FileReader{Reader: owner, Exact: true}, admitted: true},
		{name: "declaration", reader: manifest.FileReader{Reader: owner, Declaration: id}, admitted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.Index.Readers = map[string][]manifest.FileReader{"child.yaml": {tc.reader}}
			record := manifest.DiscoveryRecord{Path: "child.yaml", File: "child.yaml", Authored: id}
			scope := l.rootAdmission(record.Path, record.File, true)
			assert.Equal(t, scope.admits(l, id, &record), tc.admitted)
		})
	}
}
