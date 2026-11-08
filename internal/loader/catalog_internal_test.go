package loader

import (
	"io/fs"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCatalog_FreezeAndClose(t *testing.T) {
	t.Parallel()
	for _, restricted := range []bool{false, true} {
		t.Run(strconv.FormatBool(restricted), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			var denied atomic.Bool
			var calls atomic.Int64
			files := catalogReadFS{FS: fstest.MapFS{
				"declarations/child.yaml": {
					Data: []byte(
						testutil.KustomizationYAML(
							"child",
							"apps",
							"  path: ./payload\n  sourceRef: {kind: GitRepository, name: local, namespace: sources}\n",
						),
					),
				},
				"payload/kustomization.yaml": {Data: []byte("resources: []\n")},
				"generator/kustomization.yaml": {
					Data: []byte("configMapGenerator:\n- name: opaque\n  files: [literal.yaml]\n"),
				},
				"generator/literal.yaml": {
					Data: []byte(testutil.KustomizationYAML("literal", "apps", "  path: ./hidden\n")),
				},
			}, denied: &denied, calls: &calls}
			input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: files}
			st := store.New()
			source := &manifest.GitRepository{Name: "local", Namespace: "sources"}
			external := &manifest.GitRepository{Name: "remote", Namespace: "sources"}
			st.AddObject(
				&manifest.GitRepository{
					Name:      manifest.BootstrapSourceID.Name,
					Namespace: manifest.BootstrapSourceID.Namespace,
				},
			)
			st.AddObject(source)
			st.AddObject(external)
			st.SetArtifact(manifest.BootstrapSourceID, input)
			st.SetArtifact(source.Named(), input)
			l := New(st)
			l.SourceRoot = root
			child := testutil.KustomizationID("apps", "child")
			request := selection.Request{}
			if restricted {
				request.Roots = []manifest.NamedResource{child}
			}
			var err error
			l.Options.Session, err = selection.New(st, l, request)
			assert.NoError(t, err)
			assert.Equal(t, l.Options.Session != nil, restricted)
			_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			st.SetDiscoveryIndex(l.Index)
			object := l.CatalogObject(child)
			assert.FatalIf(t, object == nil, "missing retained child declaration")
			literal := testutil.KustomizationID("apps", "literal")
			assert.Equal(t, l.CatalogObject(literal), manifest.BaseManifest(nil))
			factory := &manifest.ResourceSet{Name: "factory", Namespace: "apps"}
			owner := &manifest.Kustomization{
				Name:            "owner",
				Namespace:       "apps",
				Path:            "./declarations",
				TargetNamespace: "apps",
			}
			owner.SourceRef.Kind, owner.SourceRef.Name, owner.SourceRef.Namespace = source.Named().Kind, source.Name, source.Namespace
			st.AddObject(factory)
			unrelated := owner.Clone()
			unrelated.Path = "./elsewhere"
			st.RecordEmitted(
				factory.Named(),
				&store.Emission{Children: []store.Child{{Obj: unrelated}}, Complete: true},
				nil,
			)
			catalog := l.DetachCatalog()
			assert.Equal(t, l.DetachCatalog(), catalog)
			var before store.ReadSet
			before.Init(st, manifest.NamedResource{}, nil)
			defer before.Retire()
			assert.Equal(t, selection.Root(catalog.RootEvidence(child, &before)).Independent, true)
			st.RecordEmitted(
				factory.Named(),
				&store.Emission{Children: []store.Child{{Obj: owner}}, Complete: true},
				nil,
			)
			assert.Equal(t, before.Valid(), false)
			l.Close()
			denied.Store(true)
			count := calls.Load()
			assert.Equal(t, catalog.CatalogObject(child), object)
			assert.Equal(t, catalog.CatalogObject(literal), manifest.BaseManifest(nil))
			var covered store.ReadSet
			covered.Init(st, manifest.NamedResource{}, nil)
			defer covered.Retire()
			evidence := catalog.RootEvidence(child, &covered)
			assert.Equal(t, evidence.Record.Path, filepath.Join(root, "declarations", "child.yaml"))
			assert.Equal(t, evidence.Scope.Initial, true)
			assert.Equal(t, selection.Root(evidence).Admitted, false)
			assert.Equal(t, selection.Root(evidence).Independent, false)
			assert.Equal(t, catalog.SourceExternal(source.Named(), &covered), false)
			assert.Equal(t, catalog.SourceExternal(external.Named(), &covered), true)
			assert.Equal(t, covered.Valid(), true)
			st.RecordEmitted(factory.Named(), &store.Emission{Complete: true}, nil)
			assert.Equal(t, covered.Valid(), false)
			var restored store.ReadSet
			restored.Init(st, manifest.NamedResource{}, nil)
			defer restored.Retire()
			result := selection.Root(catalog.RootEvidence(child, &restored))
			assert.Equal(t, result.Admitted, true)
			assert.Equal(t, result.Independent, true)
			assert.Equal(t, result.Entry, true)
			assert.Equal(t, restored.Valid(), true)
			assert.Equal(t, calls.Load(), count)
			for _, doc := range catalog.files {
				assert.Equal(t, len(doc.objects), 0)
				assert.Equal(t, doc.body == nil, true)
				assert.Equal(t, doc.builtInput == nil, true)
			}
			catalog.Close()
			assert.Equal(t, reflect.ValueOf(*catalog).IsZero(), true)
			catalog.Close()
		})
	}
}

type catalogReadFS struct {
	fs.FS

	denied *atomic.Bool
	calls  *atomic.Int64
}

func (f catalogReadFS) Open(name string) (fs.File, error) {
	f.calls.Add(1)
	if f.denied.Load() {
		return nil, fs.ErrPermission
	}
	return f.FS.Open(name)
}
