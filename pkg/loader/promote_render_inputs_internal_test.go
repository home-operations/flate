package loader

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRegisterBuilt_RenderInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, before, after, file string
	}{
		{name: "prune", before: "prune: true", after: "prune: false", file: "child.yaml"},
		{name: "path", before: "path: ./app", after: "path: ./other", file: "kustomization.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [child.yaml]\n")
			child := testutil.KustomizationYAML("child", "apps", "  path: ./app\n  prune: true\n")
			testutil.WriteFile(t, root, "child.yaml", child)
			st := store.New()
			admitForTest(st, root)
			owner := &manifest.Kustomization{Name: "entry", Namespace: "apps", Path: "."}
			st.AddObject(owner)
			l := New(st)
			l.SourceRoot = root
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			docs := builtObjectsForTest(t, strings.Replace(child, tc.before, tc.after, 1))
			assert.NoError(t, l.RegisterBuilt(
				t.Context(),
				supportedBuiltInputForTest(
					l,
					&BuiltInput{Root: root, Owner: owner.Named(), Source: manifest.BootstrapSourceID},
				),
				docs,
			))
			record, ok := l.Index.Lookup(docs[0].Obj.Named())
			assert.Equal(t, ok, true)
			assert.Equal(t, record.File, tc.file)
			assert.Equal(t, record.Builder, "kustomization.yaml")
		})
	}
}
