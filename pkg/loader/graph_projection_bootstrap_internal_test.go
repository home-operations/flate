package loader

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/store"
)

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
