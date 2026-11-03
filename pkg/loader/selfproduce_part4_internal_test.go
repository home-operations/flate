package loader

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_TransformedRunnableIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"children/kustomization.yaml",
		"resources: [child.yaml, release.yaml]\nnamePrefix: pre-\nnameSuffix: -post\n",
	)
	testutil.WriteFile(t, root, "children/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./app\n"))
	testutil.WriteFile(
		t,
		root,
		"children/release.yaml",
		"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: release, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: chart}}\n",
	)
	st := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./children"}
	st.AddObject(owner)
	index := rendererFixture(t, loaderFixture(st, nil), root)
	for _, authored := range []manifest.NamedResource{testutil.KustomizationID("apps", "child"), testutil.HelmReleaseID("apps", "release")} {
		id := authored
		id.Name = "pre-" + authored.Name + "-post"
		record, known := index.Lookup(id)
		assert.Equal(t, known, true)
		assert.Equal(t, record.Authored, authored)
		assert.Equal(t, record.Path, "")
		assert.Equal(t, index.IsFileIndexed(id), false)
		productions := st.Productions(id, index)
		{
			assert.FatalIf(t, len(productions) != 1, "productions for %s: %v", id, productions)
		}
		assert.Equal(t, productions[0].Owner, owner.Named())
		assert.Equal(t, productions[0].Authored, authored)
		assert.Equal(t, productions[0].Materializer(id), owner.Named())
		assert.Equal(t, st.GetObject(id), nil)
	}
}
