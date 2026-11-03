package discovery

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestAdvance_ReferencedProducerRequiresReconciliation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/parent.yaml",
		testutil.KustomizationYAML(
			"parent",
			"apps",
			"  path: ./parent\n"+
				"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"+
				"  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
		),
	)
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"parent/child.yaml",
		testutil.KustomizationYAML(
			"child",
			"apps",
			"  path: ./child\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: []\n")
	st := store.New()
	l := loader.New(st)
	l.Options.Filter = change.NewFilter()
	session, err := Begin(t.Context(), Config{
		Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st, Loader: l,
		Trees:     kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, task.NewBounded(2)),
		Selection: manifest.Selection{Changed: true},
	})
	assert.NoError(t, err)
	child := testutil.KustomizationID("apps", "child")
	parent := testutil.KustomizationID("apps", "parent")
	record, _ := l.Index.Lookup(parent)
	declaration := l.IndexedObject(parent, record)
	if declaration.Obj == nil {
		t.Fatal("parent declaration missing from the index")
	}
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "flux", manifest.NamedResource{})
	st.RecordEmitted(entry.Named(), &store.Emission{Children: []store.Child{declaration}, Complete: true}, nil)
	st.AddObject(declaration.Obj, declaration.Doc)
	assert.Equal(t, st.GetDeclaration(parent) != nil, true)
	assert.Equal(t, len(st.Productions(child, l.Index)) != 0, true)
	_, err = session.Advance(t.Context(), &loader.Admission{Needed: []manifest.NamedResource{child}})
	assert.NoError(t, err)
	for owner := range session.prepared {
		if owner.Named() == parent {
			t.Fatal("producer with unresolved substitutions supplied prepared declarations")
		}
	}
}
