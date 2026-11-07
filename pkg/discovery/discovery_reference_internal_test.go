package discovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
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
	configureSelection(t, l, selection.Request{Changed: true})
	session, err := Begin(t.Context(), Config{
		Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st, Loader: l,
		Trees:     kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, task.NewBounded(2)),
		Selection: selection.Request{Changed: true},
	})
	assert.NoError(t, err)
	child := testutil.KustomizationID("apps", "child")
	parent := testutil.KustomizationID("apps", "parent")
	record, _ := l.Index.Lookup(parent)
	declaration := l.IndexedObject(parent, record)
	assert.FatalIf(t, declaration.Obj == nil, "%v", "parent declaration missing from the index")
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "flux", manifest.NamedResource{})
	st.RecordEmitted(entry.Named(), &store.Emission{Children: []store.Child{declaration}, Complete: true}, nil)
	st.AddObject(declaration.Obj, declaration.Doc)
	assert.Equal(t, st.GetDeclaration(parent) != nil, true)
	assert.Equal(t, len(st.Productions(child, l.Index)) != 0, true)
	_, err = session.Advance(t.Context(), &selection.Admission{Needed: []manifest.NamedResource{child}})
	assert.NoError(t, err)
	for owner := range session.prepared {
		assert.FatalIf(
			t,
			owner == parent,
			"%v",
			"producer with unresolved substitutions supplied prepared declarations",
		)
	}
}

func TestAdvance_ProducerDemandRejectsChangedAuthority(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"before preparation", "during preparation"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "outside/kustomization.yaml", "resources: []\n")
			st := store.New()
			owner := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "outside"}
			replacement := owner.Clone()
			replacement.Path = "replaced"
			observed := &rebindingEntryFS{FS: os.DirFS(root)}
			input := &store.SourceArtifact{LocalPath: root, FS: observed}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			st.AddObject(owner)
			var guard store.ReadSet
			guard.Init(st, manifest.NamedResource{}, nil)
			defer st.Retire(&guard)
			guard.GetDeclaration(owner.Named())
			if phase == "before preparation" {
				st.AddObject(replacement)
			} else {
				observed.rebind = func() {
					if observed.changed.CompareAndSwap(false, true) {
						st.AddObject(replacement)
					}
				}
			}
			d := &Session{loader: loader.New(st), cfg: Config{
				Store: st, Input: input, Path: root, RepoRoot: root,
				Trees: kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, task.NewBounded(2)),
			}}
			defer d.Close()
			prepared, err := d.prepareDemandedProducers(t.Context(), []selection.Demand{{
				Identity: owner.Named(), Witness: owner, Reads: &guard,
			}}, root)
			assert.NoError(t, err)
			assert.Equal(t, prepared, false)
			if phase == "before preparation" {
				assert.Equal(t, d.prepared[owner.Named()] == nil, true)
			} else {
				assert.Equal(t, observed.changed.Load(), true)
				assert.Equal(t, d.prepared[owner.Named()].registered, false)
			}
		})
	}
}
