package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestAdmit_UnchangedInputs(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [peers.yaml]\n")
	var docs strings.Builder
	for i := range 4 {
		if i != 0 {
			docs.WriteString("---\n")
		}
		docs.WriteString(testutil.KustomizationYAML(fmt.Sprintf("peer-%d", i), "apps", "  path: ./app\n"))
	}
	testutil.WriteFile(t, root, "app/peers.yaml", docs.String())
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	before := l.Kustomizations()
	assert.Equal(t, len(before), 4)
	for _, child := range before {
		assert.FatalIf(
			t,
			child.Doc == nil || child.Doc["spec"].(map[string]any)["path"] != "./app",
			"admitted peer lost its document: %v",
			child.Doc,
		)
	}
	allocs := testing.AllocsPerRun(10, func() { l.Admit(root) })
	t.Logf("unchanged admission: %.0f allocations", allocs)
	assert.FatalIf(t, allocs > 3, "unchanged admission repeated scope work: %.0f allocations", allocs)
	assert.FatalIf(
		t,
		!slices.EqualFunc(l.Kustomizations(), before, func(a, b store.Child) bool { return a.Obj == b.Obj }),
		"%v",
		"unchanged admission replaced peer identities",
	)
	assert.Equal(t, len(l.Index.Claims), 1)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestAdmitsRoot_PlainDiscoveryShortcut(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"peers.yaml",
		testutil.KustomizationYAML(
			"one",
			"apps",
			"  path: .\n",
		)+"---\n"+testutil.KustomizationYAML(
			"two",
			"apps",
			"  path: .\n",
		),
	)
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	id := testutil.KustomizationID("apps", "one")
	file := filepath.Join(root, "peers.yaml")
	allocs := testing.AllocsPerRun(10, func() {
		if !selection.Root(rootAtForTest(l, l.IndexedObject(id, l.Index.Records[id]).Obj, file)).Admitted {
			t.Fatal("plain discovery candidate was rejected")
		}
	})
	assert.FatalIf(t, allocs != 0, "plain discovery admission allocated: %.0f allocations", allocs)
}

func TestAdmit_SelfApplyingTransformedPackage(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/system/kustomization.yaml", `resources: [sync.yaml, cm.yaml]
patches:
  - target: {kind: ConfigMap}
    patch: |
      - op: replace
        path: /data/value
        value: after
`)
	testutil.WriteFile(t, root, "flux/system/sync.yaml", testutil.KustomizationYAML(
		"sync", "flux-system", "  path: ./flux\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
	))
	testutil.WriteFile(t, root, "flux/system/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: sample}
data: {value: before}
`)
	testutil.WriteFile(t, root, "flux/apps/ks.yaml", testutil.KustomizationYAML(
		"apps", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
	))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\n")
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{})
	_, err := l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	assert.Equal(t, l.scopes.complete, true)
	sync := testutil.KustomizationID("flux-system", "sync")
	prediction, present := l.catalogOccurrence(sync)
	assert.Equal(t, present, true)
	assert.Equal(t, prediction.requiresBuild, true)
	assert.Equal(t, st.GetDeclaration(sync) == nil, true)
	assert.Equal(t, st.GetDeclaration(testutil.KustomizationID("flux-system", "apps")) != nil, true)
	l.Admit(root)
	assert.Equal(t, l.scopes.complete, true)
	assert.Equal(t, st.GetDeclaration(sync) == nil, true)

	testutil.WriteFile(t, root, "flux/system/kustomization.yaml", "resources: [sync.yaml, cm.yaml]\n")
	input, _ := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	replacement := *input
	replacement.Content = "untransformed"
	st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	_, err = l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	prediction, present = l.catalogOccurrence(sync)
	assert.Equal(t, present, true)
	assert.Equal(t, prediction.requiresBuild, false)
	assert.Equal(t, st.GetDeclaration(sync) != nil, true)
}

func TestAdmit_ContextualNamespaceReusesOwners(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "namespace: apps\nresources: [peers.yaml]\n")
	testutil.WriteFile(t, root, "app/peers.yaml",
		testutil.KustomizationYAML("one", "", "  path: ./app\n")+"---\n"+
			testutil.KustomizationYAML("two", "", "  path: ./app\n"))
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	before := l.kustomizations()
	assert.Equal(t, len(before), 2)
	generation := l.construction
	l.Admit(root)
	assert.Equal(t, l.construction, generation)
	after := l.kustomizations()
	assert.Equal(t, slices.EqualFunc(before, after, func(a, b store.Child) bool { return a.Obj == b.Obj }), true)
	for _, child := range after {
		assert.Equal(t, child.Obj.Named().Namespace, "apps")
		assert.Equal(t, st.GetDeclaration(child.Obj.Named()) == nil, true)
	}
}
