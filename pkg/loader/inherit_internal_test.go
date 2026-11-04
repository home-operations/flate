package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	before := l.Kustomizations()
	assert.Equal(t, len(before), 4)
	for _, child := range before {
		{
			assert.FatalIf(
				t,
				child.Doc == nil || child.Doc["spec"].(map[string]any)["path"] != "./app",
				"admitted peer lost its document: %v",
				child.Doc,
			)
		}
	}
	allocs := testing.AllocsPerRun(10, func() { l.Admit(root) })
	t.Logf("unchanged admission: %.0f allocations", allocs)
	assert.FatalIf(t, allocs > 3, "unchanged admission repeated scope work: %.0f allocations", allocs)
	{
		assert.FatalIf(
			t,
			!slices.EqualFunc(l.Kustomizations(), before, func(a, b store.Child) bool { return a.Obj == b.Obj }),
			"%v",
			"unchanged admission replaced peer identities",
		)
	}
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
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	id := testutil.KustomizationID("apps", "one")
	file := filepath.Join(root, "peers.yaml")
	allocs := testing.AllocsPerRun(10, func() {
		if !l.AdmitsRoot(l.IndexedObject(id, l.Index.Records[id]).Obj, file) {
			t.Fatal("plain discovery candidate was rejected")
		}
	})
	assert.FatalIf(t, allocs != 0, "plain discovery admission allocated: %.0f allocations", allocs)
}
