package loader

import (
	"fmt"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_EmittedChildKeepsUnrelatedInventoryClosed(t *testing.T) {
	t.Parallel()
	seedKSStoreResult := seedKSStore(1000)
	st, files := seedKSStoreResult.store, seedKSStoreResult.files
	index := (&filterFixture{files: files, root: "/repo", objects: st}).index()
	filter := newAdmission(t, admissionConfig{
		changes: change.NewSet([]string{"apps/app-0/ks.yaml"}), index: index, inventory: st, root: "/repo",
	})
	parent := testutil.KustomizationID("flux-system", "app-0")
	assert.Equal(t, filter.ShouldReconcile(parent), true)
	child := store.Child{Obj: &manifest.ConfigMap{Name: "child", Namespace: "apps"}}
	filter.AddEmitted(parent, &child, st)
	assert.Equal(t, filter.ShouldReconcile(child.Obj.Named()), true)
	if len(filter.last.states) > 4 {
		t.Fatalf("one emitted child prepared %d objects; want at most 4", len(filter.last.states))
	}
}

func publishedAdmissionDeclarations(t *testing.T, count int) *Loader {
	t.Helper()
	root := t.TempDir()
	files := fstest.MapFS{}
	paths := make([]string, count)
	for i := range paths {
		name := fmt.Sprintf("item-%05d", i)
		paths[i] = name + ".yaml"
		files[paths[i]] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML(name, "apps", "  suspend: true\n"))}
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Changes = change.NewSet(paths)
	l.Options.Filter = change.NewFilter()
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	for _, published := range []bool{false, true} {
		reads := &admissionArtifactReads{admissionReader: st}
		c := admissionClosure{loader: l, objects: reads}
		c.proposal = &c.result
		proposal, err := c.evaluate(t.Context(), new(PrepareRequest{Invocation: true}))
		assert.NoError(t, err)
		assert.Equal(t, len(proposal.Selected), count)
		wantObjects := count
		if published {
			wantObjects = 0
		}
		assert.Equal(t, len(proposal.Objects), wantObjects)
		t.Logf(
			"%d declarations, published %v: %d artifact reads, %d inventories",
			count,
			published,
			reads.calls,
			reads.lists,
		)
		if reads.calls > 32*count+32 {
			t.Errorf(
				"%d changed declarations caused %d artifact reads; want at most %d",
				count,
				reads.calls,
				32*count+32,
			)
		}
		assert.Equal(t, reads.lists, 1)
		for i := range proposal.Objects {
			child := &proposal.Objects[i]
			st.AddObject(child.Obj, child.Doc)
		}
	}
	return l
}
