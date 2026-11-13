package discovery

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestEntryQualification_SharedRouteWithdrawal(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	first := &manifest.ResourceSet{Name: "first", Namespace: "apps"}
	later := &manifest.ResourceSet{Name: "later", Namespace: "apps"}
	shared := &manifest.ResourceSet{Name: "shared", Namespace: "apps"}
	st.AddObject(first)
	st.AddObject(later)
	for _, parent := range []*manifest.ResourceSet{first, later} {
		st.RecordEmitted(parent.Named(), &store.Emission{
			Children: []store.Child{{Obj: shared}}, Complete: true,
		}, nil)
	}
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	qualifyForTest(t, l, first.Named(), later.Named())
	t.Cleanup(l.Retire)
	a := &Admission{input: input, path: filepath.Join(root, "entry"), loader: l, entryDemand: true}
	selected, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, selected, (*manifest.Kustomization)(nil))
	assert.Equal(t, binding, Binding{})
	assert.Diff(t, a.EntryProducers(), []manifest.NamedResource{shared.Named()})
	applier := &manifest.Kustomization{Name: "applier", Namespace: "apps", Path: "./entry"}
	publish := func() {
		var reads store.ReadSet
		reads.Init(st, shared.Named(), nil)
		reads.GetDeclaration(shared.Named())
		output := &store.Emission{Children: []store.Child{{Obj: applier}}, Complete: true}
		reads.Bind(output)
		st.RecordEmitted(shared.Named(), output, nil)
		reads.Accept(store.AssessmentReconciled)
		st.Retire(&reads)
	}
	publish()
	selected, binding, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, selected == nil, "accepted shared output did not propose an entry")
	assert.Equal(t, selected.Named(), applier.Named())
	assert.Equal(t, st.EmissionOwner(shared.Named()), later.Named())
	withdraw := make(chan struct{})
	withdrawn := make(chan struct{})
	go func() {
		defer close(withdrawn)
		<-withdraw
		st.RecordEmitted(first.Named(), &store.Emission{Complete: true}, nil)
	}()
	close(withdraw)
	<-withdrawn
	assert.Equal(t, binding.Reads.Valid(), false)
	assert.Equal(t, acceptEntryForTest(t, a, &binding), Binding{})
	assert.Equal(t, st.EmissionOwner(shared.Named()), later.Named())
	publish()
	selected, binding, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, selected == nil, "remaining shared route lost its accepted output")
	assert.Equal(t, selected.Named(), testutil.KustomizationID("apps", "applier"))
	assert.Equal(t, acceptEntryForTest(t, a, &binding).Producer, applier.Named())
	assert.Equal(t, len(a.EntryProducers()), 0)
	assert.Equal(t, len(st.Warnings()), 0)
}
