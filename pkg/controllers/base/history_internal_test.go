package base

import (
	"iter"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/selection"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

//nolint:gocognit // Both publication orders MUST share the same complete history assertions.
func TestKeepEmitted_MatchesHistoryBeforePublication(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"mutable", "pinned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			source := &manifest.GitRepository{Name: "content", Namespace: "apps"}
			owner := testutil.KustomizationID("apps", "emitter")
			child := &manifest.Kustomization{
				Name:      "child",
				Namespace: "apps",
				Path:      "apps/red",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name},
			}
			var selections [2][]manifest.NamedResource
			for side := range 2 {
				st := store.New()
				st.AddObject(source)
				art := &store.SourceArtifact{
					FS:        fstest.MapFS{"apps/red/file": {Data: []byte("changed")}},
					LocalPath: root,
				}
				if mode == "pinned" {
					art.LocalPath, art.LocalRoot, art.Key, art.Content = "/selected", root, "commit", "generation"
				}
				st.SetArtifact(source.Named(), art)
				l, _ := admissiontest.New(
					t,
					admissiontest.Config{
						Changes: change.NewSet([]string{"apps/red/file"}),
						Index: &manifest.DiscoveryIndex{
							Records: map[manifest.NamedResource]manifest.DiscoveryRecord{},
						},
						Inventory: st,
						Root:      root,
					},
				)
				filter := l.Options.Session
				production := manifest.Production{Owner: owner, Source: manifest.BootstrapSourceID}
				if side == 0 {
					proposal, err := admissiontest.Prepare(
						t.Context(), t, l,
						selection.PrepareRequest{
							Generation: nil,
							Reads:      nil,
							History: map[manifest.NamedResource]struct {
								Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
								Complete bool
							}{owner: {Records: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
								yield(
									child.Named(),
									manifest.DiscoveryRecord{
										Refs:         []manifest.NamedResource{source.Named()},
										ContentRoots: map[manifest.NamedResource][]string{source.Named(): {child.Path}},
										Productions:  []manifest.Production{production},
									},
								)
							}, Complete: true}},
							Invocation: true,
						},
					)
					assert.NoError(t, err)
					historyController := New(st, nil)
					historyController.Configure(Options{Session: l.Options.Session, Loader: l})
					_, commitErr := historyController.KeepEmitted(
						t.Context(),
						manifest.NamedResource{},
						proposal.Request,
					)
					proposal.Retire()
					assert.NoError(t, commitErr)
				} else {
					controller := New(st, task.NewBounded(2))
					controller.Configure(Options{Session: filter, Loader: l})
					kept1, admissionErr1 := controller.KeepEmitted(
						t.Context(),
						owner,
						selection.PrepareRequest{
							Generation: []store.Child{{Obj: child, Productions: []manifest.Production{production}}},
							Reads:      nil,
						},
					)
					assert.FatalIf(t, admissionErr1 != nil, "%v", admissionErr1)
					assert.Equal(t, kept1, mode == "mutable")
				}
				assert.Equal(t, filter.Snapshot().Reconcile(child.Named()), mode == "mutable")
				assert.Equal(t, filter.Snapshot().Reconcile(owner), mode == "mutable")
				assert.Equal(t, st.GetObject(child.Named()) == nil, true)
				for _, id := range []manifest.NamedResource{child.Named(), owner, source.Named(), manifest.BootstrapSourceID} {
					if filter.Snapshot().Reconcile(id) {
						selections[side] = append(selections[side], id)
					}
				}
			}
			assert.Diff(t, selections[0], selections[1])
		})
	}
}

func TestKeepEmitted_OverridesHistoricalReferences(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	oldSource := &manifest.GitRepository{Name: "old", Namespace: "apps"}
	newSource := &manifest.GitRepository{Name: "new", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{oldSource, newSource})
	for _, source := range []*manifest.GitRepository{oldSource, newSource} {
		st.SetArtifact(
			source.Named(),
			&store.SourceArtifact{FS: fstest.MapFS{"apps/red/file": {Data: []byte("value")}}, LocalPath: root},
		)
	}
	child := &manifest.Kustomization{
		Name:      "child",
		Namespace: "apps",
		Path:      "apps/red",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: newSource.Named().Kind, Name: newSource.Name},
	}
	owner := testutil.KustomizationID("apps", "emitter")
	l, _ := admissiontest.New(
		t,
		admissiontest.Config{Changes: change.NewSet([]string{"apps/red/file"}), Index: nil, Inventory: st, Root: root},
	)
	filter := l.Options.Session
	proposal, err := admissiontest.Prepare(
		t.Context(), t, l,
		selection.PrepareRequest{Generation: nil, Reads: nil, History: map[manifest.NamedResource]struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{owner: {Records: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
			yield(
				child.Named(),
				manifest.DiscoveryRecord{
					Refs:         []manifest.NamedResource{oldSource.Named()},
					ContentRoots: map[manifest.NamedResource][]string{oldSource.Named(): {"elsewhere"}},
				},
			)
		}}}, Invocation: true},
	)
	assert.NoError(t, err)
	historyController := New(st, nil)
	historyController.Configure(Options{Session: l.Options.Session, Loader: l})
	_, commitErr := historyController.KeepEmitted(t.Context(), manifest.NamedResource{}, proposal.Request)
	proposal.Retire()
	assert.NoError(t, commitErr)
	controller := New(st, task.NewBounded(2))
	controller.Configure(Options{Session: filter, Loader: l})
	kept2, admissionErr2 := controller.KeepEmitted(
		t.Context(),
		owner,
		selection.PrepareRequest{Generation: []store.Child{{Obj: child}}, Reads: nil},
	)
	assert.FatalIf(t, admissionErr2 != nil, "%v", admissionErr2)
	assert.Equal(t, kept2, true)
	assert.Equal(t, filter.Snapshot().Reconcile(newSource.Named()), true)
	assert.Equal(t, filter.Snapshot().Reconcile(oldSource.Named()), false)
	assert.Equal(t, st.GetObject(child.Named()) == nil, true)
}
