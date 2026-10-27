package base

import (
	"iter"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestKeepEmitted_MatchesHistoryBeforePublication(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"mutable", "pinned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			source := &manifest.GitRepository{Name: "content", Namespace: "apps"}
			owner := testutil.KustomizationID("apps", "emitter")
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "apps/red", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name}}
			var selections [2][]string
			for side := range 2 {
				st := store.New()
				st.AddObject(source)
				art := &store.SourceArtifact{FS: fstest.MapFS{"apps/red/file": {Data: []byte("changed")}}, LocalPath: root}
				if mode == "pinned" {
					art.LocalPath, art.LocalRoot, art.Key, art.Content = "/selected", root, "commit", "generation"
				}
				st.SetArtifact(source.Named(), art)
				filter := change.NewFilter(change.NewSet([]string{"apps/red/file"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}, st, root)
				production := manifest.Production{Owner: owner, Source: manifest.BootstrapSourceID}
				if side == 0 {
					filter.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{owner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
						yield(child.Named(), manifest.DiscoveryRecord{Refs: []manifest.NamedResource{source.Named()}, ContentRoots: map[manifest.NamedResource][]string{source.Named(): {child.Path}}, Productions: []manifest.Production{production}})
					}}, map[manifest.NamedResource]bool{owner: true})
				} else {
					controller := New(st, task.NewBounded(2))
					controller.Configure(Options{Filter: filter})
					assert.Equal(t, controller.KeepEmitted(owner, store.Child{Obj: child, Productions: []manifest.Production{production}}, nil), mode == "mutable")
				}
				assert.Equal(t, filter.ShouldReconcile(child.Named()), mode == "mutable")
				assert.Equal(t, filter.ShouldReconcile(owner), mode == "mutable")
				assert.Equal(t, st.GetObject(child.Named()) == nil, true)
				selections[side] = filter.KeepNames()
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
		st.SetArtifact(source.Named(), &store.SourceArtifact{FS: fstest.MapFS{"apps/red/file": {Data: []byte("value")}}, LocalPath: root})
	}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "apps/red", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: newSource.Named().Kind, Name: newSource.Name}}
	owner := testutil.KustomizationID("apps", "emitter")
	filter := change.NewFilter(change.NewSet([]string{"apps/red/file"}), nil, st, root)
	filter.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{owner: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		yield(child.Named(), manifest.DiscoveryRecord{Refs: []manifest.NamedResource{oldSource.Named()}, ContentRoots: map[manifest.NamedResource][]string{oldSource.Named(): {"elsewhere"}}})
	}}, map[manifest.NamedResource]bool{})
	controller := New(st, task.NewBounded(2))
	controller.Configure(Options{Filter: filter})
	assert.Equal(t, controller.KeepEmitted(owner, store.Child{Obj: child}, nil), true)
	assert.Equal(t, filter.ShouldReconcile(newSource.Named()), true)
	assert.Equal(t, filter.ShouldReconcile(oldSource.Named()), false)
	assert.Equal(t, st.GetObject(child.Named()) == nil, true)
}
