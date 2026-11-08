package discovery

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRun_AnonymousAdmissionPreparation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
		"entry/release.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: apps}\nspec:\n  interval: 1h\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		},
	}}
	var work manifest.WorkRecorder
	st := store.New()
	st.SetWorkTree(manifest.TreeID{}, work.Scope())
	l := loader.New(st)
	l.Options.Changes = change.NewSet([]string{"entry/release.yaml"})
	request := selection.Request{Changed: true}
	configureSelection(t, l, request)
	session, err := Begin(t.Context(), Config{
		Input: input, Path: filepath.Join(root, "entry"), Store: st, Loader: l,
		Trees: testTrees(t), Work: work.Scope(), Selection: request,
	})
	assert.NoError(t, err)
	defer session.Close()
	preparation := session.prepareRequest(&selection.PrepareRequest{Invocation: true})
	proposal, err := session.prepare(t.Context(), preparation)
	assert.NoError(t, err)
	advanced, err := session.Advance(t.Context(), proposal)
	assert.NoError(t, err)
	assert.Equal(t, advanced, false)
	assert.Equal(t, st.GetDeclaration(manifest.NamedResource{
		Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "app",
	}) != nil, true)
	var counts manifest.WorkCounts
	snapshot := work.Scope().Snapshot(0)
	for i := range snapshot {
		row := &snapshot[i]
		if row.Key.Owner == (manifest.NamedResource{}) && row.Key.Phase == manifest.WorkPreparation &&
			row.Key.Input == "admission" {
			counts.Add(&row.Counts)
		}
	}
	assert.Equal(t, counts.OwnerPrepares, uint64(1))
}
