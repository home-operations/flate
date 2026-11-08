package loader

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func preparedDeclarationPair(t *testing.T, id, owner manifest.NamedResource) (
	[2]*store.Store, [2]*store.SourceArtifact, [2]map[manifest.NamedResource]store.Child,
) {
	t.Helper()
	stores := [2]*store.Store{store.New(), store.New()}
	inputs := [2]*store.SourceArtifact{}
	var prepared [2]map[manifest.NamedResource]store.Child
	for side, st := range stores {
		inputs[side] = &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}}
		st.SetArtifact(manifest.BootstrapSourceID, inputs[side])
		st.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					id: {Productions: []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID}}},
				},
			},
		)
		obj := &manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "./payload"}
		doc := map[string]any{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind":       id.Kind,
			"metadata":   map[string]any{"name": id.Name, "namespace": id.Namespace},
			"spec":       map[string]any{"path": obj.Path, "unknown": "stable"},
		}
		prepared[side] = map[manifest.NamedResource]store.Child{id: {Obj: obj, Doc: doc}}
	}
	return stores, inputs, prepared
}
