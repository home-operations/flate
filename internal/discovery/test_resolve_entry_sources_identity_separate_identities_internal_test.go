package discovery

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func testResolveEntrySourcesIdentitySeparateIdentities(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
			"kubernetes/clusters/c0/entry": {}, "kubernetes/apps/x/file": {},
		}},
	)
	ids := make([]manifest.NamedResource, 0, 12)
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
		for _, namespace := range []string{"one", "two"} {
			for _, name := range []string{"source", "other"} {
				id := manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      kind,
					Namespace: namespace,
					Name:      name,
				}
				ids = append(ids, id)
				st.AddObject(
					&manifest.Kustomization{
						Name:      kind + name + namespace,
						Namespace: "consumer",
						Path:      "./apps/x",
						SourceRef: kustomizev1.CrossNamespaceSourceReference{
							Kind:      kind,
							Name:      name,
							Namespace: namespace,
						},
					},
				)
			}
		}
	}
	wrong := manifest.NamedResource{
		Group:     "wrong.group",
		Kind:      manifest.KindOCIRepository,
		Namespace: "one",
		Name:      "source",
	}
	st.AddObject(
		&manifest.Kustomization{
			Name:      "wrong",
			Namespace: "consumer",
			Path:      "./apps/x",
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				APIVersion: wrong.Group + "/v1",
				Kind:       wrong.Kind,
				Namespace:  wrong.Namespace,
				Name:       wrong.Name,
			},
		},
	)
	d := Session{cfg: Config{Path: filepath.Join(root, "kubernetes", "clusters", "c0"), Store: st}}
	count, err := d.resolveEntrySources(t.Context(), true)
	assert.FatalIf(t, err != nil || count != 4, "resolution = %d, %v, want %d", count, err, len(ids))
	for _, id := range ids {
		artifact := st.GetArtifact(id, store.ArtifactContent)
		if id.Kind != manifest.KindGitRepository {
			assert.FatalIf(
				t,
				artifact != nil || st.GetObject(id) != nil,
				"missing non-Git source aliased: %s",
				id,
			)
			continue
		}
		assert.FatalIf(
			t,
			artifact == nil || artifact.(*store.SourceArtifact).LocalRoot != root ||
				st.GetObject(id).Named() != id,
			"Git identity changed: %s",
			id,
		)
	}
	assert.FatalIf(
		t,
		st.GetObject(wrong) != nil || st.GetArtifact(wrong, store.ArtifactContent) != nil,
		"%v",
		"wrong-group identity collided with an entry source",
	)
}
