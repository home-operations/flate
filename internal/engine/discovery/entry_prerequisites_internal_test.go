package discovery

import (
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEntryCandidateReady_UnconditionalBootstrap(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		shared bool
	}{
		{name: "bootstrap", shared: true},
		{name: "separate source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, source)
			owner := &manifest.Kustomization{
				Name: "owner", Namespace: "apps", Path: "./missing",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository, Name: "entry",
				},
			}
			if !tc.shared {
				source = &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}}
			}
			sourceID := owner.EffectiveSourceID()
			st.SetArtifact(sourceID, source)
			d := newDiscoverer(new(testConfig(t, Config{Store: st})))
			t.Cleanup(d.Close)
			var reads store.ReadSet
			reads.Init(st, owner.Named(), nil)
			t.Cleanup(reads.ReleaseCapture)
			t.Cleanup(reads.Retire)
			assert.Equal(t, d.entryCandidateReady(t.Context(), &reads, owner), tc.shared)
			assert.Equal(t, d.prepared[owner.Named()] == nil, tc.shared)
			st.SetArtifact(sourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
			assert.Equal(t, reads.Valid(), false)
		})
	}
}
