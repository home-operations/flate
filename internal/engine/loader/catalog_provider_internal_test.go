package loader

import (
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCatalog_SourceExternal(t *testing.T) {
	t.Parallel()
	st := store.New()
	l := New(st)
	generated := (&manifest.ExternalArtifact{Name: "local", Namespace: "apps"}).Named()
	remote := &manifest.GitRepository{Name: "remote", Namespace: "apps"}
	owner := &manifest.Kustomization{Name: "generator", Namespace: "apps"}
	st.AddObject(remote)
	st.AddObject(owner)
	index := l.Index
	index.Records[generated] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: owner.Named()}}}
	st.SetDiscoveryIndex(index)
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer reads.Retire()
	l.SourceProvider = func(id manifest.NamedResource, capture *store.ReadSet, evidence *manifest.DiscoveryIndex) store.SourceProvider {
		assert.Equal(t, capture, &reads)
		assert.Equal(t, evidence, index)
		assert.Equal(t, capture.GetArtifact(id, store.ArtifactContent), store.Artifact(nil))
		if id == remote.Named() {
			assert.Equal(t, capture.GetDeclaration(id), manifest.BaseManifest(remote))
			return store.ProviderFetch
		}
		assert.Equal(t, capture.GetDeclaration(id), manifest.BaseManifest(nil))
		assert.Diff(t, capture.Productions(id, evidence), []manifest.Production{{Owner: owner.Named()}})
		assert.Equal(t, capture.GetArtifact(owner.Named(), store.ArtifactContent), store.Artifact(nil))
		return store.ProviderGenerator
	}
	catalog := l.DetachCatalog()
	l.Close()
	defer catalog.Close()
	assert.Equal(t, catalog.SourceExternal(generated, &reads), false)
	assert.Equal(t, catalog.SourceExternal(remote.Named(), &reads), true)
	assert.Equal(t, reads.Valid(), true)
	st.RecordEmitted(owner.Named(), &store.Emission{Complete: true}, nil)
	assert.Equal(t, reads.Valid(), false)
}
