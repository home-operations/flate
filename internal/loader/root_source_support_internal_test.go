package loader

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRootSupport_SuppliedDeclarationRetainsRepositoryAuthority(t *testing.T) {
	t.Parallel()
	st := store.New()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	source := (&manifest.GitRepository{Namespace: "sources", Name: "repository"}).Named()
	input := &store.SourceArtifact{LocalPath: t.TempDir(), Content: "initial", FS: fstest.MapFS{}}
	st.SetArtifact(source, input)
	st.AddDeclaration(store.Declaration{
		Obj: manifest.NewConfigMap(id, map[string]any{"VALUE": "supplied"}), Origin: manifest.SuppliedDeclaration,
	})
	record := manifest.DiscoveryRecord{
		Path: filepath.Join(input.LocalPath, "settings.yaml"), Authored: id,
		Productions: []manifest.Production{{Source: source, Authored: id}},
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: record}}
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer reads.Retire()
	evidence := selection.RootEvidence{
		Identity: id,
		Record:   selection.RootRecordFrom(&record),
		Occurrence: selection.OccurrenceFacts{
			Declaration: selection.OccurrenceID{Source: input.LocalPath, File: "settings.yaml"},
		},
		Scope: selection.RootScopeFacts{NamespaceEligible: true, Candidate: true},
	}
	rootSupport(&evidence, &reads, index, &manifest.DiscoveryIndex{}, t.TempDir())
	assert.Equal(t, evidence.Supplied, true)
	assert.Equal(t, evidence.Sources[0].Source, source)
	assert.Equal(t, evidence.Sources[0].Available, true)
	assert.Equal(t, reads.Valid(), true)
	replacement := *input
	replacement.Content = "replacement"
	st.SetArtifact(source, &replacement)
	assert.Equal(t, reads.Valid(), false)
}
