package loader

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRootOwners_RelativeSourceBoundary(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: ".", FS: fstest.MapFS{}})
	owner := &manifest.Kustomization{Namespace: "apps", Name: "owner", Path: "."}
	factory := (&manifest.ResourceSet{Namespace: "apps", Name: "factory"}).Named()
	st.AddObject(owner)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		owner.Named(): {Productions: []manifest.Production{{Owner: factory}}},
	}}
	owners := rootCoveringFacts(st, index)
	for _, tc := range []struct {
		name    string
		file    string
		covered bool
	}{
		{name: "descendant", file: "nested/input.yaml", covered: true},
		{name: "source itself", file: ".", covered: true},
		{name: "parent", file: "../input.yaml"},
		{name: "absolute", file: filepath.Join(t.TempDir(), "input.yaml")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			evidence := selection.RootEvidence{
				Record:     selection.RootRecord{Path: tc.file},
				Occurrence: selection.OccurrenceFacts{Declaration: selection.OccurrenceID{Source: "."}},
			}
			assert.Equal(t, slices.Contains(owners.candidates(&evidence), owner.Named()), tc.covered)
		})
	}
}
