package loader

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestPrepare_ProductionReaderRoles(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		materialize bool
		needed      bool
	}{
		{name: "content", needed: true},
		{name: "materializer", materialize: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			owner := &manifest.Kustomization{Name: "workload", Namespace: "apps", Path: "workload"}
			reader := &manifest.HelmRelease{Name: "generator", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{owner, reader})
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			file := "entry/workload.yaml"
			production := manifest.Production{
				Owner: owner.Named(), Authored: owner.Named(), Source: manifest.BootstrapSourceID,
				Path: filepath.Join(root, filepath.FromSlash(file)), File: file,
			}
			l := New(st)
			l.SourceRoot = root
			l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Productions: []manifest.Production{production}}
			l.Index.Readers = map[string][]manifest.FileReader{
				"entry": {{Reader: reader.Named(), Source: manifest.BootstrapSourceID, Materialize: tc.materialize}},
			}
			l.Options.Filter = change.NewFilter()
			l.Options.Filter.AddEmitted([]manifest.NamedResource{owner.Named()}, nil, nil, 0)
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet([]string{"workload/cm.yaml"})
			proposal, err := l.Prepare(
				t.Context(),
				store.Child{Obj: owner, Productions: []manifest.Production{production}},
				nil,
				nil,
			)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Needed, reader.Named()), tc.needed)
		})
	}
}
