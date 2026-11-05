package loader

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_RetiresReplacedConstructionIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		incoming bool
	}{{name: "retired"}, {name: "current incoming", incoming: true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Filter = change.NewFilter()
			l.Options.Changes = change.NewSet([]string{"hr.yaml"})
			unqualified := manifest.NamedResource{
				Group: manifest.HelmReleaseDomain,
				Kind:  manifest.KindHelmRelease,
				Name:  "demo",
			}
			qualified := unqualified
			qualified.Namespace = "apps"
			l.Index.Records[unqualified] = manifest.DiscoveryRecord{
				Authored: unqualified, Path: filepath.Join(root, "hr.yaml"), File: "hr.yaml",
			}
			var preparation *Preparation
			defer func() { preparation.Retire() }()
			before, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true, Preparation: &preparation})
			assert.NoError(t, err)
			assert.Diff(t, before.Needed, []manifest.NamedResource{unqualified})
			assert.Diff(t, before.Selected, []manifest.NamedResource{unqualified})
			before.Retire()
			delete(l.Index.Records, unqualified)
			l.Index.Records[qualified] = manifest.DiscoveryRecord{
				Authored: unqualified, Path: filepath.Join(root, "hr.yaml"), File: "hr.yaml",
			}
			request := PrepareRequest{Invocation: true, Preparation: &preparation}
			want := []manifest.NamedResource{qualified}
			if tc.incoming {
				request.Generation = []store.Child{{
					Obj: &manifest.HelmRelease{Name: unqualified.Name},
					Productions: []manifest.Production{{
						Source: manifest.BootstrapSourceID, Authored: unqualified,
						Path: filepath.Join(root, "hr.yaml"), File: "hr.yaml",
					}},
				}}
				want = []manifest.NamedResource{unqualified, qualified}
			}
			after, err := l.Prepare(t.Context(), request)
			assert.NoError(t, err)
			defer after.Retire()
			assert.Diff(t, after.Needed, want)
			assert.Diff(t, after.Selected, want)
		})
	}
}
