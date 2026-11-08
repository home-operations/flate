package loader

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
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
			configureSelection(t, l, selection.Request{Changed: true})

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
			var preparation *selection.Preparation
			defer func() { preparation.Retire() }()
			before, err := prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true, Preparation: &preparation},
			)
			assert.NoError(t, err)
			assert.Diff(t, before.Needed, []manifest.NamedResource{unqualified})
			assert.Diff(t, before.Selected, []manifest.NamedResource{unqualified})
			before.Retire()
			delete(l.Index.Records, unqualified)
			l.Index.Records[qualified] = manifest.DiscoveryRecord{
				Authored: unqualified, Path: filepath.Join(root, "hr.yaml"), File: "hr.yaml",
			}
			request := selection.PrepareRequest{Invocation: true, Preparation: &preparation}
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
			after, err := prepareSelection(t.Context(), t, l, request)
			assert.NoError(t, err)
			defer after.Retire()
			assert.Diff(t, after.Needed, want)
			assert.Diff(t, after.Selected, want)
		})
	}
}
