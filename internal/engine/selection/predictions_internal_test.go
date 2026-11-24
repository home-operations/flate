package selection

import (
	"hash/maphash"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSnapshot_OrderingPredictions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		suspended bool
		want      []manifest.NamedResource
	}{
		{name: "active", want: []manifest.NamedResource{testutil.HelmReleaseID("web", "db")}},
		{name: "suspended", suspended: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reader := &manifest.HelmRelease{
				Name: "web", Namespace: "web",
				Suspend:   tc.suspended,
				DependsOn: []helmv2.DependencyReference{{Name: "db"}},
			}
			id := reader.Named()
			st := store.New()
			st.AddObject(reader)
			decision, _ := (&Decision{seed: maphash.MakeSeed(), changedOnly: true}).include(&Admission{
				Fatal:                []manifest.NamedResource{id},
				orderingDeclarations: map[manifest.NamedResource]orderingDeclaration{id: {object: reader}},
			}, true)
			snapshot := Snapshot{Store: st, Facts: &solverFacts{}, Decision: decision}
			var got []manifest.NamedResource
			for predicted, refs := range snapshot.Predictions() {
				assert.Equal(t, predicted, id)
				for ref := range refs {
					got = append(got, ref.NamedResource)
				}
			}
			assert.Diff(t, got, tc.want)
		})
	}
}
