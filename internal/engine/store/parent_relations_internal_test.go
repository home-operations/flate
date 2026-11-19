package store

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_ParentRelations(t *testing.T) {
	t.Parallel()
	for _, structural := range []bool{false, true} {
		name := "rendered"
		if structural {
			name = "structural"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New()
			a, z, parent := ownerID("a"), ownerID("z"), ownerID("structural")
			child := emissionChild("shared", "value")
			id := child.Obj.Named()
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Productions: []manifest.Production{
					{Owner: id},
					{Owner: ownerID("authored"), Authored: id},
					{Owner: ownerID("declared"), Declaration: id},
				}},
			}}
			if structural {
				record := index.Records[id]
				record.Parent = parent
				index.Records[id] = record
			}
			s.SetDiscoveryIndex(index)
			for _, producer := range []manifest.NamedResource{z, a} {
				s.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
				s.RecordEmitted(producer, &Emission{Children: []Child{child}, Complete: true}, nil)
			}
			for _, view := range []*manifest.DiscoveryIndex{index, nil} {
				got, found := s.Parent(id, view)
				assert.Equal(t, found, true)
				want := a
				if structural {
					want = parent
				}
				assert.Equal(t, got, want)
				assert.Diff(t, slices.Collect(s.RenderedParents(id, view)), []manifest.NamedResource{a, z})
				for first := range s.RenderedParents(id, view) {
					assert.Equal(t, first, a)
					break
				}
			}
			s.RecordEmitted(a, &Emission{Complete: true}, nil)
			assert.Diff(t, slices.Collect(s.RenderedParents(id, index)), []manifest.NamedResource{z})
		})
	}
}
