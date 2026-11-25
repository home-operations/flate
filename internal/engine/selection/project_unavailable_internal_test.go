package selection

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestProject_PassedProducerAncestry(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"independent", "emitted", "withdrawn"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
			st.AddObject(parent)
			switch name {
			case "emitted":
				st.RecordEmitted(
					parent.Named(),
					&store.Emission{Children: []store.Child{{Obj: child}}, Complete: true},
					nil,
				)
				assert.Equal(t, st.EmissionOwner(child.Named()), parent.Named())
			case "withdrawn":
				st.AddObject(child)
				assert.Equal(t, st.DeleteObject(child.Named()), true)
				assert.Equal(t, st.GetDeclaration(child.Named()), nil)
			default:
				st.AddObject(child)
				assert.Equal(t, st.GetDeclaration(child.Named()) == child, true)
			}
			st.UpdateStatus(child.Named(), store.StatusInfo{Status: store.StatusReady})
			failed := store.FailureStatus(errors.New("parent failed"))
			st.UpdateStatus(parent.Named(), failed)
			side := selectionSide{run: Snapshot{
				Store: st,
				Index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					child.Named(): {Parent: parent.Named()},
				}},
			}}
			assert.Equal(t, side.localStatus(child.Named()).Outcome(), store.OutcomePassed)
			got, _ := side.walkUnavailable(
				child.Named(),
				&selectionSide{},
				ancestryCache{owner: &side.run, resolved: make(map[manifest.NamedResource]ancestryStatus)},
				nil,
			)
			want := failed
			if name == "independent" {
				want = store.StatusInfo{}
			}
			assert.Equal(t, got.status, want)
		})
	}
	for _, tc := range []struct {
		name         string
		intermediate bool
	}{
		{name: "prior emitted identity"},
		{name: "prior intermediate identity", intermediate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			currentStore, originalStore := store.New(), store.New()
			raw := &manifest.Kustomization{Name: "raw", Namespace: "apps"}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
			descendant := &manifest.Kustomization{Name: "descendant", Namespace: "apps"}
			transformed := &manifest.Kustomization{Name: "transformed", Namespace: "apps"}
			priorOwner := &manifest.Kustomization{Name: "prior-owner", Namespace: "apps"}
			currentStore.AddObject(raw)
			currentStore.AddObject(child)
			currentStore.AddObject(descendant)
			currentStore.UpdateStatus(raw.Named(), store.StatusInfo{Status: store.StatusReady})
			failed := store.FailureStatus(errors.New("prior owner failed"))
			currentStore.UpdateStatus(priorOwner.Named(), failed)
			originalStore.AddObject(priorOwner)
			originalStore.RecordEmitted(
				priorOwner.Named(),
				&store.Emission{Children: []store.Child{{Obj: transformed}}, Complete: true},
				nil,
			)
			assert.Equal(t, originalStore.EmissionOwner(transformed.Named()), priorOwner.Named())

			records := map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw.Named(): {}, child.Named(): {Parent: raw.Named()}, descendant.Named(): {Parent: child.Named()},
			}
			authored := raw.Named()
			if tc.intermediate {
				authored = child.Named()
			}
			record := records[authored]
			record.File, record.Authored = "apps/producer.yaml", authored
			record.Productions = []manifest.Production{{Source: manifest.BootstrapSourceID}}
			records[authored] = record
			previous := record
			previous.Parent = priorOwner.Named()
			current := selectionSide{result: new(Result), run: Snapshot{
				Store: currentStore,
				Index: &manifest.DiscoveryIndex{Records: records},
			}}
			original := selectionSide{run: Snapshot{
				Store: originalStore,
				Index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					transformed.Named(): previous,
				}},
			}}

			current.prepareUnavailable(&original, []manifest.NamedResource{descendant.Named()})
			assert.Equal(t, current.result.Unavailable[descendant.Named()], failed)

			current.result = new(Result)
			candidates := []manifest.NamedResource{raw.Named(), child.Named(), descendant.Named()}
			current.prepareUnavailable(&original, candidates)
			for _, id := range candidates {
				want := failed
				if tc.intermediate && id == raw.Named() {
					want = store.StatusInfo{}
				}
				assert.Equal(t, current.result.Unavailable[id], want)
			}
		})
	}
}
