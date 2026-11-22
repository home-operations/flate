package selection

import (
	"fmt"
	"hash/maphash"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestProject_ScopedEmitterOutput(t *testing.T) {
	t.Parallel()
	for _, selected := range []bool{false, true} {
		t.Run(fmt.Sprintf("emitter selected %t", selected), func(t *testing.T) {
			t.Parallel()
			parent := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{})
			applier := &manifest.Kustomization{Name: "sync", Namespace: "apps"}
			emitter := &manifest.ResourceSet{Name: "emitter", Namespace: "apps"}
			child := &manifest.ConfigMap{Name: "required", Namespace: "apps"}
			raw := &manifest.ConfigMap{Name: "raw", Namespace: "apps"}
			st := store.New()
			for _, object := range []manifest.BaseManifest{parent, applier, emitter, child} {
				st.AddObject(object)
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				child.Named(): {Parent: emitter.Named()},
			}}
			admission := &Admission{Needed: []manifest.NamedResource{child.Named()}}
			admission.Selected = append(admission.Selected, parent.Named())
			if selected {
				admission.Selected = append(admission.Selected, emitter.Named())
			}
			decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(admission, true)
			document := testutil.ObjectDocument(child)
			snapshot := Snapshot{
				Store: st, Index: index, Decision: decision, Complete: true,
				Entry: EntryIdentity{Producer: applier.Named(), Key: parent.Named()},
				Emissions: []EmissionFacts{
					{
						Emitter:   emitter,
						Parent:    parent,
						Documents: []DocumentFact{{Document: document, Identity: child.Named()}},
					},
					{
						Emitter: parent, Parent: parent,
						Documents: []DocumentFact{{Document: testutil.ObjectDocument(raw), Identity: raw.Named()}},
					},
				},
			}
			results, failures := Project([]Snapshot{snapshot}, nil, Request{Roots: []manifest.NamedResource{{
				Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps",
			}}})
			assert.NoError(t, failures.Selected)
			assert.NoError(t, failures.Remaining)
			assert.Equal(t, len(results), 1)
			if selected {
				assert.Equal(t, len(results[0].Manifests), 1)
				assert.Diff(t, results[0].Manifests[0].Documents, []map[string]any{document})
			} else {
				assert.Equal(t, len(results[0].Manifests), 0)
			}
			assert.Equal(t, decision.Reconcile(child.Named()), true)
		})
	}
}
