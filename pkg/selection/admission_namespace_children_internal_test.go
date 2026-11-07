package selection

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_EmittedNamespaceChildrenChanged(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		inherited bool
		stored    bool
	}{
		{name: "incoming inherited", inherited: true},
		{name: "incoming changed production"},
		{name: "stored inherited", inherited: true, stored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			stable := &manifest.Kustomization{Name: "stable", Namespace: "apps"}
			changed := &manifest.Kustomization{Name: "changed", Namespace: "apps", Path: "changed"}
			st.AddObject(parent)
			changes := change.NewSet([]string{"production.yaml", "changed.yaml"})
			facts := &solverFacts{
				store:   st,
				changes: changes,
				index:   manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}

			root := parent.Named()
			root.Name = ""
			session, err := New(st, facts, Request{Roots: []manifest.NamedResource{root}, Changed: true})
			assert.NoError(t, err)
			var selected []manifest.NamedResource
			if tc.inherited {
				selected = append(selected, parent.Named())
			}
			publishDecision(t, session, &Admission{
				Needed: []manifest.NamedResource{parent.Named()}, Selected: selected,
				Dirty: selected, Ordinary: []manifest.NamedResource{parent.Named()},
			}, 0)
			children := make([]store.Child, 0, 2)
			for _, obj := range []*manifest.Kustomization{stable, changed} {
				declaration := obj.Clone()
				declaration.Path = ""
				production := manifest.Production{
					Owner:       parent.Named(),
					Declaration: obj.Named(),
					Source:      manifest.BootstrapSourceID,
					File:        "production.yaml",
				}
				facts.index.Records[obj.Named()] = manifest.DiscoveryRecord{
					File: obj.Name + ".yaml", Builder: "kustomization.yaml", Declaration: declaration,
					Productions: []manifest.Production{production},
				}
				children = append(children, store.Child{Obj: obj, Productions: []manifest.Production{production}})
			}
			if tc.stored {
				st.RecordEmitted(parent.Named(), &store.Emission{Children: children, Complete: true}, nil)
				proposal, err := prepareSolver(
					t.Context(),
					t,
					session,
					PrepareRequest{Changes: changes, Invocation: true},
				)
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Selected, stable.Named()), false)
				assert.Equal(t, slices.Contains(proposal.Dirty, stable.Named()), false)
				assert.Equal(t, slices.Contains(proposal.Selected, changed.Named()), true)
				assert.Equal(t, slices.Contains(proposal.Dirty, changed.Named()), true)
				return
			}
			for _, child := range children {
				proposal, err := prepareSolver(
					t.Context(),
					t,
					session,
					PrepareRequest{Changes: changes, Generation: []store.Child{child}},
				)
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Selected, child.Obj.Named()), child.Obj == changed)
				assert.Equal(t, slices.Contains(proposal.Dirty, child.Obj.Named()), child.Obj == changed)
			}
		})
	}
}
