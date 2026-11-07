package selection

import (
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_EmittedChildKeepsUnrelatedInventoryClosed(t *testing.T) {
	t.Parallel()
	st := store.New()
	facts := &solverFacts{
		store:   st,
		changes: change.NewSet([]string{"apps/app-0/ks.yaml"}),
		index:   manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
	}
	facts.context.Root = "/repo"
	for i := range 1000 {
		obj := &manifest.Kustomization{
			Name:      fmt.Sprintf("app-%d", i),
			Namespace: "flux-system",
			Path:      fmt.Sprintf("./apps/app-%d", i),
		}
		st.AddObject(obj)
		facts.index.Records[obj.Named()] = manifest.DiscoveryRecord{File: fmt.Sprintf("apps/app-%d/ks.yaml", i)}
	}
	session, err := New(st, facts, Request{Changed: true})
	assert.NoError(t, err)
	initial, err := prepareSolver(t.Context(), t, session, PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	_, committed, err := session.commitSolver(t.Context(), t, initial, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	parent := testutil.KustomizationID("flux-system", "app-0")
	assert.Equal(t, session.Snapshot().Reconcile(parent), true)
	child := store.Child{Obj: &manifest.ConfigMap{Name: "child", Namespace: "apps"}}
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	proposal, err := prepareSolver(
		t.Context(),
		t,
		session,
		PrepareRequest{Preparation: &preparation, Generation: []store.Child{child}, Owner: parent},
	)
	assert.NoError(t, err)
	_, committed, err = session.commitSolver(t.Context(), t, proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, session.Snapshot().Reconcile(child.Obj.Named()), true)
	if len(preparation.closure.states) > 4 {
		t.Fatalf("one emitted child prepared %d objects; want at most 4", len(preparation.closure.states))
	}
}

func TestAdmission_ReferenceForecastNeedsDeclaration(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "factory", Namespace: "apps"}
	child := &manifest.HelmRelease{Name: "application", Namespace: "apps"}
	facts := &solverFacts{store: st, index: manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			child.Named(): {
				File: "factory.yaml", Parent: owner.Named(), Productions: []manifest.Production{{Owner: owner.Named()}},
			},
		},
	}}
	st.SetDiscoveryIndex(&facts.index)
	st.AddObjects([]manifest.BaseManifest{owner, child})
	assert.Equal(t, st.GetObject(child.Named()) == child, true)
	assert.Equal(t, st.GetDeclaration(child.Named()) == nil, true)
	session, err := New(st, facts, Request{Roots: []manifest.NamedResource{child.Named()}})
	assert.NoError(t, err)
	var preparation *Preparation
	t.Cleanup(func() { preparation.Retire() })
	proposal, err := session.Prepare(t.Context(), PrepareRequest{Invocation: true, Preparation: &preparation})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, child.Named()), true)
	assert.Equal(t, slices.Contains(proposal.Ordinary, child.Named()), true)
	assert.Equal(t, slices.Contains(proposal.Selected, child.Named()), false)
	proposal.Retire()
	proposal, err = session.Prepare(t.Context(), PrepareRequest{
		Preparation: &preparation, Owner: owner.Named(), Generation: []store.Child{{Obj: child}},
	})
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, slices.Contains(proposal.Needed, child.Named()), true)
	assert.Equal(t, slices.Contains(proposal.Ordinary, child.Named()), true)
	assert.Equal(t, slices.Contains(proposal.Selected, child.Named()), true)
}
