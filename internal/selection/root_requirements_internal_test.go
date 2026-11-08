package selection

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSession_PendingRootsCaptureMissingDeclaration(t *testing.T) {
	t.Parallel()
	st := store.New()
	object := &manifest.Kustomization{Name: "pending", Namespace: "apps"}
	id := object.Named()
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{id}})
	facts := session.facts.(*solverFacts)
	record := manifest.DiscoveryRecord{Path: "pending.yaml", File: "pending.yaml", Declaration: object}
	facts.index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{id: record}
	facts.roots = map[manifest.NamedResource]RootEvidence{id: {
		Identity: id,
		Record:   RootRecordFrom(&record),
		Scope:    RootScopeFacts{Eligibility: RootEligible, NamespaceEligible: true},
	}}
	_, retry := publishDecision(t, session, new(Admission{
		Needed: []manifest.NamedResource{id}, Selected: []manifest.NamedResource{id},
	}), 0)
	assert.Equal(t, retry, false)
	reads := session.PendingRoots()
	if reads == nil {
		t.Fatal("selected missing root did not request acquisition")
	}
	defer st.Retire(reads)
	assert.Equal(t, reads.ID(), manifest.NamedResource{})
	assert.Diff(t, maps.Collect(reads.Required()), map[manifest.NamedResource]bool{id: false})
	assert.Equal(t, reads.Valid(), true)
	st.AddObject(object)
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, session.PendingRoots() == nil, true)
}
