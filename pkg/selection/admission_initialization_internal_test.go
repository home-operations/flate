package selection

import (
	"context"
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type initialCancellationFacts struct {
	census

	onIndex func()
}

func (f initialCancellationFacts) SelectionIndex() *manifest.DiscoveryIndex {
	f.onIndex()
	return f.census.SelectionIndex()
}

func TestAdmissionStart_CancellationDuringRound(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := testutil.KustomizationID("apps", "owner")
	facts := &solverFacts{
		store: st, changes: change.NewSet([]string{"owner.yaml"}),
		index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			owner: {File: "owner.yaml"},
		}},
	}
	c := solverClosure(t, st, facts)
	c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {}}
	c.replace = map[manifest.NamedResource]bool{owner: true}
	c.proposal.Request.Changes = facts.changes
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	canceled := false
	c.facts = initialCancellationFacts{census: facts, onIndex: func() {
		if c.states[owner].generation == 2 {
			canceled = true
			cancel()
		}
	}}
	err := c.start(ctx, func(materializerDemand, manifest.NamedResource) bool { return false })
	assert.Equal(t, canceled, true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, c.states[owner].generation, 2)
	assert.Equal(t, c.replace[owner], false)
}
