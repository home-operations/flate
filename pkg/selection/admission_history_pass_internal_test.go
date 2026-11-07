package selection

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_HistoryProductionPass(t *testing.T) {
	t.Parallel()
	st := store.New()
	facts := &solverFacts{
		store:   st,
		changes: change.NewSet([]string{"owner.yaml"}),
		index:   manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
	}
	owner := testutil.KustomizationID("apps", "owner")
	child := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "child"}
	want := []manifest.Production{{Owner: owner, Authored: child, File: "child.yaml"}}
	facts.index.Records[owner] = manifest.DiscoveryRecord{File: "owner.yaml"}
	facts.index.Records[child] = manifest.DiscoveryRecord{File: "child.yaml", Productions: want}
	c := solverClosure(t, st, facts)
	c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {}}
	c.replace = map[manifest.NamedResource]bool{owner: true}
	c.proposal.Request.Changes = facts.changes
	assert.Equal(t, len(c.productions(child)), 0)
	assert.NoError(t, c.start(t.Context(), func(materializerDemand, manifest.NamedResource) bool { return false }))
	assert.Equal(t, c.states[owner].changed, true)
	assert.Equal(t, len(c.historyProductions), 0)
	assert.Equal(t, c.replace[owner], false)
	assert.Diff(t, c.productions(child), want)
}
