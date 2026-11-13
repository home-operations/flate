package selection

import (
	"iter"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_HistoryAvailability(t *testing.T) {
	t.Parallel()
	owner := testutil.KustomizationID("apps", "owner")
	child := testutil.KustomizationID("apps", "child")
	for _, tc := range []struct {
		name     string
		owner    manifest.NamedResource
		present  bool
		records  bool
		complete bool
	}{
		{name: "absent", owner: owner},
		{name: "unavailable", owner: owner, present: true},
		{name: "incomplete", owner: owner, present: true, records: true},
		{name: "complete empty", owner: owner, present: true, records: true, complete: true},
		{name: "zero owner complete empty", present: true, records: true, complete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request := PrepareRequest{History: map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{}}
			refs := []manifest.NamedResource{manifest.BootstrapSourceID}
			if tc.present {
				history := request.History[tc.owner]
				history.Complete = tc.complete
				if tc.records {
					history.Records = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
						if !tc.complete {
							yield(child, manifest.DiscoveryRecord{Refs: refs})
						}
					}
				}
				request.History[tc.owner] = history
			}
			st := store.New()
			facts := &solverFacts{store: st}
			closure := solverClosure(t, st, facts)
			closure.history = request.History
			closure.readHistory()
			records, available := closure.accepted[tc.owner]
			assert.Equal(t, available, tc.records)
			assert.Equal(t, closure.replace[tc.owner], tc.complete)
			assert.Equal(t, len(records) == 1, tc.records && !tc.complete)
			if tc.records && !tc.complete {
				record := closure.record(child)
				assert.Diff(t, record.Refs, refs)
				record.Refs[0] = owner
				assert.Diff(t, refs, []manifest.NamedResource{manifest.BootstrapSourceID})
				assert.Diff(t, records[child].Refs, refs)
			}
			assert.Equal(t, len(request.History) == 1, tc.present)
			history, present := request.History[tc.owner]
			assert.Equal(t, present, tc.present)
			assert.Equal(t, history.Records != nil, tc.records)
			assert.Equal(t, history.Complete, tc.complete)
		})
	}
}

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
