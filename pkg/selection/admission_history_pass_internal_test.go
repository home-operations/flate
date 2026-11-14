package selection

import (
	"encoding/json"
	"iter"
	"testing"
	"testing/fstest"

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

func TestAdmission_HistoryContentAvailability(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		external    bool
		declaration bool
		content     bool
		root        string
		complete    bool
		resolved    string
	}{
		{name: "unpublished source", external: true, root: "payload", complete: true, resolved: "payload"},
		{name: "declared source without content", external: true, declaration: true, root: "payload", complete: true, resolved: "payload"},
		{name: "readable source unresolved", external: true, declaration: true, content: true, root: "payload", resolved: "payload"},
		{name: "local source unresolved", root: "payload", resolved: "payload"},
		{name: "unresolved parent traversal", external: true, root: "payload/../other", resolved: "."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			source := &manifest.ExternalArtifact{Name: "bundle", Namespace: "apps"}
			if tc.declaration {
				st.AddObject(source)
			}
			if tc.content {
				st.SetArtifact(source.Named(), &store.SourceArtifact{FS: fstest.MapFS{}})
			}
			facts := &solverFacts{store: st, sources: map[manifest.NamedResource]SourceFacts{
				source.Named(): {External: tc.external},
			}}
			c := solverClosure(t, st, facts)
			owner := testutil.KustomizationID("apps", "owner")
			c.replace = map[manifest.NamedResource]bool{owner: true}
			c.proposal.preparation = &Preparation{}
			defer c.proposal.preparation.Retire()
			record := manifest.DiscoveryRecord{ContentRoots: map[manifest.NamedResource][]string{
				source.Named(): {tc.root},
			}}
			c.resolveHistoricalRecord(owner, &record)
			assert.Equal(t, len(c.proposal.demands), 1)
			c.proposal.preparation.acceptAcquired([]Acquisition{{Demand: c.proposal.demands[0]}})
			c.proposal.demands = nil
			record.ContentRoots[source.Named()] = []string{tc.root}
			c.replace[owner] = true
			c.resolveHistoricalRecord(owner, &record)
			assert.Equal(t, len(c.proposal.demands), 0)
			assert.Equal(t, c.replace[owner], tc.complete)
			assert.Diff(t, record.ContentRoots, map[manifest.NamedResource][]string{source.Named(): {tc.resolved}})
		})
	}
}

func TestAdmission_HistoricalDeclarationEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		complete bool
		changed  bool
		accepted bool
	}{
		{name: "matching complete history", complete: true, accepted: true},
		{name: "incomplete history"},
		{name: "different declaration", complete: true, changed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			application := `{"apiVersion":"kustomize.toolkit.fluxcd.io/v1","kind":"Kustomization","metadata":{"name":"child","namespace":"apps"},"spec":{"path":"leaf"}}`
			var document map[string]any
			assert.NoError(t, json.Unmarshal([]byte(application), &document))
			parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			child := parsed.Object.(*manifest.Kustomization)
			if tc.changed {
				child = child.Clone()
				child.Path = "other"
			}
			owner := testutil.KustomizationID("apps", "owner")
			st := store.New()
			facts := &solverFacts{store: st, index: manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{child.Named(): {
					Declaration: child, Parent: owner, Builder: "native",
				}},
			}}
			c := solverClosure(t, st, facts)
			c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{
				owner: {child.Named(): {Application: application}},
			}
			c.replace = map[manifest.NamedResource]bool{owner: tc.complete}
			evidence := c.declarationEvidence(child.Named())
			assert.Equal(t, evidence.accepted, tc.accepted)
			assert.Equal(t, evidence.authored, tc.accepted)
			assert.Equal(t, evidence.known(), tc.accepted)
			assert.Equal(t, evidence.child.Obj == child, tc.accepted)
			assert.Equal(t, st.GetDeclaration(child.Named()), manifest.BaseManifest(nil))
		})
	}
}
