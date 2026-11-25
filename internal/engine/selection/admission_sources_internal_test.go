package selection

import (
	"maps"
	"slices"
	"testing"
	"testing/fstest"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func testMaterializerOwnerEvidence(t *testing.T) {
	t.Helper()
	t.Parallel()
	for _, tc := range []materializerOwnerCase{
		{name: "unrelated owner without peer", side: manifest.TreeHead, laterDemand: true},
		{name: "paired identical files", side: manifest.TreeHead},
		{
			name: "base-only copied file", side: manifest.TreeHead, wantDemand: true,
			file: "payload/old.yaml", fileSide: manifest.TreeBase,
		},
		{
			name: "head-only copied file", side: manifest.TreeBase, wantDemand: true,
			file: "payload/new.yaml", fileSide: manifest.TreeHead,
		},
		{
			name: "outside copy path", side: manifest.TreeHead, wantDemand: true,
			file: "charts/app/values.yaml", fileSide: manifest.TreeHead,
		},
		{name: "changed reference", side: manifest.TreeHead, wantDemand: true},
		{name: "incomplete source", side: manifest.TreeHead, wantDemand: true},
		{name: "stale source", side: manifest.TreeHead, wantDemand: true},
		{name: "unproven declaration file", side: manifest.TreeHead, wantDemand: true},
		{name: "changed declaration", side: manifest.TreeHead, wantDemand: true},
		{name: "unproven live witness", side: manifest.TreeHead, wantDemand: true},
		{name: "different source revision", side: manifest.TreeHead, wantDemand: true},
		{name: "non-source input", side: manifest.TreeHead, wantDemand: true},
		{name: "non-static output", side: manifest.TreeHead, wantDemand: true},
		{name: "missing file coordinate", side: manifest.TreeHead, wantDemand: true},
		{name: "unresolved read", side: manifest.TreeHead, wantDemand: true},
	} {
		t.Run(tc.name, tc.run)
	}
}

type materializerOwnerCase struct {
	name        string
	side        manifest.TreeSide
	file        string
	fileSide    manifest.TreeSide
	wantDemand  bool
	laterDemand bool
}

func (tc materializerOwnerCase) run(t *testing.T) {
	t.Parallel()
	stores := [2]*store.Store{store.New(), store.New()}
	st := stores[tc.side]
	facts := &solverFacts{
		store:   st,
		context: SourceContext{Side: tc.side, SideKnown: true, Root: t.TempDir()},
		sources: make(map[manifest.NamedResource]SourceFacts),
	}
	source := (&manifest.ExternalArtifact{Name: "package", Namespace: "apps"}).Named()
	production, input := unrelatedMaterializer(facts, stores, tc.side, source)
	inputID := input.Named()
	if tc.name == "non-source input" {
		file := facts.index.Records[production.Owner].File
		object := &manifest.ResourceSet{Name: "generator", Namespace: "apps"}
		object.InputsFrom = []fluxopv1.InputProviderReference{{Name: "provider"}}
		object.Resources = []*apiextensionsv1.JSON{{Raw: []byte(
			`{"apiVersion":"source.toolkit.fluxcd.io/v1","kind":"ExternalArtifact","metadata":{"name":"package","namespace":"apps"}}`,
		)}}
		production.Owner, production.Declaration = object.Named(), object.Named()
		for _, s := range stores {
			s.AddObject(object)
		}
		facts.index.Records[production.Owner] = manifest.DiscoveryRecord{
			Authored: production.Owner, File: file, Declaration: object, StaticOutput: true,
		}
		facts.occurrences[production.Owner] = OccurrenceFacts{
			Identity: production.Owner, Authored: production.Owner, Object: object, Located: true, CatalogKnown: true,
			Declaration: OccurrenceID{Source: facts.context.Root, File: file},
			Refs:        slices.Collect(manifest.References(object, manifest.ReferenceRelations)),
		}
	}
	id := production.Owner
	record, occurrence := facts.index.Records[id], facts.occurrences[id]
	files := fstest.MapFS{"payload/unchanged.yaml": {}}
	if tc.file != "" {
		files[tc.file] = &fstest.MapFile{}
		stores[tc.fileSide].SetArtifact(inputID, &store.SourceArtifact{FS: files})
	}
	switch tc.name {
	case "paired identical files":
		st.SetArtifact(inputID, &store.SourceArtifact{FS: files})
	case "changed reference":
		updated := *input
		updated.URL = "https://example.test/replacement"
		st.AddObject(&updated)
	case "incomplete source":
		stores[1-tc.side].SetArtifact(inputID, nil)
	case "unproven declaration file":
		occurrence.Declaration.Source = t.TempDir()
	case "unproven live witness":
		replacement := new(*record.Declaration.(*manifest.RawObject))
		replacement.Spec = maps.Clone(replacement.Spec)
		replacement.Spec["commonMetadata"] = map[string]any{"labels": map[string]any{"team": "apps"}}
		st.AddObject(replacement)
	case "different source revision":
		stores[manifest.TreeBase].SetArtifact(inputID, &store.SourceArtifact{FS: files, Revision: "older"})
	case "non-static output":
		record.StaticOutput = false
	case "missing file coordinate":
		record.File = ""
	case "unresolved read":
		record.ContentRoots = map[manifest.NamedResource][]string{inputID: {"link/../payload"}}
	case "changed declaration":
		stores[manifest.TreeBase].SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
			LocalPath: facts.context.Root, FS: fstest.MapFS{"unrelated.yaml": {}, "old.yaml": {}},
		})
	}
	facts.index.Records[id], facts.occurrences[id] = record, occurrence
	changes, err := change.Qualify(t.Context(), nil, change.SourcePair{Stores: stores}, manifest.WorkScope{})
	assert.NoError(t, err)
	if tc.name == "changed declaration" {
		peerRecord := record
		peerRecord.File = "old.yaml"
		peer := &manifest.DiscoveryIndex{
			Records:         map[manifest.NamedResource]manifest.DiscoveryRecord{id: peerRecord},
			CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
			Declarations: map[manifest.NamedResource]map[string]manifest.FileDeclarations{
				manifest.BootstrapSourceID: {
					"old.yaml": {Complete: true, Objects: map[manifest.NamedResource]string{id: "before"}},
				},
			},
		}
		facts.index.CompleteSources = peer.CompleteSources
		facts.index.Declarations = map[manifest.NamedResource]map[string]manifest.FileDeclarations{
			manifest.BootstrapSourceID: {
				record.File: {Complete: true, Objects: map[manifest.NamedResource]string{id: "after"}},
			},
		}
		changes, err = changes.WithReaders(t.Context(), [2]*manifest.DiscoveryIndex{peer, &facts.index},
			[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{})
		assert.NoError(t, err)
	}
	assert.Equal(t, changes.PendingSourceOwner(source, tc.side), manifest.NamedResource{})
	if tc.name == "stale source" {
		_, snapshots, _ := changes.ForSource(inputID)
		stale := *snapshots[tc.side]
		stale.Revision = "different"
		st.SetArtifact(inputID, &stale)
	}
	facts.changes = changes
	c := solverClosure(t, st, facts)
	c.context = facts.context
	c.proposal.Request.Changes, c.proposal.Request.construction = changes, true
	c.proposal.preparation = new(Preparation)
	t.Cleanup(c.proposal.preparation.Retire)
	c.states[source] = admissionState{productionsKnown: true, productions: []manifest.Production{production}}
	counted := &materializerStatusReads{Store: st, owner: id}
	c.objects = counted
	var want []Demand
	if tc.name == "unresolved read" {
		want = append(want, Demand{Source: inputID, Root: "link/../payload", History: true})
	}
	if tc.wantDemand {
		want = append(want, Demand{Identity: id, Materialize: true})
	}
	assert.Equal(t, c.initialSourceChanged(source), false)
	assert.Equal(t, c.initialSourceChanged(source), false)
	if tc.name == "unresolved read" {
		demands := slices.Collect(c.proposal.Demands())
		for i := range demands {
			demand := &demands[i]
			*demand = Demand{
				Identity:    demand.Identity,
				Source:      demand.Source,
				Root:        demand.Root,
				History:     demand.History,
				Materialize: demand.Materialize,
			}
		}
		assert.Diff(t, demands, want)
		assert.Equal(t, c.states[id].affectedKnown, false)
		assert.Equal(t, counted.reads, 1)
	} else {
		assert.Diff(t, slices.Collect(c.proposal.Demands()), want)
		assert.Equal(t, counted.reads, len(want))
	}
	if tc.laterDemand {
		comparison := change.Comparison{Known: true, Changed: true}
		c.prepareMaterializer(id, comparison, false)
		c.prepareMaterializer(id, comparison, false)
		assert.Diff(t, slices.Collect(c.proposal.Demands()), []Demand{{Identity: id, Materialize: true}})
		assert.Equal(t, counted.reads, 1)
	}
}
