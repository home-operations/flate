package selection

import (
	"iter"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestPreparation_CompletedAbsenceRetainsExactSupport(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	source := (&manifest.GitRepository{Name: "source", Namespace: "apps"}).Named()
	input := &store.SourceArtifact{Content: "same", Digest: "same", Revision: "before"}
	session.store.SetArtifact(source, input)
	var original, current store.ReadSet
	original.Init(session.store, manifest.NamedResource{}, nil)
	defer original.Retire()
	original.GetArtifact(source, store.ArtifactContent|store.ArtifactRevision)
	current.Init(session.store, manifest.NamedResource{}, nil)
	defer current.Retire()
	witness := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	demand := Demand{
		Identity: witness.Named(), Source: source, Root: "payload", Witness: witness, Artifact: input, Reads: &original,
		Occurrence: OccurrenceFacts{
			Declaration: OccurrenceID{Source: "source", File: "child.yaml", Document: 1}, Witness: witness,
		},
	}
	preparation := &Preparation{closure: admissionClosure{session: session}}
	defer preparation.Retire()
	preparation.acceptAcquired([]Acquisition{{Demand: demand, Available: false, Reads: &original}})
	result, found := preparation.acquisition(&demand, &current)
	assert.Equal(t, found, true)
	assert.Equal(t, result.Available, false)
	assert.Equal(t, result.Reads == &current, true)
	assert.Equal(t, result.Demand.Reads == &current, true)
	assert.Equal(t, current.Valid(), true)
	result, found = preparation.acquisition(&demand, &current)
	assert.Equal(t, found, true)
	assert.Equal(t, result.Available, false)
	other := demand
	other.Source = testutil.KustomizationID("apps", "other-source")
	_, found = preparation.acquisition(&other, &current)
	assert.Equal(t, found, false)
	other = demand
	other.Occurrence.Witness = witness.Clone()
	_, found = preparation.acquisition(&other, &current)
	assert.Equal(t, found, false)
	other = demand
	other.Occurrence.Declaration.Item = 1
	_, found = preparation.acquisition(&other, &current)
	assert.Equal(t, found, false)
	replacement := *input
	replacement.Revision = "after"
	session.store.SetArtifact(source, &replacement)
	assert.Equal(t, current.Valid(), false)
	_, found = preparation.acquisition(&demand, &current)
	assert.Equal(t, found, false)
	assert.Equal(t, len(preparation.acquired), 0)
}

func TestPreparation_RetireReleasesCapturedState(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	var preparation *Preparation
	proposal, err := session.Prepare(t.Context(), PrepareRequest{Preparation: &preparation})
	assert.NoError(t, err)
	proposal.Retire()
	preparation.Retire()
	assert.Equal(t, preparation.evidence == nil && preparation.closure.session == nil, true)
}

func TestSession_RefreshHistoryGuardsAttemptedAbsence(t *testing.T) {
	t.Parallel()
	st := store.New()
	input := &store.SourceArtifact{Content: "same", Digest: "same", Revision: "before"}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./before"}
	st.AddObject(owner)
	session := newSolverSession(t, st, nil, Request{Changed: true})
	previous := &Admission{Needed: []manifest.NamedResource{owner.Named()}}
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	request := PrepareRequest{Preparation: &preparation}
	proposal, err := session.RefreshHistory(t.Context(), previous, request)
	assert.NoError(t, err)
	demands := slices.Collect(proposal.Demands())
	assert.Equal(t, len(demands), 1)
	assert.Equal(t, demands[0].Artifact, input)
	assert.Equal(t, demands[0].Witness, manifest.BaseManifest(owner))
	request = proposal.Request
	request.History = map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}{owner.Named(): {}}
	request.Acquired = []Acquisition{{Demand: demands[0], Reads: demands[0].Reads}}
	next, err := session.RefreshHistory(t.Context(), previous, request)
	proposal.Retire()
	assert.NoError(t, err)
	assert.Equal(t, len(slices.Collect(next.Demands())), 0)
	assert.Diff(t, next.Needed, previous.Needed)
	request = next.Request
	request.Acquired = nil
	replacement := *input
	replacement.Revision = "after"
	st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	proposal, err = session.RefreshHistory(t.Context(), previous, request)
	next.Retire()
	assert.NoError(t, err)
	demands = slices.Collect(proposal.Demands())
	assert.Equal(t, len(demands), 1)
	assert.Equal(t, demands[0].Artifact, &replacement)
	request = proposal.Request
	request.Acquired = []Acquisition{{Demand: demands[0], Reads: demands[0].Reads}}
	next, err = session.RefreshHistory(t.Context(), previous, request)
	proposal.Retire()
	assert.NoError(t, err)
	assert.Equal(t, len(slices.Collect(next.Demands())), 0)
	changed := owner.Clone()
	changed.Path = "./after"
	st.AddObject(changed)
	request = next.Request
	request.Acquired = nil
	proposal, err = session.RefreshHistory(t.Context(), previous, request)
	next.Retire()
	assert.NoError(t, err)
	defer proposal.Retire()
	demands = slices.Collect(proposal.Demands())
	assert.Equal(t, len(demands), 1)
	assert.Equal(t, demands[0].Witness, manifest.BaseManifest(changed))
	assert.Equal(t, session.Snapshot().Reconcile(owner.Named()), false)
}

func TestAdmission_EntryReceiptRequiresHistoricalReach(t *testing.T) {
	t.Parallel()
	entry := &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"}
	native := manifest.NewEntryKustomization(entry.Source, entry.Path, manifest.NamedResource{}).Named()
	previous := testutil.KustomizationID("apps", "previous")
	for _, tc := range []struct {
		name      string
		owner     manifest.NamedResource
		entry     *manifest.EntryApplication
		supported bool
		bound     bool
		want      bool
	}{
		{name: "no history", entry: entry},
		{name: "native history", owner: native, entry: entry, want: true},
		{name: "previous history", owner: previous, entry: entry, want: true},
		{name: "supported", owner: native, entry: entry, supported: true},
		{name: "bound", owner: native, entry: entry, bound: true, supported: true},
		{name: "unsupported bound", owner: native, entry: entry, bound: true, want: true},
		{name: "no entry", owner: native},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			session := newPublicationSession(t)
			var input *store.SourceArtifact
			producer := previous
			if tc.bound {
				producer = native
				input = &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}}
				session.store.SetArtifact(manifest.BootstrapSourceID, input)
				session.store.AddObject(
					manifest.NewEntryKustomization(entry.Source, entry.Path, manifest.NamedResource{}),
				)
			}
			route := &EntryRoute{Application: tc.entry, Previous: previous, Bound: tc.bound}
			if tc.supported {
				guard := &store.ReadSet{}
				guard.Init(session.store, previous, nil)
				guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
				t.Cleanup(func() { session.store.Retire(guard) })
				route.Producer, route.Reads = producer, guard
			}
			session.PublishEntry(route)
			request := PrepareRequest{}
			if tc.owner != (manifest.NamedResource{}) {
				evidence := manifest.SourceEvidence{Path: "entry/child.yaml"}
				if input != nil {
					evidence.Source = input.HistoryIdentity(manifest.BootstrapSourceID)
				}
				request.HistoryOwners = map[manifest.NamedResource][]manifest.SourceEvidence{tc.owner: {evidence}}
			}
			proposal, err := session.Prepare(t.Context(), request)
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, proposal.EntryReceipt != nil, tc.want)
			if tc.bound && tc.supported {
				assert.Equal(t, len(proposal.Request.HistoryOwners), 1)
				assert.Equal(t, len(slices.Collect(proposal.Demands())), 0)
				assert.Equal(t, slices.Contains(proposal.Ordinary, native), false)
				assert.Equal(t, len(request.HistoryOwners), 1)
			}
			_, committed, err := session.Commit(t.Context(), proposal, Construction)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			assert.Equal(t, proposal.EntryReceipt != nil, tc.want)
		})
	}
}

func TestPreparation_RootResolutionFollowsSourceGeneration(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	source := (&manifest.GitRepository{Name: "source", Namespace: "apps"}).Named()
	input := &store.SourceArtifact{Content: "before", FS: fstest.MapFS{}}
	session.store.SetArtifact(source, input)
	sibling := &manifest.ConfigMap{Name: "sibling", Namespace: "apps"}
	session.store.AddObject(sibling)
	var original, current store.ReadSet
	original.Init(session.store, manifest.NamedResource{}, nil)
	defer original.Retire()
	original.GetArtifact(source, store.ArtifactContent)
	original.GetObject(sibling.Named())
	demand := Demand{Source: source, Root: "app/../shared", History: true, Artifact: input, Reads: &original}
	preparation := &Preparation{}
	preparation.acceptAcquired([]Acquisition{{Demand: demand, Root: "shared", Available: true, Reads: &original}})
	session.store.AddObject(manifest.NewConfigMap(sibling.Named(), map[string]any{"value": "changed"}))
	assert.Equal(t, original.Valid(), false)
	current.Init(session.store, manifest.NamedResource{}, nil)
	defer current.Retire()
	assert.Equal(t, current.GetArtifact(source, store.ArtifactContent) == input, true)
	result, known := preparation.acquisition(&demand, &current)
	assert.Equal(t, known, true)
	assert.Equal(t, result.Root, "shared")
	assert.Equal(t, current.Valid(), true)
	replacement := &store.SourceArtifact{Content: "after", FS: fstest.MapFS{}}
	session.store.SetArtifact(source, replacement)
	assert.Equal(t, current.Valid(), false)
	demand.Artifact = replacement
	_, known = preparation.acquisition(&demand, &current)
	assert.Equal(t, known, false)
}
