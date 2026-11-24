package selection

import (
	"context"
	"errors"
	"iter"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
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

func TestPreparation_ReusesOccurrenceAfterUnrelatedPublication(t *testing.T) {
	t.Parallel()
	st := store.New()
	unrelated := &manifest.Kustomization{Name: "unrelated", Namespace: "apps"}
	object := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
	st.AddObject(unrelated)
	var before, after store.ReadSet
	before.Init(st, object.Named(), nil)
	after.Init(st, object.Named(), nil)
	t.Cleanup(before.Retire)
	t.Cleanup(after.Retire)
	before.GetObject(unrelated.Named())
	input := &store.SourceArtifact{Content: "same"}
	demand := Demand{
		Identity: object.Named(), Source: manifest.BootstrapSourceID, Reads: &before, Artifact: input,
		Occurrence: OccurrenceFacts{Identity: object.Named(), Authored: object.Named()},
	}
	st.DeleteObject(unrelated.Named())
	assert.Equal(t, before.Valid(), false)
	var p Preparation
	p.acceptAcquired([]Acquisition{{Demand: demand, Child: store.Child{Obj: object}, Available: true}})

	demand.Reads = &after
	result, known := p.acquisition(&demand, &after)
	assert.Equal(t, known, true)
	assert.Equal(t, result.Child.Obj, manifest.BaseManifest(object))
	assert.Equal(t, result.Reads, nil)
	assert.Equal(t, result.Demand.Reads, nil)
	demand.Artifact = &store.SourceArtifact{Content: "changed"}
	_, known = p.acquisition(&demand, &after)
	assert.Equal(t, known, false)
	demand.Artifact = input
	demand.Occurrence.Authored.Namespace = "other"
	_, known = p.acquisition(&demand, &after)
	assert.Equal(t, known, false)
}

func TestAdmission_RecapturesPrerequisitesAfterFixedOccurrence(t *testing.T) {
	t.Parallel()
	st := store.New()
	object := &manifest.Kustomization{Name: "dependency", Namespace: "apps", Path: "old"}
	id := object.Named()
	st.AddObject(object)
	var original, current, changed store.ReadSet
	original.Init(st, id, nil)
	current.Init(st, id, nil)
	changed.Init(st, id, nil)
	t.Cleanup(original.Retire)
	t.Cleanup(current.Retire)
	t.Cleanup(changed.Retire)
	original.GetObject(id)

	c := solverClosure(t, st, &solverFacts{})
	c.proposal.reads = &current
	request := PrepareRequest{Acquired: []Acquisition{{
		Demand: Demand{
			Occurrence: OccurrenceFacts{Identity: id},
			Artifact:   &store.SourceArtifact{Content: "original"},
		},
		Child: store.Child{Obj: object}, Available: true,
	}}}
	assert.Equal(t, c.matchesPreparation(&original, &request, true), true)
	replacement := object.Clone()
	replacement.Path = "new"
	st.AddObject(replacement)
	c.proposal.reads = &changed
	assert.Equal(t, c.matchesPreparation(&original, &request, true), false)
}

func TestPreparation_RetireReleasesCapturedState(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	var preparation *Preparation
	proposal, err := session.prepareTest(t.Context(), PrepareRequest{Preparation: &preparation})
	assert.NoError(t, err)
	proposal.Retire()
	preparation.Retire()
	assert.Equal(t, preparation.evidence == nil && preparation.closure.session == nil, true)
}

func TestSession_RefreshHistoryGuardsAttemptedAbsence(t *testing.T) {
	t.Parallel()
	st := store.New()
	input := &store.SourceArtifact{Provider: store.ProviderInput, Content: "same", Digest: "same", Revision: "before"}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./before"}
	st.AddObject(owner)
	session := newSolverSession(t, st, nil, Request{Changed: true})
	previous := &AdmissionResult{Needed: []manifest.NamedResource{owner.Named()}}
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	request := PrepareRequest{Preparation: &preparation}
	proposal, err := session.refreshHistoryTest(t.Context(), previous, request)
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
	next, err := session.refreshHistoryTest(t.Context(), previous, request)
	proposal.Retire()
	assert.NoError(t, err)
	assert.Equal(t, len(slices.Collect(next.Demands())), 0)
	assert.Diff(t, next.Needed, previous.Needed)
	request = next.Request
	request.Acquired = nil
	replacement := *input
	replacement.Revision = "after"
	st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	proposal, err = session.refreshHistoryTest(t.Context(), previous, request)
	next.Retire()
	assert.NoError(t, err)
	demands = slices.Collect(proposal.Demands())
	assert.Equal(t, len(demands), 1)
	assert.Equal(t, demands[0].Artifact, &replacement)
	request = proposal.Request
	request.Acquired = []Acquisition{{Demand: demands[0], Reads: demands[0].Reads}}
	next, err = session.refreshHistoryTest(t.Context(), previous, request)
	proposal.Retire()
	assert.NoError(t, err)
	assert.Equal(t, len(slices.Collect(next.Demands())), 0)
	changed := owner.Clone()
	changed.Path = "./after"
	st.AddObject(changed)
	request = next.Request
	request.Acquired = nil
	proposal, err = session.refreshHistoryTest(t.Context(), previous, request)
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
				input = &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: t.TempDir(), FS: fstest.MapFS{}}
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
					evidence.Source = input.HistoryIdentity()
				}
				request.HistoryOwners = map[manifest.NamedResource][]manifest.SourceEvidence{tc.owner: {evidence}}
			}
			proposal, err := session.prepareTest(t.Context(), request)
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, proposal.EntryReceipt != nil, tc.want)
			if tc.bound && tc.supported {
				assert.Equal(t, len(proposal.Request.HistoryOwners), 1)
				assert.Equal(t, len(slices.Collect(proposal.Demands())), 0)
				assert.Equal(t, slices.Contains(proposal.Ordinary, native), false)
				assert.Equal(t, len(request.HistoryOwners), 1)
			}
			_, committed, err := session.publishProposal(t.Context(), proposal, Construction)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			assert.Equal(t, proposal.EntryReceipt != nil, tc.want)
		})
	}
}

func TestAdmission_EntryReceiptReachesIndexedWorkloadHistory(t *testing.T) {
	t.Parallel()
	for _, fixture := range []struct {
		name, file         string
		history, generated bool
		want               bool
	}{
		{name: "entry", file: "entry/workload.yaml", history: true, generated: true, want: true},
		{name: "unrelated", file: "other/workload.yaml", history: true, generated: true},
		{name: "no evidence", file: "entry/workload.yaml", generated: true},
		{name: "other application", file: "entry/workload.yaml", history: true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			workload := &manifest.Kustomization{Name: "workload", Namespace: "apps", Path: "./workload"}
			occurrence := applicationOccurrence(workload, root)
			occurrence.Declaration.File = fixture.file
			facts := &solverFacts{
				store: st, context: SourceContext{Root: root, EntryPath: filepath.Join(root, "entry")},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{workload.Named(): occurrence},
			}
			session, err := New(st, facts, Request{Changed: true})
			assert.NoError(t, err)
			entry := &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"}
			session.PublishEntry(&EntryRoute{
				Application: entry, Previous: testutil.KustomizationID("apps", "previous"),
				PreviousGeneratedEntry: fixture.generated,
			})
			var history []manifest.SourceEvidence
			if fixture.history {
				history = []manifest.SourceEvidence{{Path: "workload/changed.yaml"}}
			}
			proposal, err := session.prepareTest(t.Context(), PrepareRequest{
				HistoryOwners: map[manifest.NamedResource][]manifest.SourceEvidence{workload.Named(): history},
			})
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, proposal.EntryReceipt != nil, fixture.want)
		})
	}
}

func TestAdmit_OrderingComparisonAcquiresResolvedRoots(t *testing.T) {
	t.Parallel()
	const unresolved = "infra/../shared"
	for _, tc := range []struct {
		name      string
		side      manifest.TreeSide
		resolved  string
		stale     bool
		settled   bool
		cancel    bool
		affected  bool
		wantCalls int
	}{
		{name: "base component edit", side: manifest.TreeBase, resolved: "shared", affected: true, wantCalls: 1},
		{name: "head component edit", side: manifest.TreeHead, resolved: "shared", affected: true, wantCalls: 1},
		{name: "resolved outside edit", resolved: "other", wantCalls: 1},
		{name: "completed unavailable root", affected: true, wantCalls: 1},
		{name: "discard stale generation", resolved: "other", stale: true, wantCalls: 2},
		{name: "discard settled stale generation", resolved: "other", settled: true, affected: true, wantCalls: 2},
		{name: "canceled lookup", cancel: true, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := manifest.BootstrapSourceID
			st := store.New()
			root := t.TempDir()
			trees := [2]*store.SourceArtifact{
				{LocalPath: root, Content: "base", FS: fstest.MapFS{"shared/file.yaml": {Data: []byte("before")}}},
				{LocalPath: root, Content: "head", FS: fstest.MapFS{"shared/file.yaml": {Data: []byte("after")}}},
			}
			input := trees[tc.side]
			st.SetArtifact(source, input)
			owner := &manifest.Kustomization{Name: "ordering", Namespace: "apps", Path: "infra"}
			id := owner.Named()
			st.AddObject(owner)
			changes, err := change.Detect(ctx, trees[0], trees[1], change.DetectConfig{})
			assert.NoError(t, err)
			facts := orderingRootFacts{solverFacts: &solverFacts{
				store: st, changes: changes,
				context: SourceContext{Root: root, Side: tc.side, SideKnown: true},
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					id: {
						File:         "flux/ordering.yaml",
						ContentRoots: map[manifest.NamedResource][]string{source: {unresolved}},
					},
				}},
				sources: map[manifest.NamedResource]SourceFacts{source: {Paired: true}},
			}}
			replaceInput := func() {
				replacement := *input
				replacement.Content = "replacement"
				input = &replacement
				st.SetArtifact(source, input)
			}
			calls := 0
			facts.resolve = func(ctx context.Context, reads *store.ReadSet, actual manifest.NamedResource, path string) (string, bool, error) {
				calls++
				assert.Equal(t, path, unresolved)
				assert.Equal(t, actual, source)
				assert.Equal(t, reads.Valid(), true)
				assert.Equal(t, reads.GetArtifact(source, store.ArtifactContent) == input, true)
				switch {
				case tc.cancel:
					cancel()
					return "", false, ctx.Err()
				case tc.stale && calls == 1:
					replaceInput()
					return "shared", true, nil
				case tc.settled && calls == 2:
					return "shared", true, nil
				default:
					return tc.resolved, tc.resolved != "", nil
				}
			}
			session, err := New(st, &facts, Request{Changed: true})
			assert.NoError(t, err)
			before := session.Snapshot()
			request := PrepareRequest{Changes: changes, OrderingCandidates: []manifest.NamedResource{id}}
			var preparation *Preparation
			invalidated := false
			if tc.settled {
				request.Preparation = &preparation
				defer func() { preparation.Retire() }()
				facts.inspect = func() {
					c := &preparation.closure
					if invalidated || calls != 1 || c.evaluationContext != nil || !c.states[id].affectedKnown {
						return
					}
					assert.Equal(t, len(c.proposal.demands), 0)
					assert.Equal(t, c.states[id].changed, false)
					assert.Equal(t, c.proposal.reads.Valid(), true)
					replaceInput()
					assert.Equal(t, c.proposal.reads.Valid(), false)
					invalidated = true
				}
			}
			result, accepted, err := session.Admit(ctx, AdmissionOptions{Request: request, Stage: Execution})
			assert.Equal(t, invalidated, tc.settled)
			assert.Equal(t, calls, tc.wantCalls)
			assert.Equal(t, session.Snapshot() == before, true)
			if tc.cancel {
				assert.Equal(t, errors.Is(err, context.Canceled), true)
				assert.Equal(t, accepted, false)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, accepted, true)
			assert.Equal(t, result.OrderingChanged[id], tc.affected)
		})
	}
}

type orderingRootFacts struct {
	*solverFacts

	resolve func(context.Context, *store.ReadSet, manifest.NamedResource, string) (string, bool, error)
	inspect func()
}

func (f *orderingRootFacts) SelectionIndex() *manifest.DiscoveryIndex {
	if f.inspect != nil {
		f.inspect()
	}
	return f.solverFacts.SelectionIndex()
}

//nolint:gocritic // The fixture implements the census result signature.
func (f *orderingRootFacts) ResolveRoot(
	ctx context.Context, reads *store.ReadSet, source manifest.NamedResource, root string,
) (string, bool, error) {
	return f.resolve(ctx, reads, source, root)
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
	session.store.AddObject(
		&manifest.ConfigMap{
			Name:      sibling.Named().Name,
			Namespace: sibling.Named().Namespace,
			Data:      map[string]any{"value": "changed"},
		},
	)
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
