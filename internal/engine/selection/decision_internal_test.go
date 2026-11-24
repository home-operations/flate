package selection

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFilter_UniversalAndRestricted(t *testing.T) {
	t.Parallel()
	id := testutil.KustomizationID("apps", "app")
	var universal *Decision
	needed, selected, dirty, revision := universal.Reconcile(
		id,
	), universal.Output(
		id,
	), universal.Changed(
		id,
	), universal.version()
	assert.Equal(t, needed && selected && dirty, true)
	assert.Equal(t, revision, uint64(0))
	full, err := New(store.New(), &publicationFacts{}, Request{})
	assert.NoError(t, err)
	assert.FatalIf(t, full == nil, "unrestricted request has no session")
	decision := full.Snapshot()
	assert.FatalIf(t, decision == nil, "unrestricted session has no decision")
	assert.Equal(t, decision.roles(id), uint8(0))
	assert.Equal(t, decision.Reconcile(id), true)
	assert.Equal(t, decision.Output(id), true)
	assert.Equal(t, decision.Changed(id), true)
	assert.Equal(t, decision.OrdinaryDemand(id), true)
	restricted := newPublicationSession(t).Snapshot()
	assert.Equal(t, restricted != nil, true)
	assert.Equal(t, restricted.Reconcile(id), false)
}

func TestFilter_UnionRejectsStaleProposal(t *testing.T) {
	t.Parallel()
	selected := testutil.KustomizationID("apps", "selected")
	support := testutil.KustomizationID("inputs", "support")
	rejected := testutil.KustomizationID("apps", "rejected")
	f := newPublicationSession(t)
	added, retry := publishDecision(t, f,
		new(Admission{
			Needed:   []manifest.NamedResource{support, selected},
			Selected: []manifest.NamedResource{selected},
			Dirty:    []manifest.NamedResource{selected},
			Ordinary: slices.Concat([]manifest.NamedResource{support, selected}, []manifest.NamedResource{selected}),
		}),
		0,
	)
	want := []manifest.NamedResource{selected, support}
	slices.SortFunc(want, manifest.NamedResource.Compare)
	assert.Diff(t, added, want)
	assert.Equal(t, retry, false)
	snapshot := f.Snapshot()
	needed, chosen, dirty, revision := snapshot.Reconcile(
		support,
	), snapshot.Output(
		support,
	), snapshot.Changed(
		support,
	), snapshot.version()
	assert.Equal(t, needed && !chosen && !dirty, true)
	assert.Equal(t, revision, uint64(1))
	assert.Diff(t, f.Snapshot().namespaces(), map[string]struct{}{"apps": {}})
	added, retry = publishDecision(t, f,
		new(Admission{Needed: []manifest.NamedResource{rejected}, Ordinary: []manifest.NamedResource{rejected}}),
		0,
	)
	assert.Equal(t, retry, true)
	assert.Equal(t, len(added), 0)
	assert.Equal(t, f.Snapshot().Reconcile(rejected), false)
	added, retry = publishDecision(t, f,
		new(Admission{Selected: []manifest.NamedResource{support}, Ordinary: []manifest.NamedResource{support}}),
		revision,
	)
	assert.Equal(t, retry, false)
	assert.Equal(t, len(added), 0)
	snapshot = f.Snapshot()
	needed, chosen, dirty, revision = snapshot.Reconcile(
		support,
	), snapshot.Output(
		support,
	), snapshot.Changed(
		support,
	), snapshot.version()
	assert.Equal(t, needed && chosen && !dirty, true)
	assert.Equal(t, revision, uint64(2))
	assert.Diff(t, f.Snapshot().namespaces(), map[string]struct{}{"apps": {}, "inputs": {}})
	names := decisionNames(f.Snapshot())
	assert.Equal(t, int64(len(names)), int64(2))
	assert.Diff(t, names, []string{selected.String(), support.String()})
	hidden := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	_, retry = publishDecision(t, f,
		new(Admission{Selected: []manifest.NamedResource{hidden}, Ordinary: []manifest.NamedResource{hidden}}),
		revision,
	)
	assert.Equal(t, retry, false)
	assert.Diff(t, f.Snapshot().namespaces(), map[string]struct{}{"apps": {}, "inputs": {}})
}

func TestDecision_ImmutableSnapshots(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	id := testutil.KustomizationID("apps", "candidate")
	initial := session.Snapshot()
	_, retry := publishDecision(t, session, new(Admission{Needed: []manifest.NamedResource{id}}), initial.version())
	assert.Equal(t, retry, false)
	support := session.Snapshot()
	_, retry = publishDecision(t, session, new(Admission{Selected: []manifest.NamedResource{id}}), support.version())
	assert.Equal(t, retry, false)
	assert.Equal(t, initial.Reconcile(id), false)
	assert.Equal(t, support.Reconcile(id), true)
	assert.Equal(t, support.Output(id), false)
	assert.Equal(t, support.OrdinaryDemand(id), false)
	assert.Equal(t, session.Snapshot().Output(id), true)
	assert.Equal(t, session.Snapshot().OrdinaryDemand(id), true)
	assert.Equal(t, support.version(), uint64(1))
	assert.Equal(t, initial.version(), uint64(0))
}

func TestSession_StaleCaptureRejectsPublication(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	source := (&manifest.GitRepository{Name: "source", Namespace: "apps"}).Named()
	input := &store.SourceArtifact{Content: "same", Digest: "same", Revision: "before"}
	session.store.SetArtifact(source, input)
	var reads store.ReadSet
	reads.Init(session.store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	reads.GetArtifact(source, store.ArtifactContent|store.ArtifactRevision)
	child := &manifest.ConfigMap{Name: "child", Namespace: "apps"}
	before := session.Snapshot()
	proposal := publicationProposal(session, new(Admission{
		Needed: []manifest.NamedResource{child.Named()}, Selected: []manifest.NamedResource{child.Named()},
		Objects: []store.Child{{Obj: child}},
	}), before.version())
	proposal.Request.Reads = &reads
	var events atomic.Int64
	defer session.store.AddListener(
		store.EventObjectAdded,
		func(manifest.NamedResource, any) { events.Add(1) },
		false,
	)()
	replacement := *input
	replacement.Revision = "after"
	session.store.SetArtifact(source, &replacement)
	assert.Equal(t, reads.Valid(), false)
	_, committed, err := session.publishProposal(t.Context(), proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, false)
	assert.Equal(t, session.Snapshot() == before, true)
	assert.Equal(t, session.Snapshot().Reconcile(child.Named()), false)
	assert.Equal(t, events.Load(), int64(0))
	assert.Equal(t, session.store.GetObject(child.Named()), nil)
}

func TestSession_PublicationAcrossSelectionRevision(t *testing.T) {
	t.Parallel()
	for _, related := range []bool{false, true} {
		name := "unrelated selection"
		if related {
			name = "observed selection"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			child := &manifest.ConfigMap{Name: "child", Namespace: "apps"}
			session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{child.Named()}})
			before := session.Snapshot()
			_, retry := publishDecision(t, session, new(Admission{}), before.version())
			assert.Equal(t, retry, false)
			proposal, err := session.Prepare(t.Context(), PrepareRequest{
				Generation: []store.Child{{Obj: child}},
			})
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, len(proposal.demands), 0)
			_, observed := proposal.preparation.closure.states[child.Named()]
			assert.Equal(t, observed, true)
			id := testutil.KustomizationID("apps", "unrelated")
			if related {
				id = child.Named()
			}
			published := make(chan bool, 1)
			go func() {
				_, retry := publishDecision(t, session, new(Admission{Needed: []manifest.NamedResource{id}}),
					proposal.Revision)
				published <- !retry
			}()
			assert.Equal(t, <-published, true)
			_, committed := session.publishAdmission(t.Context(), proposal, Execution)
			assert.Equal(t, committed, !related)
			assert.Equal(t, session.Snapshot().Reconcile(id), true)
			assert.Equal(t, session.Snapshot().Output(child.Named()), !related)
		})
	}
}

func TestSession_ConcurrentMembershipBeforeArrival(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	present := &manifest.ConfigMap{Name: "required", Namespace: "apps"}
	child := &manifest.ConfigMap{Name: "child", Namespace: "apps"}
	session.store.AddObject(present)
	before := session.Snapshot()
	var notifications [2]atomic.Int64
	defer session.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		assert.Equal(t, session.Snapshot().Reconcile(id), true)
		if id == present.Named() {
			notifications[0].Add(1)
		} else if id == child.Named() {
			notifications[1].Add(1)
		}
	}, false)()
	updates := [2]*Admission{
		{Needed: []manifest.NamedResource{present.Named()}, Ordinary: []manifest.NamedResource{present.Named()}},
		{
			Needed: []manifest.NamedResource{child.Named()}, Selected: []manifest.NamedResource{child.Named()},
			Request: PrepareRequest{Generation: []store.Child{{}, {Obj: child}}},
		},
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range updates {
		workers.Go(func() {
			<-start
			_, retry := publishDecision(t, session, updates[i], before.version())
			if retry {
				_, retry = publishDecision(t, session, updates[i], session.Snapshot().version())
				assert.Equal(t, retry, false)
			}
		})
	}
	close(start)
	workers.Wait()
	assert.Equal(t, before.Reconcile(present.Named()), false)
	assert.Equal(t, before.Reconcile(child.Named()), false)
	assert.Equal(t, session.Snapshot().Reconcile(present.Named()), true)
	assert.Equal(t, session.Snapshot().Output(present.Named()), false)
	assert.Equal(t, session.Snapshot().Reconcile(child.Named()), true)
	assert.Equal(t, session.Snapshot().Output(child.Named()), true)
	assert.Equal(t, session.store.GetObject(child.Named()), nil)
	assert.Equal(t, notifications[0].Load(), int64(1))
	assert.Equal(t, notifications[1].Load(), int64(0))
	session.store.AddObject(child)
	assert.Equal(t, notifications[0].Load(), int64(1))
	assert.Equal(t, notifications[1].Load(), int64(1))
}

func TestSession_NoChangeFixpoint(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	id := testutil.KustomizationID("apps", "app")
	update := &Admission{Needed: []manifest.NamedResource{id}, Selected: []manifest.NamedResource{id}}
	_, retry := publishDecision(t, session, update, 0)
	assert.Equal(t, retry, false)
	before := session.Snapshot()
	included, retry := publishDecision(t, session, new(Admission{}), before.version())
	assert.Equal(t, retry, false)
	assert.Equal(t, len(included), 0)
	assert.Equal(t, session.Snapshot() == before, true)
	included, retry = publishDecision(t, session, new(Admission{
		Needed: []manifest.NamedResource{id}, Selected: []manifest.NamedResource{id},
	}), before.version())
	assert.Equal(t, retry, false)
	assert.Equal(t, len(included), 0)
	assert.Equal(t, session.Snapshot() == before, true)
}

func TestDecision_DirtyRefiresUnchanged(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	object := &manifest.Kustomization{Name: "candidate", Namespace: "apps"}
	id := object.Named()
	session.Include(id)
	session.store.AddObject(object)
	_, retry := publishDecision(t, session, new(Admission{
		Needed: []manifest.NamedResource{id}, Selected: []manifest.NamedResource{id},
		Ordinary: []manifest.NamedResource{id},
	}), session.Snapshot().version())
	assert.Equal(t, retry, false)
	before := session.Snapshot()
	assert.Equal(t, before.Changed(id), false)
	session.store.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged})
	var notifications atomic.Int64
	t.Cleanup(session.store.AddListener(store.EventStatusUpdated, func(changed manifest.NamedResource, payload any) {
		assert.Equal(t, changed, id)
		assert.Equal(t, session.Snapshot().Changed(id), true)
		assert.Equal(t, payload.(store.StatusInfo).Outcome(), store.OutcomePending)
		notifications.Add(1)
	}, false))
	_, retry = publishDecision(t, session, new(Admission{Dirty: []manifest.NamedResource{id}}), before.version())
	assert.Equal(t, retry, false)
	assert.Equal(t, before.Changed(id), false)
	assert.Equal(t, session.Snapshot().Changed(id), true)
	status, found := session.store.GetStatus(id)
	assert.Equal(t, found, true)
	assert.Equal(t, status.Outcome(), store.OutcomePending)
	assert.Equal(t, notifications.Load(), int64(1))
	dirty := session.Snapshot()
	_, retry = publishDecision(t, session, new(Admission{Dirty: []manifest.NamedResource{id}}), dirty.version())
	assert.Equal(t, retry, false)
	assert.Equal(t, session.Snapshot() == dirty, true)
	assert.Equal(t, notifications.Load(), int64(1))
}

func TestDecision_FirstExecutionReplacesConstruction(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	provisional := testutil.KustomizationID("apps", "provisional")
	settled := testutil.KustomizationID("apps", "settled")
	provisionalObject := &manifest.Kustomization{Name: provisional.Name, Namespace: provisional.Namespace}
	settledObject := &manifest.Kustomization{Name: settled.Name, Namespace: settled.Namespace}
	for _, object := range []*manifest.Kustomization{provisionalObject, settledObject} {
		session.Include(object.Named())
		session.store.AddObject(object)
	}
	admitted := func(id manifest.NamedResource) bool {
		var reads store.ReadSet
		reads.Init(session.store, id, nil)
		defer session.store.Retire(&reads)
		return reads.Admitted()
	}
	var notifications atomic.Int64
	t.Cleanup(session.store.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) {
		notifications.Add(1)
	}, false))
	proposal := publicationProposal(session, new(Admission{
		Needed: []manifest.NamedResource{provisional}, Ordinary: []manifest.NamedResource{provisional},
		Selected: []manifest.NamedResource{provisional}, Dirty: []manifest.NamedResource{provisional},
		Request: PrepareRequest{Generation: []store.Child{{Obj: provisionalObject}}},
	}), 0)
	_, committed, err := session.publishProposal(t.Context(), proposal, Construction)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	construction := session.Snapshot()
	assert.Equal(t, construction.Reconcile(provisional), true)
	assert.Equal(t, construction.OrdinaryDemand(provisional), true)
	assert.Equal(t, construction.Output(provisional), false)
	assert.Equal(t, construction.Changed(provisional), false)
	assert.Equal(t, admitted(provisional), false)
	assert.Equal(t, notifications.Load(), int64(0))
	application := &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"}
	continuation, err := session.prepareTest(t.Context(), PrepareRequest{
		Generation: []store.Child{{Obj: provisionalObject, Entry: application}},
	})
	assert.NoError(t, err)
	assert.Equal(t, len(continuation.Needed), 0)
	_, committed, err = session.publishProposal(t.Context(), continuation, Construction)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, construction.Reconcile(provisional), true)
	assert.Equal(t, admitted(provisional), false)
	assert.Equal(t, notifications.Load(), int64(0))
	continuation.Retire()
	proposal = publicationProposal(session, new(Admission{
		Needed: []manifest.NamedResource{settled}, Selected: []manifest.NamedResource{settled},
		Dirty: []manifest.NamedResource{settled}, Ordinary: []manifest.NamedResource{settled},
	}), construction.version())
	_, committed, err = session.publishProposal(t.Context(), proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, session.Snapshot().Reconcile(provisional), false)
	assert.Equal(t, session.Snapshot().OrdinaryDemand(provisional), false)
	assert.Equal(t, session.Snapshot().Output(settled), true)
	assert.Equal(t, session.Snapshot().Changed(settled), true)
	assert.Equal(t, construction.Reconcile(provisional), true)
	assert.Equal(t, admitted(provisional), false)
	assert.Equal(t, admitted(settled), true)
	assert.Equal(t, notifications.Load(), int64(1))
	proposal = publicationProposal(
		session,
		new(Admission{Needed: []manifest.NamedResource{provisional}}),
		session.Snapshot().version(),
	)
	_, committed, err = session.publishProposal(t.Context(), proposal, Construction)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, session.Snapshot().execution, true)
	assert.Equal(t, session.Snapshot().Output(settled), true)
	assert.Equal(t, session.Snapshot().Reconcile(provisional), false)
	assert.Equal(t, admitted(provisional), false)
	assert.Equal(t, notifications.Load(), int64(1))
	proposal = publicationProposal(
		session,
		&Admission{Needed: []manifest.NamedResource{provisional}},
		session.Snapshot().version(),
	)
	_, committed, err = session.publishProposal(t.Context(), proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, session.Snapshot().Reconcile(provisional), true)
	assert.Equal(t, admitted(provisional), true)
	assert.Equal(t, notifications.Load(), int64(2))
	_, retry := publishDecision(
		t,
		session,
		new(Admission{Selected: []manifest.NamedResource{provisional}}),
		session.Snapshot().version(),
	)
	assert.Equal(t, retry, false)
	assert.Equal(t, session.Snapshot().Output(provisional), true)
	assert.Equal(t, session.Snapshot().Output(settled), true)

	t.Run("unrestricted inclusion", func(t *testing.T) {
		t.Parallel()
		session, err := New(store.New(), &publicationFacts{}, Request{})
		assert.NoError(t, err)
		assert.FatalIf(t, session == nil, "unrestricted request has no session")
		object := &manifest.Kustomization{Name: "included", Namespace: "apps"}
		var notifications atomic.Int64
		t.Cleanup(session.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
			assert.Equal(t, id, object.Named())
			notifications.Add(1)
		}, false))
		session.store.AddObject(object)
		assert.Equal(t, notifications.Load(), int64(1))
		for _, stage := range []CommitStage{Construction, Execution} {
			proposal := publicationProposal(session, &Admission{
				Needed: []manifest.NamedResource{object.Named()}, Ordinary: []manifest.NamedResource{object.Named()},
			}, session.Snapshot().version())
			_, committed, err := session.publishProposal(t.Context(), proposal, stage)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			beforeInclude := notifications.Load()
			session.Include(object.Named())
			var reads store.ReadSet
			reads.Init(session.store, object.Named(), nil)
			assert.Equal(t, reads.Admitted(), true)
			assert.Equal(t, reads.ControllerDemand(object.Named()), true)
			session.store.Retire(&reads)
			assert.Equal(t, session.Snapshot().Reconcile(object.Named()), true)
			assert.Equal(t, session.Snapshot().Output(object.Named()), true)
			assert.Equal(t, notifications.Load(), beforeInclude)
		}
	})

	t.Run("carried identity", func(t *testing.T) {
		t.Parallel()
		session := newPublicationSession(t)
		object := &manifest.Kustomization{Name: "carried", Namespace: "apps"}
		unselected := &manifest.Kustomization{Name: "unselected", Namespace: "apps"}
		session.store.AddObject(object)
		session.store.AddObject(unselected)
		session.Include(object.Named())
		var observation store.ReadSet
		observation.Init(session.store, unselected.Named(), nil)
		defer session.store.Retire(&observation)
		assert.Equal(t, observation.Admitted(), true)
		var notifications atomic.Int64
		defer session.store.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) {
			notifications.Add(1)
		}, false)()
		proposal := publicationProposal(session, &Admission{
			Needed: []manifest.NamedResource{object.Named()}, Ordinary: []manifest.NamedResource{object.Named()},
		}, 0)
		_, committed, err := session.publishProposal(t.Context(), proposal, Construction)
		assert.NoError(t, err)
		assert.Equal(t, committed, true)
		construction := session.Snapshot()
		session.Include(object.Named())
		var reads store.ReadSet
		reads.Init(session.store, object.Named(), nil)
		defer session.store.Retire(&reads)
		assert.Equal(t, reads.Admitted(), false)
		assert.Equal(t, construction.Reconcile(object.Named()), true)
		assert.Equal(t, notifications.Load(), int64(0))
		proposal = publicationProposal(session, &Admission{
			Needed: []manifest.NamedResource{object.Named()}, Ordinary: []manifest.NamedResource{object.Named()},
			Request: PrepareRequest{Invocation: true, Generation: []store.Child{{Obj: unselected}}},
		}, construction.version())
		_, committed, err = session.publishProposal(t.Context(), proposal, Execution)
		assert.NoError(t, err)
		assert.Equal(t, committed, true)
		assert.Equal(t, session.Snapshot().Reconcile(object.Named()), true)
		assert.Equal(t, construction.Reconcile(object.Named()), true)
		assert.Equal(t, notifications.Load(), int64(1))
		assert.Equal(t, observation.Valid(), true)
		var accepted store.ReadSet
		accepted.Init(session.store, object.Named(), nil)
		defer session.store.Retire(&accepted)
		assert.Equal(t, accepted.Admitted(), true)
		session.Include(unselected.Named())
		var excluded store.ReadSet
		excluded.Init(session.store, unselected.Named(), nil)
		defer session.store.Retire(&excluded)
		assert.Equal(t, excluded.Admitted(), false)
		assert.Equal(t, observation.Valid(), false)
		assert.Equal(t, notifications.Load(), int64(1))
	})
}

func TestSession_DiscardConstructionPreservesExecution(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		stage        CommitStage
		unrestricted bool
	}{
		{"construction", Construction, false},
		{"execution", Execution, false},
		{"unrestricted construction", Construction, true},
		{"unrestricted execution", Execution, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			session := newPublicationSession(t)
			if tc.unrestricted {
				var err error
				session, err = New(store.New(), &publicationFacts{}, Request{})
				assert.NoError(t, err)
				assert.FatalIf(t, session == nil, "unrestricted request has no session")
			}
			id := testutil.KustomizationID("apps", "candidate")
			proposal := publicationProposal(session, new(Admission{
				Needed: []manifest.NamedResource{id}, Selected: []manifest.NamedResource{id},
				Dirty: []manifest.NamedResource{id}, Ordinary: []manifest.NamedResource{id},
			}), 0)
			_, committed, err := session.publishProposal(t.Context(), proposal, tc.stage)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			accepted := session.Snapshot()
			stale := publicationProposal(session, new(Admission{}), accepted.version())
			defer stale.Retire()
			session.DiscardConstruction()
			current := session.Snapshot()
			assert.Equal(t, accepted.Reconcile(id), true)
			assert.Equal(t, current.Reconcile(id), tc.unrestricted || tc.stage == Execution)
			assert.Equal(t, current.Output(id), tc.unrestricted || tc.stage == Execution)
			assert.Equal(t, current.Changed(id), tc.unrestricted || tc.stage == Execution)
			assert.Equal(t, current.OrdinaryDemand(id), tc.unrestricted || tc.stage == Execution)
			assert.Equal(t, current == accepted, tc.stage == Execution)
			if tc.unrestricted {
				absent := testutil.KustomizationID("apps", "absent")
				assert.Equal(t, current.roles(absent), uint8(0))
				assert.Equal(t, current.Reconcile(absent), true)
				assert.Equal(t, current.Output(absent), true)
				assert.Equal(t, current.Changed(absent), true)
				assert.Equal(t, current.OrdinaryDemand(absent), true)
			}
			if tc.stage == Construction {
				assert.Equal(t, current.version() > accepted.version(), true)
				assert.Equal(t, session.publish(stale, accepted, accepted), false)
			}
		})
	}
}

func TestDecision_ReadinessDemand(t *testing.T) {
	t.Parallel()
	id := testutil.KustomizationID("apps", "candidate")
	for _, changed := range []bool{false, true} {
		name := "named"
		if changed {
			name = "changed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, err := New(
				store.New(),
				&publicationFacts{},
				Request{Changed: changed, Roots: []manifest.NamedResource{id}},
			)
			assert.NoError(t, err)
			decision, included := session.Snapshot().include(&Admission{Readiness: []manifest.NamedResource{id}}, true)
			var want []manifest.NamedResource
			if !changed {
				want = []manifest.NamedResource{id}
			}
			assert.Diff(t, included, want)
			assert.Equal(t, decision.Reconcile(id), !changed)
			assert.Equal(t, decision.ReadinessDemand(id), true)
			assert.Equal(t, decision.OrdinaryDemand(id), false)
			assert.Equal(t, decision.Output(id), false)
			assert.Equal(t, decision.reportsFailure(id), false)

			next, _ := decision.include(&Admission{Fatal: []manifest.NamedResource{id}}, true)
			assert.Equal(t, next.reportsFailure(id), true)
			assert.Equal(t, next.Output(id), false)
		})
	}
}

func TestSession_ConstructionPublishesReadySupportWithPendingDemand(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	id := testutil.KustomizationID("apps", "support")
	proposal := publicationProposal(session, new(Admission{Needed: []manifest.NamedResource{id}}), 0)
	proposal.demands = []Demand{{Identity: testutil.KustomizationID("apps", "pending")}}
	_, committed, err := session.publishProposal(t.Context(), proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, false)
	assert.Equal(t, session.Snapshot().Reconcile(id), false)
	_, committed, err = session.publishProposal(t.Context(), proposal, Construction)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, session.Snapshot().Reconcile(id), true)
	assert.Equal(t, session.Snapshot().Output(id), false)
}

func TestDecision_OrderingDeclarationAuthority(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	id := testutil.KustomizationID("apps", "prerequisite")
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	indexed := &manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "indexed"}
	effective := &manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "effective"}
	input := &store.SourceArtifact{Content: "revision"}
	evidence := orderingDeclaration{
		artifact: input,
		object:   indexed, source: manifest.BootstrapSourceID, owner: owner.Named(),
	}
	session.store.AddObject(owner)
	session.store.SetArtifact(manifest.BootstrapSourceID, input)
	session.store.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			id: {Productions: []manifest.Production{{Owner: owner.Named(), Source: manifest.BootstrapSourceID}}},
		}},
	)
	decision, _ := session.Snapshot().include(&Admission{
		Readiness:            []manifest.NamedResource{id},
		orderingDeclarations: map[manifest.NamedResource]orderingDeclaration{id: evidence},
	}, true)
	reader := testutil.KustomizationID("apps", "reader")
	var original, moved, replaced, published, withdrawn store.ReadSet
	original.Init(session.store, reader, nil)
	defer original.Retire()
	assert.Equal(t, decision.OrderingDeclaration(&original, id, false), manifest.BaseManifest(indexed))
	assert.Equal(t, decision.OrderingDeclaration(&original, id, true), nil)

	source := &manifest.GitRepository{Name: "replacement", Namespace: "apps"}
	session.store.AddObject(source)
	session.store.SetArtifact(source.Named(), input)
	evidence.source = source.Named()
	decision, _ = decision.include(&Admission{
		orderingDeclarations: map[manifest.NamedResource]orderingDeclaration{id: evidence},
	}, true)
	moved.Init(session.store, reader, nil)
	defer moved.Retire()
	assert.Equal(t, decision.OrderingDeclaration(&moved, id, false), manifest.BaseManifest(indexed))
	assert.Equal(t, moved.Valid(), true)

	session.store.SetArtifact(source.Named(), &store.SourceArtifact{Content: "new-revision"})
	assert.Equal(t, original.Valid(), true)
	assert.Equal(t, moved.Valid(), false)
	replaced.Init(session.store, reader, nil)
	defer replaced.Retire()
	assert.Equal(t, decision.OrderingDeclaration(&replaced, id, false), nil)

	session.store.RecordEmitted(owner.Named(), &store.Emission{
		Children: []store.Child{{Obj: effective}}, Complete: true,
	}, nil)
	assert.Equal(t, original.Valid(), false)
	published.Init(session.store, reader, nil)
	defer published.Retire()
	assert.Equal(t, decision.OrderingDeclaration(&published, id, false), manifest.BaseManifest(effective))
	assert.Equal(t, decision.OrderingDeclaration(&published, id, true), manifest.BaseManifest(effective))

	session.store.RecordEmitted(owner.Named(), &store.Emission{Complete: true}, nil)
	assert.Equal(t, published.Valid(), false)
	withdrawn.Init(session.store, reader, nil)
	defer withdrawn.Retire()
	assert.Equal(t, decision.OrderingDeclaration(&withdrawn, id, false), nil)
}

func TestAdmission_ReusesIndexedOrderingDeclaration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		fresh bool
	}{
		{name: "same declaration"},
		{name: "retained catalog declaration"},
		{name: "projected catalog declaration"},
		{name: "source replacement", fresh: true},
		{name: "moved declaration", fresh: true},
		{name: "different list item", fresh: true},
		{name: "different authored identity", fresh: true},
		{name: "different witness", fresh: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			source := manifest.BootstrapSourceID
			root := t.TempDir()
			input := &store.SourceArtifact{LocalPath: root, Content: "old"}
			id := testutil.KustomizationID("apps", "dependency")
			object := &manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "full-spec"}
			coordinate := OccurrenceID{Source: root, File: "dependency.yaml"}
			occurrence := OccurrenceFacts{Declaration: coordinate, Identity: id, Authored: id, Mounted: input}
			switch tc.name {
			case "retained catalog declaration":
				occurrence.Parsed = object
			case "projected catalog declaration":
				occurrence.Authored.Namespace = "authored"
				occurrence.Parsed = object.WithNamespace(occurrence.Authored.Namespace)
			case "source replacement":
				occurrence.Mounted = &store.SourceArtifact{LocalPath: root, Content: "new"}
			case "moved declaration":
				occurrence.Declaration.Document++
			case "different list item":
				occurrence.Declaration.Item++
			case "different authored identity":
				occurrence.Authored.Namespace = "other"
			case "different witness":
				occurrence.Witness = object
			}
			st.SetArtifact(source, occurrence.Mounted)
			facts := &solverFacts{store: st, occurrences: map[manifest.NamedResource]OccurrenceFacts{id: occurrence}}
			c := solverClosure(t, st, facts)
			c.proposal.preparation = &Preparation{}
			if occurrence.Parsed == nil {
				c.decision, _ = c.decision.include(&Admission{
					orderingDeclarations: map[manifest.NamedResource]orderingDeclaration{
						id: {
							declarationIdentity: declarationIdentity{
								artifact: input, occurrence: coordinate, authored: id,
							},
							object: object, source: source,
						},
					},
				}, true)
			}

			result := c.indexedOrderingDeclaration(t.Context(), manifest.NamedResource{}, &occurrence)
			if tc.fresh {
				assert.Equal(t, result.object, nil)
				assert.Equal(t, len(c.proposal.demands), 1)
				return
			}
			assert.Equal(t, manifest.Equal(result.object, object), true)
			assert.Equal(t, len(c.proposal.demands), 0)
		})
	}
}

func TestDecision_OrderingExpressionChange(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	reader := testutil.KustomizationID("apps", "reader")
	dependency := testutil.KustomizationID("apps", "dependency")
	old := store.OrderingEdge{Target: dependency, ReadyExpr: "true"}
	current := store.OrderingEdge{Target: dependency, ReadyExpr: "self.metadata.name == dep.metadata.name"}
	before, _ := session.Snapshot().include(&Admission{
		Ordinary:  []manifest.NamedResource{reader},
		Readiness: []manifest.NamedResource{dependency},
		ordering:  map[manifest.NamedResource][]store.OrderingEdge{reader: {old}},
	}, true)
	var reads store.ReadSet
	reads.Init(session.store, reader, nil)
	defer reads.Retire()
	reads.AssessOrdering()
	reads.RequireOrdering(&current)
	assert.Equal(t, before.MatchesOrdering(&reads), false)
	after, _ := before.include(&Admission{
		ordering: map[manifest.NamedResource][]store.OrderingEdge{reader: {current}},
	}, true)
	assert.Equal(t, after.MatchesOrdering(&reads), true)
	assert.Equal(t, before.MatchesOrdering(&reads), false)
	assert.Equal(t, after.version(), before.version()+1)
	assert.Equal(t, after.OrdinaryDemand(dependency), false)
}

func TestDecision_OrderingDemandWithdrawal(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	first := testutil.KustomizationID("apps", "first")
	second := testutil.KustomizationID("apps", "second")
	middle := testutil.KustomizationID("infra", "middle")
	leaf := testutil.KustomizationID("infra", "leaf")
	decision, _ := session.Snapshot().include(&Admission{
		Ordinary: []manifest.NamedResource{first, second},
		Needed:   []manifest.NamedResource{middle, leaf},
		ordering: map[manifest.NamedResource][]store.OrderingEdge{
			first: {{Target: middle}}, second: {{Target: middle}}, middle: {{Target: leaf}}, leaf: {{Target: middle}},
		},
	}, true)
	for _, id := range []manifest.NamedResource{middle, leaf} {
		assert.Equal(t, decision.ReadinessDemand(id), true)
		assert.Equal(t, decision.Output(id), false)
	}
	next, _ := decision.include(&Admission{
		ordering: map[manifest.NamedResource][]store.OrderingEdge{first: nil},
	}, true)
	assert.Equal(t, next.ReadinessDemand(leaf), true)
	final, retired := next.include(&Admission{
		ordering: map[manifest.NamedResource][]store.OrderingEdge{second: nil},
	}, true)
	assert.Diff(t, retired, []manifest.NamedResource{leaf, middle})
	for _, id := range retired {
		assert.Equal(t, final.ReadinessDemand(id), false)
		assert.Equal(t, next.ReadinessDemand(id), true)
	}
}
