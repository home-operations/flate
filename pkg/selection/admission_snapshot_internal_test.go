package selection

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type countedAdmissionFacts struct {
	census

	roots       map[manifest.NamedResource]int
	occurrences map[manifest.NamedResource]int
	sources     map[manifest.NamedResource]int
	onRoot      func()
}

func TestAdmission_ClosureFactsFollowEmits(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	st.AddObject(parent)
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{parent.Named()}})
	evidence := session.facts.(*solverFacts)
	evidence.roots = make(map[manifest.NamedResource]RootEvidence)
	facts := &countedAdmissionFacts{
		census: session.facts, roots: map[manifest.NamedResource]int{}, sources: map[manifest.NamedResource]int{},
	}
	session.facts = facts
	_, retry := publishDecision(t, session, new(Admission{
		Needed: []manifest.NamedResource{parent.Named()}, Selected: []manifest.NamedResource{parent.Named()},
		Dirty: []manifest.NamedResource{parent.Named()}, Ordinary: []manifest.NamedResource{parent.Named()},
	}), session.Snapshot().version())
	assert.Equal(t, retry, false)
	var reads store.ReadSet
	reads.Init(st, parent.Named(), nil)
	defer reads.Close()
	var work manifest.WorkRecorder
	reads.SetWork(&work)
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	const emits = 32
	for i := range emits {
		child := &manifest.Kustomization{Name: fmt.Sprintf("child-%d", i), Namespace: "apps"}
		evidence.roots[child.Named()] = RootEvidence{
			Identity: child.Named(), Captured: true,
			Scope: RootScopeFacts{NamespaceEligible: true, Candidate: true},
		}
		request := PrepareRequest{
			Preparation: &preparation, Owner: parent.Named(), Reads: &reads,
			Generation: []store.Child{{Obj: child}},
		}
		proposal, err := session.Prepare(t.Context(), request)
		assert.NoError(t, err)
		_, committed, err := session.Commit(t.Context(), proposal, Execution)
		assert.NoError(t, err)
		assert.Equal(t, committed, true)
		assert.Equal(t, session.Snapshot().Output(child.Named()), true)
		assert.Equal(t, facts.roots[child.Named()], 1)
		assert.Equal(t, len(preparation.closure.states) <= 4, true)
		assert.Equal(t, facts.sources[manifest.BootstrapSourceID], 1)
		clear(facts.roots)
		proposal, err = session.Prepare(t.Context(), request)
		assert.NoError(t, err)
		assert.Equal(t, proposal.reused, true)
		assert.Equal(t, len(facts.roots), 0)
		proposal.Retire()
		clear(facts.sources)
	}
	assert.Equal(t, work.Scope().Admission().Counts.OwnerPrepares, uint64(emits))
}

func (f *countedAdmissionFacts) RootEvidence(id manifest.NamedResource, reads *store.ReadSet) RootEvidence {
	f.roots[id]++
	if f.onRoot != nil {
		onRoot := f.onRoot
		f.onRoot = nil
		onRoot()
	}
	return f.census.RootEvidence(id, reads)
}

func TestAdmission_MembershipChangeUsesSuccessorSnapshot(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	st.AddObject(parent)
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{parent.Named()}})
	_, retry := publishDecision(t, session, new(Admission{
		Needed:   []manifest.NamedResource{parent.Named(), child.Named()},
		Ordinary: []manifest.NamedResource{parent.Named()},
	}), session.Snapshot().version())
	assert.Equal(t, retry, false)
	before := session.Snapshot()
	facts := &countedAdmissionFacts{census: session.facts, roots: map[manifest.NamedResource]int{}}
	facts.onRoot = func() {
		_, retry := publishDecision(t, session, new(Admission{
			Needed: []manifest.NamedResource{parent.Named()}, Selected: []manifest.NamedResource{parent.Named()},
			Dirty: []manifest.NamedResource{parent.Named()}, Ordinary: []manifest.NamedResource{parent.Named()},
		}), session.Snapshot().version())
		assert.Equal(t, retry, false)
	}
	session.facts = facts
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	var reads store.ReadSet
	reads.Init(st, parent.Named(), nil)
	reads.RequireQualification(child.Named())
	defer reads.Close()
	proposal, err := session.Prepare(t.Context(), PrepareRequest{
		Preparation: &preparation, Owner: parent.Named(), Reads: &reads,
		Generation: []store.Child{{Obj: child}},
	})
	assert.NoError(t, err)
	assert.Equal(t, facts.onRoot == nil, true)
	assert.Equal(t, proposal.Revision, before.version())
	assert.Equal(t, slices.Contains(proposal.Selected, parent.Named()), false)
	_, committed, err := session.Commit(t.Context(), proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, session.Snapshot().Output(child.Named()), true)
}

func TestAdmission_UnchangedClosureReusesSnapshot(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"required", "emitted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}},
			)
			object := &manifest.Kustomization{Name: "app", Namespace: "apps", Path: "app"}
			st.AddObject(object)
			session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{object.Named()}})
			facts := &countedAdmissionFacts{census: session.facts, roots: map[manifest.NamedResource]int{}}
			session.facts = facts
			var preparation *Preparation
			defer func() { preparation.Retire() }()
			var reads store.ReadSet
			reads.Init(st, object.Named(), nil)
			defer reads.Close()
			request := PrepareRequest{Preparation: &preparation, Reads: &reads}
			switch mode {
			case "required":
				reads.RequireData(object.Named())
			case "emitted":
				request.Generation = []store.Child{{Obj: object}}
			}
			proposal, err := session.Prepare(t.Context(), request)
			assert.NoError(t, err)
			_, committed, err := session.Commit(t.Context(), proposal, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			assert.Equal(t, session.Snapshot().Output(object.Named()), mode != "required")
			assert.Equal(t, facts.roots[object.Named()], 1)
			clear(facts.roots)
			_, retry := publishDecision(t, session, new(Admission{
				Needed: []manifest.NamedResource{{Kind: manifest.KindConfigMap, Name: "unrelated"}},
			}), session.Snapshot().version())
			assert.Equal(t, retry, false)
			proposal, err = session.Prepare(t.Context(), request)
			assert.NoError(t, err)
			assert.Equal(t, proposal.reused, true)
			assert.Equal(t, len(facts.roots), 0)
			assert.Equal(t, slices.Contains(proposal.Needed, object.Named()), true)
			_, committed, err = session.Commit(t.Context(), proposal, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
		})
	}
}

func TestAdmission_RootSnapshotRetainsCurrentDemand(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "child", Namespace: "apps"}
	facts := &solverFacts{store: st, roots: map[manifest.NamedResource]RootEvidence{
		id: {Identity: id, Captured: true, Scope: RootScopeFacts{NamespaceEligible: true, Candidate: true}},
	}}
	c := solverClosure(t, st, facts)
	reads := &admissionArtifactReads{admissionReader: st}
	c.objects = reads
	assert.Equal(t, c.rootEvidence(id).Entry, false)
	before := reads.calls
	c.add(id, admissionRole{needed: true})
	assert.Equal(t, c.rootEvidence(id).Entry, true)
	assert.Equal(t, reads.calls, before)
}

func TestAdmission_InvocationSeesNewObjects(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
	st.AddObject(first)
	session := newSolverSession(t, st, nil, Request{Changed: true})
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	request := PrepareRequest{Preparation: &preparation, Invocation: true}
	proposal, err := session.Prepare(t.Context(), request)
	assert.NoError(t, err)
	_, committed, err := session.Commit(t.Context(), proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	late := &manifest.Kustomization{Name: "late", Namespace: "apps"}
	st.AddObject(late)
	proposal, err = session.Prepare(t.Context(), request)
	assert.NoError(t, err)
	assert.Equal(t, proposal.reused, false)
	assert.Equal(t, slices.Contains(proposal.Selected, late.Named()), true)
	_, committed, err = session.Commit(t.Context(), proposal, Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	proposal, err = session.Prepare(t.Context(), PrepareRequest{Preparation: &preparation})
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, proposal.reused, false)
	assert.Equal(t, len(proposal.Needed), 0)
}

func TestAdmission_IncomingDeclarationKeepsAcceptedProducer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		incoming, changed bool
	}{
		{name: "stored"},
		{name: "arriving", incoming: true},
		{name: "changed", incoming: true, changed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}},
			)
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "app"}
			st.AddObject(parent)
			_, _, output := st.GetEmission(parent.Named())
			output.Children, output.Complete = []store.Child{{Obj: child}}, true
			st.RecordEmitted(parent.Named(), &output, nil)
			output.Finish()
			session := newSolverSession(
				t,
				st,
				nil,
				Request{Changed: true, Roots: []manifest.NamedResource{child.Named()}},
			)
			_, retry := publishDecision(t, session, new(Admission{
				Needed: []manifest.NamedResource{parent.Named(), child.Named()},
			}), session.Snapshot().version())
			assert.Equal(t, retry, false)
			incoming := child.Clone()
			if tc.changed {
				incoming.Path = "changed"
			}
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			reads.RequireQualification(child.Named())
			defer reads.Close()
			request := PrepareRequest{Reads: &reads}
			if tc.incoming {
				request.Generation = []store.Child{{Obj: incoming}}
			}
			proposal, err := session.Prepare(t.Context(), request)
			assert.NoError(t, err)
			defer proposal.Retire()
			evidence := proposal.preparation.closure.declarationEvidence(child.Named())
			assert.Equal(t, evidence.accepted, !tc.changed)
			assert.Equal(t, slices.Contains(proposal.Ordinary, parent.Named()), tc.changed)
		})
	}
}

func (f *countedAdmissionFacts) Occurrence(id manifest.NamedResource) (OccurrenceFacts, bool) {
	if f.occurrences != nil {
		f.occurrences[id]++
	}
	return f.census.Occurrence(id)
}

func (f *countedAdmissionFacts) SourceFacts(id manifest.NamedResource, reads *store.ReadSet) SourceFacts {
	if reads != nil && f.sources != nil {
		f.sources[id]++
	}
	return f.census.SourceFacts(id, reads)
}

func TestAdmission_FrozenCatalogSharedAcrossEmits(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	st.AddObject(parent)
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{parent.Named()}})
	evidence := session.facts.(*solverFacts)
	evidence.context.Frozen = true
	evidence.index.Records = make(map[manifest.NamedResource]manifest.DiscoveryRecord)
	evidence.occurrences = make(map[manifest.NamedResource]OccurrenceFacts)
	for i := range 64 {
		obj := &manifest.ConfigMap{Name: fmt.Sprintf("unrelated-%d", i), Namespace: "apps"}
		record := manifest.DiscoveryRecord{Declaration: obj}
		evidence.index.Records[obj.Named()] = record
		evidence.occurrences[obj.Named()] = OccurrenceFacts{
			Identity: obj.Named(), Authored: obj.Named(), Catalog: obj, Record: record, CatalogKnown: true,
		}
	}
	facts := &countedAdmissionFacts{
		census: evidence, roots: map[manifest.NamedResource]int{},
		occurrences: map[manifest.NamedResource]int{}, sources: map[manifest.NamedResource]int{},
	}
	session.facts = facts
	_, retry := publishDecision(t, session, new(Admission{
		Needed: []manifest.NamedResource{parent.Named()}, Selected: []manifest.NamedResource{parent.Named()},
		Dirty: []manifest.NamedResource{parent.Named()}, Ordinary: []manifest.NamedResource{parent.Named()},
	}), session.Snapshot().version())
	assert.Equal(t, retry, false)
	var reads store.ReadSet
	reads.Init(st, parent.Named(), nil)
	defer reads.Close()
	for i := range 4 {
		child := &manifest.Kustomization{Name: fmt.Sprintf("child-%d", i), Namespace: "apps"}
		proposal, err := session.Prepare(t.Context(), PrepareRequest{
			Owner: parent.Named(), Reads: &reads, Generation: []store.Child{{Obj: child}},
		})
		assert.NoError(t, err)
		assert.Equal(t, facts.sources[manifest.BootstrapSourceID], 1)
		clear(facts.sources)
		_, committed, err := session.Commit(t.Context(), proposal, Execution)
		assert.NoError(t, err)
		assert.Equal(t, committed, true)
	}
	for id := range evidence.index.Records {
		assert.Equal(t, facts.occurrences[id], 1)
	}
}

func TestAdmission_QualificationPromotionPreservesInputChange(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed_%t", changed), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}},
			)
			source := &manifest.OCIRepository{Name: "chart", Namespace: "apps"}
			consumer := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
			consumer.ChartRef = &helmv2.CrossNamespaceSourceReference{
				Kind: manifest.KindOCIRepository, Name: source.Name,
			}
			st.AddObject(source)
			st.AddObject(consumer)
			session := newSolverSession(t, st, nil, Request{Changed: true})
			_, retry := publishDecision(t, session, new(Admission{
				Needed: []manifest.NamedResource{source.Named(), consumer.Named()},
			}), session.Snapshot().version())
			assert.Equal(t, retry, false)
			update := &Admission{Ordinary: []manifest.NamedResource{source.Named()}}
			if changed {
				update.Dirty = []manifest.NamedResource{source.Named()}
			}
			_, retry = publishDecision(t, session, update, session.Snapshot().version())
			assert.Equal(t, retry, false)
			assert.Equal(t, session.Snapshot().Changed(source.Named()), true)
			proposal, err := session.Prepare(t.Context(), PrepareRequest{Generation: []store.Child{{Obj: consumer}}})
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, slices.Contains(proposal.Selected, consumer.Named()), changed)
			assert.Equal(t, slices.Contains(proposal.Dirty, consumer.Named()), changed)
		})
	}
}

func TestAdmission_HistoryRefreshReleasesUnsupportedRouteDemand(t *testing.T) {
	t.Parallel()
	session := newPublicationSession(t)
	st := session.store
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	producer := &manifest.HelmRelease{Name: "generator", Namespace: "apps"}
	st.AddObject(producer)
	application := &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"}
	previous := &Admission{
		Needed: []manifest.NamedResource{producer.Named()},
		entry:  &EntryRoute{Application: application},
	}
	var support store.ReadSet
	support.Init(st, manifest.BootstrapSourceID, nil)
	support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	defer support.Close()
	session.PublishEntry(&EntryRoute{
		Application: application, Producer: producer.Named(), Root: producer.Named(), Reads: &support, Bound: true,
	})
	proposal, err := session.RefreshHistory(
		t.Context(), previous, PrepareRequest{Invocation: true, Changes: change.NewSet(nil)},
	)
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, proposal.entrySupported(), true)
	assert.Equal(t, slices.Contains(proposal.Needed, producer.Named()), false)
}

func TestAdmission_EntrySourceRefreshPrecedesMaterialization(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"pending", "unchanged", "changed", "failed"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			input := &store.SourceArtifact{LocalPath: root, Content: "chart", FS: fstest.MapFS{}}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			source := &manifest.GitRepository{Name: "self", Namespace: "apps"}
			producer := &manifest.HelmRelease{Name: "producer", Namespace: "apps"}
			credentials := &manifest.Secret{Name: "credentials", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{source, producer, credentials})
			var fetchReads store.ReadSet
			fetchReads.Init(st, source.Named(), nil)
			fetchReads.GetObject(credentials.Named())
			defer fetchReads.Close()
			st.SetArtifact(source.Named(), input.WithReads(&fetchReads))
			st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
			var guard store.ReadSet
			guard.Init(st, manifest.BootstrapSourceID, nil)
			guard.GetArtifact(source.Named(), store.ArtifactContent)
			defer guard.Close()
			replacement := *credentials
			replacement.Data = map[string]any{"token": "new"}
			st.AddObject(&replacement)
			assert.Equal(t, fetchReads.Valid(), false)
			switch state {
			case "unchanged", "changed":
				refreshed := *input
				if state == "changed" {
					refreshed.Content = "changed"
				}
				st.SetArtifact(source.Named(), &refreshed)
			case "failed":
				st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusFailed})
			}
			facts := &solverFacts{
				store: st, context: SourceContext{Root: root},
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					producer.Named(): {},
				}},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{
					producer.Named(): applicationOccurrence(producer, root),
				},
			}
			closure := solverClosure(t, st, facts)
			closure.context = facts.context
			closure.proposal.reads = &closure.proposal.owned
			closure.proposal.reads.Init(st, manifest.NamedResource{}, nil)
			defer closure.proposal.reads.Close()
			closure.objects = closure.proposal.reads
			closure.proposal.entry = &EntryRoute{
				Application: &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"},
				Producer:    producer.Named(), Reads: &guard,
			}
			closure.materializers(&materializerDemand{
				target: source.Named(), source: root, application: filepath.Join(root, "entry"), required: true,
			}, manifest.NamedResource{}, closure.materializerCandidates(false))
			assert.Equal(t, closure.role(producer.Named()).needed, state == "changed" || state == "failed")
			assert.Equal(t, closure.role(source.Named()).needed, state == "pending")
			assert.Equal(t, closure.role(source.Named()).ordinary, state == "pending")
		})
	}
}
