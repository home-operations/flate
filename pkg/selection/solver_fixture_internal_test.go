package selection

import (
	"context"
	"path/filepath"
	"testing"

	_ "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type solverMissingPolicy struct {
	allowed bool
}

//nolint:gocritic // The fixture matches the policy value contract.
func (p *solverMissingPolicy) Missing(manifest.Reference, store.ReferenceFacts) (bool, bool, bool) {
	return p != nil && p.allowed, false, false
}

//nolint:gocritic // Test requests keep independent snapshots.
func (s *Session) prepareTest(ctx context.Context, request PrepareRequest) (*Admission, error) {
	if request.Missing == nil {
		request.Missing = (*solverMissingPolicy)(nil)
	}
	return s.Prepare(ctx, request)
}

//nolint:gocritic // Test requests keep independent snapshots.
func (s *Session) refreshHistoryTest(
	ctx context.Context,
	previous *Admission,
	request PrepareRequest,
) (*Admission, error) {
	if request.Missing == nil {
		request.Missing = (*solverMissingPolicy)(nil)
	}
	return s.RefreshHistory(ctx, previous, request)
}

type solverFacts struct {
	store        *store.Store
	reader       admissionReader
	context      SourceContext
	changes      *change.Set
	index        manifest.DiscoveryIndex
	candidates   map[manifest.NamedResource]store.Child
	occurrences  map[manifest.NamedResource]OccurrenceFacts
	roots        map[manifest.NamedResource]RootEvidence
	producers    map[manifest.BaseManifest]ProducerFacts
	sources      map[manifest.NamedResource]SourceFacts
	inputs       map[manifest.NamedResource]*store.SourceArtifact
	declarations map[manifest.NamedResource]manifest.BaseManifest
}

func (f *solverFacts) SelectionIndex() *manifest.DiscoveryIndex { return &f.index }
func (f *solverFacts) Occurrence(id manifest.NamedResource) (OccurrenceFacts, bool) {
	occurrence, ok := f.occurrences[id]
	return occurrence, ok
}

func (f *solverFacts) AdmissionCandidate(id manifest.NamedResource) store.Child {
	return f.candidates[id]
}

//nolint:gocritic // The fixture implements the census method exactly.
func (f *solverFacts) OccurrenceRoots(id manifest.NamedResource) (manifest.NamedResource, []string) {
	occurrence, known := f.occurrences[id]
	if !known || !occurrence.CatalogKnown || len(occurrence.Roots) == 0 {
		return manifest.NamedResource{}, nil
	}
	source, _, _ := occurrence.SourcePath()
	return source, occurrence.Roots
}

func (f *solverFacts) PreparedObject(id manifest.NamedResource, _ *store.ReadSet) (store.Child, bool) {
	return f.roots[id].BuiltChild, f.roots[id].BuiltCurrent
}

func (f *solverFacts) RootEvidence(id manifest.NamedResource, _ *store.ReadSet) RootEvidence {
	return f.roots[id]
}

func (f *solverFacts) ProducerFacts(obj manifest.BaseManifest) ProducerFacts {
	if facts, known := f.producers[obj]; known {
		return facts
	}
	source, path, hasPath := obj.SourcePath()
	return ProducerFacts{
		Declaration: obj,
		Occurrence: OccurrenceFacts{
			Identity: obj.Named(),
			Authored: obj.Named(),
			Object:   obj,
			Catalog:  obj,
			Source:   source,
			Path:     path,
			HasPath:  hasPath,
		},
	}
}

func (f *solverFacts) SourceFacts(id manifest.NamedResource, reads *store.ReadSet) SourceFacts {
	facts := f.sources[id]
	facts.Comparison = f.changes
	if f.store == nil {
		return facts
	}
	facts.Context, facts.Source = f.context, id
	facts.Changes, facts.Snapshots, facts.Declarations = f.changes.ForSource(id)
	facts.Delta, facts.Files = facts.Changes, sourceFilesForTest(facts.Changes.Paths())
	facts.Origin, facts.ToIndex = f.context.Root, "."
	var objects interface {
		GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
		GetObject(manifest.NamedResource) manifest.BaseManifest
	} = f.store
	if reads != nil {
		objects = reads
	}
	if f.reader != nil {
		objects = f.reader
	}
	facts.Input, _ = objects.GetArtifact(id, store.ArtifactContent|store.ArtifactRevision).(*store.SourceArtifact)
	facts.Declaration = objects.GetObject(id)
	if facts.Path == "" {
		if facts.Input != nil {
			facts.Path = facts.Input.LocalPath
		} else {
			facts.Path = f.context.Root
		}
	}
	if facts.Input != nil {
		facts.Origin = facts.Input.OriginPath(facts.Input.LocalPath)
		facts.ToIndex, _ = filepath.Rel(f.context.Root, facts.Origin)
	}
	if reads != nil {
		previous, known := f.inputs[id]
		facts.Derived = !known || previous != facts.Input || f.declarations[id] != facts.Declaration
		if f.inputs == nil {
			f.inputs = make(map[manifest.NamedResource]*store.SourceArtifact)
			f.declarations = make(map[manifest.NamedResource]manifest.BaseManifest)
		}
		f.inputs[id], f.declarations[id] = facts.Input, facts.Declaration
	}

	return facts
}

func newSolverSession(t *testing.T, st *store.Store, changes *change.Set, request Request) *Session {
	t.Helper()
	facts := &solverFacts{store: st, changes: changes}
	if input, ok := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact); ok {
		facts.context.Root = input.LocalPath
	}
	session, err := New(st, facts, request)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func solverClosure(t *testing.T, st *store.Store, facts *solverFacts) admissionClosure {
	t.Helper()
	session, err := New(st, facts, Request{Changed: true})
	if err != nil {
		t.Fatal(err)
	}
	return admissionClosure{
		session: session, decision: session.Snapshot(), facts: facts, objects: st, missing: (*solverMissingPolicy)(nil),
		states: map[manifest.NamedResource]admissionState{}, proposal: new(Admission),
	}
}

//nolint:gocritic // Each proposal MUST retain an independent immutable request snapshot.
func prepareSolver(ctx context.Context, t *testing.T, session *Session, request PrepareRequest) (*Admission, error) {
	t.Helper()
	if request.Preparation == nil {
		var preparation *Preparation
		request.Preparation = &preparation
		t.Cleanup(func() { preparation.Retire() })
	}
	proposal, err := session.prepareTest(ctx, request)
	for err == nil && len(proposal.demands) != 0 {
		acquired := make([]Acquisition, 0, len(proposal.demands))
		for i := range proposal.demands {
			demand := proposal.demands[i]
			child := solverChild(session, demand.Identity)
			acquired = append(
				acquired,
				Acquisition{Demand: demand, Child: child, Available: child.Obj != nil, Reads: demand.Reads},
			)
		}
		request = proposal.Request
		request.Acquired = acquired
		next, nextErr := session.prepareTest(ctx, request)
		proposal.Retire()
		proposal, err = next, nextErr
	}
	return proposal, err
}

func (s *Session) commitSolver(
	ctx context.Context,
	t *testing.T,
	proposal *Admission,
	stage CommitStage,
) (*Admission, bool, error) {
	t.Helper()
	for {
		current, committed, err := s.Commit(ctx, proposal, stage)
		if err != nil || committed || current == nil || len(current.demands) == 0 {
			return current, committed, err
		}
		request := current.Request
		request.Acquired = make([]Acquisition, 0, len(current.demands))
		for i := range current.demands {
			demand := current.demands[i]
			child := solverChild(s, demand.Identity)
			request.Acquired = append(
				request.Acquired,
				Acquisition{Demand: demand, Child: child, Available: child.Obj != nil, Reads: demand.Reads},
			)
		}
		next, err := prepareSolver(ctx, t, s, request)
		current.Retire()
		if err != nil {
			return nil, false, err
		}
		proposal = next
	}
}

func evaluateSolver(t *testing.T, c *admissionClosure, request *PrepareRequest) (*Admission, error) {
	t.Helper()
	facts := c.facts.(*solverFacts)
	c.context, c.changes = facts.context, facts.changes
	request.Changes = facts.changes
	if request.Missing == nil {
		request.Missing = c.missing
	}
	c.missing = request.Missing
	if len(request.Generation) == 0 {
		request.Generation = c.generation
	}
	c.generation, c.owner, c.history = request.Generation, request.Owner, request.History
	preparation := &Preparation{closure: *c}
	var reads store.ReadSet
	reads.Init(c.session.store, manifest.NamedResource{}, nil)
	t.Cleanup(reads.Retire)
	for {
		c.decision = c.session.Snapshot()
		c.proposal = &Admission{Request: *request, reads: &reads, preparation: preparation}
		c.proposal.Revision = c.session.Snapshot().version()
		c.resetDemand()
		proposal, err := c.evaluate(t.Context(), request)
		if err != nil || len(proposal.demands) == 0 {
			return proposal, err
		}
		acquired := make([]Acquisition, 0, len(proposal.demands))
		for i := range proposal.demands {
			demand := proposal.demands[i]
			child := facts.candidates[demand.Identity]
			if child.Obj == nil {
				child.Obj = c.session.store.GetDeclaration(demand.Identity)
			}
			acquired = append(
				acquired,
				Acquisition{Demand: demand, Child: child, Available: child.Obj != nil, Reads: &reads},
			)
		}
		preparation.acceptAcquired(acquired)
	}
}

func solverChild(session *Session, id manifest.NamedResource) store.Child {
	child := store.Child{Obj: session.store.GetDeclaration(id)}
	if child.Obj == nil {
		child = session.facts.(*solverFacts).candidates[id]
	}
	return child
}

func sourceFilesForTest(paths []string) []SourceFile {
	files := make([]SourceFile, len(paths))
	for i, file := range paths {
		files[i] = SourceFile{Path: file, Indexed: file}
	}
	return files
}
