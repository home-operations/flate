package selection

import (
	"context"
	"hash/maphash"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Session publishes immutable decisions from guarded admission proposals.
type Session struct {
	store    *store.Store
	facts    census
	request  Request
	mu       sync.Mutex
	decision atomic.Pointer[Decision]
	entry    atomic.Pointer[EntryRoute]
}

// New returns nil for unrestricted full execution.
func New(st *store.Store, facts census, request Request) (*Session, error) {
	if !request.Changed &&
		(len(request.Roots) == 0 || request.includesRenderKinds(true) && request.Roots[0].Namespace == "") {
		return nil, nil //nolint:nilnil // A nil session is the unrestricted execution contract.
	}
	s := &Session{store: st, facts: facts, request: request}
	s.decision.Store(&Decision{seed: maphash.MakeSeed()})
	return s, nil
}

// Snapshot permits nil sessions and returns an immutable decision.
func (s *Session) Snapshot() *Decision {
	if s == nil {
		return nil
	}
	return s.decision.Load()
}

// DiscardConstruction ends an aborted invocation without changing accepted execution membership.
func (s *Session) DiscardConstruction() {
	if s == nil {
		return
	}
	s.mu.Lock()
	current := s.decision.Load()
	if !current.execution {
		s.decision.Store(&Decision{seed: current.seed, revision: current.revision + 1})
	}
	s.mu.Unlock()
}

// Include MUST precede an execution's capture of a newly arrived identity.
func (s *Session) Include(id manifest.NamedResource) {
	if s == nil {
		return
	}
	decision := s.Snapshot()
	s.store.SetIncluded(
		id,
		decision.execution && decision.Reconcile(id),
		decision.execution && decision.OrdinaryDemand(id),
	)
}

func (d *Decision) version() uint64 {
	if d == nil {
		return 0
	}
	return d.revision
}

// Admission is a temporary proposal computed outside execution-membership locks.
type Admission struct {
	Needed       []manifest.NamedResource
	Selected     []manifest.NamedResource
	Dirty        []manifest.NamedResource
	Ordinary     []manifest.NamedResource
	Objects      []store.Child
	Blocked      []string
	Revision     uint64
	Namespaces   []string
	EntryReceipt *manifest.EntryApplication
	entry        *EntryRoute
	entryReads   *store.ReadSet
	owned        store.ReadSet
	reads        *store.ReadSet
	preparation  *Preparation
	local        *Preparation
	Request      PrepareRequest
	stale        bool
	reused       bool
	sources      []manifest.NamedResource
	demands      []Demand
	producers    []producerObligation

	producerOwner *producerObligation
}

// PrepareRequest describes the evidence and selection intent for an admission proposal.
type PrepareRequest struct {
	Preparation       **Preparation
	Changes           *change.Set
	HistoryOwners     map[manifest.NamedResource][]manifest.SourceEvidence
	Acquired          []Acquisition
	AllowMissingKinds []string
	// Generation MUST remain immutable until its preparation retires.
	Generation []store.Child
	Owner      manifest.NamedResource
	Reads      *store.ReadSet
	Invocation bool
	History    map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}
}

// Preparation MUST remain with one logical admission owner until its final consumer.
type Preparation struct {
	closure   admissionClosure
	evidence  *store.ReadSet
	completed *Admission
	changes   *change.Set
	entry     *EntryRoute
	acquired  map[acquisitionKey]Acquisition
	matched   map[*store.ReadSet]bool

	producerOwner *producerObligation
}

// Retire MUST follow every active proposal and preserve accepted store borrowers.
func (p *Preparation) Retire() {
	if p == nil {
		return
	}
	if p.closure.session != nil {
		p.closure.session.store.Retire(p.evidence)
	}
	*p = Preparation{}
}

// CommitStage separates construction admission from execution membership.
type CommitStage uint8

const (
	// Construction admits dependencies without execution membership.
	Construction CommitStage = iota
	// Execution commits the settled invocation selection.
	Execution
)

// Retire ends proposal payload access, preserving borrowed guards and identity results.
func (a *Admission) Retire() {
	if a == nil || a.preparation == nil {
		return
	}
	preparation := a.preparation
	preparation.closure.session.store.Retire(a.reads)
	clear(preparation.matched)
	local := a.local != nil && a.Request.Preparation == nil
	if preparation.closure.proposal == a {
		preparation.closure.objects, preparation.closure.proposal = nil, nil
	}
	a.reads, a.preparation, a.sources, a.Objects, a.demands = nil, nil, nil, nil, nil
	a.Request = PrepareRequest{}
	if local {
		preparation.Retire()
		a.local = nil
	}
}

type admissionReader interface {
	SourceAdmission(manifest.NamedResource, manifest.NamedResource) bool
	Lookup(manifest.NamedResource) (manifest.BaseManifest, manifest.DeclarationPolicy)
	WorkScope() manifest.WorkScope
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetDeclaration(manifest.NamedResource) manifest.BaseManifest
	ReferenceFacts(manifest.NamedResource, *manifest.DiscoveryIndex, manifest.NamedResource) store.ReferenceFacts
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
	GetStatus(manifest.NamedResource) (store.StatusInfo, bool)
	Productions(manifest.NamedResource, *manifest.DiscoveryIndex) []manifest.Production
	IncomingProductions(store.Child, *manifest.DiscoveryIndex) []manifest.Production
	GetEmission(manifest.NamedResource) (manifest.BaseManifest, map[string]any, store.Emission)
	ListObjects(string, string) []manifest.BaseManifest
	ListProductionCandidates(
		string,
		string,
		*manifest.DiscoveryIndex,
		map[manifest.NamedResource]bool,
	) []manifest.BaseManifest
}

// Demand borrows its artifact and capture until the successor preparation consumes its acquisition.
type Demand struct {
	Identity   manifest.NamedResource
	Occurrence OccurrenceFacts
	Source     manifest.NamedResource
	Root       string
	Ordinary   bool
	History    bool
	Complete   bool
	Artifact   *store.SourceArtifact
	Reads      *store.ReadSet
	Witness    manifest.BaseManifest
}

// Acquisition borrows an exact driver's result until its preparation consumes it.
type Acquisition struct {
	Demand    Demand
	Child     store.Child
	Root      string
	Available bool
	Reads     *store.ReadSet
}

type acquisitionKey struct {
	identity, source   manifest.NamedResource
	occurrence         OccurrenceID
	root               string
	history            bool
	witness            manifest.BaseManifest
	declarationWitness any
	artifact           *store.SourceArtifact
}

func (d *Demand) key() acquisitionKey {
	return acquisitionKey{
		identity: d.Identity, source: d.Source, occurrence: d.Occurrence.Declaration,
		root: d.Root, history: d.History, witness: d.Witness, artifact: d.Artifact,
		declarationWitness: d.Occurrence.Witness,
	}
}

func (p *Preparation) acceptAcquired(acquired []Acquisition) {
	clear(p.matched)
	for i := range acquired {
		result := acquired[i]
		if result.Reads == nil {
			result.Reads = result.Demand.Reads
		}
		if !result.Reads.Valid() || !result.Demand.Reads.Valid() {
			continue
		}
		if p.acquired == nil {
			p.acquired = make(map[acquisitionKey]Acquisition, len(acquired))
		}
		p.acquired[result.Demand.key()] = result
	}
}

func (p *Preparation) acquisition(demand *Demand, reads *store.ReadSet) (Acquisition, bool) {
	key := demand.key()
	result, known := p.acquired[key]
	if !known {
		return Acquisition{}, false
	}
	if !result.Reads.Valid() || !result.Demand.Reads.Valid() {
		delete(p.acquired, key)
		return Acquisition{}, false
	}
	if result.Reads != reads {
		matched, captured := p.matched[result.Reads]
		if !captured {
			matched = result.Reads == nil || reads.Matches(result.Reads)
			if p.matched == nil {
				p.matched = make(map[*store.ReadSet]bool)
			}
			p.matched[result.Reads] = matched
		}
		if !matched {
			delete(p.acquired, key)
			return Acquisition{}, false
		}
	}
	result.Reads, result.Demand.Reads = reads, reads
	p.acquired[key] = result
	return result, true
}

// Demands MUST be copied before Commit or Retire ends the proposal lifetime.
func (a *Admission) Demands() iter.Seq[Demand] {
	return func(yield func(Demand) bool) {
		for i := range a.demands {
			if !yield(a.demands[i]) {
				return
			}
		}
	}
}

// Commit publishes guarded membership before admitting any object.
func (s *Session) Commit(ctx context.Context, proposal *Admission, stage CommitStage) (*Admission, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			proposal.Retire()
			return nil, false, err //nolint:wrapcheck // Cancellation preserves the caller's error identity.
		}
		if !s.guardCurrent(proposal) {
			proposal.Retire()
			return nil, false, nil
		}
		if stage == Execution && len(proposal.demands) != 0 {
			return proposal, false, nil
		}
		current := s.Snapshot()
		next, included := current.include(proposal, stage == Execution)
		if s.publish(proposal, current, next) {
			for _, id := range included {
				s.store.SetIncluded(id, true, next.OrdinaryDemand(id))
			}
			if proposal.admitObjects(ctx, s) {
				proposal.accept(stage, next.version())
				return proposal, true, nil
			}
		}
		nextProposal, err := s.retry(ctx, proposal)
		if err != nil || nextProposal == nil {
			return nil, false, err
		}
		proposal = nextProposal
	}
}

func (s *Session) publish(proposal *Admission, current, next *Decision) bool {
	s.mu.Lock()
	valid := proposal.reads.Valid() && s.guardCurrent(proposal) &&
		s.decision.Load() == current && proposal.Revision == current.version()
	if valid {
		s.decision.Store(next)
	}
	s.mu.Unlock()
	return valid
}

func (s *Session) retry(ctx context.Context, proposal *Admission) (*Admission, error) {
	if proposal.Request.Reads != nil && !proposal.Request.Reads.Valid() {
		proposal.Retire()
		return nil, nil //nolint:nilnil // A stale caller cannot authorize a retry proposal.
	}
	request := proposal.Request
	var local *Preparation
	if request.Preparation == nil && proposal.local != nil {
		local, proposal.local = proposal.local, nil
		request.Preparation = &local
	}
	next, err := s.Prepare(ctx, request)
	proposal.Retire()
	if err != nil {
		local.Retire()
		return nil, err
	}
	if local != nil {
		next.local = local
		next.Request.Preparation = nil
	}
	return next, nil
}

func (a *Admission) accept(stage CommitStage, revision uint64) {
	a.Revision = revision
	if stage == Execution {
		a.preparation.completed = a
		a.preparation.changes = a.Request.Changes
		a.preparation.entry = a.entry
	}
	a.Retire()
}

func (s *Session) guardCurrent(a *Admission) bool {
	return !a.stale && (a.Request.Reads == nil || a.Request.Reads.Valid()) &&
		a.entry == s.entry.Load() &&
		a.entryReads.Valid()
}

func (a *Admission) admitObjects(ctx context.Context, session *Session) bool {
	if !a.reused {
		for i := range a.Objects {
			if ctx.Err() != nil || !a.reads.Valid() || !session.guardCurrent(a) {
				return false
			}
			a.reads.Admit(a.Objects[i], a.sources[i], manifest.NamedResource{})
		}
	}
	return ctx.Err() == nil && a.reads.Valid() && session.guardCurrent(a)
}

//nolint:gocritic // The queued demand retains an independent immutable snapshot.
func (c *admissionClosure) acquire(demand Demand) (Acquisition, bool) {
	demand.Reads = c.proposal.reads
	if result, known := c.proposal.preparation.acquisition(&demand, c.proposal.reads); known {
		return result, true
	}
	key := demand.key()
	for i := range c.proposal.demands {
		if c.proposal.demands[i].key() == key {
			return Acquisition{}, false
		}
	}
	c.proposal.demands = append(c.proposal.demands, demand)
	return Acquisition{}, false
}
