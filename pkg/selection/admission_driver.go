package selection

import (
	"context"
	"iter"
	"maps"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// AdmissionOptions MUST retain the request and original caller authority across retries.
type AdmissionOptions struct {
	Request      PrepareRequest
	Previous     *AdmissionResult
	Stage        CommitStage
	Authority    *store.ReadSet
	EntryHistory struct {
		Owner   manifest.NamedResource
		Records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
	}
}

// AdmissionResult owns immutable observations from accepted publication, beyond proposal retirement.
type AdmissionResult struct {
	Request      PrepareRequest
	EntryReceipt *manifest.EntryApplication
	Objects      []store.Child
	Demands      []Demand
	Absent       []manifest.NamedResource
	Needed       []manifest.NamedResource
	Selected     []manifest.NamedResource
	Dirty        []manifest.NamedResource
	Ordinary     []manifest.NamedResource
	Blocked      []string
	Revision     uint64
	Namespaces   []string

	entry         *EntryRoute
	entryReads    *store.ReadSet
	producers     []producerObligation
	producerOwner *producerObligation
}

// Admit MUST capture accepted observations before retiring the proposal.
//
//nolint:cyclop,funlen,gocognit,gocritic,gocyclo // One owner MUST retain acquisitions and first-attempt presence through publication retries.
func (s *Session) Admit(ctx context.Context, options AdmissionOptions) (AdmissionResult, bool, error) {
	request := options.Request
	local := request.Preparation == nil
	var preparation *Preparation
	if local {
		request.Preparation = &preparation
		defer func() { preparation.Retire() }()
	}
	var proposal *Admission
	defer func() { proposal.Retire() }()
	var proof *store.ReadSet
	defer func() { s.store.Retire(proof) }()
	ownedHistory := false
	insertHistory := func(owner manifest.NamedResource, records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], complete bool) {
		if !ownedHistory {
			request.History = maps.Clone(request.History)
			ownedHistory = true
		}
		if request.History == nil {
			request.History = map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{}
		}
		request.History[owner] = struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{Records: records, Complete: complete}
	}
	forgetInvalidHistory := func(acquired *Acquisition) bool {
		demand := &acquired.Demand
		if !demand.History || demand.Root != "" || acquired.Reads.Valid() && demand.Reads.Valid() {
			return false
		}
		if !ownedHistory {
			return true
		}
		for i := range request.Acquired {
			current := &request.Acquired[i]
			sameHistory := current.Demand.History && current.Demand.Root == "" &&
				current.Demand.Identity == demand.Identity
			if sameHistory && current.Reads.Valid() && current.Demand.Reads.Valid() {
				return true
			}
		}
		if entry := options.EntryHistory; entry.Owner == demand.Identity {
			insertHistory(entry.Owner, entry.Records, true)
		} else if original, supplied := options.Request.History[demand.Identity]; supplied {
			insertHistory(demand.Identity, original.Records, original.Complete)
		} else {
			delete(request.History, demand.Identity)
		}
		return true
	}

	if entry := options.EntryHistory; entry.Owner != (manifest.NamedResource{}) {
		insertHistory(entry.Owner, entry.Records, true)
	}
	var absent map[manifest.NamedResource]struct{}
	var publication uint64
	var publishing *Preparation
	defer func() {
		if publishing != nil {
			publishing.closure.publication = 0
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return AdmissionResult{}, false, err //nolint:wrapcheck // Cancellation preserves the caller's error identity.
		}
		if !options.Authority.Valid() {
			return AdmissionResult{}, false, nil
		}
		if options.Authority != nil && (proof == nil || !proof.Valid()) {
			s.store.Retire(proof)
			proof = s.admissionProof(options.Authority)
			request.Reads = proof
		}
		// Rejected history MUST leave derivation before its successor computes membership.
		if current := *request.Preparation; current != nil && current.closure.session == s {
			for key := range current.acquired {
				acquired := current.acquired[key]
				if forgetInvalidHistory(&acquired) {
					delete(current.acquired, key)
				}
			}
		}
		for i := range request.Acquired {
			forgetInvalidHistory(&request.Acquired[i])
		}
		var next *Admission
		var err error
		if options.Previous != nil {
			next, err = s.refreshHistory(ctx, options.Previous, request)
		} else {
			next, err = s.Prepare(ctx, request)
		}
		proposal.Retire()
		proposal = next
		if err != nil {
			return AdmissionResult{}, false, err
		}
		if !s.guardCurrent(proposal) {
			if options.Authority == nil && !request.Reads.Valid() {
				return AdmissionResult{}, false, nil
			}
			continue
		}
		request = proposal.Request
		request.Acquired = nil
		for i := range proposal.demands {
			demand := &proposal.demands[i]
			result := Acquisition{Demand: *demand, Reads: demand.Reads}
			switch {
			case demand.History && demand.Root == "":
				history := request.History[demand.Identity]
				if request.ReadHistory != nil {
					history.Records, history.Complete = request.ReadHistory(ctx, demand)
				}
				insertHistory(demand.Identity, history.Records, history.Complete)
				result.Available = history.Records != nil
			case demand.History:
				result.Root, result.Available, err = s.facts.ResolveRoot(ctx, demand.Reads, demand.Source, demand.Root)
			case demand.Occurrence.Identity != (manifest.NamedResource{}):
				result.Child, err = s.facts.OccurrenceObject(ctx, &demand.Occurrence, demand.Artifact)
				result.Available = result.Child.Obj != nil
			case options.Stage == Execution:
			default:
				continue
			}
			if err != nil {
				return AdmissionResult{}, false, err
			}
			request.Acquired = append(request.Acquired, result)
		}
		if len(request.Acquired) != 0 {
			continue
		}
		if !options.Authority.Valid() {
			return AdmissionResult{}, false, nil
		}
		if publication == 0 {
			publishing = proposal.preparation
			publishing.publication++
			publication = publishing.publication
			publishing.closure.publication = publication
		}
		for i := range proposal.Objects {
			id := proposal.Objects[i].Obj.Named()
			state := proposal.preparation.closure.states[id]
			if state.publication == publication {
				continue
			}
			state.publication = publication
			proposal.preparation.closure.states[id] = state
			if s.store.GetObject(id) == nil {
				if absent == nil {
					absent = map[manifest.NamedResource]struct{}{}
				}
				absent[id] = struct{}{}
			}
		}
		decision, accepted := s.publishAdmission(ctx, proposal, options.Stage)
		if !accepted {
			continue
		}
		if !options.Authority.Valid() {
			return AdmissionResult{}, false, nil
		}
		result := proposal.result()
		result.Revision = decision.version()
		for i := range result.Objects {
			id := result.Objects[i].Obj.Named()
			if _, missing := absent[id]; missing {
				result.Absent = append(result.Absent, id)
			}
		}
		if local {
			result.Request.Preparation = nil
			result.Request.Acquired = nil
		}
		if options.Authority != nil {
			result.Request.Reads = options.Authority
		}
		proposal.accept(options.Stage, decision)
		return result, true, nil
	}
}

func (s *Session) publishAdmission(ctx context.Context, proposal *Admission, stage CommitStage) (*Decision, bool) {
	if ctx.Err() != nil || !s.guardCurrent(proposal) || stage == Execution && len(proposal.demands) != 0 {
		return nil, false
	}
	current := s.Snapshot()
	next, included := current.include(proposal, stage == Execution)
	if !s.publish(proposal, current, next) {
		return nil, false
	}
	for _, id := range included {
		s.store.SetIncluded(id, true, next.OrdinaryDemand(id))
	}
	return next, proposal.admitObjects(ctx, s)
}

func (s *Session) admissionProof(authority *store.ReadSet) *store.ReadSet {
	proof := &store.ReadSet{}
	proof.Init(s.store, authority.ID(), nil)
	proof.SetWork(authority.Work())
	for id, qualification := range authority.Required() {
		if qualification {
			proof.RequireQualification(id)
		} else {
			proof.RequireData(id)
		}
	}
	return proof
}

func (a *Admission) result() AdmissionResult {
	return AdmissionResult{
		Request: a.Request, EntryReceipt: a.EntryReceipt, Objects: a.Objects, Demands: a.demands,
		Needed: a.Needed, Selected: a.Selected, Dirty: a.Dirty, Ordinary: a.Ordinary,
		Blocked: a.Blocked, Revision: a.Revision, Namespaces: a.Namespaces,
		entry: a.entry, entryReads: a.entryReads, producers: a.producers, producerOwner: a.producerOwner,
	}
}
