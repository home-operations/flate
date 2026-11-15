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

type admissionDriver struct {
	session      *Session
	options      *AdmissionOptions
	request      PrepareRequest
	ownedHistory bool
	proposal     *Admission
	proof        *store.ReadSet
	absent       map[manifest.NamedResource]struct{}
	publication  uint64
	publishing   *Preparation
}

// Admit MUST capture accepted observations before retiring the proposal.
//
//nolint:gocritic // One owner MUST retain acquisitions and first-attempt presence through publication retries.
func (s *Session) Admit(ctx context.Context, options AdmissionOptions) (AdmissionResult, bool, error) {
	driver := admissionDriver{session: s, options: &options, request: options.Request}
	local := driver.request.Preparation == nil
	var preparation *Preparation
	if local {
		driver.request.Preparation = &preparation
		defer func() { preparation.Retire() }()
	}
	defer driver.retire()
	if entry := options.EntryHistory; entry.Owner != (manifest.NamedResource{}) {
		driver.insertHistory(entry.Owner, entry.Records, true)
	}
	for {
		switch err := ctx.Err(); {
		case err != nil:
			return AdmissionResult{}, false, err
		case !options.Authority.Valid():
			return AdmissionResult{}, false, nil
		}
		if err := driver.prepare(ctx); err != nil {
			return AdmissionResult{}, false, err
		}
		if !s.guardCurrent(driver.proposal) {
			if options.Authority == nil && !driver.request.Reads.Valid() {
				return AdmissionResult{}, false, nil
			}
			continue
		}
		if err := driver.acquire(ctx); err != nil {
			return AdmissionResult{}, false, err
		}
		switch {
		case len(driver.request.Acquired) != 0:
			continue
		case !options.Authority.Valid():
			return AdmissionResult{}, false, nil
		}
		driver.capturePresence()
		decision, accepted := s.publishAdmission(ctx, driver.proposal, options.Stage)
		switch {
		case !accepted:
			continue
		case !options.Authority.Valid():
			return AdmissionResult{}, false, nil
		}
		return driver.accept(decision, local), true, nil
	}
}

func (d *admissionDriver) retire() {
	if d.publishing != nil {
		d.publishing.closure.publication = 0
	}
	d.session.store.Retire(d.proof)
	d.proposal.Retire()
}

func (d *admissionDriver) insertHistory(
	owner manifest.NamedResource,
	records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
	complete bool,
) {
	if !d.ownedHistory {
		previous := d.request.History
		d.request.History = make(map[manifest.NamedResource]struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}, len(previous)+1)
		maps.Copy(d.request.History, previous)
		d.ownedHistory = true
	}
	d.request.History[owner] = struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}{Records: records, Complete: complete}
}

func (d *admissionDriver) forgetInvalidHistory(acquired *Acquisition) bool {
	demand := &acquired.Demand
	if !demand.History || demand.Root != "" || acquired.Reads.Valid() && demand.Reads.Valid() {
		return false
	}
	if !d.ownedHistory {
		return true
	}
	for i := range d.request.Acquired {
		current := &d.request.Acquired[i]
		sameHistory := current.Demand.History && current.Demand.Root == "" &&
			current.Demand.Identity == demand.Identity
		if sameHistory && current.Reads.Valid() && current.Demand.Reads.Valid() {
			return true
		}
	}
	original, supplied := d.options.Request.History[demand.Identity]
	if entry := d.options.EntryHistory; entry.Owner == demand.Identity {
		original.Records, original.Complete, supplied = entry.Records, true, true
	}
	if supplied {
		d.insertHistory(demand.Identity, original.Records, original.Complete)
	} else {
		delete(d.request.History, demand.Identity)
	}
	return true
}

func (d *admissionDriver) prepare(ctx context.Context) error {
	if authority := d.options.Authority; authority != nil && (d.proof == nil || !d.proof.Valid()) {
		d.session.store.Retire(d.proof)
		d.proof = d.session.admissionProof(authority)
		d.request.Reads = d.proof
	}
	// Rejected history MUST leave derivation before its successor computes membership.
	if current := *d.request.Preparation; current != nil && current.closure.session == d.session {
		for key := range current.acquired {
			acquired := current.acquired[key]
			if d.forgetInvalidHistory(&acquired) {
				delete(current.acquired, key)
			}
		}
	}
	for i := range d.request.Acquired {
		d.forgetInvalidHistory(&d.request.Acquired[i])
	}
	var next *Admission
	var err error
	if d.options.Previous != nil {
		next, err = d.session.refreshHistory(ctx, d.options.Previous, d.request)
	} else {
		next, err = d.session.Prepare(ctx, d.request)
	}
	d.proposal.Retire()
	d.proposal = next
	return err
}

func (d *admissionDriver) acquire(ctx context.Context) error {
	d.request = d.proposal.Request
	d.request.Acquired = nil
	for i := range d.proposal.demands {
		demand := &d.proposal.demands[i]
		result := Acquisition{Demand: *demand, Reads: demand.Reads}
		var err error
		switch {
		case demand.History && demand.Root == "":
			history := d.request.History[demand.Identity]
			if d.request.ReadHistory != nil {
				history.Records, history.Complete = d.request.ReadHistory(ctx, demand)
			}
			d.insertHistory(demand.Identity, history.Records, history.Complete)
			result.Available = history.Records != nil
		case demand.History:
			result.Root, result.Available, err = d.session.facts.ResolveRoot(
				ctx, demand.Reads, demand.Source, demand.Root,
			)
		case demand.Occurrence.Identity != (manifest.NamedResource{}):
			result.Child, err = d.session.facts.OccurrenceObject(ctx, &demand.Occurrence, demand.Artifact)
			result.Available = result.Child.Obj != nil
		case d.options.Stage == Execution:
		default:
			continue
		}
		if err != nil {
			return err
		}
		d.request.Acquired = append(d.request.Acquired, result)
	}
	return nil
}

func (d *admissionDriver) capturePresence() {
	if d.publication == 0 {
		d.publishing = d.proposal.preparation
		d.publishing.publication++
		d.publication = d.publishing.publication
		d.publishing.closure.publication = d.publication
	}
	for i := range d.proposal.Objects {
		id := d.proposal.Objects[i].Obj.Named()
		state := d.proposal.preparation.closure.states[id]
		if state.publication == d.publication {
			continue
		}
		state.publication = d.publication
		d.proposal.preparation.closure.states[id] = state
		if d.session.store.GetObject(id) != nil {
			continue
		}
		if d.absent == nil {
			d.absent = map[manifest.NamedResource]struct{}{}
		}
		d.absent[id] = struct{}{}
	}
}

func (d *admissionDriver) accept(decision *Decision, local bool) AdmissionResult {
	result := d.proposal.result()
	result.Revision = decision.version()
	for i := range result.Objects {
		id := result.Objects[i].Obj.Named()
		if _, missing := d.absent[id]; missing {
			result.Absent = append(result.Absent, id)
		}
	}
	if local {
		result.Request.Preparation = nil
		result.Request.Acquired = nil
	}
	if d.options.Authority != nil {
		result.Request.Reads = d.options.Authority
	}
	d.proposal.accept(d.options.Stage, decision)
	return result
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
