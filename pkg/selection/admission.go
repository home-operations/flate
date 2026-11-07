package selection

import (
	"context"
	"iter"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type (
	admissionRole  struct{ needed, selected, dirty, ordinary bool }
	admissionState struct {
		ordinal, incoming                         int
		productions, incomingProductions          []manifest.Production
		role, walked                              admissionRole
		consumer, generation, restoration, claim  int
		changed, seed, direct, rescan, requested  bool
		walkedOnce, productionsKnown, listed      bool
		eligible, declarationChanged, staticOwner bool
		comparingDeclaration                      bool
		initialKnown, directKnown                 bool
		affected, affectedKnown                   bool
	}
)

type admissionIdentity struct {
	id           manifest.NamedResource
	stableOwner  int
	stableObject manifest.BaseManifest
}

type admissionClosure struct {
	changes            *change.Set
	configuredInput    *store.SourceArtifact
	candidates         [2][2][]OccurrenceFacts
	deriving           bool
	frozen, sideKnown  bool
	side               manifest.TreeSide
	identities         []admissionIdentity
	session            *Session
	facts              census
	context            SourceContext
	historyOwners      map[manifest.NamedResource][]manifest.SourceEvidence
	allowMissingKinds  []string
	objects            admissionReader
	generation         []store.Child
	owner              manifest.NamedResource
	proposal           *Admission
	states             map[manifest.NamedResource]admissionState
	counts             [4]int
	collecting         bool
	ownerCandidates    []RootCoveringOwner
	ownersKnown        bool
	children           map[manifest.NamedResource]store.Child
	queue              []manifest.NamedResource
	roots              []string
	searched           map[materializerDemand]bool
	inputScratch       [1]manifest.NamedResource
	accepted           map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord
	historyProductions map[manifest.NamedResource][]manifest.Production
	replace            map[manifest.NamedResource]bool
	history            map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}
}

// Prepare computes the local declaration and input closure without publishing membership or objects.
// History is invocation-local borrowed evidence; complete inventories replace only their owner's support.
//
//nolint:funlen // Preparation MUST keep one owner and capture through each acquisition retry.
func (l *Session) Prepare(
	ctx context.Context,
	request PrepareRequest, //nolint:gocritic // Admission retains an independent snapshot of the caller request.
) (*Admission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	entry := l.entry.Load()
	currentSource := l.facts.SourceFacts(manifest.BootstrapSourceID, nil)
	if request.Changes == nil {
		request.Changes = currentSource.Comparison
	}
	var preparation *Preparation
	if request.Preparation != nil {
		preparation = *request.Preparation
	}
	if preparation == nil {
		preparation = &Preparation{closure: admissionClosure{session: l, facts: l.facts}}
		if request.Preparation != nil {
			*request.Preparation = preparation
		}
	}
	closure := &preparation.closure
	if closure.session != l {
		return nil, manifest.NewError(manifest.ErrInput, nil, "admission preparation belongs to a different loader")
	}
	closure.context = currentSource.Context
	proposal := &Admission{
		Request: request, preparation: preparation, entry: entry, producerOwner: preparation.producerOwner,
	}
	if request.Preparation == nil {
		proposal.local = preparation
	}
	if preparation.reuse(&request, proposal, currentSource.ConfiguredInput) {
		return proposal, nil
	}
	preparation.completed = nil
	proposal.reads = &proposal.owned
	owner := manifest.NamedResource{}
	if request.Reads != nil {
		owner = request.Reads.ID()
	}
	proposal.reads.Init(l.store, owner, nil)
	if request.Reads != nil {
		proposal.reads.SetWork(request.Reads.Work())
	}
	closure.proposal, closure.objects, closure.deriving = proposal, proposal.reads, false
	if !l.guardCurrent(proposal) {
		proposal.stale = true
		return proposal, nil
	}

	preparation.acceptAcquired(request.Acquired)
	if err := closure.refreshPrerequisites(ctx, preparation.evidence, &request); err != nil {
		proposal.Retire()
		return nil, err
	}
	defer l.store.Retire(preparation.evidence)
	preparation.evidence = proposal.reads
	closure.resetDemand()
	closure.generation, closure.owner, closure.history = request.Generation, request.Owner, request.History
	closure.historyOwners, closure.allowMissingKinds = request.HistoryOwners, request.AllowMissingKinds
	closure.changes = request.Changes
	closure.configuredInput = currentSource.ConfiguredInput
	closure.frozen, closure.sideKnown, closure.side = closure.context.Frozen, closure.context.SideKnown, closure.context.Side
	proposal.Revision = l.Snapshot().version()
	_, err := closure.evaluate(ctx, &request)
	if err == nil {
		err = closure.capturePrerequisites(ctx)
	}
	if closure.deriving {
		proposal.reads.WorkScope().Admission().Finish(err)
	}
	if err != nil {
		proposal.Retire()
		return nil, err
	}
	proposal.captureSources()
	return proposal, nil
}

func (p *Preparation) reuse(request *PrepareRequest, proposal *Admission, configuredInput *store.SourceArtifact) bool {
	previous := p.completed
	c := &p.closure
	if previous == nil || len(request.Acquired) != 0 || request.Changes != p.changes ||
		configuredInput != c.configuredInput ||
		proposal.entry != p.entry ||
		!slices.Equal(request.AllowMissingKinds, c.allowMissingKinds) ||
		len(request.HistoryOwners) != 0 ||
		request.Generation == nil ||
		request.Invocation ||
		len(request.History) != 0 ||
		len(request.Generation) != len(c.generation) ||
		(len(c.generation) != 0 && &request.Generation[0] != &c.generation[0]) ||
		request.Owner != c.owner ||
		c.frozen != c.context.Frozen ||
		c.sideKnown != c.context.SideKnown ||
		c.side != c.context.Side ||
		previous.Revision != c.session.Snapshot().version() ||
		!p.evidence.Valid() ||
		!c.session.guardCurrent(proposal) {
		return false
	}
	owner := manifest.NamedResource{}
	if request.Reads != nil {
		owner = request.Reads.ID()
		for id := range request.Reads.Required() {
			if !slices.Contains(previous.Ordinary, id) {
				return false
			}
		}
	}
	if owner != p.evidence.ID() {
		return false
	}
	proposal.Needed, proposal.Selected, proposal.Dirty = previous.Needed, previous.Selected, previous.Dirty
	proposal.Ordinary, proposal.Blocked, proposal.Namespaces = previous.Ordinary, previous.Blocked, previous.Namespaces
	proposal.EntryReceipt = previous.EntryReceipt
	proposal.producers, proposal.producerOwner = previous.producers, previous.producerOwner
	proposal.Revision, proposal.reads, proposal.reused = previous.Revision, p.evidence, true
	if previous.entryReads != nil {
		proposal.entryReads = proposal.reads
	}
	return true
}

func (c *admissionClosure) derive() {
	if !c.deriving && c.proposal != nil {
		c.deriving = true
		c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
	}
}

func (c *admissionClosure) refreshPrerequisites(
	ctx context.Context,
	previous *store.ReadSet,
	request *PrepareRequest,
) error {
	c.ownerCandidates, c.ownersKnown = c.ownerCandidates[:0], false
	if previous == nil {
		return nil
	}
	if err := c.capturePrerequisites(ctx); err != nil {
		return err
	}
	interpretation := c.frozen == c.context.Frozen && c.sideKnown == c.context.SideKnown && c.side == c.context.Side
	for id, state := range c.states {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation MUST preserve the caller's error identity.
		}
		if !state.initialKnown && !state.productionsKnown {
			continue
		}
		matched := c.matchesPrerequisites(id, &state, previous, request)
		if !matched || !interpretation {
			state.initialKnown, state.directKnown, state.productionsKnown = false, false, false
			state.seed, state.affectedKnown = false, false
			state.productions = nil
			if state.ordinal != 0 {
				identity := &c.identities[state.ordinal-1]
				identity.stableObject, identity.stableOwner = nil, 0
			}
			delete(c.children, id)
		}
		c.states[id] = state
	}
	return nil
}

func (c *admissionClosure) matchesPrerequisites(
	id manifest.NamedResource,
	state *admissionState,
	previous *store.ReadSet,
	request *PrepareRequest,
) bool {
	matched := c.matchesDeclarationContent(id, state, previous, request)
	object := c.objects.GetObject(id)

	for source := range c.sources(id) {
		c.boundSource(source)
		before, beforeInputs, beforeDeclarations := c.changes.ForSource(source)
		current, currentInputs, currentDeclarations := c.proposal.Request.Changes.ForSource(source)
		matched = before == current && beforeInputs == currentInputs && beforeDeclarations == currentDeclarations &&
			c.proposal.reads.MatchesContent(previous, source) && matched
	}
	for ref := range c.references(id, object) {
		if ref.Selector != nil {
			c.objects.ListObjects(ref.Group, ref.Kind)
		}
		if ref.Name != "" {
			c.objects.GetObject(ref.NamedResource)
			matched = c.proposal.reads.MatchesContent(previous, ref.NamedResource) && matched
		}
	}
	return matched
}

func (c *admissionClosure) matchesDeclarationContent(
	id manifest.NamedResource,
	state *admissionState,
	previous *store.ReadSet,
	request *PrepareRequest,
) bool {
	productions := c.currentProductions(id)
	c.objects.GetObject(id)
	matched := c.proposal.reads.MatchesContent(previous, id)
	record, _ := c.facts.SelectionIndex().Lookup(id)
	if child := c.children[id]; child.Obj != nil {
		indexed := record.Builder != "" && c.registeredRoot(&record) != ""
		if child.SourceSupport != (manifest.NamedResource{}) || indexed {
			// Admission changes the target's presence, but the source still owns its derivation.
			current, document, output := c.objects.GetEmission(id)
			output.Finish()
			matched = manifest.Equal(child.Obj, current) && manifest.ValueEqual(child.Doc, document) &&
				slices.Equal(state.productions, productions)
		}
	}
	if incoming := request.incoming(id); incoming.Obj != nil {
		if previousChild := c.incoming(id); previousChild.Obj != nil {
			if request.Owner != c.owner ||
				!slices.Equal(c.generation[state.incoming-1].Productions, incoming.Productions) {
				incoming.Productions = c.incomingProductions(&incoming, request.Owner, request.Reads)
				productions = c.objects.IncomingProductions(incoming, c.facts.SelectionIndex())
			}
			matched = manifest.Equal(previousChild.Obj, incoming.Obj) &&
				manifest.ValueEqual(previousChild.Doc, incoming.Doc) && slices.Equal(state.productions, productions)
		} else {
			current, document, output := c.objects.GetEmission(id)
			output.Finish()
			matched = matched && manifest.Equal(current, incoming.Obj) &&
				manifest.ValueEqual(document, incoming.Doc)
		}
	}
	return matched
}

func (c *admissionClosure) capturePrerequisites(ctx context.Context) error {
	for id, state := range c.states {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation MUST preserve the caller's error identity.
		}
		if !state.initialKnown && !state.productionsKnown {
			continue
		}
		c.currentProductions(id)
		object := c.objects.GetObject(id)
		for source := range c.sources(id) {
			c.boundSource(source)
		}
		for ref := range c.references(id, object) {
			if ref.Selector != nil {
				c.objects.ListObjects(ref.Group, ref.Kind)
			}
			if ref.Name != "" {
				c.objects.GetObject(ref.NamedResource)
			}
		}
	}
	return nil
}

func (c *admissionClosure) resetDemand() {
	c.counts = [4]int{}
	c.collecting = false
	c.queue, c.roots = c.queue[:0], c.roots[:0]
	clear(c.searched)
	clear(c.historyProductions)
	clear(c.accepted)
	clear(c.replace)
	for id, state := range c.states {
		state.role, state.walked = admissionRole{}, admissionRole{}
		state.consumer, state.restoration, state.claim, state.incoming = 0, 0, 0, 0
		state.incomingProductions = nil
		state.changed, state.rescan, state.requested, state.walkedOnce, state.listed = false, false, false, false, false
		c.states[id] = state
	}
}

func (c *admissionClosure) captureIncoming() {
	var guard *store.ReadSet
	if c.proposal != nil {
		guard = c.proposal.Request.Reads
	}
	if len(c.generation) != 0 && c.states == nil {
		c.states = map[manifest.NamedResource]admissionState{}
	}
	for i := range c.generation {
		child := &c.generation[i]
		if child.Obj == nil {
			continue
		}
		id := child.Obj.Named()
		state := c.states[id]
		state.incoming, state.incomingProductions = i+1, c.incomingProductions(child, c.owner, guard)
		c.states[id] = state
		if guard == nil {
			continue
		}
		for ref := range manifest.References(child.Obj, manifest.ReferenceRelations) {
			if ref.Selector != nil {
				guard.ListObjects(ref.Group, ref.Kind)
			}
		}
	}
}

func (c *admissionClosure) incomingProductions(
	child *store.Child,
	owner manifest.NamedResource,
	guard *store.ReadSet,
) []manifest.Production {
	id := child.Obj.Named()
	productions := child.Productions
	if len(productions) == 0 && owner == (manifest.NamedResource{}) {
		if guard != nil {
			productions = guard.Promotions(id, nil)
		} else {
			productions = c.objects.Productions(id, nil)
		}
	}
	if owner != (manifest.NamedResource{}) && owner != id {
		productions = append(slices.Clone(productions), manifest.Production{Owner: owner})
	}
	return productions
}

func (c *admissionClosure) incoming(id manifest.NamedResource) store.Child {
	state := c.states[id]
	index := state.incoming - 1
	if index >= 0 && index < len(c.generation) {
		child := c.generation[index]
		if child.Obj != nil && child.Obj.Named() == id {
			child.Productions = state.incomingProductions
			return child
		}
	}
	return store.Child{}
}

func (r *PrepareRequest) incoming(id manifest.NamedResource) store.Child {
	for i := range r.Generation {
		child := r.Generation[i]
		if child.Obj != nil && child.Obj.Named() == id {
			return child
		}
	}
	return store.Child{}
}

func (c *admissionClosure) seedConstruction() {
	if c.context.Frozen {
		return
	}
	for i := range c.context.Roots {
		root := &c.context.Roots[i]
		if Root(RootEvidence{Native: root}).Construction && (root.Entry || c.rootRequired(root.Path)) {
			c.roots = append(c.roots, root.Path)
		}
	}
}

func (a *Admission) captureSources() {
	for i := range a.Objects {
		child := &a.Objects[i]
		source := child.SourceSupport
		if source == (manifest.NamedResource{}) {
			source = manifest.BootstrapSourceID
			for j := range child.Productions {
				production := &child.Productions[j]
				if production.Source != (manifest.NamedResource{}) {
					source = production.Source
					break
				}
			}
		}
		a.reads.GetArtifact(source, store.ArtifactContent)
		a.sources = append(a.sources, source)
	}
}

func (c *admissionClosure) selectsChild(id manifest.NamedResource) bool {
	selection := c.session.request
	return len(selection.Roots) == 0 || selection.Matches(id) || selection.Roots[0].Name != "" ||
		selection.Roots[0].Namespace == ""
}

func (c *admissionClosure) evaluate(ctx context.Context, request *PrepareRequest) (*Admission, error) {
	c.captureIncoming()
	if len(c.history) != 0 {
		c.readHistory()
	}
	var candidatesKnown [2]bool
	materializers := func(demand materializerDemand, consumer manifest.NamedResource) bool {
		mode := 0
		if demand.ordering || demand.application != "" {
			mode = 1
		}
		if !candidatesKnown[mode] {
			c.candidates[mode] = c.materializerCandidates(mode == 1)
			candidatesKnown[mode] = true
		}
		return c.materializers(new(demand), consumer, c.candidates[mode])
	}
	if err := c.seedAdmission(ctx, request, materializers); err != nil {
		return nil, err
	}
	return c.finishAdmission(ctx, materializers)
}

func (c *admissionClosure) role(id manifest.NamedResource) admissionRole {
	role := c.states[id].role
	snapshot := c.session.Snapshot()
	if !snapshot.execution {
		return role
	}
	needed, selected, dirty := snapshot.Reconcile(id), snapshot.Output(id), snapshot.Changed(id)
	role.needed, role.selected, role.dirty = role.needed || needed, role.selected || selected, role.dirty || dirty
	role.ordinary = role.ordinary || c.session.Snapshot().OrdinaryDemand(id)
	return role
}

func (c *admissionClosure) add(id manifest.NamedResource, role admissionRole) {
	if id == (manifest.NamedResource{}) || !role.needed {
		return
	}
	previous := c.role(id)
	role.needed, role.selected, role.dirty = true, role.selected || previous.selected, role.dirty || previous.dirty
	role.ordinary = role.ordinary || previous.ordinary || role.selected
	if c.session.Snapshot().execution && previous == role &&
		(!c.proposal.Request.Invocation || len(c.generation) != 0) &&
		(c.incoming(id).Obj == nil) {
		record := c.record(id)
		if !Root(c.facts.RootEvidence(id, c.proposal.reads)).InitialAdmitted && record.Path == "" &&
			c.candidate(id).Obj == nil ||
			c.objects.GetObject(id) != nil {
			return
		}
	}
	state := c.states[id]
	if state.role == role {
		return
	}
	c.assignRole(id, role)
}

func (c *admissionClosure) assignRole(id manifest.NamedResource, role admissionRole) {
	record := c.record(id)
	policy := record.Origin.Policy(id, nil)
	if c.incoming(id).Obj != nil {
		policy = c.incoming(id).Policy(id)
	} else if record.Path == "" && record.Builder == "" {
		_, policy = c.objects.Lookup(id)
		policy.Root = policy.Root && c.objects.GetDeclaration(id) != nil
	}
	if !policy.Root {
		role.selected = false
	}
	state := c.states[id]
	if c.states == nil {
		c.states = map[manifest.NamedResource]admissionState{}
	}
	for axis, present := range [4]bool{role.needed, role.selected, role.dirty, role.ordinary} {
		previous := [4]bool{state.role.needed, state.role.selected, state.role.dirty, state.role.ordinary}
		if present && !previous[axis] {
			c.counts[axis]++
		}
	}
	state.role = role
	c.states[id] = state
	if !c.collecting {
		c.queue = append(c.queue, id)
	}
}

func (c *admissionClosure) markChanged(id manifest.NamedResource) {
	state := c.states[id]
	state.changed = true
	c.states[id] = state
}

func (c *admissionClosure) seedAdmission(
	ctx context.Context,
	request *PrepareRequest,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	if request.Invocation {
		if err := c.start(ctx, materializers); err != nil {
			return err
		}
	}
	if request.Reads != nil {
		for id, qualification := range request.Reads.Required() {
			if c.states == nil {
				c.states = map[manifest.NamedResource]admissionState{}
			}
			state := c.states[id]
			state.requested = request.Reads.ID() == (manifest.NamedResource{})
			c.states[id] = state
			role := c.role(id)
			role.needed = true
			role.ordinary = role.ordinary || !qualification
			c.add(id, role)
		}
	}
	for i := range c.generation {
		if child := &c.generation[i]; child.Obj != nil {
			if err := c.selectIncoming(child.Obj.Named()); err != nil {
				return err
			}
		}
	}
	return nil
}

//nolint:funlen,gocognit // The generation and role inventory MUST finalize in one pass before guarded publication.
func (c *admissionClosure) finishAdmission(
	ctx context.Context,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) (*Admission, error) {
	if err := c.walk(ctx, materializers); err != nil {
		return nil, err
	}
	c.seedConstruction()
	c.admitChangedRoots()
	c.requestEntryReceipt()
	for i := range c.generation {
		child := c.generation[i]
		if child.Obj == nil ||
			child.Entry == nil &&
				(child.SourceSupport == (manifest.NamedResource{}) || !c.role(child.Obj.Named()).needed) ||
			c.session.store.GetDeclaration(child.Obj.Named()) == child.Obj {
			continue
		}
		if c.children == nil {
			c.children = map[manifest.NamedResource]store.Child{}
		}
		child.Productions = c.currentProductions(child.Obj.Named())
		c.children[child.Obj.Named()] = child
	}
	if total := c.counts[0] + c.counts[1] + c.counts[2] + c.counts[3]; total != 0 {
		roles := make([]manifest.NamedResource, total)
		c.proposal.Needed = roles[:0:c.counts[0]]
		c.proposal.Selected = roles[c.counts[0] : c.counts[0] : c.counts[0]+c.counts[1]]
		c.proposal.Dirty = roles[c.counts[0]+c.counts[1] : c.counts[0]+c.counts[1] : c.counts[0]+c.counts[1]+c.counts[2]]
		c.proposal.Ordinary = roles[c.counts[0]+c.counts[1]+c.counts[2] : c.counts[0]+c.counts[1]+c.counts[2]]
	}
	c.proposal.Objects = slices.Grow(c.proposal.Objects, len(c.children))
	for id, state := range c.states {
		if state.role.needed && len(c.historyOwners[id]) != 0 {
			c.requestHistory(id)
		}
		if state.role.needed && id.KindInfo().Runnable {
			evidence := c.declarationEvidence(id)
			if !evidence.known() {
				productions := c.productions(id)
				for i := range productions {
					c.requestProducer(id, &productions[i])
				}
			}
		}
		if state.role.needed {
			c.proposal.Needed = append(c.proposal.Needed, id)
		}
		if state.role.selected {
			c.proposal.Selected = append(c.proposal.Selected, id)
		}
		if state.role.dirty {
			c.proposal.Dirty = append(c.proposal.Dirty, id)
		}
		if state.role.ordinary {
			c.proposal.Ordinary = append(c.proposal.Ordinary, id)
		}
	}
	for id := range c.children {
		c.proposal.Objects = append(c.proposal.Objects, c.children[id])
	}
	slices.SortFunc(c.proposal.Needed, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Selected, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Dirty, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Ordinary, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Objects, func(a, b store.Child) int { return a.Obj.Named().Compare(b.Obj.Named()) })
	sortProducerObligations(c.proposal.producers)
	slices.Sort(c.proposal.Blocked)
	c.proposal.Blocked = slices.Compact(c.proposal.Blocked)
	for _, root := range c.proposal.Blocked {
		owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, root, manifest.NamedResource{}).Named()
		c.acquire(Demand{Identity: owner, Source: manifest.BootstrapSourceID, Root: root})
	}
	return c.proposal, ctx.Err() //nolint:wrapcheck // Admission cancellation preserves the original context error identity.
}
