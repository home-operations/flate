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
		captured                                  bool
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
	sourceFacts        map[manifest.NamedResource]*SourceFacts
	sourceReads        *store.ReadSet
	candidates         [2][2][]*OccurrenceFacts
	candidatesKnown    [2]bool
	deriving           bool
	frozen, sideKnown  bool
	side               manifest.TreeSide
	identities         []admissionIdentity
	session            *Session
	decision           *Decision
	facts              census
	nativeRoots        []NativeRootFacts
	excludedNative     map[manifest.NamedResource]bool
	catalog            *admissionCatalog
	context            SourceContext
	historyOwners      map[manifest.NamedResource][]manifest.SourceEvidence
	allowMissingKinds  []string
	objects            admissionReader
	nextGeneration     map[manifest.NamedResource]int
	generation         []store.Child
	owner              manifest.NamedResource
	proposal           *Admission
	states             map[manifest.NamedResource]admissionState
	counts             [4]int
	collecting         bool
	ownerCandidates    []RootCoveringOwner
	ownersKnown        bool
	capturedRoots      map[manifest.NamedResource]*RootEvidence
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
	if closure.context.Frozen {
		closure.catalog = l.catalogSnapshot(closure.context.Roots)
	}
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
	defer func() {
		closure.capturedRoots = nil
		closure.ownerCandidates = nil
		closure.candidates = [2][2][]*OccurrenceFacts{}
	}()
	closure.decision = l.Snapshot()
	if !l.guardCurrent(proposal) {
		proposal.stale = true
		return proposal, nil
	}

	closure.captureGeneration(&request)
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
	proposal.Revision = closure.decision.version()
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
	//nolint:gocritic // Acquisition values MUST remain immutable while this preparation borrows them.
	for key, result := range preparation.acquired {
		if result.Reads != proposal.reads {
			delete(preparation.acquired, key)
		}
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
		request.Invocation || p.invocation ||
		len(request.History) != 0 ||
		len(request.Generation) != len(c.generation) ||
		(len(c.generation) != 0 && &request.Generation[0] != &c.generation[0]) ||
		request.Owner != c.owner ||
		c.frozen != c.context.Frozen ||
		c.sideKnown != c.context.SideKnown ||
		c.side != c.context.Side ||
		!p.evidence.Valid() ||
		!c.session.guardCurrent(proposal) {
		return false
	}
	decision := c.session.Snapshot()
	if previous.Revision != decision.version() {
		for id := range c.states {
			if p.decision.roles(id) != decision.roles(id) {
				return false
			}
		}
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
	proposal.Revision, proposal.reads, proposal.reused = decision.version(), p.evidence, true
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
	clear(c.capturedRoots)
	if previous == nil {
		//nolint:gocritic // Map values MUST remain independent snapshots until publication.
		for id, state := range c.states {
			state.captured = false
			c.states[id] = state
		}
		return nil
	}
	interpretation := c.frozen == c.context.Frozen && c.sideKnown == c.context.SideKnown && c.side == c.context.Side
	if c.matchesPreparation(previous, request, interpretation) {
		//nolint:gocritic // Map values MUST remain independent snapshots until publication.
		for id, state := range c.states {
			state.captured = true
			c.states[id] = state
		}
		return nil
	}
	//nolint:gocritic // Map values MUST remain independent snapshots until publication.
	for id, state := range c.states {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation MUST preserve the caller's error identity.
		}
		if !state.initialKnown && !state.productionsKnown {
			state.captured = false
			c.states[id] = state
			continue
		}
		matched := c.matchesPrerequisites(id, &state, previous, request)
		state.captured = matched && interpretation
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
		indexed := record.Builder != "" && c.capturedRoot(id).PreparedRoot != ""
		if child.SourceSupport != (manifest.NamedResource{}) || indexed {
			// Admission changes the target's presence, but the source still owns its derivation.
			current, document, output := c.objects.GetEmission(id)
			output.Finish()
			matched = manifest.Equal(child.Obj, current) && manifest.ValueEqual(child.Doc, document) &&
				slices.Equal(state.productions, productions)
		}
	}
	if index := c.nextGeneration[id]; index != 0 {
		incoming := request.Generation[index-1]
		if previousChild := c.incoming(id); previousChild.Obj != nil {
			usesPromotions := request.Owner == (manifest.NamedResource{}) && len(incoming.Productions) == 0
			if usesPromotions || request.Owner != c.owner ||
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
	//nolint:gocritic // Map values MUST remain independent snapshots until publication.
	for id, state := range c.states {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation MUST preserve the caller's error identity.
		}
		if state.captured || !state.initialKnown && !state.productionsKnown {
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
	c.candidatesKnown = [2]bool{}
	c.queue, c.roots = c.queue[:0], c.roots[:0]
	clear(c.searched)
	clear(c.historyProductions)
	clear(c.accepted)
	clear(c.replace)
	//nolint:gocritic // Map values MUST remain independent snapshots until publication.
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

	if err := c.seedAdmission(ctx, request, c.materializers); err != nil {
		return nil, err
	}
	return c.finishAdmission(ctx, c.materializers)
}

func (c *admissionClosure) role(id manifest.NamedResource) admissionRole {
	state, known := c.states[id]
	role := state.role
	snapshot := c.decision
	if !snapshot.execution {
		return role
	}
	if !known {
		if c.states == nil {
			c.states = make(map[manifest.NamedResource]admissionState)
		}
		c.states[id] = state
	}
	roles := snapshot.roles(id)
	role.needed = role.needed || roles&needed != 0
	role.selected = role.selected || roles&selected != 0
	// Promotion requires reconciliation without proving that input content changed.
	role.dirty = role.dirty || roles&dirty != 0 && roles&promoted == 0
	role.ordinary = role.ordinary || roles&ordinary != 0
	return role
}

func (c *admissionClosure) add(id manifest.NamedResource, role admissionRole) {
	if id == (manifest.NamedResource{}) || !role.needed {
		return
	}
	previous := c.role(id)
	role.needed, role.selected, role.dirty = true, role.selected || previous.selected, role.dirty || previous.dirty
	role.ordinary = role.ordinary || previous.ordinary || role.selected
	if c.decision.execution && previous == role &&
		(!c.proposal.Request.Invocation || len(c.generation) != 0) &&
		(c.incoming(id).Obj == nil) {
		record := c.record(id)
		if record.Path == "" && c.facts.AdmissionCandidate(id).Obj == nil &&
			!Root(*c.capturedRoot(id)).InitialAdmitted || c.objects.GetObject(id) != nil {
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
	//nolint:gocritic // Map values MUST remain independent snapshots until publication.
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

func (c *admissionClosure) captureGeneration(request *PrepareRequest) {
	if request.Owner != c.owner || len(request.Generation) != len(c.generation) ||
		len(request.Generation) != 0 && &request.Generation[0] != &c.generation[0] {
		clear(c.nextGeneration)
		if len(request.Generation) != 0 && c.nextGeneration == nil {
			c.nextGeneration = make(map[manifest.NamedResource]int, len(request.Generation))
		}
		for i := range request.Generation {
			if object := request.Generation[i].Obj; object != nil {
				c.nextGeneration[object.Named()] = i + 1
			}
		}
		replaced := len(request.Generation) != len(c.generation) ||
			slices.ContainsFunc(request.Generation, func(child store.Child) bool {
				return child.Obj != nil && c.incoming(child.Obj.Named()).Obj != child.Obj
			})
		if !request.Invocation && len(c.identities) == 0 && len(c.generation) != 0 && replaced {
			clear(c.states)
			clear(c.children)
		}
	}
}

func (c *admissionClosure) matchesPreparation(
	previous *store.ReadSet,
	request *PrepareRequest,
	interpretation bool,
) bool {
	sameInterpretation := interpretation && c.context.Frozen && c.changes == request.Changes && request.Owner == c.owner
	if !sameInterpretation {
		return false
	}
	historyOnly := !slices.ContainsFunc(
		request.Acquired,
		func(a Acquisition) bool { return !a.Demand.History || a.Demand.Root == "" },
	)
	sameGeneration := len(request.Generation) == len(c.generation) &&
		(len(c.generation) == 0 || &request.Generation[0] == &c.generation[0])
	return historyOnly && sameGeneration && c.proposal.reads.Matches(previous)
}
