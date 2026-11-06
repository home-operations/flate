package loader

import (
	"context"
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Admission is a temporary proposal computed outside execution-membership locks.
type Admission struct {
	Needed      []manifest.NamedResource
	Selected    []manifest.NamedResource
	Dirty       []manifest.NamedResource
	Ordinary    []manifest.NamedResource
	Objects     []store.Child
	Blocked     []string
	Revision    uint64
	Namespaces  []string
	owned       store.ReadSet
	reads       *store.ReadSet
	preparation *Preparation
	local       *Preparation
	Request     PrepareRequest
	stale       bool
	reused      bool
	sources     []manifest.NamedResource
}

// PrepareRequest describes the evidence and selection intent for an admission proposal.
type PrepareRequest struct {
	Preparation **Preparation
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
}

// Retire MUST follow every active proposal and preserve accepted store borrowers.
func (p *Preparation) Retire() {
	if p == nil {
		return
	}
	if p.closure.loader != nil {
		p.closure.loader.Store.Retire(p.evidence)
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
	preparation.closure.loader.Store.Retire(a.reads)
	local := a.local != nil && a.Request.Preparation == nil
	if preparation.closure.proposal == a {
		preparation.closure.objects, preparation.closure.proposal = nil, nil
	}
	a.reads, a.preparation, a.sources, a.Objects = nil, nil, nil, nil
	a.Request = PrepareRequest{}
	if local {
		preparation.Retire()
		a.local = nil
	}
}

// Commit publishes captured proposal children after their execution membership.
func (l *Loader) Commit(ctx context.Context, proposal *Admission, stage CommitStage) (bool, error) {
	original := proposal
	if original.local != nil {
		original.Request.Preparation = &original.local
		defer func() {
			original.local.Retire()
			original.local = nil
		}()
	}
	defer func() {
		original.Needed, original.Selected, original.Dirty = proposal.Needed, proposal.Selected, proposal.Dirty
		original.Ordinary, original.Objects, original.Blocked = proposal.Ordinary, proposal.Objects, proposal.Blocked
		original.Revision, original.Namespaces = proposal.Revision, proposal.Namespaces
	}()
	for {
		if err := ctx.Err(); err != nil {
			proposal.Retire()
			return false, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		if !proposal.guardCurrent() {
			proposal.Retire()
			return false, nil
		}
		committed := proposal.reads.Valid() && l.commitMembership(proposal, stage)
		if committed {
			proposal.admitObjects(ctx)
		}
		committed = committed && ctx.Err() == nil && proposal.reads.Valid() && proposal.guardCurrent()
		if committed {
			l.acceptAdmission(proposal, stage)
			return true, nil
		}
		request := proposal.Request
		proposal.Retire()
		if request.Reads != nil && !request.Reads.Valid() {
			return false, nil
		}
		next, err := l.Prepare(ctx, request)
		if err != nil {
			return false, err
		}
		proposal = next
	}
}

func (l *Loader) acceptAdmission(a *Admission, stage CommitStage) {
	if stage == Construction {
		l.constructionPurpose.Store(new(a.Ordinary))
	} else if a.preparation != nil {
		a.Revision = l.Options.Filter.Membership(manifest.NamedResource{}).Revision
		a.preparation.completed = a
	}
	a.Retire()
}

func (a *Admission) guardCurrent() bool {
	return !a.stale && (a.Request.Reads == nil || a.Request.Reads.Valid())
}

func (l *Loader) commitMembership(proposal *Admission, stage CommitStage) bool {
	if stage == Construction {
		return true
	}
	added, retry := l.Options.Filter.AddEmitted(change.MembershipUpdate{
		Needed: proposal.Needed, Selected: proposal.Selected, Dirty: proposal.Dirty, Ordinary: proposal.Ordinary,
	}, proposal.Revision)
	if retry {
		return false
	}
	l.constructionPurpose.Store(nil)
	for _, id := range added {
		l.Store.SetIncluded(id, true, l.Options.Filter.OrdinaryDemand(id))
	}
	return true
}

func (a *Admission) admitObjects(ctx context.Context) {
	if a.reused {
		return
	}
	for i := range a.Objects {
		if ctx.Err() != nil || !a.reads.Valid() || !a.guardCurrent() {
			return
		}
		child := a.Objects[i]
		a.reads.Admit(child, a.sources[i], manifest.NamedResource{})
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
	candidates         [2][2][]Occurrence
	deriving           bool
	frozen, sideKnown  bool
	side               manifest.TreeSide
	comparisons        *admissionSources
	identities         []admissionIdentity
	loader             *Loader
	objects            admissionReader
	generation         []store.Child
	owner              manifest.NamedResource
	proposal           *Admission
	states             map[manifest.NamedResource]admissionState
	counts             [4]int
	collecting         bool
	ownerCandidates    []manifest.BaseManifest
	ownersKnown        bool
	appKnown           bool
	applied            bool
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
func (l *Loader) Prepare(
	ctx context.Context,
	request PrepareRequest, //nolint:gocritic // Admission retains an independent snapshot of the caller request.
) (*Admission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	var preparation *Preparation
	if request.Preparation != nil {
		preparation = *request.Preparation
	}
	if preparation == nil {
		preparation = &Preparation{closure: admissionClosure{loader: l}}
		if request.Preparation != nil {
			*request.Preparation = preparation
		}
	}
	closure := &preparation.closure
	if closure.loader != l {
		return nil, manifest.NewError(manifest.ErrInput, nil, "admission preparation belongs to a different loader")
	}
	proposal := &Admission{Request: request, preparation: preparation}
	if request.Preparation == nil {
		proposal.local = preparation
	}
	if preparation.reuse(&request, proposal) {
		return proposal, nil
	}
	preparation.completed = nil
	proposal.reads = &proposal.owned
	owner := manifest.NamedResource{}
	if request.Reads != nil {
		owner = request.Reads.ID()
	}
	proposal.reads.Init(l.Store, owner, nil)
	if request.Reads != nil {
		proposal.reads.SetWork(request.Reads.Work())
	}
	closure.proposal, closure.objects, closure.deriving = proposal, proposal.reads, false
	if !proposal.guardCurrent() {
		proposal.stale = true
		return proposal, nil
	}
	defer closure.releaseComparisons()
	if err := closure.refreshPrerequisites(ctx, preparation.evidence, &request); err != nil {
		proposal.Retire()
		return nil, err
	}
	preparation.evidence = proposal.reads
	closure.resetDemand()
	closure.generation, closure.owner, closure.history = request.Generation, request.Owner, request.History
	closure.changes = l.Options.Changes
	closure.frozen, closure.sideKnown, closure.side = l.frozen, l.sideKnown, l.side
	proposal.Revision = l.Options.Filter.Membership(manifest.NamedResource{}).Revision
	closure.seedConstruction()
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

func (p *Preparation) reuse(request *PrepareRequest, proposal *Admission) bool {
	previous := p.completed
	c := &p.closure
	if previous == nil || request.Generation == nil || request.Invocation || len(request.History) != 0 ||
		len(request.Generation) != len(c.generation) ||
		(len(c.generation) != 0 && &request.Generation[0] != &c.generation[0]) || request.Owner != c.owner ||
		c.frozen != c.loader.frozen || c.sideKnown != c.loader.sideKnown || c.side != c.loader.side ||
		previous.Revision != c.loader.Options.Filter.Membership(manifest.NamedResource{}).Revision ||
		!p.evidence.Valid() || !proposal.guardCurrent() {
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
	proposal.Revision, proposal.reads, proposal.reused = previous.Revision, p.evidence, true
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
	interpretation := c.frozen == c.loader.frozen && c.sideKnown == c.loader.sideKnown && c.side == c.loader.side
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
		current, currentInputs, currentDeclarations := c.loader.Options.Changes.ForSource(source)
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
	record, _ := c.loader.Index.Lookup(id)
	if child := c.children[id]; child.Obj != nil {
		indexed := record.Builder != "" && c.loader.registeredRoot(&record) != ""
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
				productions = c.objects.IncomingProductions(incoming, c.loader.Index)
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
	c.collecting, c.appKnown, c.applied = false, false, false
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
	l := c.loader
	if l.frozen {
		return
	}
	if !l.sideKnown && l.Options.Changes != nil {
		input := c.objects.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		_, snapshots, _ := l.Options.Changes.ForSource(manifest.BootstrapSourceID)
		for side, snapshot := range snapshots {
			if snapshot != nil && snapshot == input {
				l.side, l.sideKnown = manifest.TreeSide(side), true
				break
			}
		}
	}
	if l.scopes == nil {
		return
	}
	for _, root := range l.StandaloneRoots(l.SourceRoot) {
		if filepath.Join(l.SourceRoot, filepath.FromSlash(root)) == l.entryPath {
			c.roots = append(c.roots, root)
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
	selection := c.loader.Options.Selection
	return selection.Matches(id) || selection.Roots[0].Name != "" || selection.Roots[0].Namespace == ""
}

func (c *admissionClosure) evaluate(ctx context.Context, request *PrepareRequest) (*Admission, error) {
	defer c.releaseComparisons()
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
	membership := c.loader.Options.Filter.Membership(id)
	needed, selected, dirty := membership.Needed, membership.Selected, membership.Dirty
	role.needed, role.selected, role.dirty = role.needed || needed, role.selected || selected, role.dirty || dirty
	role.ordinary = role.ordinary || c.loader.Options.Filter.OrdinaryDemand(id)
	return role
}

func (c *admissionClosure) add(id manifest.NamedResource, role admissionRole) {
	if id == (manifest.NamedResource{}) || !role.needed {
		return
	}
	previous := c.role(id)
	role.needed, role.selected, role.dirty = true, role.selected || previous.selected, role.dirty || previous.dirty
	role.ordinary = role.ordinary || previous.ordinary || role.selected
	if previous == role && (!c.proposal.Request.Invocation || len(c.generation) != 0) &&
		(c.incoming(id).Obj == nil) {
		record := c.record(id)
		if !c.loader.roots.get(id).admitted && record.Path == "" && c.loader.candidates[id].Obj == nil ||
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

func (c *admissionClosure) finishAdmission(
	ctx context.Context,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) (*Admission, error) {
	if err := c.walk(ctx, materializers); err != nil {
		return nil, err
	}
	if err := c.admitChangedRoots(ctx); err != nil {
		return nil, err
	}
	for i := range c.generation {
		child := c.generation[i]
		if child.Obj == nil || child.Entry == nil || c.loader.Store.GetDeclaration(child.Obj.Named()) == child.Obj {
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
	slices.Sort(c.proposal.Blocked)
	c.proposal.Blocked = slices.Compact(c.proposal.Blocked)
	return c.proposal, ctx.Err() //nolint:wrapcheck // Admission cancellation preserves the original context error identity.
}
