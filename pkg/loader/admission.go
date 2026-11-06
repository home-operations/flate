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
	Needed     []manifest.NamedResource
	Selected   []manifest.NamedResource
	Dirty      []manifest.NamedResource
	Ordinary   []manifest.NamedResource
	Objects    []store.Child
	Blocked    []string
	Revision   uint64
	Namespaces []string
	reads      *store.ReadSet
	Request    PrepareRequest
	stale      bool
	sources    []manifest.NamedResource
}

// PrepareRequest describes the evidence and selection intent for an admission proposal.
type PrepareRequest struct {
	Incoming   store.Child
	Reads      *store.ReadSet
	Invocation bool
	History    map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}
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
	if a != nil {
		a.reads.ReleaseCapture()
		a.reads.Retire()
		a.reads = nil
		a.Objects, a.sources = nil, nil
		a.Request = PrepareRequest{}
	}
}

// Commit publishes captured proposal children after their execution membership.
func (l *Loader) Commit(ctx context.Context, proposal *Admission, stage CommitStage) (bool, error) {
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
			if stage == Construction {
				l.constructionPurpose.Store(new(proposal.Ordinary))
			}
			proposal.Retire()
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
		*proposal = *next
	}
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
		ordinal                                   int
		productions                               []manifest.Production
		role, walked                              admissionRole
		consumer, generation, restoration, claim  int
		changed, seed, direct, rescan, requested  bool
		walkedOnce, productionsKnown, listed      bool
		eligible, declarationChanged, staticOwner bool
		comparingDeclaration                      bool
	}
)

type admissionIdentity struct {
	id           manifest.NamedResource
	stableOwner  int
	stableObject manifest.BaseManifest
}

type admissionClosure struct {
	comparisons        *admissionSources
	identities         []admissionIdentity
	loader             *Loader
	objects            admissionReader
	incoming           store.Child
	proposal           *Admission
	states             map[manifest.NamedResource]admissionState
	result             Admission
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
	closure := admissionClosure{loader: l, incoming: request.Incoming, history: request.History}
	proposal := &closure.result
	proposal.Request = request
	proposal.reads = &store.ReadSet{}
	owner := manifest.NamedResource{}
	if request.Reads != nil {
		owner = request.Reads.ID()
	}
	proposal.reads.Init(l.Store, owner, nil)
	if request.Reads != nil {
		proposal.reads.SetWork(request.Reads.Work())
	}
	if !proposal.guardCurrent() {
		proposal.stale = true
		return proposal, nil
	}
	closure.proposal, closure.objects = proposal, proposal.reads
	work := proposal.reads.WorkScope().Admission()
	work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
	proposal.Revision = l.Options.Filter.Membership(manifest.NamedResource{}).Revision
	closure.captureIncoming()
	closure.seedConstruction()
	proposal, err := closure.evaluate(ctx, &request)
	work.Finish(err)
	if err != nil {
		closure.result.Retire()
		return nil, err
	}
	proposal.captureSources()
	closure = admissionClosure{result: *proposal}
	return &closure.result, nil
}

func (c *admissionClosure) captureIncoming() {
	if c.incoming.Obj == nil {
		return
	}
	if len(c.incoming.Productions) == 0 {
		c.incoming.Productions = c.proposal.reads.Promotions(c.incoming.Obj.Named(), nil)
	}
	guard := c.proposal.Request.Reads
	if guard == nil {
		return
	}
	for ref := range manifest.References(c.incoming.Obj, manifest.ReferenceRelations) {
		if ref.Selector != nil {
			guard.ListObjects(ref.Group, ref.Kind)
		}
	}
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
	if len(c.history) != 0 {
		c.readHistory()
	}
	var candidates [2][2][]Occurrence
	var candidatesKnown [2]bool
	materializers := func(demand materializerDemand, consumer manifest.NamedResource) bool {
		mode := 0
		if demand.ordering || demand.application != "" {
			mode = 1
		}
		if !candidatesKnown[mode] {
			candidates[mode] = c.materializerCandidates(mode == 1)
			candidatesKnown[mode] = true
		}
		return c.materializers(new(demand), consumer, candidates[mode])
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
	if previous == role && (!c.proposal.Request.Invocation || c.incoming.Obj != nil) &&
		(c.incoming.Obj == nil || c.incoming.Obj.Named() != id) {
		record := c.record(id)
		if !c.loader.roots[id].admitted && record.Path == "" && c.loader.candidates[id].Obj == nil ||
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
	if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
		policy = c.incoming.Policy(id)
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
	if c.incoming.Obj != nil {
		return c.selectIncoming()
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
	if c.incoming.Entry != nil && c.loader.Store.GetDeclaration(c.incoming.Obj.Named()) != c.incoming.Obj {
		if c.children == nil {
			c.children = map[manifest.NamedResource]store.Child{}
		}
		c.children[c.incoming.Obj.Named()] = c.incoming
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
