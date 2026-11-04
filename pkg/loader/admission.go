package loader

import (
	"context"
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Admission is a temporary proposal computed outside execution-membership locks.
type Admission struct {
	Needed     []manifest.NamedResource
	Selected   []manifest.NamedResource
	Dirty      []manifest.NamedResource
	Objects    []store.Child
	Blocked    []string
	Revision   uint64
	Namespaces []string
}

type admissionReader interface {
	Lookup(manifest.NamedResource) (manifest.BaseManifest, manifest.DeclarationPolicy)
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetDeclaration(manifest.NamedResource) manifest.BaseManifest
	ReferenceFacts(manifest.NamedResource, *manifest.DiscoveryIndex, bool, manifest.NamedResource) store.ReferenceFacts
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
	GetStatus(manifest.NamedResource) (store.StatusInfo, bool)
	Productions(manifest.NamedResource, *manifest.DiscoveryIndex) []manifest.Production
	IncomingProductions(store.Child, *manifest.DiscoveryIndex) []manifest.Production
	GetEmission(manifest.NamedResource) (manifest.BaseManifest, map[string]any, store.Emission)
	ListObjects(string, string) []manifest.BaseManifest
}

type (
	admissionRole  struct{ needed, selected, dirty bool }
	admissionState struct {
		ordinal                                   int
		productions                               []manifest.Production
		role, walked                              admissionRole
		consumer, generation, restoration, claim  int
		changed, seed, direct, rescan             bool
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
	counts             [3]int
	collecting         bool
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
	incoming store.Child,
	reads *store.ReadSet,
	history map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	},
) (*Admission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Admission cancellation preserves the original context error identity.
	}
	if l.Options.Filter == nil {
		return &Admission{}, nil
	}
	closure := admissionClosure{loader: l, incoming: incoming, history: history}
	proposal := &closure.result
	closure.proposal = proposal
	_, _, _, proposal.Revision = l.Options.Filter.Membership(manifest.NamedResource{})
	var objects admissionReader = l.Store
	if reads != nil {
		objects = reads
	}
	if !l.frozen && !l.sideKnown && l.Options.Changes != nil {
		input := objects.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		_, snapshots, _ := l.Options.Changes.ForSource(manifest.BootstrapSourceID)
		for side, snapshot := range snapshots {
			if snapshot != nil && snapshot == input {
				l.side, l.sideKnown = manifest.TreeSide(side), true
				break
			}
		}
	}
	closure.objects = objects
	if !l.frozen && l.scopes != nil {
		for _, root := range l.StandaloneRoots(l.SourceRoot) {
			if filepath.Join(l.SourceRoot, filepath.FromSlash(root)) == l.entryPath {
				closure.roots = append(closure.roots, root)
			}
		}
	}
	proposal, err := closure.evaluate(ctx, reads)
	if err != nil {
		return nil, err
	}
	closure = admissionClosure{result: *proposal}
	return &closure.result, nil
}

func (c *admissionClosure) selectsChild(id manifest.NamedResource) bool {
	selection := c.loader.Options.Selection
	return selection.Matches(id) || selection.Roots[0].Name != "" || selection.Roots[0].Namespace == ""
}

func (c *admissionClosure) evaluate(ctx context.Context, reads *store.ReadSet) (*Admission, error) {
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
	if err := c.seedAdmission(ctx, reads, materializers); err != nil {
		return nil, err
	}
	return c.finishAdmission(ctx, materializers)
}

func (c *admissionClosure) role(id manifest.NamedResource) admissionRole {
	role := c.states[id].role
	needed, selected, dirty, _ := c.loader.Options.Filter.Membership(id)
	role.needed, role.selected, role.dirty = role.needed || needed, role.selected || selected, role.dirty || dirty
	return role
}

func (c *admissionClosure) add(id manifest.NamedResource, role admissionRole) {
	if id == (manifest.NamedResource{}) || !role.needed {
		return
	}
	previous := c.role(id)
	role.needed, role.selected, role.dirty = true, role.selected || previous.selected, role.dirty || previous.dirty
	if previous == role && (c.incoming.Obj == nil || c.incoming.Obj.Named() != id) {
		record := c.record(id)
		if !c.loader.roots[id] && record.Path == "" && c.loader.candidates[id].Obj == nil ||
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
	for axis, present := range [3]bool{role.needed, role.selected, role.dirty} {
		previous := [3]bool{state.role.needed, state.role.selected, state.role.dirty}
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
	reads *store.ReadSet,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	switch {
	case c.incoming.Obj == nil && reads != nil:
		for id := range reads.Required() {
			role := c.role(id)
			if !role.needed || c.objects.GetDeclaration(id) == nil {
				role.needed = true
				c.assignRole(id, role)
			}
		}
	case c.incoming.Obj == nil:
		if err := c.start(ctx, materializers); err != nil {
			return err
		}
	default:
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
	if total := c.counts[0] + c.counts[1] + c.counts[2]; total != 0 {
		roles := make([]manifest.NamedResource, total)
		c.proposal.Needed = roles[:0:c.counts[0]]
		c.proposal.Selected = roles[c.counts[0] : c.counts[0] : c.counts[0]+c.counts[1]]
		c.proposal.Dirty = roles[c.counts[0]+c.counts[1] : c.counts[0]+c.counts[1]]
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
	}
	for _, child := range c.children {
		c.proposal.Objects = append(c.proposal.Objects, child)
	}
	slices.SortFunc(c.proposal.Needed, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Selected, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Dirty, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Objects, func(a, b store.Child) int { return a.Obj.Named().Compare(b.Obj.Named()) })
	slices.Sort(c.proposal.Blocked)
	c.proposal.Blocked = slices.Compact(c.proposal.Blocked)
	return c.proposal, ctx.Err() //nolint:wrapcheck // Admission cancellation preserves the original context error identity.
}
