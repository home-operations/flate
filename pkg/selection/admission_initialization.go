package selection

import (
	"context"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/pkg/store"
)

//nolint:funlen // Claims and restoration worklists MUST grow in this frame to retain stack allocation.
func (c *admissionClosure) start(
	ctx context.Context,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	c.collecting = true
	c.queue = c.queue[:0]
	objects := c.session.store.Objects()
	c.indexStartObjects(objects)

	edges := make([]admissionStartEdge, 0, len(c.identities))
	link := func(from, to manifest.NamedResource) {
		state := c.states[from]
		edges = append(
			edges,
			admissionStartEdge{consumer: to, next: state.consumer, generation: c.states[to].generation},
		)
		state.consumer = len(edges)
		c.states[from] = state
	}

	var restorations []admissionStartRestoration

	var seeds admissionSeeds

	var claims []admissionStartClaim
	claimFile := func(id manifest.NamedResource, file string, ancestor bool) {
		state := c.states[id]
		claims = append(claims, admissionStartClaim{file: file, next: state.claim, ancestor: ancestor})
		state.claim = len(claims)
		c.states[id] = state
	}
	for source := range c.proposal.Request.Changes.Sources() {
		for file := range c.sourceFiles(source) {
			c.seedChangedReaders(file)
			index := *c.facts.SelectionIndex()
			index.Readers = nil
			ownership := index.OwnersOfFile(file)
			for _, owner := range ownership.Owners {
				claimFile(owner, file, false)
			}
			for _, ancestor := range ownership.Ancestors {
				claimFile(ancestor, file, true)
			}
		}
	}
	known := make([]bool, len(c.session.request.Roots))
	scan := func(id manifest.NamedResource) {
		obj, record, changed := c.prepareInitialIdentity(id)
		seeds.scanReferences(c, id, obj, link)
		c.linkInitialParent(id, &record, obj, link)
		productions := seeds.scanProductions(c, id, obj, link)
		c.recordDirectState(id, &record, changed, claims)
		c.noteKnownSelection(id, obj, productions, known)
	}
	for _, identity := range c.identities {
		id := identity.id
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		scan(id)
		restorations = c.recordInitialRestorations(id, restorations)
	}
	c.queue = slices.Grow(c.queue, len(c.states))
	for {
		c.prepareInitialChanges(seeds, edges)
		//nolint:intrange // Change propagation MUST extend the worklist during traversal.
		for cursor := 0; cursor < len(c.queue); cursor++ {
			id := c.queue[cursor]
			c.propagateInitialChange(id, edges)
		}
		{
			rescanned, err := c.rescanInitialChanges(ctx, edges, restorations, scan)
			if err != nil {
				return err
			}
			if !rescanned {
				break
			}
		}
	}
	c.admitInitialStates(claims)
	c.finishStart(known, materializers)

	return nil
}

func (c *admissionClosure) admitChangedRoots() {
	for _, root := range c.roots {
		var identities []manifest.NamedResource
		for i := range c.context.Roots {
			if facts := &c.context.Roots[i]; facts.Path == root {
				identities = facts.Identities
				break
			}
		}
		matches := c.rootMatches(root, identities)
		entry := filepath.Join(c.context.Root, filepath.FromSlash(root)) == c.context.EntryPath &&
			c.session.request.Matches(manifest.NamedResource{})
		if !matches && !entry {
			continue
		}
		id := manifest.NewEntryKustomization(manifest.BootstrapSourceID, root, manifest.NamedResource{}).Named()
		if c.rootRequired(root) || !c.session.request.Changed || c.proposal.Request.Changes == nil ||
			c.affected(id, c.object(id)) || entry && c.literalEntryChanged() {
			c.proposal.Blocked = append(c.proposal.Blocked, root)
		}
	}
}

func (c *admissionClosure) rootMatches(root string, identities []manifest.NamedResource) bool {
	matches := c.rootRequired(root) || slices.ContainsFunc(
		identities,
		func(id manifest.NamedResource) bool { return c.session.request.Matches(id) },
	)
	if !matches {
		delta, _, _ := c.delta(manifest.BootstrapSourceID)
		for id, input := range delta.Inputs(manifest.NamedResource{}) {
			if c.session.request.Matches(id) &&
				slices.ContainsFunc(input.Declarations, func(file string) bool {
					return containsPath(c.context.Root, filepath.Join(c.context.Root, root)) &&
						containsPath(
							filepath.Join(c.context.Root, root),
							filepath.Join(c.context.Root, filepath.FromSlash(file)),
						)
				}) {
				matches = true
				break
			}
		}
	}

	return matches
}

func (c *admissionClosure) rootRequired(root string) bool {
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, root, manifest.NamedResource{}).Named()
	if role := c.role(owner); role.needed && role.ordinary && c.states[owner].requested {
		return true
	}
	for id := range c.states {
		role := c.role(id)
		if !id.KindInfo().Runnable || !role.needed || !role.ordinary {
			continue
		}
		if role.selected && !c.states[id].requested {
			continue
		}
		evidence := c.declarationEvidence(id)
		if !evidence.known() && evidence.owner == owner {
			return true
		}
	}
	return false
}

//nolint:gocognit // Initial obligations MUST seed one closure before its work queue is built.
func (c *admissionClosure) finishStart(
	known []bool,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) {
	if c.context.NativeEntry && c.session.request.Matches(manifest.NamedResource{}) {
		path, _ := filepath.Rel(c.context.Root, c.context.EntryPath)
		owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(path), manifest.NamedResource{}).
			Named()
		if object := c.object(owner); object != nil && c.affected(owner, object) {
			c.add(owner, admissionRole{needed: true, ordinary: true})
		}
	}
	slices.Sort(c.proposal.Namespaces)
	c.proposal.Namespaces = slices.Compact(c.proposal.Namespaces)
	for i, target := range c.session.request.Roots {
		if !known[i] || target.Name == "" && target.Namespace != "" {
			materializers(
				materializerDemand{target: target, source: c.context.Root, required: target.Name != ""},
				manifest.NamedResource{},
			)
		}
	}
	if route := c.proposal.entry; route != nil && route.Application != nil &&
		!c.proposal.entrySupported() && c.context.PlainEntry && c.literalEntryChanged() && !c.pendingEntrySources() {
		application := route.Application
		root := manifest.NewEntryKustomization(application.Source, application.Path, manifest.NamedResource{}).Named()
		c.acquire(Demand{Identity: root, Source: application.Source, Root: application.Path})
		materializers(materializerDemand{
			target: application.Source, source: c.context.Root,
			application: filepath.Join(c.context.Root, filepath.FromSlash(application.Path)), required: true,
		}, manifest.NamedResource{})
	}

	for _, id := range slices.SortedFunc(maps.Keys(c.historyOwners), manifest.NamedResource.Compare) {
		if route := c.proposal.entry; route != nil && route.Bound &&
			(route.Producer == id || route.Root == id) && c.proposal.entrySupported() {
			continue
		}
		if !c.replace[id] && c.historyOwnerSupported(id) {
			c.add(id, admissionRole{needed: true, ordinary: true})
		}
	}
	c.queue = c.queue[:0]
	c.collecting = false
	//nolint:gocritic // Map values MUST remain independent snapshots until publication.
	for id, state := range c.states {
		if !state.role.needed && c.namespaceProductionDemand(id) {
			c.add(id, admissionRole{needed: true, ordinary: true})
		}
		if c.states[id].role.needed {
			c.queue = append(c.queue, id)
		}
	}
}

func (c *admissionClosure) historyOwnerSupported(id manifest.NamedResource) bool {
	if !c.declarationLocated(id, c.objects) {
		return false
	}
	record := c.record(id)
	root := c.rootEvidence(id)
	if !root.Admitted && c.objects.GetDeclaration(id) == nil && record.Builder == "" && len(c.productions(id)) == 0 {
		return false
	}
	matches := func(source manifest.NamedResource) bool {
		input, _ := c.objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
		if input == nil || !input.Reads.Valid() {
			return false
		}
		matchesInput := func(input *store.SourceArtifact) bool {
			return input != nil && input.Reads.Valid() &&
				slices.ContainsFunc(c.historyOwners[id], func(evidence manifest.SourceEvidence) bool {
					return evidence.Source == input.HistoryIdentity(source)
				})
		}
		if matchesInput(input) {
			return true
		}
		facts := c.boundSource(source)
		return facts.Paired && slices.ContainsFunc(facts.Snapshots[:], matchesInput)
	}
	if matches(c.source(id)) {
		return true
	}
	for source := range c.sources(id) {
		if matches(source) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) indexStartObjects(objects []manifest.BaseManifest) {
	capacity := max(len(c.facts.SelectionIndex().Records), len(objects))
	if c.states == nil {
		c.states = make(map[manifest.NamedResource]admissionState, capacity)
	}
	if c.identities == nil {
		c.identities = make([]admissionIdentity, 0, capacity)
	}
	index := func(id manifest.NamedResource) {
		state := c.states[id]
		if state.listed {
			return
		}
		state.listed = true
		if state.ordinal == 0 {
			state.ordinal = len(c.identities) + 1
			c.identities = append(c.identities, admissionIdentity{id: id})
		}
		c.states[id] = state
	}
	for id := range c.facts.SelectionIndex().Records {
		index(id)
	}
	for _, records := range c.accepted {
		for id := range records {
			index(id)
		}
	}
	for _, obj := range objects {
		index(obj.Named())
	}
	delta, _, _ := c.delta(manifest.BootstrapSourceID)
	for id := range delta.Inputs(manifest.NamedResource{}) {
		if c.changedInput(id, id) {
			index(id)
		}
	}
	c.retireIdentities()
}

func (c *admissionClosure) retireIdentities() {
	previous := len(c.identities)
	c.identities = slices.DeleteFunc(c.identities, func(identity admissionIdentity) bool {
		if c.states[identity.id].listed {
			return false
		}
		state := c.states[identity.id]
		// First-attempt presence MUST survive inventory pruning within one admission call.
		if state.incoming != 0 || state.publication != 0 && state.publication == c.publication {
			c.states[identity.id] = admissionState{
				incoming:            state.incoming,
				incomingProductions: state.incomingProductions,
				publication:         state.publication,
			}
		} else {
			delete(c.states, identity.id)
		}
		return true
	})
	if len(c.identities) != previous {
		for i := range c.identities {
			identity := &c.identities[i]
			identity.stableOwner, identity.stableObject = 0, nil
			state := c.states[identity.id]
			state.ordinal = i + 1
			c.states[identity.id] = state
		}
	}
}

type admissionStartEdge struct {
	consumer         manifest.NamedResource
	next, generation int
}

type admissionStartRestoration struct {
	child manifest.NamedResource
	next  int
}

type admissionStartSeed struct {
	target, origin   manifest.NamedResource
	generation       int
	requiresConsumer bool
}

type admissionStartClaim struct {
	file     string
	next     int
	ancestor bool
}

type admissionSeeds []admissionStartSeed

func (s *admissionSeeds) scanReferences(
	c *admissionClosure,
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	link func(from, to manifest.NamedResource),
) {
	for ref := range c.references(id, obj) {
		if !c.reverse(id, obj, new(ref)) {
			continue
		}
		inputs, err := c.matchingInputs(new(ref), true)
		if err != nil {
			continue
		}
		for _, input := range inputs {
			if c.changedInput(id, input) {
				*s = append(
					*s,
					admissionStartSeed{target: input, origin: id, generation: c.states[id].generation},
				)
			}
			link(input, id)
		}
	}
}

func (s *admissionSeeds) scanProductions(
	c *admissionClosure,
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	link func(from, to manifest.NamedResource),
) []manifest.Production {
	productions := c.productions(id)
	{
		pValues := productions
		for pIndex := range pValues {
			p := pValues[pIndex]
			owner := p.Materializer(id)
			if owner == (manifest.NamedResource{}) && p.Owner != (manifest.NamedResource{}) {
				continue
			}
			if p.Declaration != (manifest.NamedResource{}) {
				if p.Authored == id && p.Declaration != id {
					continue
				}
				owner = p.Declaration
				if c.changedProduction(p) {
					*s = append(
						*s,
						admissionStartSeed{
							target:           owner,
							origin:           id,
							generation:       c.states[id].generation,
							requiresConsumer: true,
						},
					)
				}
			}
			if owner != id && owner != (manifest.NamedResource{}) && !owner.ReportHidden() &&
				!c.unchangedDeclaration(id, owner, obj) {
				link(owner, id)
			}
		}
	}
	return productions
}

func (c *admissionClosure) recordDirectState(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	changed bool,
	claims []admissionStartClaim,
) {
	state := c.states[id]
	if state.directKnown {
		state.requested = state.requested || c.namedSelection(id)
		c.states[id] = state
		return
	}
	c.derive()
	state.direct, state.directKnown = changed, state.initialKnown
	state.requested = state.requested || c.namedSelection(id)
	state.eligible = record.Origin.Policy(id, nil).Root && c.session.request.Matches(id)
	state.declarationChanged = state.eligible && !changed &&
		(c.fileChanged(c.source(id), record.File) || c.inputsChanged(c.source(id), record.Inputs))
	for next := state.claim; next != 0; next = claims[next-1].next {
		claim := claims[next-1]
		if claim.ancestor {
			continue
		}
		for source := range c.sources(id) {
			state.direct = state.direct ||
				c.changedClaim(new(manifest.FileReader{Reader: id, Source: source}), claim.file)
		}
	}
	c.states[id] = state
}

func (c *admissionClosure) namedSelection(id manifest.NamedResource) bool {
	for _, root := range c.session.request.Roots {
		if root.Name != "" && root.Name == id.Name && root.Group == id.Group && root.Kind == id.Kind &&
			(root.Namespace == "" || root.Namespace == id.Namespace) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) noteKnownSelection(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	productions []manifest.Production,
	known []bool,
) {
	for i, target := range c.session.request.Roots {
		if !known[i] && id.Group == target.Group && id.Kind == target.Kind &&
			(target.Name == "" || id.Name == target.Name) &&
			(target.Namespace == "" || id.Namespace == target.Namespace) &&
			(c.isFileIndexed(id) || obj != nil || len(productions) != 0) {
			known[i] = true
		}
	}
}

func (c *admissionClosure) activateInitialSeeds(seeds admissionSeeds, edges []admissionStartEdge) {
	{
		seedValues := seeds
		for seedIndex := range seedValues {
			seed := seedValues[seedIndex]
			state := c.states[seed.origin]
			if seed.generation != state.generation {
				continue
			}
			if !seed.requiresConsumer {
				c.markChanged(seed.target)
				continue
			}
			for next := state.consumer; next != 0; next = edges[next-1].next {
				edge := edges[next-1]
				if edge.generation == c.states[edge.consumer].generation {
					c.markChanged(seed.target)
					break
				}
			}
		}
	}
}

func (c *admissionClosure) propagateInitialChange(id manifest.NamedResource, edges []admissionStartEdge) {
	for next := c.states[id].consumer; next != 0; next = edges[next-1].next {
		edge := edges[next-1]
		consumer := edge.consumer
		if edge.generation != c.states[consumer].generation || c.states[consumer].changed {
			continue
		}
		obj := c.object(consumer)
		unchanged := false
		for ref := range c.references(consumer, obj) {
			if ref.NamedResource != id {
				continue
			}
			comparison := c.outputChanged(consumer, new(ref), obj)
			unchanged = comparison.Known && !comparison.Changed
			if !unchanged {
				break
			}
		}
		if !unchanged {
			c.markChanged(consumer)
			c.queue = append(c.queue, consumer)
		}
	}
}

func (c *admissionClosure) seedChangedReaders(file string) {
	for reader := range c.facts.SelectionIndex().ReadersOfFile(file) {
		c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{ReaderEdges: 1})
		if c.changedClaim(new(reader), file) {
			if reader.Materialize {
				if !c.replace[reader.Reader] {
					c.add(reader.Reader, admissionRole{needed: true, ordinary: true})
				}
				continue
			}
			state := c.states[reader.Reader]
			state.seed = true
			c.states[reader.Reader] = state
		}
	}
}

func (c *admissionClosure) admitAncestorClaims(
	id manifest.NamedResource,
	state *admissionState,
	claims []admissionStartClaim,
) {
	for next := state.claim; next != 0; next = claims[next-1].next {
		claim := claims[next-1]
		if !claim.ancestor {
			continue
		}
		for source := range c.sources(id) {
			if c.changedClaim(new(manifest.FileReader{Reader: id, Source: source}), claim.file) {
				c.add(id, admissionRole{needed: true, ordinary: true})
			}
		}
	}
}

func (c *admissionClosure) selectInitialState(id manifest.NamedResource, state *admissionState) {
	if !state.listed {
		return
	}
	if state.changed && c.session.request.MatchesScope(id) {
		c.add(id, admissionRole{needed: true, selected: state.eligible && !id.ReportHidden(), dirty: true})
		return
	}
	if !state.eligible {
		return
	}
	if state.changed || !c.session.request.Changed || c.proposal.Request.Changes == nil {
		c.add(id, admissionRole{needed: true, selected: !id.ReportHidden(), dirty: true})
	} else if state.declarationChanged {
		c.add(id, admissionRole{needed: true, selected: true})
	}
}

func (c *admissionClosure) recordInitialEntry(id manifest.NamedResource, entry *manifest.NamedResource) {
	if id.ReportHidden() {
		if producer, ok := c.object(id).(interface {
			EmissionPolicy() manifest.EmissionPolicy
		}); ok {
			policy := producer.EmissionPolicy()
			if policy.DocumentScope {
				*entry = id
			}
		}
	}
}

func (c *admissionClosure) rescanInitialChanges(
	ctx context.Context,
	edges []admissionStartEdge,
	restorations []admissionStartRestoration,
	scan func(manifest.NamedResource),
) (bool, error) {
	c.queue = c.queue[:0]
	rescan := func(id manifest.NamedResource) {
		state := c.states[id]
		if state.rescan {
			return
		}
		state.rescan = true
		state.initialKnown, state.directKnown, state.affectedKnown = false, false, false
		c.states[id] = state
		c.queue = append(c.queue, id)
	}
	for owner, replacing := range c.replace {
		if !replacing || !c.states[owner].changed {
			continue
		}
		c.replace[owner] = false
		rescan(owner)
		for next := c.states[owner].restoration; next != 0; next = restorations[next-1].next {
			rescan(restorations[next-1].child)
		}
	}
	if len(c.queue) == 0 {
		return false, nil
	}

	for i := range c.identities {
		identity := &c.identities[i]
		identity.stableOwner, identity.stableObject = 0, nil
	}
	clear(c.historyProductions)
	//nolint:intrange // rescan MUST extend the worklist during traversal.
	for cursor := 0; cursor < len(c.queue); cursor++ {
		for next := c.states[c.queue[cursor]].consumer; next != 0; next = edges[next-1].next {
			edge := edges[next-1]
			if edge.generation == c.states[edge.consumer].generation {
				rescan(edge.consumer)
			}
		}
	}
	for _, id := range c.queue {
		if err := ctx.Err(); err != nil {
			return false, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		scan(id)
		state := c.states[id]
		state.rescan = false
		c.states[id] = state
	}
	return true, nil
}

func (c *admissionClosure) prepareInitialChanges(seeds admissionSeeds, edges []admissionStartEdge) {
	c.queue = c.queue[:0]
	//nolint:gocritic // Map values MUST remain independent snapshots until publication.
	for id, state := range c.states {
		state.changed = state.seed || state.direct
		c.states[id] = state
	}
	c.activateInitialSeeds(seeds, edges)
	//nolint:gocritic // Map values MUST remain independent snapshots until publication.
	for id, state := range c.states {
		if state.changed {
			c.queue = append(c.queue, id)
		}
	}
}

func (c *admissionClosure) admitInitialStates(claims []admissionStartClaim) {
	var entry manifest.NamedResource
	//nolint:gocritic // Map values MUST remain independent snapshots until publication.
	for id, state := range c.states {
		c.recordInitialEntry(id, &entry)
		if state.changed && !id.ReportHidden() && id.Namespace != "" {
			c.proposal.Namespaces = append(c.proposal.Namespaces, id.Namespace)
		}
		c.admitAncestorClaims(id, &state, claims)
		c.selectInitialState(id, &state)
	}
	if entry != (manifest.NamedResource{}) {
		status, _ := c.objects.GetStatus(entry)
		if status.Outcome() == store.OutcomeFailed {
			c.add(entry, admissionRole{needed: true, ordinary: true})
		}
	}
}

func (c *admissionClosure) linkInitialParent(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	obj manifest.BaseManifest,
	link func(from, to manifest.NamedResource),
) {
	if parent := record.Parent; parent != (manifest.NamedResource{}) && parent != id && !parent.ReportHidden() &&
		!c.unchangedDeclaration(id, parent, obj) {
		link(parent, id)
	}
}

//nolint:ireturn // Identity preparation MUST preserve the polymorphic manifest contract.
func (c *admissionClosure) prepareInitialIdentity(
	id manifest.NamedResource,
) (manifest.BaseManifest, manifest.DiscoveryRecord, bool) {
	state := c.states[id]
	state.generation++
	c.states[id] = state
	obj := c.object(id)
	record := c.record(id)
	if state.initialKnown {
		return obj, record, state.direct
	}
	c.derive()
	changed := c.affected(id, obj) || c.changedInput(id, id)
	state = c.states[id]
	state.initialKnown = c.proposal.Request.Changes == nil || state.affectedKnown
	c.states[id] = state
	return obj, record, changed
}

func (c *admissionClosure) recordInitialRestorations(
	id manifest.NamedResource,
	restorations []admissionStartRestoration,
) []admissionStartRestoration {
	{
		productionValues2 := c.states[id].productions
		for productionIndex2 := range productionValues2 {
			production := productionValues2[productionIndex2]
			if !c.replace[production.Owner] {
				continue
			}
			state := c.states[production.Owner]
			restorations = append(restorations, admissionStartRestoration{child: id, next: state.restoration})
			state.restoration = len(restorations)
			c.states[production.Owner] = state
		}
	}
	return restorations
}
