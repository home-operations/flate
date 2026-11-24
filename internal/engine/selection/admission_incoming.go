package selection

import (
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

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

func (c *admissionClosure) selectIncoming(id manifest.NamedResource) error {
	role := c.role(id)
	if c.session.comparisonOnly {
		if role.needed || role.readiness {
			c.add(id, role)
		}
		return nil
	}
	outside := c.outsideEntry(c.owner, id)
	if outside && c.proposal.Request.AwaitEmission && c.session.request.Matches(id) &&
		!c.scopes.scopes.Scope(c.owner).Current() {
		return EntryMembershipFailure(c.context.EntryPath, c.owner, id)
	}
	if !role.needed && (outside || c.incoming(id).Entry != nil) {
		return nil
	}
	eligible := !outside && c.session.request.Matches(id)
	if c.decision.unrestricted {
		c.add(id, admissionRole{needed: true, selected: eligible, dirty: true, ordinary: true})
		return nil
	}
	var affected, footprintRead bool
	if eligible && !c.session.request.Changed {
		role.needed, role.selected, role.dirty = true, true, true
	}
	role = c.productionRole(id, role, eligible)
	if c.incomingInScope(id, role, eligible) {
		affected, footprintRead = c.affected(id, c.incoming(id).Obj), true
	}
	if c.incomingInScope(id, role, eligible) && (affected || c.changedInput(id, id)) {
		role.needed = true
		if eligible {
			role.selected, role.dirty = true, true
		}
	}
	return c.completeSelectIncoming(
		&completeSelectIncomingState{
			id:            &id,
			role:          &role,
			eligible:      &eligible,
			affected:      &affected,
			footprintRead: &footprintRead,
		},
	)
}

func (c *admissionClosure) incomingInScope(id manifest.NamedResource, role admissionRole, eligible bool) bool {
	settled := role.selected && role.dirty
	return !settled && (eligible || role.needed || c.matchesChangedScope(id))
}

func (c *admissionClosure) productionRole(id manifest.NamedResource, role admissionRole, eligible bool) admissionRole {
	if comparison, generated := c.generatedRead(id, nil); generated && !comparison.Changed {
		return role
	}
	{
		productionValues := c.productions(id)
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]
			parent := c.role(production.Owner)
			inherited := parent.selected && parent.dirty && c.selectsChild(production.Owner, id)
			inherited = inherited && (!production.Owner.ReportHidden() || !c.isFileIndexed(id))
			selected := inherited || eligible && c.scopes.scopes.allowsProduction(&production, id) &&
				c.changedProduction(production)
			if selected &&
				(!c.session.request.Changed || !c.unchangedDeclaration(id, production.Owner, c.incoming(id).Obj)) {
				role.needed, role.selected, role.dirty = true, true, true
			}
		}
	}

	return role
}

func (c *admissionClosure) reverseInputRole(
	id manifest.NamedResource,
	role admissionRole,
	eligible bool,
) admissionRole {
	object := c.object(id)
	for ref := range c.references(id, object) {
		if !c.reverse(id, object, &ref) {
			continue
		}
		inputs, known, err := c.matchingInputs(new(ref), true)
		if err != nil || !known {
			continue
		}
		for _, input := range inputs {
			if c.reverseInputChanged(id, input, new(ref)) {
				role.needed = true
				if eligible {
					role.selected, role.dirty = true, true
				}
			}
		}
	}
	return role
}

func (c *admissionClosure) reverseInputChanged(id, input manifest.NamedResource, ref *manifest.Reference) bool {
	if ref.Ordering {
		state := c.states[input]
		if c.role(input).dirty || state.changed || state.readinessAffected ||
			c.orderingDeclarationChanged(input) {
			c.reachReadinessReader(id)
		}
		return false
	}
	if !c.role(input).dirty && !c.affected(input, c.object(input)) && !c.changedInput(id, input) {
		return false
	}
	return c.reverseChanged(id, c.object(id), ref)
}

func (c *admissionClosure) sourceRole(id manifest.NamedResource, role admissionRole) admissionRole {
	source, path, applies := c.incoming(id).Obj.SourcePath()
	if !applies || role.needed {
		return role
	}
	if output, ok := c.objects.GetArtifact(id, store.ArtifactContent).(*store.Emission); ok && output.Complete &&
		output.OutputReads.Valid() {
		return role
	}
	if c.incoming(id).Entry != nil {
		return role
	}
	if c.namespaceProductionDemand(id) {
		role.needed, role.ordinary = true, true
	}
	//nolint:gocritic // The iterator supplies immutable value snapshots.
	for target, record := range c.coveredDeclarations(source, path) {
		c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{FileEntries: 1})
		targetRole := c.role(target)
		needed := targetRole.needed && (!targetRole.readiness || targetRole.ordinary || !c.session.request.Changed)
		if !needed || record.BuilderOwner != (manifest.NamedResource{}) ||
			!c.coversDeclaration(new(rootOwnerFact(c.incoming(id).Obj, c.rootReader())), target, targetedClaimScope) {
			continue
		}
		evidence := c.declarationEvidence(target)
		if evidence.accepted {
			continue
		}
		if record.Parent != (manifest.NamedResource{}) && record.Parent != id &&
			c.objects.GetDeclaration(target) != nil {
			continue
		}
		if c.rootEvidence(target).Admitted {
			continue
		}
		role.needed = true
		break
	}

	return role
}

func (c *admissionClosure) selectInputReaders(id manifest.NamedResource, orderingOnly bool) error {
	key := id
	key.Name = ""
	for _, readers := range [][]manifest.NamedResource{c.context.InputReaders[id], c.context.InputReaders[key]} {
		for _, reader := range readers {
			if err := c.selectInputReader(id, reader, orderingOnly); err != nil {
				return err
			}
		}
	}
	return nil
}

type completeSelectIncomingState struct {
	id            *manifest.NamedResource
	role          *admissionRole
	eligible      *bool
	affected      *bool
	footprintRead *bool
}

func (c *admissionClosure) completeSelectIncoming(scope *completeSelectIncomingState) error {
	if c.incomingInScope(*scope.id, *scope.role, *scope.eligible) {
		(*scope.role) = c.reverseInputRole((*scope.id), (*scope.role), (*scope.eligible))
	}
	(*scope.role) = c.sourceRole((*scope.id), (*scope.role))
	c.add((*scope.id), (*scope.role))
	if !scope.role.dirty && !(*scope.footprintRead) {
		(*scope.affected) = c.affected((*scope.id), c.incoming(*scope.id).Obj)
	}
	if scope.role.dirty || (*scope.affected) {
		if err := c.selectInputReaders((*scope.id), false); err != nil {
			return err
		}
	}
	return nil
}

func (c *admissionClosure) selectInputReader(id, reader manifest.NamedResource, orderingOnly bool) error {
	object := c.object(reader)
	for ref := range c.references(reader, object) {
		if orderingOnly && !ref.Ordering {
			continue
		}
		matches, err := c.matchesReaderInput(id, reader, object, &ref)
		if err != nil {
			return err
		}
		if !matches {
			continue
		}
		if ref.Ordering {
			c.reachReadinessReader(reader)
			continue
		}
		if !c.session.request.Matches(reader) || !c.role(id).dirty && !c.affected(id, c.object(id)) {
			continue
		}
		if !c.reverseChanged(reader, object, new(ref)) {
			continue
		}
		c.add(reader, admissionRole{needed: true, selected: true, dirty: true})
	}
	return nil
}

func (c *admissionClosure) matchesReaderInput(
	id, reader manifest.NamedResource, object manifest.BaseManifest, ref *manifest.Reference,
) (bool, error) {
	if ref.Group != id.Group || ref.Kind != id.Kind || ref.Namespace != id.Namespace {
		return false, nil
	}
	if !c.reverse(reader, object, ref) {
		return false, nil
	}
	inputs, known, err := c.matchingInputs(ref, true)
	return known && slices.Contains(inputs, id), err
}

func (c *admissionClosure) reachReadinessReader(id manifest.NamedResource) {
	state := c.states[id]
	if state.readinessAffected || !c.session.request.Changed || c.collecting && state.changed {
		return
	}
	state.readinessAffected, state.walkedOnce = true, false
	c.states[id] = state
	queued := len(c.queue)
	if !c.collecting && c.session.request.Matches(id) && !id.ReportHidden() {
		c.add(id, admissionRole{readiness: true, fatal: true})
	}
	if len(c.queue) == queued {
		c.queue = append(c.queue, id)
	}
}

func (c *admissionClosure) namespaceProductionDemand(id manifest.NamedResource) bool {
	if !slices.ContainsFunc(c.session.request.Roots, func(root manifest.NamedResource) bool {
		return root.Name == "" && root.Namespace != ""
	}) {
		return false
	}
	object := c.object(id)
	if object == nil {
		return false
	}
	if _, _, applies := object.SourcePath(); !applies {
		return false
	}
	namespaceSelected := manifest.KindOf(object).Parent &&
		slices.ContainsFunc(c.session.request.Roots, func(root manifest.NamedResource) bool {
			return root.Name == "" && root.Namespace != "" && object.EmissionPolicy().MatchesNamespace(root)
		})
	if !namespaceSelected {
		return false
	}
	neededProducer := slices.ContainsFunc(c.productions(id), func(p manifest.Production) bool {
		return p.Owner != (manifest.NamedResource{}) && c.role(p.Owner).needed
	})
	if !neededProducer {
		return false
	}
	return !c.session.request.Changed || c.affected(id, c.object(id)) || c.changedInput(id, id)
}

func (c *admissionClosure) coveredDeclarations(
	source manifest.NamedResource,
	path string,
) iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] {
	if c.catalog == nil {
		return c.facts.SelectionIndex().Files()
	}
	return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		delta, input, _ := c.delta(source)
		if input == nil {
			return
		}
		root := input.OriginPath(filepath.Join(input.LocalPath, path))
		if !containsPath(input.OriginPath(input.LocalPath), root) {
			return
		}
		records := c.facts.SelectionIndex().Records
		if !c.catalog.yieldCoveredFiles(records, root, yield) {
			return
		}
		for id := range c.comparisonInputs(delta, manifest.NamedResource{}) {
			record := records[id]
			if record.File != "" && record.Path == "" && record.BuilderOwner == (manifest.NamedResource{}) &&
				!yield(id, record) {
				return
			}
		}
	}
}

func (c *admissionCatalog) yieldCoveredFiles(
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
	root string,
	yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool,
) bool {
	compare := func(id manifest.NamedResource, path string) int {
		return strings.Compare(records[id].Path, path)
	}
	first, _ := slices.BinarySearchFunc(c.files, root, compare)
	for _, id := range c.files[first:] {
		record := records[id]
		if record.Path != root {
			break
		}
		if !yield(id, record) {
			return false
		}
	}
	prefix := strings.TrimSuffix(root, string(filepath.Separator)) + string(filepath.Separator)
	first, _ = slices.BinarySearchFunc(c.files, prefix, compare)
	for _, id := range c.files[first:] {
		record := records[id]
		if !strings.HasPrefix(record.Path, prefix) {
			break
		}
		if !yield(id, record) {
			return false
		}
	}
	return true
}
