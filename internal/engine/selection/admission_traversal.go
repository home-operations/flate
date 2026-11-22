package selection

import (
	"cmp"
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/internal/engine/store"
)

func (c *admissionClosure) walk(
	ctx context.Context,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	if len(c.queue) == 0 {
		return nil
	}
	//nolint:intrange // add MUST extend the worklist during traversal.
	for cursor := 0; cursor < len(c.queue); cursor++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := c.queue[cursor]
		if c.decision.unrestricted && (c.incoming(id).Obj != nil || c.objects.GetDeclaration(id) != nil) {
			continue
		}
		if state := c.states[id]; state.readinessAffected && !state.walkedOnce {
			if err := c.selectInputReaders(id, true); err != nil {
				return err
			}
		}
		role := c.role(id)
		if !role.needed && !role.readiness {
			continue
		}
		if err := c.walkObject(ctx, id, materializers); err != nil {
			return err
		}
	}
	return nil
}

func (c *admissionClosure) completeHydrate(
	ctx context.Context,
	id, owner manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) manifest.BaseManifest {
	if child := c.facts.AdmissionCandidate(id); child.Obj != nil && record.Path == "" &&
		!Root(*c.capturedRoot(id)).InitialIndependent {
		if c.children == nil {
			c.children = map[manifest.NamedResource]store.Child{}
		}
		c.children[id] = child
		delete(c.hydrated, id)
		return child.Obj
	}
	occurrence, ok := c.occurrence(id)
	if !ok {
		return nil
	}
	prepared := c.prepareOccurrence(ctx, owner, &occurrence, true)
	if prepared == nil {
		return nil
	}
	if c.children == nil {
		c.children = map[manifest.NamedResource]store.Child{}
	}
	if c.hydrated == nil {
		c.hydrated = make(map[manifest.NamedResource]*occurrencePreparation)
	}
	c.children[id] = prepared.child
	c.hydrated[id] = prepared
	return prepared.child.Obj
}

func (c *admissionClosure) hydrationSource(
	id manifest.NamedResource,
	mounted *store.SourceArtifact,
) (*store.SourceArtifact, manifest.NamedResource) {
	if mounted == nil {
		return nil, manifest.NamedResource{}
	}
	input := c.facts.SourceFacts(manifest.BootstrapSourceID, nil)
	source := manifest.BootstrapSourceID
	if mounted != input.ConfiguredInput && mounted != input.Input {
		source = c.source(id)
	}
	candidate, _ := c.objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	// The canonical identity can publish authored material while the checkout still supplies declarations.
	checkout := mounted == input.ConfiguredInput && candidate != nil && candidate.Provider != store.ProviderInput
	if mounted != candidate && !checkout || !mounted.Reads.Valid() || !c.objects.SourceAdmission(id, source) {
		return nil, manifest.NamedResource{}
	}
	return mounted, source
}

func (c *admissionClosure) readerEmissionAllowed(id, readerID manifest.NamedResource) bool {
	object := c.object(readerID)
	if object == nil {
		object = c.catalogObject(readerID)
	}
	return object == nil || object.EmissionPolicy().MatchesNamespace(id)
}

func (c *admissionClosure) selectIncomingProducers(
	id manifest.NamedResource,
	role admissionRole,
	evidence *declarationEvidence,
) {
	if incoming := c.incoming(id); incoming.Obj == nil || incoming.VisibleGeneration {
		return
	}
	lookupRecord, _ := c.facts.SelectionIndex().Lookup(id)
	{
		productionValues3 := lookupRecord.Productions
		for productionIndex3 := range productionValues3 {
			production := productionValues3[productionIndex3]
			owner := production.Materializer(id)
			if !evidence.allowsOwner(owner) {
				continue
			}
			if c.isFileIndexed(owner) || c.object(owner) != nil {
				c.add(owner, admissionRole{needed: true, ordinary: true, dirty: role.dirty && c.role(owner).selected})
			}
		}
	}
}

func (c *admissionClosure) selectInventoryChildren(id manifest.NamedResource, role admissionRole) {
	if !role.selected || !role.dirty || !c.session.store.HasInventory(id) {
		return
	}
	_, _, output := c.objects.GetEmission(id)
	for i := range output.Children {
		child := &output.Children[i]
		if !c.selectsChild(id, child.Obj.Named()) {
			continue
		}
		if id.ReportHidden() && c.isFileIndexed(child.Obj.Named()) {
			continue
		}
		if comparison, generated := c.generatedRead(child.Obj.Named(), nil); generated && !comparison.Changed {
			continue
		}
		if c.unchangedDeclaration(child.Obj.Named(), id, child.Obj) {
			continue
		}
		c.add(child.Obj.Named(), admissionRole{needed: true, selected: true, dirty: true})
	}
	output.Finish()
}

func (c *admissionClosure) selectReferenceInputs(
	id manifest.NamedResource,
	refValue *manifest.Reference,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	ref := *refValue
	inputs, known, err := c.matchingInputs(new(ref), false)
	if err != nil {
		return err
	}
	if !known {
		return nil
	}
	for _, input := range inputs {
		c.add(input, admissionRole{needed: true, ordinary: true, dirty: c.states[input].changed})
	}
	if len(inputs) == 0 {
		materializers(materializerDemand{target: ref.NamedResource, source: c.declarationSource(id)}, id)
	}
	return nil
}

func (c *admissionClosure) materializeReferenceInput(
	id manifest.NamedResource,
	refValue *manifest.Reference,
	wasNeeded bool,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) {
	ref := *refValue
	if ref.Ordering || wasNeeded && (ref.Optional || ref.Acquisition || !ref.ContentInput()) {
		return
	}
	_, indexed := c.facts.SelectionIndex().Lookup(ref.NamedResource)
	unresolved := !indexed && !c.isFileIndexed(ref.NamedResource) && c.object(ref.NamedResource) == nil
	if !unresolved || c.objects.GetArtifact(ref.NamedResource, store.ArtifactContent) != nil {
		return
	}
	input := c.objects.ReferenceFacts(ref.NamedResource, c.facts.SelectionIndex(), id)
	admission := RequiredInput
	if c.missing != nil {
		admission = c.missing.Admission(new(ref), new(input))
	}
	if admission == AvailableInput {
		return
	}
	requiredContent := !ref.Acquisition && ref.ContentInput()
	required := requiredContent && admission == RequiredInput && !c.changedInput(id, ref.NamedResource)
	materializers(
		materializerDemand{
			target:   ref.NamedResource,
			key:      ref.Key,
			use:      ref.Use,
			source:   c.declarationSource(id),
			required: required,
		},
		id,
	)
}

func (c *admissionClosure) productionOwner(
	id manifest.NamedResource,
	pValue *manifest.Production,
	record *manifest.DiscoveryRecord,
	evidence *declarationEvidence,
) manifest.NamedResource {
	p := *pValue
	owner := p.Materializer(id)
	if evidence.independent && owner.ReportHidden() && owner == record.BuilderOwner {
		owner = manifest.NamedResource{}
	}
	unmaterialized := owner == (manifest.NamedResource{}) && !evidence.independent &&
		p.Declaration == (manifest.NamedResource{}) && p.Authored != (manifest.NamedResource{})
	if unmaterialized && (c.incoming(id).Obj == nil) &&
		c.objects.GetDeclaration(id) == nil {
		owner = p.Owner
	}
	return owner
}

func (c *admissionClosure) productionMaterialized(
	id manifest.NamedResource,
	production *manifest.Production,
	record *manifest.DiscoveryRecord,
	evidence *declarationEvidence,
) bool {
	owned := production.Owner != (manifest.NamedResource{}) && production.Owner != id
	declared := production.Declaration != (manifest.NamedResource{}) && production.Declaration != id
	if owned || declared {
		return true
	}
	if !evidence.independent || c.incoming(id).Obj == nil {
		return false
	}
	_, known := c.states[id]
	reached := c.decision.unrestricted || c.decisionRoles(id, known)&needed != 0
	unowned := production.Owner == (manifest.NamedResource{}) && production.Declaration == (manifest.NamedResource{})
	if !reached || !unowned {
		return false
	}
	located := production.Path == record.Path && production.File == record.File && production.Source == c.source(id)
	return located && manifest.Resolve(production.Authored, id.Namespace, "") == id
}

func (c *admissionClosure) walkObject(
	ctx context.Context,
	id manifest.NamedResource,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	selectedPending := c.entryPending[id]
	role := c.role(id)
	state := c.states[id]
	if selectedPending || state.walkedOnce && state.walked == role {
		return nil
	}
	state.walked, state.walkedOnce = role, true
	c.states[id] = state
	if c.session.request.Changed && role.readiness && !role.ordinary {
		return c.walkOrderingObject(ctx, id, materializers)
	}
	obj := c.object(id)
	record := c.record(id)

	root := c.rootEvidence(id)
	evidence := c.declarationEvidence(id)
	evidence.independent = root.Independent || c.bootstrapPrerequisite(id)
	evidence.qualification = !role.ordinary && !role.readiness
	bootstrap := evidence.independent && Root(*c.capturedRoot(id)).InitialIndependent
	parent := record.Parent
	if !evidence.known() {
		parent = cmp.Or(root.Owner, parent)
	}
	if !bootstrap && evidence.allowsOwner(parent) {
		c.add(parent, admissionRole{needed: true, ordinary: true, dirty: c.states[parent].changed})
	}
	applicationSource, pathBearing := c.catalogApplicationSource(id)
	materialized := c.selectProductions(id, &record, role, &evidence)
	c.selectIncomingProducers(id, role, &evidence)

	needsOwner := (!bootstrap || !state.requested) && !materialized
	pending := needsOwner && c.declarationNeedsOwner(id, obj, &record, &evidence) &&
		materializers(materializerDemand{target: id, source: c.declarationSource(id)}, id)
	if !pending && role.selected && pathBearing {
		pending = c.demandEntryPath(id, applicationSource, &record)
	}
	if pending && pathBearing {
		evidence.independent = false
	}

	if !pending {
		obj = c.hydrateDeclaration(ctx, id, obj, &evidence)
	}
	c.selectInventoryChildren(id, role)

	return c.walkReferences(id, obj, materializers)
}

func (c *admissionClosure) walkOrderingObject(
	ctx context.Context,
	id manifest.NamedResource,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	if object := c.hydratedDeclaration(id); object != nil {
		return c.walkReferences(id, object, materializers)
	}
	evidence := c.declarationEvidence(id)
	evidence.independent = c.rootEvidence(id).Independent || c.bootstrapPrerequisite(id)
	if evidence.authored || evidence.independent || evidence.built {
		c.hydrateDeclaration(ctx, id, nil, &evidence)
	}
	obj := c.objects.GetDeclaration(id)
	if child := c.children[id]; child.Obj != nil {
		obj = child.Obj
	}
	if incoming := c.incoming(id); incoming.Obj != nil {
		obj = incoming.Obj
	}
	if obj != nil {
		return c.walkReferences(id, obj, materializers)
	}
	occurrence, known := c.catalogOccurrence(id)
	if !known {
		return nil
	}
	if occurrence.Uncertain || occurrence.Identity.Name != occurrence.Authored.Name ||
		occurrence.RequiresBuild && !occurrence.OrderingKnown {
		if c.states[id].readinessAffected {
			c.needOrderingOwner(id, evidence.owner)
		}
		return nil
	}
	candidate := c.indexedOrderingDeclaration(ctx, evidence.owner, &occurrence)
	if candidate.object == nil {
		return nil
	}
	if c.proposal.orderingDeclarations == nil {
		c.proposal.orderingDeclarations = make(map[manifest.NamedResource]orderingDeclaration)
	}
	c.proposal.orderingDeclarations[id] = candidate
	return c.walkReferences(id, candidate.object, materializers)
}

func (c *admissionClosure) indexedOrderingDeclaration(
	ctx context.Context,
	owner manifest.NamedResource,
	occurrence *OccurrenceFacts,
) orderingDeclaration {
	// Ordering reads borrow the declaration without granting authority to render it.
	prepared := c.prepareOccurrence(ctx, owner, occurrence, false)
	if prepared == nil {
		return orderingDeclaration{}
	}
	return orderingDeclaration{
		declarationIdentity: prepared.key.declarationIdentity,
		object:              prepared.child.Obj, source: prepared.key.source, owner: owner,
	}
}

func (c *admissionClosure) needOrderingOwner(id, owner manifest.NamedResource) {
	if owner == (manifest.NamedResource{}) || owner == id || c.role(owner).ordinary {
		return
	}
	c.add(owner, admissionRole{needed: true, ordinary: true, dirty: c.states[owner].changed})
}

func (c *admissionClosure) requireOrderingOperand(id manifest.NamedResource) {
	if id.Name == "" || c.incoming(id).Obj != nil || c.children[id].Obj != nil || c.objects.GetDeclaration(id) != nil {
		return
	}
	if _, known := c.catalogOccurrence(id); known {
		c.needOrderingOwner(id, c.declarationEvidence(id).owner)
	}
}

func (c *admissionClosure) declarationNeedsOwner(
	id manifest.NamedResource,
	object manifest.BaseManifest,
	record *manifest.DiscoveryRecord,
	evidence *declarationEvidence,
) bool {
	if evidence.authored || evidence.built {
		return false
	}
	_, pathBearing := c.catalogApplicationSource(id)
	selected := c.role(id).selected
	if record.Path == "" {
		return false
	}
	if c.selectedDeclarationFileChanged(id, object, record, selected) {
		return true
	}
	return (selected || pathBearing) && !evidence.independent && c.objects.GetDeclaration(id) == nil
}

func (c *admissionClosure) catalogApplicationSource(id manifest.NamedResource) (manifest.NamedResource, bool) {
	occurrence, known := c.catalogOccurrence(id)
	if !known {
		return manifest.NamedResource{}, false
	}
	source, _, pathBearing := occurrence.SourcePath()
	return source, pathBearing
}

func (c *admissionClosure) walkReferences(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	generatedTargets := c.referenceTargets(id, obj)
	role := c.role(id)
	orderingDemand := role.ordinary || role.readiness
	evaluationOnly := c.session.request.Changed && role.readiness && !role.ordinary
	var orderingInputs []store.OrderingEdge
	references := c.references(id, obj)
	if evaluationOnly {
		references = manifest.References(obj, manifest.ReferenceAll)
	}
	for ref := range references {
		if ref.Ordering && !orderingDemand || evaluationOnly && !ref.Ordering {
			continue
		}
		if c.localGenerationReference(id, new(ref), generatedTargets) ||
			!c.prepareReferenceInput(id, new(ref), materializers) {
			continue
		}
		if err := c.walkReference(id, new(ref), materializers); err != nil {
			return err
		}
		if ref.Ordering && ref.Name != "" {
			orderingInputs = append(orderingInputs, store.OrderingEdgeFor(&ref.DependencyRef))
		}
	}
	if len(orderingInputs) != 0 || len(c.decision.ordering[id]) != 0 {
		slices.SortFunc(orderingInputs, store.OrderingEdge.Compare)
		c.setOrdering(id, slices.Compact(orderingInputs))
	}
	return nil
}

func (c *admissionClosure) referenceTargets(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
) []manifest.ProducerTarget {
	if obj != nil {
		targets, _ := manifest.GeneratedTargets(obj)
		return targets
	}
	if occurrence, ok := c.catalogOccurrence(id); ok {
		return occurrence.Targets[len(occurrence.AuthoredTargets):]
	}
	return nil
}

func (c *admissionClosure) walkReference(
	id manifest.NamedResource, ref *manifest.Reference,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	if ref.Name == "" {
		if ref.Selector != nil {
			return c.selectReferenceInputs(id, ref, materializers)
		}
		return nil
	}
	target := ref.NamedResource
	wasNeeded := c.role(target).needed
	c.add(target, admissionRole{
		needed: !ref.Ordering || !c.session.request.Changed, ordinary: !ref.Ordering, readiness: ref.Ordering,
		dirty: !ref.Ordering && c.states[target].changed,
	})
	c.materializeReferenceInput(id, ref, wasNeeded, materializers)
	return nil
}

func (c *admissionClosure) localGenerationReference(
	id manifest.NamedResource, ref *manifest.Reference, targets []manifest.ProducerTarget,
) bool {
	target := ref.NamedResource
	if slices.ContainsFunc(targets, func(candidate manifest.ProducerTarget) bool {
		return manifest.Resolve(candidate.NamedResource, "", id.Namespace) == target
	}) {
		return true
	}
	record, _ := c.facts.SelectionIndex().Lookup(target)
	return slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
		return p.Owner == id && p.Declaration == id
	})
}

func (c *admissionClosure) prepareReferenceInput(
	id manifest.NamedResource,
	refValue *manifest.Reference,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) bool {
	ref := *refValue
	switch {
	case ref.Ordering:
		if ref.Optional {
			return false
		}
		if c.session.request.Changed && ref.ReadyExpr != nil && *ref.ReadyExpr != "" {
			c.requireOrderingOperand(id)
			c.requireOrderingOperand(ref.NamedResource)
		}
		if c.needsOrderingMaterializer(id, ref.NamedResource) {
			materializers(
				materializerDemand{target: ref.NamedResource, source: c.declarationSource(id), ordering: true},
				id,
			)
		}
	case !ref.Optional && !ref.ContentInput() && ref.Gate == 0 && ref.Use != "materializer":
		return false
	}
	state := c.states[ref.NamedResource]
	state.requested = true
	c.states[ref.NamedResource] = state
	return true
}

func (c *admissionClosure) needsOrderingMaterializer(reader, target manifest.NamedResource) bool {
	if !c.session.request.Changed {
		return !c.isFileIndexed(target) && c.object(target) == nil
	}
	if target.Name == "" || !c.role(reader).selected || c.objects.GetDeclaration(target) != nil {
		return false
	}
	if c.incoming(target).Obj != nil || c.children[target].Obj != nil {
		return false
	}
	_, located := c.occurrence(target)
	return !located
}

func (c *admissionClosure) selectProductionReaders(
	id manifest.NamedResource,
	pValue *manifest.Production,
	owner manifest.NamedResource,
) {
	p := *pValue
	if !c.session.comparisonOnly && owner != (manifest.NamedResource{}) && filepath.IsAbs(p.Path) {
		file := c.coordinates(new(p)).indexed
		for reader := range c.facts.SelectionIndex().ReadersOfFile(file) {
			if c.proposal.Request.Invocation && !c.context.Frozen && c.role(id).selected && id.KindInfo().Runnable &&
				p.Authored == id && c.objects.GetDeclaration(id) == nil &&
				reader.Reader == owner && reader.Source == p.Source && reader.Declaration == (manifest.NamedResource{}) {
				continue
			}
			c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{ReaderEdges: 1})
			if reader.Declaration != (manifest.NamedResource{}) {
				continue
			}
			if !c.readerEmissionAllowed(id, reader.Reader) {
				continue
			}
			location := c.boundSource(p.Source)
			if reader.Source != p.Source && (location.Pinned || location.External) {
				continue
			}
			c.add(reader.Reader, admissionRole{needed: true, ordinary: true})
		}
	}
}

func (c *admissionClosure) selectProductions(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	role admissionRole,
	evidence *declarationEvidence,
) bool {
	materialized := record.Parent != (manifest.NamedResource{}) && record.Parent != id ||
		record.BuilderOwner != (manifest.NamedResource{}) && record.BuilderOwner != id
	if !role.selected && c.unavailableGeneratedData(id) {
		return true
	}
	bootstrap := evidence.independent && Root(*c.capturedRoot(id)).InitialIndependent
	productions := c.productions(id)
	for i := range productions {
		production := &productions[i]
		if bootstrap && production.Owner != (manifest.NamedResource{}) && production.Owner != id {
			continue
		}
		owner := c.productionOwner(id, production, record, evidence)
		if !evidence.allowsOwner(owner) {
			continue
		}
		materialized = materialized || c.productionMaterialized(id, production, record, evidence)
		c.selectProductionRoles(production, owner, role)
		if !slices.ContainsFunc(productions[:i], func(previous manifest.Production) bool {
			return previous.Materializer(id) == production.Materializer(id) &&
				previous.Source == production.Source &&
				previous.Path == production.Path &&
				filepath.IsAbs(production.Path)
		}) {
			c.selectProductionReaders(id, production, owner)
		}
	}
	return materialized
}

func (c *admissionClosure) unavailableGeneratedData(id manifest.NamedResource) bool {
	if !id.KindInfo().Data {
		return false
	}
	input := c.objects.ReferenceFacts(id, c.facts.SelectionIndex(), id)
	return input.Data == nil && input.Generated && !input.Offline &&
		!slices.ContainsFunc(input.Productions, func(p manifest.Production) bool {
			return p.Declaration.KindInfo().Parent
		})
}

func (c *admissionClosure) selectProductionRoles(
	production *manifest.Production,
	owner manifest.NamedResource,
	role admissionRole,
) {
	c.add(owner, admissionRole{
		needed:   true,
		ordinary: true,
		dirty:    c.states[owner].changed || role.dirty && c.role(owner).selected,
	})
	declaration := production.Declaration
	c.add(declaration, admissionRole{
		needed:   true,
		ordinary: true,
		dirty:    c.states[declaration].changed || role.dirty && c.role(declaration).selected,
	})
}

func (c *admissionClosure) hydratedDeclaration(id manifest.NamedResource) manifest.BaseManifest {
	if object := c.incoming(id).Obj; object != nil {
		return object
	}
	if object := c.children[id].Obj; object != nil {
		return object
	}
	_, policy := c.objects.Lookup(id)
	if object := c.objects.GetDeclaration(id); object != nil &&
		(policy.Root || policy.Supplied() && !c.rootEvidence(id).Admitted) {
		return object
	}
	return nil
}

func (c *admissionClosure) hydrateDeclaration(
	ctx context.Context,
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	evidence *declarationEvidence,
) manifest.BaseManifest {
	if c.hydratedDeclaration(id) != nil {
		return obj
	}
	if c.awaitsEntryPublication(id) {
		return obj
	}

	if evidence.built && c.hydrateBuilt(id, &evidence.child) {
		return evidence.child.Obj
	}
	if evidence.built {
		return obj
	}
	record := c.record(id)
	if !evidence.independent {
		role := c.role(id)
		demandsHydration := role.readiness || (role.needed && role.ordinary)
		if !demandsHydration || !evidence.authored || evidence.awaitingPublication {
			return obj
		}
	}
	return c.completeHydrate(ctx, id, evidence.owner, &record)
}

func (c *admissionClosure) awaitsEntryPublication(id manifest.NamedResource) bool {
	if !c.context.NativeEntry || !c.role(id).selected {
		return false
	}
	physical := *c.capturedRoot(id)
	physical.Captured = false
	if Root(physical).Independent {
		return false
	}
	path, _ := filepath.Rel(c.context.Root, c.context.EntryPath)
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(path), manifest.NamedResource{}).
		Named()
	status, _ := c.objects.GetStatus(owner)
	return c.decision.Output(manifest.NamedResource{}) ||
		status.Outcome() != store.OutcomeFailed && (c.role(owner).needed || !c.session.request.Changed)
}

func (c *admissionClosure) hydrateBuilt(id manifest.NamedResource, child *store.Child) bool {
	role := c.role(id)
	qualification := !role.selected || c.proposal.Request.construction &&
		slices.ContainsFunc(c.proposal.demands, func(demand Demand) bool {
			return demand.Identity == id && demand.Materialize
		})
	controllerDemand := role.ordinary || role.readiness && !c.session.request.Changed
	preparation := role.needed && controllerDemand && qualification && id.KindInfo().Runnable
	if !Root(*c.capturedRoot(id)).InitialIndependent && !c.states[id].requested &&
		!preparation &&
		(role.ordinary || !role.needed) {
		return false
	}
	if c.children == nil {
		c.children = map[manifest.NamedResource]store.Child{}
	}
	c.children[id] = *child
	delete(c.hydrated, id)
	return true
}

func (c *admissionClosure) selectedDeclarationFileChanged(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	record *manifest.DiscoveryRecord,
	selected bool,
) bool {
	return selected &&
		(c.fileChanged(c.source(id), record.File) || id.Namespace == "" && id.KindInfo().Namespaced() && c.affected(id, obj))
}
