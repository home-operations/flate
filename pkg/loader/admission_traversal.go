package loader

import (
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
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
			return err //nolint:wrapcheck // Admission cancellation preserves the original context error identity.
		}
		id := c.queue[cursor]
		if err := c.walkObject(ctx, id, materializers); err != nil {
			return err
		}
	}
	return nil
}

type completeHydrateState struct {
	independent bool
	id          *manifest.NamedResource
	record      *manifest.DiscoveryRecord
	obj         *manifest.BaseManifest
}

//nolint:ireturn // Hydration MUST retain each concrete manifest type behind the common contract.
func (c *admissionClosure) completeHydrate(
	ctx context.Context,
	scope *completeHydrateState,
) (manifest.BaseManifest, error) {
	if child := c.loader.candidates[(*scope.id)]; child.Obj != nil && scope.record.Path == "" &&
		!c.loader.roots.get(*scope.id).independent {
		if c.children == nil {
			c.children = map[manifest.NamedResource]store.Child{}
		}
		c.children[(*scope.id)] = child
		(*scope.obj) = child.Obj
		return (*scope.obj), nil
	}
	var occurrence Occurrence
	var ok bool
	if scope.independent && c.loader.entry != entryKustomize {
		occurrence, ok = c.loader.catalogOccurrence(*scope.id)
	} else {
		occurrence, ok = c.loader.occurrence(*scope.id)
	}
	if !ok {
		return (*scope.obj), nil
	}
	root := occurrence.Declaration.Source
	input, support := c.hydrationSource(*scope.id, root)
	if input == nil {
		return (*scope.obj), nil
	}
	child, err := c.loader.OccurrenceObject(ctx, new(occurrence), input)
	if err != nil {
		return nil, err
	}
	if child.Obj != nil {
		child.SourceSupport = support
		child.Productions = slices.Clone(c.productions((*scope.id)))
		if c.children == nil {
			c.children = map[manifest.NamedResource]store.Child{}
		}
		c.children[(*scope.id)] = child
		(*scope.obj) = child.Obj
	}
	return (*scope.obj), nil
}

func (c *admissionClosure) hydrationSource(
	id manifest.NamedResource,
	root string,
) (*store.SourceArtifact, manifest.NamedResource) {
	source := c.source(id)
	candidate, _ := c.objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	if candidate == nil || candidate != c.loader.documents.artifacts[root] || !c.objects.SourceAdmission(id, source) {
		return nil, manifest.NamedResource{}
	}
	return candidate, source
}

func (c *admissionClosure) readerEmissionAllowed(id, readerID manifest.NamedResource) bool {
	object := c.object(readerID)
	if object == nil {
		object = c.loader.CatalogObject(readerID)
	}
	if producer, ok := object.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	}); ok {
		if !producer.EmissionPolicy().MatchesNamespace(id) {
			return false
		}
	}
	return true
}

func (c *admissionClosure) selectIncomingProducers(
	id manifest.NamedResource,
	role admissionRole,
	evidence *declarationEvidence,
) {
	lookupRecord, _ := c.loader.Index.Lookup(id)
	{
		productionValues3 := lookupRecord.Productions
		for productionIndex3 := range productionValues3 {
			production := productionValues3[productionIndex3]
			owner := production.Materializer(id)
			if !evidence.allowsOwner(owner) {
				continue
			}
			if c.loader.IsFileIndexed(owner) || c.object(owner) != nil {
				c.add(owner, admissionRole{needed: true, ordinary: true, dirty: role.dirty && c.role(owner).selected})
			}
		}
	}
}

func (c *admissionClosure) selectInventoryChildren(id manifest.NamedResource, role admissionRole) {
	if !role.selected || !role.dirty || !c.loader.Store.HasInventory(id) {
		return
	}
	_, _, output := c.objects.GetEmission(id)
	for i := range output.Children {
		child := &output.Children[i]
		if !c.selectsChild(child.Obj.Named()) {
			continue
		}
		if id.ReportHidden() && c.loader.IsFileIndexed(child.Obj.Named()) {
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
	inputs, err := c.matchingInputs(new(ref), false)
	if err != nil {
		return err
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
	_, indexed := c.loader.Index.Lookup(ref.NamedResource)
	unresolved := !indexed && !c.loader.IsFileIndexed(ref.NamedResource) && c.object(ref.NamedResource) == nil
	if !unresolved || c.objects.GetArtifact(ref.NamedResource, store.ArtifactContent) != nil {
		return
	}
	input := c.objects.ReferenceFacts(ref.NamedResource, c.loader.Index, id)
	if input.Data != nil {
		return
	}
	requiredContent := !ref.Optional && !ref.Acquisition && ref.ContentInput()
	allowedMissing := ref.Use == "valuesFrom" && slices.Contains(c.loader.Options.AllowMissingKinds, ref.Kind)
	required := requiredContent && !allowedMissing && !input.AllowsAbsence() && !c.changedInput(id, ref.NamedResource)
	materializers(
		materializerDemand{target: ref.NamedResource, source: c.declarationSource(id), required: required},
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
	needed := c.loader.Options.Filter.Membership(id).Needed
	unowned := production.Owner == (manifest.NamedResource{}) && production.Declaration == (manifest.NamedResource{})
	if !needed || !unowned {
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
	role := c.role(id)
	state := c.states[id]
	if state.walkedOnce && state.walked == role {
		return nil
	}
	state.walked, state.walkedOnce = role, true
	c.states[id] = state
	obj := c.object(id)
	record := c.record(id)

	root := c.rootEvidence(id)
	evidence := c.declarationEvidence(id)
	evidence.independent = root.independent || c.bootstrapPrerequisite(id)
	evidence.qualification = !role.ordinary
	bootstrap := evidence.independent && c.loader.roots.get(id).independent
	needsParent := !evidence.independent || !record.Parent.ReportHidden()
	if !bootstrap && needsParent && evidence.allowsOwner(record.Parent) {
		c.add(record.Parent, admissionRole{needed: true, ordinary: true, dirty: c.states[record.Parent].changed})
	}
	pathBearing := false
	var applicationSource manifest.NamedResource
	if occurrence, ok := c.loader.catalogOccurrence(id); ok {
		applicationSource, _, pathBearing = occurrence.SourcePath()
	}
	materialized := c.selectProductions(id, &record, role, &evidence)
	if c.incoming(id).Obj != nil {
		c.selectIncomingProducers(id, role, &evidence)
	}

	pending := false
	if (!bootstrap || !state.requested) && !materialized &&
		c.declarationNeedsOwner(id, obj, &record, &evidence) {
		pending = materializers(materializerDemand{target: id, source: c.declarationSource(id)}, id)
	}
	if !pending && role.selected && pathBearing {
		pending = c.materializeEntryApplication(id, applicationSource, &record, materializers)
	}
	if pending && pathBearing {
		evidence.independent = false
	}

	if !pending {
		var err error
		obj, err = c.hydrateDeclaration(ctx, id, obj, &evidence)
		if err != nil {
			return err
		}
	}
	c.selectInventoryChildren(id, role)

	return c.walkReferences(id, obj, materializers)
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
	pathBearing := false
	if occurrence, ok := c.loader.catalogOccurrence(id); ok {
		_, _, pathBearing = occurrence.SourcePath()
	}
	selected := c.role(id).selected
	if record.Path == "" {
		return false
	}
	if c.selectedDeclarationFileChanged(id, object, record, selected) {
		return true
	}
	return (selected || pathBearing) && !evidence.independent && c.objects.GetDeclaration(id) == nil
}

func (c *admissionClosure) walkReferences(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	generatedTargets, _ := manifest.GeneratedTargets(obj)
	if obj == nil {
		if occurrence, ok := c.loader.catalogOccurrence(id); ok {
			generatedTargets = occurrence.facts.targets[len(occurrence.facts.authoredTargets):]
		}
	}
	for ref := range c.references(id, obj) {
		if slices.ContainsFunc(generatedTargets, func(target manifest.ProducerTarget) bool {
			return manifest.Resolve(target.NamedResource, "", id.Namespace) == ref.NamedResource
		}) {
			continue
		}
		inputRecord, _ := c.loader.Index.Lookup(ref.NamedResource)
		if slices.ContainsFunc(
			inputRecord.Productions,
			func(p manifest.Production) bool { return p.Owner == id && p.Declaration == id },
		) {
			continue
		}
		if !c.prepareReferenceInput(id, new(ref), materializers) {
			continue
		}
		if ref.Selector != nil && ref.Name == "" {
			if err := c.selectReferenceInputs(id, new(ref), materializers); err != nil {
				return err
			}
			continue
		}
		if ref.Name == "" {
			continue
		}
		wasNeeded := c.role(ref.NamedResource).needed
		c.add(
			ref.NamedResource,
			admissionRole{needed: true, ordinary: true, dirty: c.states[ref.NamedResource].changed},
		)
		c.materializeReferenceInput(id, new(ref), wasNeeded, materializers)
	}
	return nil
}

func (c *admissionClosure) prepareReferenceInput(
	id manifest.NamedResource,
	refValue *manifest.Reference,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) bool {
	ref := *refValue
	switch {
	case ref.Ordering:
		indexed := c.loader.IsFileIndexed(ref.NamedResource)
		object := c.object(ref.NamedResource)
		if !c.role(ref.NamedResource).needed && (indexed || object != nil) &&
			!c.bootstrapPrerequisite(ref.NamedResource) {
			return false
		}
		if !indexed && object == nil {
			materializers(
				materializerDemand{target: ref.NamedResource, source: c.declarationSource(id), ordering: true},
				id,
			)
		}
	case !ref.ContentInput() && ref.Gate == 0 && ref.Use != "materializer":
		return false
	default:
		state := c.states[ref.NamedResource]
		state.requested = true
		c.states[ref.NamedResource] = state
	}
	return true
}

func (c *admissionClosure) selectProductionReaders(
	id manifest.NamedResource,
	pValue *manifest.Production,
	owner manifest.NamedResource,
) {
	p := *pValue
	if owner != (manifest.NamedResource{}) && filepath.IsAbs(p.Path) {
		file := c.coordinates(new(p)).indexed
		for reader := range c.loader.Index.ReadersOfFile(file) {
			if c.proposal.Request.Invocation && !c.loader.frozen && c.role(id).selected && id.KindInfo().Runnable &&
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
			location := SourceView(c.objects, p.Source, c.loader.SourceRoot)
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
	bootstrap := evidence.independent && c.loader.roots.get(id).independent
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

//nolint:ireturn // Hydration MUST retain each concrete manifest type behind the common contract.
func (c *admissionClosure) hydrateDeclaration(
	ctx context.Context,
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	evidence *declarationEvidence,
) (manifest.BaseManifest, error) {
	if c.incoming(id).Obj != nil || c.children[id].Obj != nil {
		return obj, nil
	}
	if current, policy := c.objects.Lookup(id); current != nil &&
		(policy.Root || policy.Origin == manifest.SuppliedDeclaration && !c.rootEvidence(id).admitted) {
		return obj, nil
	}
	if !evidence.independent && c.role(id).selected && c.loader.entry == entryKustomize {
		path, _ := filepath.Rel(c.loader.SourceRoot, c.loader.entryPath)
		owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(path), manifest.NamedResource{}).
			Named()
		status, _ := c.objects.GetStatus(owner)
		if status.Outcome() != store.OutcomeFailed && (c.role(owner).needed || !c.loader.Options.Selection.Changed) {
			return obj, nil
		}
	}
	if evidence.built {
		if c.hydrateBuilt(id, &evidence.child) {
			return evidence.child.Obj, nil
		}
		return obj, nil
	}
	record := c.record(id)
	if !evidence.independent {
		role := c.role(id)
		if !role.needed || !role.ordinary || !evidence.authored {
			return obj, nil
		}
	}
	var hydrated manifest.BaseManifest
	return c.completeHydrate(
		ctx,
		&completeHydrateState{id: &id, record: &record, obj: &hydrated, independent: evidence.independent},
	)
}

func (c *admissionClosure) hydrateBuilt(id manifest.NamedResource, child *store.Child) bool {
	role := c.role(id)
	preparation := role.needed && role.ordinary && !role.selected && id.KindInfo().Structural
	if !c.loader.roots.get(id).independent && !c.states[id].requested && !preparation &&
		(role.ordinary || !role.needed) {
		return false
	}
	if c.children == nil {
		c.children = map[manifest.NamedResource]store.Child{}
	}
	c.children[id] = *child
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

func (c *admissionClosure) materializeEntryApplication(
	id, applicationSource manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) bool {
	if applicationSource.Name == "" || applicationSource == manifest.BootstrapSourceID ||
		c.loader.entry != entryPlain ||
		!safepath.Contains(c.loader.entryPath, record.Path) {
		return false
	}
	location := SourceView(c.objects, applicationSource, c.loader.SourceRoot)
	if location.Pinned || location.External {
		return false
	}
	return materializers(materializerDemand{
		target: applicationSource, source: c.declarationSource(id), application: c.loader.entryPath,
		required: c.fileChanged(c.source(id), record.File),
	}, id)
}
