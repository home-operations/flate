package loader

import (
	"context"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:ireturn // Hydration MUST retain each concrete manifest type behind the common contract.
func (c *admissionClosure) hydrate(ctx context.Context, id manifest.NamedResource) (manifest.BaseManifest, error) {
	record := c.record(id)
	var obj manifest.BaseManifest
	blocked := false
	for _, root := range c.roots {
		relative := filepath.ToSlash(root)
		if record.File == path.Join(relative, "kustomization.yaml") ||
			strings.HasPrefix(record.File, strings.TrimSuffix(relative, "/")+"/") ||
			relative == "." {
			c.proposal.Blocked = append(c.proposal.Blocked, root)
			blocked = true
		}
	}
	return c.completeHydrate(ctx, &completeHydrateState{id: &id, record: &record, obj: &obj, blocked: &blocked})
}

func (c *admissionClosure) walk(
	ctx context.Context,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	if len(c.queue) == 0 {
		return nil
	}
	var owners []manifest.BaseManifest
	ownersKnown := false
	owned := func() []manifest.BaseManifest {
		if !ownersKnown {
			for _, owner := range c.objects.ListObjects("", "") {
				if slices.ContainsFunc(c.productions(owner.Named()), func(p manifest.Production) bool {
					return p.Owner != (manifest.NamedResource{}) && p.Owner != owner.Named()
				}) {
					owners = append(owners, owner)
				}
			}
			ownersKnown = true
		}
		return owners
	}
	//nolint:intrange // add MUST extend the worklist during traversal.
	for cursor := 0; cursor < len(c.queue); cursor++ {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Admission cancellation preserves the original context error identity.
		}
		id := c.queue[cursor]
		if err := c.walkObject(ctx, id, owned, materializers); err != nil {
			return err
		}
	}
	return nil
}

type completeHydrateState struct {
	id      *manifest.NamedResource
	record  *manifest.DiscoveryRecord
	obj     *manifest.BaseManifest
	blocked *bool
}

//nolint:ireturn // Hydration MUST retain each concrete manifest type behind the common contract.
func (c *admissionClosure) completeHydrate(
	ctx context.Context,
	scope *completeHydrateState,
) (manifest.BaseManifest, error) {
	if *scope.blocked {
		return (*scope.obj), nil
	}
	if child := c.loader.candidates[(*scope.id)]; child.Obj != nil && scope.record.Path == "" {
		if c.children == nil {
			c.children = map[manifest.NamedResource]store.Child{}
		}
		c.children[(*scope.id)] = child
		(*scope.obj) = child.Obj
		return (*scope.obj), nil
	}
	occurrence, ok := c.loader.occurrence((*scope.id))
	if !ok {
		return (*scope.obj), nil
	}
	root := new(c.loader.documents).mountedRoot(scope.record.Path)
	var input *store.SourceArtifact
	for _, source := range [...]manifest.NamedResource{c.source((*scope.id)), manifest.BootstrapSourceID} {
		_, candidate, _ := c.delta(source)
		if candidate != nil && candidate == c.loader.documents.artifacts[root] {
			input = candidate
			break
		}
	}
	if input == nil {
		return (*scope.obj), nil
	}
	file, _ := filepath.Rel(root, scope.record.Path)
	occurrence.Declaration.Source, occurrence.Declaration.File = root, filepath.ToSlash(file)
	child, err := c.loader.exactObject(ctx, new(occurrence), input)
	if err != nil {
		return nil, err
	}
	if child.Obj != nil {
		child.Productions = slices.Clone(c.productions((*scope.id)))
		if c.children == nil {
			c.children = map[manifest.NamedResource]store.Child{}
		}
		c.children[(*scope.id)] = child
		(*scope.obj) = child.Obj
	}
	return (*scope.obj), nil
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

func (c *admissionClosure) selectIncomingProducers(id manifest.NamedResource, role admissionRole) {
	lookupRecord, _ := c.loader.Index.Lookup(id)
	{
		productionValues3 := lookupRecord.Productions
		for productionIndex3 := range productionValues3 {
			production := productionValues3[productionIndex3]
			if owner := production.Materializer(id); c.loader.IsFileIndexed(owner) || c.object(owner) != nil {
				c.add(owner, admissionRole{needed: true, dirty: role.dirty && c.role(owner).selected})
			}
		}
	}
}

func (c *admissionClosure) selectInventoryChildren(id manifest.NamedResource) {
	_, _, output := c.objects.GetEmission(id)
	for _, child := range output.Children {
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
		c.add(input, admissionRole{needed: true, dirty: c.states[input].changed})
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
	input := c.objects.ReferenceFacts(ref.NamedResource, c.loader.Index, false, id)
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
	independent bool,
) manifest.NamedResource {
	p := *pValue
	owner := p.Materializer(id)
	if independent && owner.ReportHidden() && owner == record.BuilderOwner {
		owner = manifest.NamedResource{}
	}
	if owner == (manifest.NamedResource{}) && !independent && p.Declaration == (manifest.NamedResource{}) &&
		p.Authored != (manifest.NamedResource{}) &&
		(c.incoming.Obj == nil || c.incoming.Obj.Named() != id) &&
		c.objects.GetDeclaration(id) == nil {
		owner = p.Owner
	}
	return owner
}

func (c *admissionClosure) incomingProductionMaterialized(
	id manifest.NamedResource,
	pValue *manifest.Production,
	record *manifest.DiscoveryRecord,
	materialized bool,
) bool {
	p := *pValue
	needed, _, _, _ := c.loader.Options.Filter.Membership(id)
	materialized = materialized ||
		needed && p.Owner == (manifest.NamedResource{}) && p.Declaration == (manifest.NamedResource{}) &&
			p.Path == record.Path && p.File == record.File && p.Source == c.source(id) &&
			manifest.Resolve(p.Authored, id.Namespace, "") == id
	return materialized
}

func (c *admissionClosure) walkObject(
	ctx context.Context,
	id manifest.NamedResource,
	owned func() []manifest.BaseManifest,
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

	independent := c.independent(id, owned)

	if !independent || !record.Parent.ReportHidden() {
		c.add(record.Parent, admissionRole{needed: true, dirty: c.states[record.Parent].changed})
	}
	pathBearing := false
	var applicationSource manifest.NamedResource
	if occurrence, ok := c.loader.occurrence(id); ok {
		applicationSource, _, pathBearing = occurrence.SourcePath()
		independent = independent || c.loader.Options.WipeSecrets && occurrence.facts.placeholder
	}
	materialized := c.selectProductions(id, &record, independent, role)
	if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
		c.selectIncomingProducers(id, role)
	}

	pending := false
	if record.Path != "" && !materialized &&
		(c.selectedDeclarationFileChanged(id, obj, &record, role.selected) || (role.selected || pathBearing) && !independent && c.objects.GetDeclaration(id) == nil) {
		pending = materializers(materializerDemand{target: id, source: c.declarationSource(id)}, id)
	}
	if !pending && role.selected && pathBearing {
		pending = c.materializeEntryApplication(id, applicationSource, &record, materializers)
	}
	if pending && pathBearing {
		independent = false
	}
	if err := c.hydrateIndependent(ctx, id, independent, &obj); err != nil {
		return err
	}
	if role.selected && role.dirty && c.loader.Store.HasInventory(id) {
		c.selectInventoryChildren(id)
	}

	return c.walkReferences(id, obj, materializers)
}

func (c *admissionClosure) walkReferences(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	materializers func(materializerDemand, manifest.NamedResource) bool,
) error {
	generated, _ := manifest.GeneratedObjects(obj)
	var generatedTargets []manifest.ProducerTarget
	if obj == nil {
		if occurrence, ok := c.loader.occurrence(id); ok {
			generatedTargets = occurrence.facts.targets[len(occurrence.facts.authoredTargets):]
		}
	}
	for ref := range c.references(id, obj) {
		if slices.ContainsFunc(
			generated,
			func(child manifest.BaseManifest) bool { return child.Named() == ref.NamedResource },
		) ||
			slices.ContainsFunc(generatedTargets, func(target manifest.ProducerTarget) bool {
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
		c.add(ref.NamedResource, admissionRole{needed: true, dirty: c.states[ref.NamedResource].changed})
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
	if ref.Ordering {
		indexed := c.loader.IsFileIndexed(ref.NamedResource)
		object := c.object(ref.NamedResource)
		if !c.role(ref.NamedResource).needed && (indexed || object != nil) {
			return false
		}
		if !indexed && object == nil {
			materializers(
				materializerDemand{target: ref.NamedResource, source: c.declarationSource(id), ordering: true},
				id,
			)
		}
	} else if !ref.ContentInput() && ref.Gate == 0 && ref.Use != "materializer" {
		return false
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
		_, file := c.coordinates(new(p))
		for reader := range c.loader.Index.ReadersOfFile(file) {
			if reader.Declaration != (manifest.NamedResource{}) {
				continue
			}
			if !c.readerEmissionAllowed(id, reader.Reader) {
				continue
			}
			_, pinned, external := SourceView(c.objects, p.Source, c.loader.SourceRoot)
			if reader.Source != p.Source && (pinned || external) {
				continue
			}
			c.add(reader.Reader, admissionRole{needed: true})
		}
	}
}

func (c *admissionClosure) selectProductions(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	independent bool,
	role admissionRole,
) bool {
	materialized := record.Parent != (manifest.NamedResource{}) && record.Parent != id ||
		record.BuilderOwner != (manifest.NamedResource{}) && record.BuilderOwner != id
	{
		pValues := c.productions(id)
		for pIndex := range pValues {
			p := pValues[pIndex]
			owner := c.productionOwner(id, new(p), record, independent)
			materialized = materialized || p.Owner != (manifest.NamedResource{}) && p.Owner != id ||
				p.Declaration != (manifest.NamedResource{}) && p.Declaration != id
			if independent && c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
				materialized = c.incomingProductionMaterialized(id, new(p), record, materialized)
			}
			c.add(
				owner,
				admissionRole{needed: true, dirty: c.states[owner].changed || role.dirty && c.role(owner).selected},
			)
			c.add(
				p.Declaration,
				admissionRole{
					needed: true,
					dirty:  c.states[p.Declaration].changed || role.dirty && c.role(p.Declaration).selected,
				},
			)
			c.selectProductionReaders(id, new(p), owner)
		}
	}
	return materialized
}

func (c *admissionClosure) hydrateIndependent(
	ctx context.Context,
	id manifest.NamedResource,
	independent bool,
	obj *manifest.BaseManifest,
) error {
	if !independent || c.incoming.Obj != nil && c.incoming.Obj.Named() == id || c.children[id].Obj != nil {
		return nil
	}
	if current, policy := c.objects.Lookup(id); current != nil && policy.Root {
		return nil
	}
	var err error
	*obj, err = c.hydrate(ctx, id)
	return err
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
	return materializers(materializerDemand{
		target: applicationSource, source: c.declarationSource(id), application: c.loader.entryPath,
		required: c.fileChanged(c.source(id), record.File),
	}, id)
}
