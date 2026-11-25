package selection

import (
	"cmp"
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/internal/engine/store"
)

func (c *admissionClosure) object(id manifest.NamedResource) manifest.BaseManifest {
	if c.incoming(id).Obj != nil {
		return c.incoming(id).Obj
	}
	if child := c.children[id]; child.Obj != nil {
		return child.Obj
	}
	if object := c.objects.GetObject(id); object != nil {
		return object
	}
	record, _ := c.facts.SelectionIndex().Lookup(id)
	return record.Declaration
}

func (c *admissionClosure) references(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
) iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		c.yieldReferences(id, obj, yield)
	}
}

func (c *admissionClosure) yieldReferences(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	yield func(manifest.Reference) bool,
) {
	occurrence, authored := c.projectedOccurrence(id, obj)
	if !authored && obj == nil {
		c.yieldIndexedReferences(id, yield)
		return
	}
	active := true
	if authored {
		active = occurrence.references(manifest.ReferenceRelations, yield)
	} else {
		manifest.References(obj, manifest.ReferenceRelations)(func(ref manifest.Reference) bool {
			active = yield(ref)
			return active
		})
	}
	if active && obj != nil && c.incoming(id).Obj == nil {
		c.yieldHistoricalReferences(id, yield)
	}
}

func (c *admissionClosure) projectedOccurrence(
	id manifest.NamedResource,
	object manifest.BaseManifest,
) (OccurrenceFacts, bool) {
	if c.incoming(id).Obj != nil {
		return OccurrenceFacts{}, false
	}
	occurrence, known := c.catalogOccurrence(id)
	record := c.facts.SelectionIndex().Records[id]
	if record.Builder != "" && (record.Declaration == nil || occurrence.Object != record.Declaration) {
		return OccurrenceFacts{}, false
	}
	if !known {
		return occurrence, false
	}
	matchesDeclaration := object == occurrence.Object || object == record.Declaration
	if object == nil || matchesDeclaration || object == occurrence.Catalog ||
		object == c.facts.AdmissionCandidate(id).Obj {
		return occurrence, true
	}
	child := c.children[id]
	return occurrence, object == child.Obj && child.SourceSupport != (manifest.NamedResource{})
}

func (c *admissionClosure) declarationSource(id manifest.NamedResource) string {
	if occurrence, ok := c.catalogOccurrence(id); ok {
		return occurrence.Declaration.Source
	}
	if _, input, _ := c.delta(c.source(id)); input != nil {
		return input.OriginPath(input.LocalPath)
	}
	return ""
}

func (c *admissionClosure) source(id manifest.NamedResource) manifest.NamedResource {
	record, _ := c.facts.SelectionIndex().Lookup(id)
	if record.Builder != "" && (c.incoming(id).Obj == nil || record.BuilderOwner == c.owner) {
		for i := range record.Productions {
			production := &record.Productions[i]
			if production.Owner == record.BuilderOwner && production.Source != (manifest.NamedResource{}) {
				return production.Source
			}
		}
	}
	occurrence, located := c.catalogOccurrence(id)
	{
		pValues := c.productions(id)
		for pIndex := range pValues {
			p := pValues[pIndex]
			if p.Source == (manifest.NamedResource{}) {
				continue
			}
			if located {
				_, input, _ := c.delta(p.Source)
				if input == nil ||
					input.LocalPath != occurrence.Declaration.Source &&
						input.OriginPath(input.LocalPath) != occurrence.Declaration.Source {
					continue
				}
			}
			return p.Source
		}
	}
	return manifest.BootstrapSourceID
}

func (c *admissionClosure) productions(id manifest.NamedResource) []manifest.Production {
	state := c.states[id]
	productions := state.productions
	if !state.productionsKnown {
		c.derive()
		productions = c.currentSourceProductions(id, &state)

		if c.states == nil {
			c.states = map[manifest.NamedResource]admissionState{}
		}
		state.productions, state.productionsKnown = productions, true
		c.states[id] = state
	}
	if len(c.accepted) == 0 {
		return productions
	}
	if cached, known := c.historyProductions[id]; known {
		return cached
	}
	productions, changed := c.mergeHistoricalProductions(id, productions)
	if changed {
		if c.historyProductions == nil {
			c.historyProductions = map[manifest.NamedResource][]manifest.Production{}
		}
		c.historyProductions[id] = productions
	}
	return productions
}

func (c *admissionClosure) bootstrapPrerequisite(
	id manifest.NamedResource,
) bool {
	record := c.record(id)
	if record.Path == "" && record.Builder == "" || !c.directBootstrap(id) {
		return false
	}
	required := func(object manifest.BaseManifest) bool {
		owner := RootCoveringOwner{Object: object}
		if object != nil {
			owner.Identity = object.Named()
			if owner.Identity != record.Parent {
				owner = rootOwnerFact(object, c.rootReader())
			}
		}
		return c.bootstrapInputRequired(&owner, id, &record, targetedClaimScope)
	}
	if required(c.object(record.Parent)) || c.incoming(id).Obj != nil && required(c.incoming(id).Obj) {
		return true
	}
	owners := c.capturedRoot(id).Scope.Owners
	for i := range owners {
		owner := &owners[i]
		if required(c.objects.GetDeclaration(owner.Identity)) {
			return true
		}
	}
	return c.hasOwner(func(owner *RootCoveringOwner) bool {
		return c.bootstrapInputRequired(owner, id, &record, fullClaimScope)
	})
}

func (c *admissionClosure) directBootstrap(id manifest.NamedResource) bool {
	occurrence, found := c.occurrence(id)
	input, _ := c.objects.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	return found && input != nil &&
		(input.LocalPath == occurrence.Declaration.Source || input.OriginPath(input.LocalPath) == occurrence.Declaration.Source) &&
		c.objects.SourceAdmission(id, manifest.BootstrapSourceID)
}

func (c *admissionClosure) bootstrapInputRequired(
	owner *RootCoveringOwner,
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	scope ownerClaimScope,
) bool {
	if owner.Object == nil {
		return false
	}
	for ref := range manifest.References(owner.Object, manifest.ReferenceRelations) {
		if !ref.Ordering && !ref.ContentInput() {
			continue
		}
		if ref.NamedResource != id {
			input := c.record(ref.NamedResource)
			productions := c.productions(ref.NamedResource)
			parent := len(input.Productions) == 0 && len(productions) == 0 && input.Parent == id
			if !parent && !slices.ContainsFunc(productions, func(production manifest.Production) bool {
				return (production.Materializer(ref.NamedResource) == id || production.Declaration == id) &&
					c.bootstrapProductionCurrent(ref.NamedResource, &production)
			}) {
				continue
			}
		}
		return record.Parent == owner.Identity || c.coversDeclaration(owner, id, scope)
	}
	return false
}

func (c *admissionClosure) bootstrapProductionCurrent(id manifest.NamedResource, production *manifest.Production) bool {
	location := c.boundSource(production.Source)
	if location.Pinned || location.External || escapedPath(production.File) {
		return false
	}
	if c.incoming(id).Obj != nil {
		return true
	}
	owner := production.Materializer(id)
	output, _ := c.objects.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
	return output == nil || !output.Complete || !output.OutputReads.Valid() ||
		slices.ContainsFunc(output.Children, func(child store.Child) bool { return child.Obj.Named() == id })
}

func (c *admissionClosure) coversDeclaration(
	owner *RootCoveringOwner,
	id manifest.NamedResource,
	scope ownerClaimScope,
) bool {
	if owner.Object == nil || owner.Input == nil {
		return false
	}
	if !manifest.KindOf(owner.Object).Parent || !owner.Object.EmissionPolicy().MatchesNamespace(id) {
		return false
	}
	return c.completeCoversDeclaration(id, owner, scope)
}

func (c *admissionClosure) unchangedDeclaration(id, owner manifest.NamedResource, object manifest.BaseManifest) bool {
	ownerOrdinal := c.states[owner].ordinal
	ordinal := c.states[id].ordinal
	if c.collecting && ownerOrdinal != 0 && ordinal != 0 {
		identity := c.identities[ordinal-1]
		if identity.stableOwner == ownerOrdinal && identity.stableObject == object {
			return true
		}
	}
	if !c.declarationUnchanged(id, owner, object) {
		return false
	}
	if c.collecting && ownerOrdinal != 0 && ordinal != 0 {
		identity := &c.identities[ordinal-1]
		identity.stableOwner, identity.stableObject = ownerOrdinal, object
	}
	return true
}

func (c *admissionClosure) declarationUnchanged(id, owner manifest.NamedResource, object manifest.BaseManifest) bool {
	declared := c.declarationChanged(id, object)
	record := c.record(id)
	produced := owner != (manifest.NamedResource{}) || record.Builder != ""
	authoritative := declared.Known || record.Authored == id || record.Builder != "" && record.BuilderOwner == owner
	if comparison := c.outputChanged(
		id,
		new(manifest.Reference{NamedResource: id}),
		object,
	); produced && comparison.Known && authoritative {
		return !comparison.Changed
	}
	if declared.Known && declared.Changed {
		return false
	}
	if stability := c.generatedDeclarationStability(id, owner, object); stability.known {
		return stability.stable
	}
	if declared.Known &&
		!c.inputsChanged(manifest.BootstrapSourceID, record.Inputs) {
		return true
	}
	if stability := c.declarationStable(id, owner, &record); stability.known {
		return stability.stable
	}
	if c.context.SideKnown && record.Authored != id {
		return false
	}
	if c.context.SideKnown && record.Builder != "" {
		stable := true
		c.checkDeclarationReferences(owner, &stable)
		if !stable {
			return false
		}
	}
	return (record.BuilderOwner == (manifest.NamedResource{}) || owner == record.BuilderOwner) &&
		record.File != record.Builder &&
		record.Declaration != nil &&
		manifest.Equal(record.Declaration, object)
}

func (c *admissionClosure) generatedDeclarationStability(
	id, owner manifest.NamedResource, object manifest.BaseManifest,
) declarationStability {
	if c.session.entryComparisonComplete && c.entryDeclarationChanged(owner, c.object(owner)) {
		return declarationStability{known: true}
	}
	route := c.proposal.entry
	if route == nil || (!route.ProjectedPatches && !c.session.entryComparisonPending) || !c.session.request.Changed {
		return declarationStability{}
	}
	if !c.session.request.Matches(manifest.NamedResource{}) || object == nil ||
		!c.role(owner).dirty || c.ownerDeclarationStable(id, owner) {
		return declarationStability{}
	}
	if c.session.entryComparisonPending {
		return declarationStability{known: true, stable: true}
	}
	// An effective declaration can change while its authored file remains stable.
	occurrence, _ := c.catalogOccurrence(id)
	equal, err := manifest.OutputEqual(cmp.Or(occurrence.Object, occurrence.Catalog), object, [2]map[string]any{})
	return declarationStability{known: true, stable: err == nil && equal}
}

func (c *admissionClosure) ownerDeclarationStable(id, owner manifest.NamedResource) bool {
	state := c.states[id]
	if state.comparingDeclaration {
		return false
	}
	state.comparingDeclaration = true
	c.states[id] = state
	generated := c.proposal.reads.GeneratedDocument(owner)
	stable := !slices.ContainsFunc(c.productions(owner), func(proof manifest.Production) bool {
		if generated {
			return proof.Declaration != owner && c.role(proof.Declaration).dirty
		}
		parent := proof.Materializer(owner)
		return parent != id && parent != owner && c.role(parent).dirty &&
			!c.unchangedDeclaration(owner, parent, c.object(owner))
	})
	state = c.states[id]
	state.comparingDeclaration = false
	c.states[id] = state
	return stable
}

func (c *admissionClosure) completeCoversDeclaration(
	id manifest.NamedResource,
	owner *RootCoveringOwner,
	scope ownerClaimScope,
) bool {
	if owner.Path == "" {
		return false
	}
	record, _ := c.indexedRecord(id)
	facts := RootEvidence{Identity: id, Record: RootRecordFrom(&record)}
	file := record.Path
	if file == "" {
		delta, _, _ := c.delta(owner.Source)
		for dependency, changed := range c.comparisonInputs(delta, id) {
			if dependency.Group != id.Group || dependency.Kind != id.Kind || dependency.Name != id.Name ||
				dependency.Namespace != "" && id.Namespace != "" && dependency.Namespace != id.Namespace {
				continue
			}
			for _, declaration := range changed.Declarations {
				facts.Record.Path = owner.Input.OriginPath(
					filepath.Join(owner.Input.LocalPath, filepath.FromSlash(declaration)),
				)
				facts.DeclarationSource = owner.Root
				if c.declarationClaimed(&facts, owner, scope) {
					return true
				}
			}
		}
		return false
	}
	facts.DeclarationSource = c.declarationSource(id)
	return c.declarationClaimed(&facts, owner, scope)
}

func (c *admissionClosure) declarationClaimed(
	facts *RootEvidence,
	owner *RootCoveringOwner,
	scope ownerClaimScope,
) bool {
	if owner.claims != nil {
		return rootClaimed(facts, owner, owner.claims)
	}
	root := c.context.Root
	if scope == fullClaimScope {
		root = c.bootstrapClaimRoot()
	}
	coordinate, known := rootClaimCoordinate(facts, owner, root)
	return known && claimContains(c.rootOwnerClaims(owner, root, scope), coordinate, facts.Identity, owner)
}

func (c *admissionClosure) yieldIndexedReferences(id manifest.NamedResource, yield func(manifest.Reference) bool) {
	if child := c.facts.AdmissionCandidate(id); child.Obj != nil && c.facts.SelectionIndex().Records[id].Builder != "" {
		manifest.References(child.Obj, manifest.ReferenceRelations)(yield)
		return
	}
	if occurrence, ok := c.catalogOccurrence(id); ok {
		occurrence.references(manifest.ReferenceRelations, yield)
		return
	}

	for _, ref := range c.record(id).Refs {
		if !yield(manifest.Reference{NamedResource: ref, Content: true}) {
			return
		}
	}
}

func (c *admissionClosure) currentProductions(id manifest.NamedResource) []manifest.Production {
	if child := c.incoming(id); child.Obj != nil {
		return c.objects.IncomingProductions(child, c.facts.SelectionIndex())
	}
	return c.objects.Productions(id, c.facts.SelectionIndex())
}

func (c *admissionClosure) currentSourceProductions(
	id manifest.NamedResource,
	state *admissionState,
) []manifest.Production {
	productions := c.currentProductions(id)
	c.extendProductions(id, state, &productions)
	return productions
}

func (c *admissionClosure) extendProductions(
	id manifest.NamedResource,
	state *admissionState,
	productions *[]manifest.Production,
) {
	if slices.ContainsFunc(
		(*productions),
		func(p manifest.Production) bool { return p.Owner != (manifest.NamedResource{}) && p.Owner != id },
	) {
		return
	}
	record, indexed := c.indexedRecord(id)
	extended := false
	if !indexed && record.Authored != (manifest.NamedResource{}) {
		c.extendProductionInput(
			id, record.Productions,
			&productionExtension{record: &record, productions: productions, state: state, extended: &extended},
		)
	}
	for source := range c.proposal.Request.Changes.Sources() {
		evidence := c.boundSource(source)
		delta := evidence.Changes
		for dependency, input := range c.comparisonInputs(delta, id) {
			if dependency.Group != id.Group || dependency.Kind != id.Kind || dependency.Name != id.Name ||
				dependency.Namespace != "" && id.Namespace != "" && dependency.Namespace != id.Namespace {
				continue
			}
			if len(input.Productions) == 0 {
				continue
			}
			if evidence.Delta != delta {
				continue
			}
			c.extendProductionInput(
				id, input.Productions,
				&productionExtension{record: &record, productions: productions, state: state, extended: &extended},
			)
		}
	}
}

type productionExtension struct {
	record      *manifest.DiscoveryRecord
	productions *[]manifest.Production
	state       *admissionState
	extended    *bool
}

func (c *admissionClosure) extendProductionInput(
	target manifest.NamedResource,
	input []manifest.Production,
	scope *productionExtension,
) {
	{
		productionValues := input
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]
			owner := production.Owner
			if scope.record.File != "" && scope.record.File != production.File ||
				!c.declarationLocated(owner, c.objects) || !c.readerEmissionAllowed(target, owner) {
				continue
			}
			if !c.materializerOutputMatches(owner, target).include {
				continue
			}
			c.qualifyProductionSource(&production)
			if !slices.Contains((*scope.productions), production) {
				if !(*scope.extended) {
					next := make(
						[]manifest.Production,
						len((*scope.productions)),
						len((*scope.productions))+len(input),
					)
					copy(next, (*scope.productions))
					(*scope.productions), (*scope.extended) = next, true
				}
				(*scope.productions) = append((*scope.productions), production)
				scope.state.staticOwner = true
			}
		}
	}
}

func (c *admissionClosure) mergeHistoricalProductions(
	id manifest.NamedResource,
	productions []manifest.Production,
) ([]manifest.Production, bool) {
	owned := slices.ContainsFunc(productions, func(p manifest.Production) bool { return c.replace[p.Owner] })
	if owned {
		productions = slices.DeleteFunc(
			slices.Clone(productions),
			func(p manifest.Production) bool { return c.replace[p.Owner] },
		)
	}
	for owner, records := range c.accepted {
		if incoming := c.incoming(id); incoming.Obj != nil && !c.acceptedDeclaration(id, owner, incoming.Obj) {
			continue
		}
		if record, ok := records[id]; ok && len(record.Productions) != 0 {
			if !owned {
				productions = slices.Clone(productions)
				owned = true
			}
			productions = append(productions, record.Productions...)
		}
	}
	return productions, owned
}

func (c *admissionClosure) declarationOwner(
	id, owner manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) (manifest.NamedResource, bool) {
	declaration := id
	_, located := c.occurrence(id)
	owned := owner == manifest.Resolve(record.Parent, "", id.Namespace)
	if !located {
		declaration = manifest.NamedResource{}
	}
	{
		pValues := c.productions(id)
		for pIndex := range pValues {
			p := pValues[pIndex]
			if p.Owner != owner {
				continue
			}
			owned = true
			if located {
				continue
			}
			origin := p.Declaration
			if origin == (manifest.NamedResource{}) || origin == id {
				origin = p.Authored
			}
			if origin == (manifest.NamedResource{}) || origin == id ||
				declaration != (manifest.NamedResource{}) && declaration != origin {
				declaration = manifest.NamedResource{}
				break
			}
			declaration = origin
		}
	}
	return declaration, owned
}

func (c *admissionClosure) checkDeclarationReferences(owner manifest.NamedResource, stable *bool) {
	if *stable {
		object := c.object(owner)
		for ref := range c.references(owner, object) {
			if ref.Ordering || !c.reverse(owner, object, &ref) {
				continue
			}
			if c.changedInput(owner, ref.NamedResource) ||
				c.affected(ref.NamedResource, c.object(ref.NamedResource)) ||
				!ref.Artifact && c.role(ref.NamedResource).dirty {
				(*stable) = false
				break
			}
		}
	}
}

func (c *admissionClosure) checkDeclarationFiles(
	source manifest.NamedResource,
	stable *bool,
	declared, record *manifest.DiscoveryRecord,
) {
	if *stable && (len(declared.Inputs) != 0 || len(record.Inputs) != 0) {
		for file := range c.sourceFiles(source) {
			if slices.Contains(declared.Inputs, file) || slices.Contains(record.Inputs, file) {
				(*stable) = false
				break
			}
		}
	}
}

type declarationStability struct{ stable, known bool }

func (c *admissionClosure) declarationStable(
	id, owner manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) declarationStability {
	declaration, owned := c.declarationOwner(id, owner, record)
	ownerRecord := c.record(owner)
	ownerFile := ownerRecord.File
	if occurrence, located := c.catalogOccurrence(owner); located {
		ownerFile = c.fileCoordinate(filepath.Join(occurrence.Declaration.Source, occurrence.Declaration.File))
	}
	declared := c.record(declaration)
	if occurrence, located := c.catalogOccurrence(
		declaration,
	); located && owner != (manifest.NamedResource{}) && owned && declared.File != "" &&
		declared.File != declared.Builder {
		source := c.source(declaration)
		delta, input, _ := c.delta(source)
		ownerKnown := ownerRecord.File != ownerRecord.Builder || c.objects.GetDeclaration(owner) != nil ||
			c.declarationEvidence(owner).accepted
		stable := ownerKnown && delta != nil && input != nil && input.FS != nil &&
			input.Reads.Valid() &&
			input.OriginPath(input.LocalPath) == occurrence.Declaration.Source &&
			!c.fileChanged(source, declared.File) &&
			!c.fileChanged(c.source(owner), ownerFile) &&
			!slices.ContainsFunc(c.productions(owner), c.changedProduction)
		c.checkDeclarationReferences(owner, &stable)
		c.checkDeclarationFiles(source, &stable, &declared, record)
		if parent := ownerRecord.Parent; stable &&
			(parent != (manifest.NamedResource{}) && parent != owner && !parent.ReportHidden()) {
			state := c.states[id]
			if state.comparingDeclaration {
				return declarationStability{stable: false, known: true}
			}
			state.comparingDeclaration = true
			c.states[id] = state
			stable = c.unchangedDeclaration(owner, parent, c.object(owner))
			state = c.states[id]
			state.comparingDeclaration = false
			c.states[id] = state
		}
		if stable {
			return declarationStability{stable: true, known: true}
		}
	}
	return declarationStability{}
}

// Declaration authority MUST precede the choice of a preparation owner.
type declarationEvidence struct {
	child               store.Child
	owner               manifest.NamedResource
	authored, built     bool
	accepted            bool
	awaitingPublication bool
	qualification       bool
	independent         bool
}

func (e *declarationEvidence) known() bool { return e.child.Obj != nil || e.authored }

func (e *declarationEvidence) allowsOwner(owner manifest.NamedResource) bool {
	materialized := e.accepted && !e.awaitingPublication
	authored := e.authored && !e.awaitingPublication
	hidden := (authored || materialized) && owner.ReportHidden()
	published := materialized && owner == e.owner
	qualifiedBuild := e.built && e.qualification && owner == e.owner
	return !hidden && !published && !qualifiedBuild
}

func (c *admissionClosure) declarationEvidence(id manifest.NamedResource) declarationEvidence {
	record := c.record(id)
	evidence := declarationEvidence{owner: c.declarationProductionOwner(id, record.Parent)}
	if incoming := c.incoming(id); incoming.Obj != nil {
		evidence.child = incoming
		evidence.accepted = c.acceptedDeclaration(id, evidence.owner, incoming.Obj)
		return evidence
	}
	if object := c.objects.GetDeclaration(id); object != nil {
		evidence.child.Obj = object
		evidence.accepted = c.acceptedDeclaration(id, evidence.owner, object)
	}

	if evidence.child.Obj == nil && record.Declaration != nil &&
		c.acceptedDeclaration(id, evidence.owner, record.Declaration) {
		evidence.child.Obj = record.Declaration
		evidence.accepted, evidence.authored = true, true
		// A frozen index MUST NOT substitute historical equality for a producer's publication.
		evidence.awaitingPublication = c.context.Frozen
	}
	if record.Builder == "" {
		if evidence.child.Obj == nil {
			c.classifyAuthoredDeclaration(id, &evidence)
		}
		return evidence
	}
	facts := c.capturedRoot(id)
	if !facts.BuiltCurrent || facts.BuiltChild.Obj == nil {
		return evidence
	}
	child := facts.BuiltChild
	if evidence.child.Obj == nil {
		evidence.child = child
	}
	evidence.built = manifest.Equal(evidence.child.Obj, child.Obj)

	return evidence
}

func (c *admissionClosure) acceptedDeclaration(id, owner manifest.NamedResource, object manifest.BaseManifest) bool {
	if c.outsideEntry(owner, id) {
		return false
	}
	// A producer MUST not authorize its pending generation with its own previous output.
	if owner != (manifest.NamedResource{}) && owner != id && (c.incoming(id).Obj == nil || owner != c.owner) {
		output, _ := c.objects.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
		if output != nil && len(output.Failures) == 0 && output.OutputReads.Valid() &&
			slices.ContainsFunc(output.Children, func(child store.Child) bool {
				return (output.Complete || child.Generated) && manifest.Equal(child.Obj, object)
			}) {
			return true
		}
	}
	if !c.replace[owner] {
		return false
	}
	record := c.accepted[owner][id]
	if record.Application == "" {
		return false
	}
	parsed, valid := parseApplication(record.Application)
	return valid && manifest.Equal(parsed, object)
}

func (c *admissionClosure) declarationProductionOwner(id, owner manifest.NamedResource) manifest.NamedResource {
	productions := c.productions(id)
	for i := range productions {
		production := &productions[i]
		materializer := production.Materializer(id)
		if materializer != (manifest.NamedResource{}) && materializer != id {
			owner = materializer
			if !owner.ReportHidden() {
				break
			}
		}
	}
	return owner
}

func (c *admissionClosure) classifyAuthoredDeclaration(
	id manifest.NamedResource,
	evidence *declarationEvidence,
) {
	occurrence, located := c.occurrence(id)
	root := c.rootEvidence(id)
	acquire := c.states[id].requested && c.bootstrapPrerequisite(id)
	eagerAuthored := c.decision.Output(manifest.NamedResource{}) && root.Admitted &&
		located && !root.Owner.ReportHidden()
	if root.Owner != (manifest.NamedResource{}) && !acquire && !eagerAuthored {
		evidence.owner = root.Owner
		return
	}
	if !located || root.Occurrence.Declaration == (OccurrenceID{}) {
		return
	}
	if root.Admitted || acquire {
		evidence.owner = manifest.NamedResource{}
	}
	if evidence.owner != (manifest.NamedResource{}) && !evidence.owner.ReportHidden() && evidence.owner != id {
		return
	}
	input, _ := c.hydrationSource(id, occurrence.Mounted)
	if input != nil {
		evidence.authored, evidence.owner = true, manifest.NamedResource{}
	}
}

func (c *admissionClosure) qualifyProductionSource(production *manifest.Production) {
	object := c.object(production.Owner)
	if object == nil {
		object = c.catalogObject(production.Owner)
	}
	if object == nil {
		return
	}
	source, _, applies := object.SourcePath()
	if !applies || source == production.Source {
		return
	}
	location := c.boundSource(source)
	if location.Pinned || location.External {
		production.Source = source
	}
}
