package loader

import (
	"encoding/json"
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:ireturn // The manifest contract requires polymorphic results.
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
	record, _ := c.loader.Index.Lookup(id)
	return record.Declaration
}

func (c *admissionClosure) references(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
) iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		if obj == nil {
			c.yieldIndexedReferences(id, yield)
			return
		}
		for ref := range manifest.References(obj, manifest.ReferenceRelations) {
			if !yield(ref) {
				return
			}
		}
		if obj != nil && (c.incoming(id).Obj == nil) {
			for ref := range c.historicalReferences(id) {
				if !yield(ref) {
					return
				}
			}
		}
	}
}

func (c *admissionClosure) declarationSource(id manifest.NamedResource) string {
	if occurrence, ok := c.loader.catalogOccurrence(id); ok {
		return occurrence.Declaration.Source
	}
	if _, input, _ := c.delta(c.source(id)); input != nil {
		return input.OriginPath(input.LocalPath)
	}
	return ""
}

func (c *admissionClosure) source(id manifest.NamedResource) manifest.NamedResource {
	occurrence, located := c.loader.catalogOccurrence(id)
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
		productions = c.currentProductions(id)
		c.extendProductions(id, &state, &productions)

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
	return c.mergeHistoricalProductions(id, productions)
}

func (c *admissionClosure) bootstrapPrerequisite(
	id manifest.NamedResource,
) bool {
	record := c.record(id)
	if record.Path == "" && record.Builder == "" || !c.directBootstrap(id) {
		return false
	}
	required := func(owner manifest.BaseManifest) bool { return c.bootstrapInputRequired(owner, id, &record) }
	if required(c.object(record.Parent)) || c.incoming(id).Obj != nil && required(c.incoming(id).Obj) {
		return true
	}
	for _, owner := range c.loader.rootAdmission(record.Path, record.File, id.KindInfo().Structural).owners {
		if required(c.objects.GetDeclaration(owner)) {
			return true
		}
	}
	return c.hasOwner(required)
}

func (c *admissionClosure) directBootstrap(id manifest.NamedResource) bool {
	occurrence, found := c.loader.occurrence(id)
	input, _ := c.objects.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	return found && input != nil &&
		(input.LocalPath == occurrence.Declaration.Source || input.OriginPath(input.LocalPath) == occurrence.Declaration.Source) &&
		c.objects.SourceAdmission(id, manifest.BootstrapSourceID)
}

func (c *admissionClosure) bootstrapInputRequired(
	owner manifest.BaseManifest,
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) bool {
	if owner == nil || record.Parent != owner.Named() && !c.coversDeclaration(owner, id) {
		return false
	}
	for ref := range manifest.References(owner, manifest.ReferenceRelations) {
		if !ref.Ordering && !ref.ContentInput() {
			continue
		}
		if ref.NamedResource == id {
			return true
		}
		input := c.record(ref.NamedResource)
		productions := c.productions(ref.NamedResource)
		if len(input.Productions) == 0 && len(productions) == 0 && input.Parent == id {
			return true
		}
		for i := range productions {
			production := &productions[i]
			if (production.Materializer(ref.NamedResource) == id || production.Declaration == id) &&
				c.bootstrapProductionCurrent(ref.NamedResource, production) {
				return true
			}
		}
	}
	return false
}

func (c *admissionClosure) bootstrapProductionCurrent(id manifest.NamedResource, production *manifest.Production) bool {
	location := SourceView(c.objects, production.Source, c.loader.SourceRoot)
	if location.Pinned || location.External || safepath.Escaped(production.File) {
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

func (c *admissionClosure) coversDeclaration(owner manifest.BaseManifest, id manifest.NamedResource) bool {
	if owner == nil {
		return false
	}
	source, dir, applies := owner.SourcePath()
	producer, ok := owner.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	})
	if !applies || !ok || !producer.EmissionPolicy().MatchesNamespace(id) {
		return false
	}
	if source.Name == "" {
		source = manifest.BootstrapSourceID
	}
	return c.completeCoversDeclaration(&id, &source, &dir)
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
	if declared.Known && declared.Changed {
		return false
	}
	record := c.record(id)
	if comparison := c.outputChanged(id, new(manifest.Reference{NamedResource: id}), object); comparison.Known &&
		(declared.Known || record.Authored == id) {
		return !comparison.Changed
	}
	if declared.Known &&
		!c.inputsChanged(manifest.BootstrapSourceID, record.Inputs) {
		return true
	}
	if stability := c.declarationStable(id, owner, &record); stability.known {
		return stability.stable
	}
	if c.loader.sideKnown && record.Authored != id {
		return false
	}
	return (record.BuilderOwner == (manifest.NamedResource{}) || owner == record.BuilderOwner) &&
		record.File != record.Builder &&
		record.Declaration != nil &&
		manifest.Equal(record.Declaration, object)
}

func (c *admissionClosure) completeCoversDeclaration(
	id *manifest.NamedResource,
	source *manifest.NamedResource,
	dir *string,
) bool {
	_, input, _ := c.delta((*source))
	file := c.record((*id)).Path
	origin := c.declarationSource((*id))
	if input != nil && file == "" {
		delta, _, _ := c.delta((*source))
		root := input.OriginPath(filepath.Join(input.LocalPath, (*dir)))
		for dependency, changed := range delta.Inputs((*id)) {
			if dependency.Group != id.Group || dependency.Kind != id.Kind || dependency.Name != id.Name ||
				dependency.Namespace != "" && id.Namespace != "" && dependency.Namespace != id.Namespace {
				continue
			}
			for _, declaration := range changed.Declarations {
				path := input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(declaration)))
				if safepath.Contains(input.OriginPath(input.LocalPath), root) && safepath.Contains(root, path) {
					return true
				}
			}
		}
	}
	if input == nil || file == "" || origin != input.OriginPath(input.LocalPath) {
		return false
	}
	root := input.OriginPath(filepath.Join(input.LocalPath, (*dir)))
	return safepath.Contains(input.OriginPath(input.LocalPath), root) && safepath.Contains(root, file)
}

func (c *admissionClosure) yieldIndexedReferences(id manifest.NamedResource, yield func(manifest.Reference) bool) {
	if occurrence, ok := c.loader.catalogOccurrence(id); ok {
		for ref := range occurrence.references() {
			if !yield(ref) {
				return
			}
		}
		return
	}
	if child := c.loader.candidates[id]; child.Obj != nil && c.loader.Index.Records[id].Builder != "" {
		for ref := range manifest.References(child.Obj, manifest.ReferenceRelations) {
			if !yield(ref) {
				return
			}
		}
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
		return c.objects.IncomingProductions(child, c.loader.Index)
	}
	return c.objects.Productions(id, c.loader.Index)
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
	record, _ := c.loader.Index.Lookup(id)
	extended := false
	for i := range c.comparisonSources().names {
		evidence := c.comparisons.names[i]
		delta := evidence.changes
		for dependency, input := range delta.Inputs(id) {
			if dependency.Group != id.Group || dependency.Kind != id.Kind || dependency.Name != id.Name ||
				dependency.Namespace != "" && id.Namespace != "" && dependency.Namespace != id.Namespace {
				continue
			}
			if len(input.Productions) == 0 {
				continue
			}
			if c.bindSource(evidence).delta != delta {
				continue
			}
			c.extendProductionInput(
				input.Productions,
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

func (c *admissionClosure) extendProductionInput(input []manifest.Production, scope *productionExtension) {
	{
		productionValues := input
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]
			owner := production.Owner
			if scope.record.File != "" && scope.record.File != production.File ||
				!c.loader.declarationLocated(owner, c.objects) {
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
) []manifest.Production {
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
	if !owned {
		return productions
	}
	if c.historyProductions == nil {
		c.historyProductions = map[manifest.NamedResource][]manifest.Production{}
	}
	c.historyProductions[id] = productions
	return productions
}

func (c *admissionClosure) declarationOwner(
	id, owner manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) (manifest.NamedResource, bool) {
	declaration := id
	_, located := c.loader.catalogOccurrence(id)
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
		for ref := range c.references(owner, c.object(owner)) {
			if !c.reverse(owner, c.object(owner), new(ref)) {
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
	if *stable {
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
	if occurrence, located := c.loader.catalogOccurrence(owner); located {
		ownerFile = c.loader.fileCoordinate(filepath.Join(occurrence.Declaration.Source, occurrence.Declaration.File))
	}
	declared := c.record(declaration)
	if occurrence, located := c.loader.catalogOccurrence(
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
	child           store.Child
	owner           manifest.NamedResource
	authored, built bool
	accepted        bool
	qualification   bool
	independent     bool
}

func (e *declarationEvidence) known() bool { return e.child.Obj != nil || e.authored }

func (e *declarationEvidence) allowsOwner(owner manifest.NamedResource) bool {
	return (!e.authored && !e.accepted || !owner.ReportHidden()) && (!e.accepted || owner != e.owner) &&
		(!e.built || !e.qualification || owner != e.owner)
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

	if record.Builder == "" {
		if evidence.child.Obj == nil {
			c.classifyAuthoredDeclaration(id, &evidence)
		}
		return evidence
	}
	root := c.loader.registeredRoot(&record)
	children, current := c.loader.BuiltChildren(root, record.BuilderOwner, c.proposal.reads)
	if !current {
		return evidence
	}
	for i := range children {
		child := &children[i]
		if child.Obj.Named() != id {
			continue
		}
		if evidence.child.Obj == nil {
			evidence.child = *child
		}
		evidence.built = manifest.Equal(evidence.child.Obj, child.Obj)
		return evidence
	}

	return evidence
}

func (c *admissionClosure) acceptedDeclaration(id, owner manifest.NamedResource, object manifest.BaseManifest) bool {
	if c.incoming(id).Obj == nil && owner != (manifest.NamedResource{}) && owner != id {
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
	var document map[string]any
	if json.Unmarshal([]byte(record.Application), &document) != nil {
		return false
	}
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	return err == nil && manifest.Equal(parsed.Object, object)
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
	occurrence, located := c.loader.occurrence(id)
	root := c.rootEvidence(id)
	if root.occurrence.Declaration == (OccurrenceID{}) {
		return
	}
	acquire := c.states[id].requested && c.bootstrapPrerequisite(id)
	if root.owner != (manifest.NamedResource{}) && !acquire {
		evidence.owner = root.owner
		return
	}
	if root.admitted || acquire {
		evidence.owner = manifest.NamedResource{}
	}
	if !located ||
		evidence.owner != (manifest.NamedResource{}) && !evidence.owner.ReportHidden() && evidence.owner != id {
		return
	}
	input, _ := c.hydrationSource(id, occurrence.Declaration.Source)
	if input != nil {
		evidence.authored, evidence.owner = true, manifest.NamedResource{}
	}
}

func (c *admissionClosure) qualifyProductionSource(production *manifest.Production) {
	object := c.object(production.Owner)
	if object == nil {
		object = c.loader.CatalogObject(production.Owner)
	}
	if object == nil {
		return
	}
	source, _, applies := object.SourcePath()
	if !applies || source == production.Source {
		return
	}
	location := SourceView(c.objects, source, c.loader.SourceRoot)
	if location.Pinned || location.External {
		production.Source = source
	}
}
