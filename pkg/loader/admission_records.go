package loader

import (
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
)

//nolint:ireturn // The manifest contract requires polymorphic results.
func (c *admissionClosure) object(id manifest.NamedResource) manifest.BaseManifest {
	if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
		return c.incoming.Obj
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
		if obj != nil && (c.incoming.Obj == nil || c.incoming.Obj.Named() != id) {
			for ref := range c.historicalReferences(id) {
				if !yield(ref) {
					return
				}
			}
		}
	}
}

func (c *admissionClosure) declarationSource(id manifest.NamedResource) string {
	if occurrence, ok := c.loader.occurrence(id); ok {
		return occurrence.Declaration.Source
	}
	if _, input, _ := c.delta(c.source(id)); input != nil {
		return input.OriginPath(input.LocalPath)
	}
	return ""
}

func (c *admissionClosure) source(id manifest.NamedResource) manifest.NamedResource {
	occurrence, located := c.loader.occurrence(id)
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

func (c *admissionClosure) independent(id manifest.NamedResource, owners func() []manifest.BaseManifest) bool {
	record := c.record(id)
	root := id
	if occurrence, ok := c.loader.occurrence(id); ok && occurrence.Authored.Namespace == "" {
		if _, exact := c.loader.Index.Lookup(id); !exact {
			root.Namespace = ""
		}
	}
	if !c.loader.AdmitsRoot(c.loader.CatalogObject(root), record.Path) ||
		record.Parent != (manifest.NamedResource{}) && record.Parent != id {
		return false
	}
	c.productions(id)
	if c.states[id].staticOwner {
		return false
	}
	if owners == nil {
		return true
	}
	for _, owner := range owners() {
		if owner.Named() != id && c.coversDeclaration(owner, id) {
			return false
		}
	}
	return true
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
	remember := func() bool {
		if c.collecting && ownerOrdinal != 0 && ordinal != 0 {
			identity := &c.identities[ordinal-1]
			identity.stableOwner, identity.stableObject = ownerOrdinal, object
		}
		return true
	}
	if changed, known := c.outputChanged(id, new(manifest.Reference{NamedResource: id}), object); known {
		if !changed {
			return remember()
		}
		return false
	}
	record := c.record(id)
	if changed, known := c.declarationChanged(
		id,
		object,
	); known && !changed &&
		!c.inputsChanged(manifest.BootstrapSourceID, record.Inputs) {
		return remember()
	}
	if stable, known := c.declarationStable(id, owner, &record); known {
		if stable {
			return remember()
		}
		return false
	}
	if (record.BuilderOwner == (manifest.NamedResource{}) || owner == record.BuilderOwner) &&
		record.File != record.Builder &&
		record.Declaration != nil &&
		manifest.Equal(record.Declaration, object) {
		return remember()
	}
	return false
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
	if occurrence, ok := c.loader.occurrence(id); ok {
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
	if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
		return c.objects.IncomingProductions(c.incoming, c.loader.Index)
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
				!c.loader.IsFileIndexed(owner) && c.objects.GetObject(owner) == nil {
				continue
			}
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
	for _, records := range c.accepted {
		if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
			break
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
	_, located := c.loader.occurrence(id)
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

func (c *admissionClosure) declarationStable(
	id, owner manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) (bool, bool) {
	declaration, owned := c.declarationOwner(id, owner, record)
	ownerRecord := c.record(owner)
	ownerFile := ownerRecord.File
	if occurrence, located := c.loader.occurrence(owner); located {
		ownerFile = c.loader.fileCoordinate(filepath.Join(occurrence.Declaration.Source, occurrence.Declaration.File))
	}
	declared := c.record(declaration)
	if occurrence, located := c.loader.occurrence(
		declaration,
	); located && owner != (manifest.NamedResource{}) && owned && declared.File != "" &&
		declared.File != declared.Builder {
		source := c.source(declaration)
		delta, input, _ := c.delta(source)
		stable := ownerRecord.File != ownerRecord.Builder && delta != nil && input != nil && input.FS != nil &&
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
				return false, true
			}
			state.comparingDeclaration = true
			c.states[id] = state
			stable = c.unchangedDeclaration(owner, parent, c.object(owner))
			state = c.states[id]
			state.comparingDeclaration = false
			c.states[id] = state
		}
		if stable {
			return true, true
		}
	}
	return false, false
}
