package selection

import (
	"cmp"
	"io/fs"
	"path"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// SetOrderingComparison MUST precede the next construction or execution round.
func (s *Session) SetOrderingComparison(owners []manifest.NamedResource) {
	s.orderingComparison, s.comparisonOnly = owners, len(owners) != 0
}

func (c *admissionClosure) delta(source manifest.NamedResource) (*change.Set, *store.SourceArtifact, bool) {
	evidence := c.boundSource(source)
	return evidence.Delta, evidence.Input, evidence.Paired
}

func (c *admissionClosure) boundSource(source manifest.NamedResource) *SourceFacts {
	if source == (manifest.NamedResource{}) {
		source = manifest.BootstrapSourceID
	}
	if c.sourceReads != c.proposal.reads {
		clear(c.sourceFacts)
		c.sourceReads = c.proposal.reads
	}
	if facts, known := c.sourceFacts[source]; known {
		return facts
	}
	facts := c.facts.SourceFacts(source, c.proposal.reads)
	if c.proposal.reads != nil {
		if c.sourceFacts == nil {
			c.sourceFacts = make(map[manifest.NamedResource]*SourceFacts)
		}
		c.sourceFacts[source] = &facts
	}
	if facts.Derived {
		c.derive()
	}
	return &facts
}

func (c *admissionClosure) fileChanged(source manifest.NamedResource, file string) bool {
	evidence := c.boundSource(source)
	if evidence.Paired && evidence.Input != nil {
		var known bool
		file, known = evidence.Input.SourcePath(c.context.Root, file)
		if !known {
			return false
		}
	}
	return evidence.Delta.Contains(file)
}

func (c *admissionClosure) inputsChanged(source manifest.NamedResource, inputs []string) bool {
	if len(inputs) == 0 {
		return false
	}
	for file := range c.sourceFiles(source) {
		if slices.Contains(inputs, file) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) outputChanged(
	id manifest.NamedResource,
	refValue *manifest.Reference,
	obj manifest.BaseManifest,
) change.Comparison {
	ref := *refValue
	if !c.context.SideKnown {
		return change.Comparison{}
	}

	if ref.BindsSource() && ref.ReadsSourceTree() && c.stableWorkingTreeBinding(ref.NamedResource) {
		return change.Comparison{Known: true}
	}

	source := c.source(id)
	_, _, paired := c.delta(source)
	if !paired {
		return change.Comparison{}
	}
	record, _ := c.facts.SelectionIndex().Lookup(id)
	if prepared := record.Declaration; prepared != nil && obj != prepared && record.Builder != "" &&
		!c.proposal.Request.Changes.OutputChanged(c.context.Side, source, ref, obj).Known &&
		manifest.Equal(obj, prepared) {
		child := c.preparedObject(id)
		var doc map[string]any
		if c.incoming(id).Obj != nil && c.incoming(id).Doc != nil {
			doc = c.incoming(id).Doc
		} else if c.objects.GetDeclaration(id) == obj {
			_, current, output := c.objects.GetEmission(id)
			defer output.Finish()
			doc = current
		}
		if child.Obj == prepared && manifest.ValueEqual(doc, child.Doc) {
			obj = prepared
		}
	}
	if ref.Use != "" && ref.Name != "" {
		input := c.object(ref.NamedResource)
		if input == nil {
			return change.Comparison{}
		}
		if comparison := c.outputChanged(
			ref.NamedResource,
			new(manifest.Reference{NamedResource: ref.NamedResource}),
			input,
		); !comparison.Known {
			return change.Comparison{}
		}
	}
	return c.proposal.Request.Changes.OutputChanged(c.context.Side, source, ref, obj)
}

// A stable working-tree binding supplies file deltas at each reader.
func (c *admissionClosure) stableWorkingTreeBinding(source manifest.NamedResource) bool {
	binding := c.boundSource(source)
	base, head := binding.Snapshots[manifest.TreeBase], binding.Snapshots[manifest.TreeHead]
	if !base.CheckoutView() || !head.CheckoutView() || binding.Input == nil {
		return false
	}
	const material = store.ArtifactContent | store.ArtifactRevision
	if !binding.Input.Reads.Valid() ||
		binding.Input.Input(material) != binding.Snapshots[c.context.Side].Input(material) {
		return false
	}
	declaration := binding.Declarations[manifest.TreeBase]
	if declaration == nil || !manifest.Equal(declaration, binding.Declarations[manifest.TreeHead]) {
		return false
	}
	bootstrap := c.boundSource(manifest.BootstrapSourceID).Snapshots
	return (base == bootstrap[manifest.TreeBase] || base.Provider == store.ProviderAlias) &&
		(head == bootstrap[manifest.TreeHead] || head.Provider == store.ProviderAlias)
}

func (c *admissionClosure) orderingDeclarationChanged(id manifest.NamedResource) bool {
	if c.declarationChanged(id, c.object(id)).Changed {
		return true
	}
	return c.context.SideKnown && c.proposal.Request.Changes.EffectiveDeclarationChanged(
		c.context.Side, id, func(source manifest.NamedResource) *store.SourceArtifact {
			return c.boundSource(source).Input
		},
	)
}

func (c *admissionClosure) declarationChanged(
	id manifest.NamedResource,
	object manifest.BaseManifest,
) change.Comparison {
	if c.proposal.Request.Changes == nil {
		return change.Comparison{}
	}
	var document map[string]any
	if c.context.SideKnown && object != nil {
		if child := c.incoming(id); child.Obj == object {
			document = child.Doc
		} else if c.objects.GetDeclaration(id) == object {
			_, current, output := c.objects.GetEmission(id)
			document = current
			output.Finish()
		} else if record, _ := c.facts.SelectionIndex().Lookup(id); record.Declaration == object && record.Builder != "" {
			child := c.preparedObject(id)
			document = child.Doc
		}
	}
	source := c.source(id)
	comparison := c.proposal.Request.Changes.DeclarationChanged(c.context.Side, source, id, nil)
	if !comparison.Known {
		return comparison
	}
	var evidence *store.DeclarationContent
	if document != nil {
		if c.declarationContents == nil {
			c.declarationContents = c.facts.DeclarationContents()
		}
		content := c.declarationContents(document)
		evidence = &content
	}
	return c.proposal.Request.Changes.DeclarationChanged(c.context.Side, source, id, evidence)
}

func (c *admissionClosure) affected(id manifest.NamedResource, obj manifest.BaseManifest) bool {
	if c.proposal.Request.Changes == nil {
		return false
	}
	state := c.states[id]
	if state.affectedKnown {
		return state.affected
	}
	result := c.deriveAffected(id, obj)
	state = c.states[id]
	state.affected, state.affectedKnown = result.changed, result.settled
	c.states[id] = state
	return result.changed
}

type affectedResult struct{ changed, settled bool }

func (c *admissionClosure) deriveAffected(id manifest.NamedResource, obj manifest.BaseManifest) affectedResult {
	if c.proposal.Request.Changes == nil {
		return affectedResult{settled: true}
	}
	if c.entryDeclarationChanged(id, obj) {
		return affectedResult{changed: true, settled: true}
	}
	record := c.contentRecord(id)
	impact := c.affectedProductions(id, &record)
	if !impact.fileOwned {
		delta, _, _ := c.delta(manifest.BootstrapSourceID)
		impact.direct = impact.direct || delta.Contains(record.File)
	}
	if impact.direct {
		comparison := c.outputChanged(id, new(manifest.Reference{NamedResource: id}), obj)
		if !comparison.Known {
			comparison = c.declarationChanged(id, obj)
		}
		if !comparison.Known || comparison.Changed {
			return affectedResult{changed: true, settled: true}
		}
	}
	if c.inputsChanged(manifest.BootstrapSourceID, record.Inputs) {
		if comparison := c.outputChanged(
			id,
			new(manifest.Reference{NamedResource: id}),
			obj,
		); !comparison.Known ||
			comparison.Changed {
			return affectedResult{changed: true, settled: true}
		}
	}
	return c.affectedContentReads(id, obj, &record)
}

func (c *admissionClosure) affectedContentReads(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	record *manifest.DiscoveryRecord,
) affectedResult {
	roots := c.contentRoots(id, obj, record)
	settled := true
	var generatedSources []manifest.NamedResource
	for ref := range c.references(id, obj) {
		paths := c.referenceReadPaths(&ref)
		comparison, generated := c.generatedKustomizeRead(id, obj, &ref)
		var result affectedResult
		switch {
		case generated:
			generatedSources = append(generatedSources, ref.NamedResource)
			result = affectedResult{changed: comparison.Changed, settled: comparison.Known}
		case len(paths.Roots)+len(paths.Files) != 0:
			result = c.affectedSourceRoots(id, ref.NamedResource, &paths, record)
		case len(roots[ref.NamedResource]) == 0:
			comparison, generated = c.generatedRead(ref.NamedResource, nil)
			if !generated || !c.reverse(id, obj, &ref) {
				continue
			}
			result = affectedResult{changed: comparison.Changed, settled: comparison.Known}
		default:
			continue
		}
		if result.changed {
			return affectedResult{changed: true, settled: true}
		}
		settled = settled && result.settled
	}
	for source, paths := range roots {
		if slices.Contains(generatedSources, source) {
			continue
		}
		result := c.affectedSourceRoots(id, source, &SourceReadPaths{Roots: paths}, record)
		if result.changed {
			return affectedResult{changed: true, settled: true}
		}
		settled = settled && result.settled
	}
	return affectedResult{settled: settled}
}

func (c *admissionClosure) entryDeclarationChanged(id manifest.NamedResource, object manifest.BaseManifest) bool {
	route := c.proposal.entry
	if !c.session.entryComparisonComplete || route == nil || route.Producer == (manifest.NamedResource{}) {
		return false
	}
	ref := manifest.Reference{NamedResource: id}
	if !c.outputChanged(id, &ref, object).Changed {
		return false
	}
	return slices.ContainsFunc(c.productions(id), func(p manifest.Production) bool {
		return p.Owner == route.Producer
	})
}

type productionImpact struct{ direct, fileOwned bool }

func (c *admissionClosure) affectedProductions(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) productionImpact {
	direct := false
	fileOwned := false
	{
		productionValues := c.productions(id)
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]
			indexed := c.coordinates(new(production)).indexed
			fileOwned = fileOwned || indexed != "" && indexed == record.File
			if production.Declaration == (manifest.NamedResource{}) &&
				(production.Materializer(id) != (manifest.NamedResource{}) || production.Owner == (manifest.NamedResource{}) || manifest.Resolve(production.Authored, id.Namespace, "") == id) &&
				c.changedProduction(production) {
				direct = true
			}
		}
	}
	return productionImpact{direct: direct, fileOwned: fileOwned}
}

type affectedLocation struct {
	base, prefix string
	known        bool
}

func (c *admissionClosure) affectedCoordinates(
	root string,
	input *store.SourceArtifact,
	paired bool,
) affectedLocation {
	base := cmp.Or(root, ".")
	prefix, known := input.IndexPath(c.context.Root, base)
	if !known {
		return affectedLocation{}
	}
	if !paired {
		base = prefix
	}
	if prefix == "." {
		prefix = ""
	}
	return affectedLocation{base: base, prefix: prefix, known: true}
}

func (c *admissionClosure) populateContentRoots(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	roots map[manifest.NamedResource][]string,
) {
	var paths []string
	initialized := false
	for root := range manifest.RenderRoots(obj) {
		if !initialized {
			initialized = true
			for source := range c.sources(id) {
				if _, input, _ := c.delta(source); c.incoming(id).Obj != nil || input != nil && input.FS != nil {
					roots[source] = nil
				}
			}
		}
		if len(roots) != 0 {
			paths = append(paths, root)
		}
	}
	for source := range roots {
		roots[source] = paths
	}
}

func (c *admissionClosure) affectedSourceRoots(
	id, source manifest.NamedResource,
	paths *SourceReadPaths,
	record *manifest.DiscoveryRecord,
) affectedResult {
	evidence := c.boundSource(source)
	if comparison, generated := c.generatedRead(source, paths); generated {
		return affectedResult{changed: comparison.Changed, settled: comparison.Known}
	}
	if evidence.Delta == nil || record.Pinned && !evidence.Paired {
		return affectedResult{settled: true}
	}
	settled := true
	for name, exact := range paths.all() {
		resolved, known := c.affectedRoot(source, name)
		if !known {
			settled = false
			continue
		}
		location := c.affectedCoordinates(resolved, evidence.Input, evidence.Paired)
		if !location.known {
			continue
		}
		var changed bool
		if exact {
			changed = evidence.Delta.Contains(location.base) && c.affectedClaim(id, location.prefix, location.prefix)
		} else {
			changed = c.affectedSubtree(id, &location, evidence)
		}
		if changed {
			return affectedResult{changed: true, settled: settled}
		}
	}
	return affectedResult{settled: settled}
}

func (c *admissionClosure) affectedSubtree(
	id manifest.NamedResource,
	location *affectedLocation,
	evidence *SourceFacts,
) bool {
	base := strings.TrimSuffix(location.base, "/")
	for _, file := range evidence.Files {
		c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{FileEntries: 1})
		matches := base == "." || file.Path == base || strings.HasPrefix(file.Path, base+"/")
		missingCoordinates := evidence.Paired && evidence.Input != nil && file.Indexed == ""
		if matches && !missingCoordinates && c.affectedClaim(id, location.prefix, file.Indexed) {
			return true
		}
	}
	return false
}

// Generated captures are comparable only at their accepted generation.
func (c *admissionClosure) generatedRead(
	source manifest.NamedResource, paths *SourceReadPaths,
) (change.Comparison, bool) {
	var compare func(*change.Set) change.Comparison
	if paths != nil && len(paths.Roots)+len(paths.Files) != 0 {
		compare = func(delta *change.Set) change.Comparison {
			if delta.Len() == 0 {
				return change.Comparison{Known: true}
			}
			resolved, known := c.resolveReadPaths(source, paths)
			if !known {
				return change.Comparison{}
			}
			return delta.ComparePaths(resolved.matches)
		}
	}
	return c.compareGeneratedRead(source, compare)
}

func (c *admissionClosure) compareGeneratedRead(
	source manifest.NamedResource, compare func(*change.Set) change.Comparison,
) (change.Comparison, bool) {
	if source == manifest.BootstrapSourceID || !source.KindInfo().SourceArtifact ||
		!c.context.SideKnown || !c.proposal.Request.Changes.Paired() {
		return change.Comparison{}, false
	}
	evidence := c.boundSource(source)
	if evidence.Input.CheckoutView() && evidence.Input.Provider == store.ProviderAlias && !evidence.External {
		return change.Comparison{}, false
	}
	input := evidence.Input
	if evidence.Paired {
		input = evidence.Snapshots[c.context.Side]
	}
	comparison, generated := c.proposal.Request.Changes.CompareSource(
		source, c.context.Side, input, compare,
	)
	if evidence.Declaration != evidence.Declarations[c.context.Side] {
		comparison = change.Comparison{}
	}
	if generated {
		return comparison, true
	}
	productions := c.productions(source)
	for i := range productions {
		owner := productions[i].Materializer(source)
		if owner != (manifest.NamedResource{}) && owner != source && !owner.KindInfo().Structural {
			return comparison, true
		}
	}
	return change.Comparison{}, false
}

func (c *admissionClosure) resolveReadPaths(
	source manifest.NamedResource, paths *SourceReadPaths,
) (SourceReadPaths, bool) {
	resolved := *paths
	copied := false
	for i, root := range paths.Roots {
		name, known := c.affectedRoot(source, root)
		if !known {
			return SourceReadPaths{}, false
		}
		if name == root {
			continue
		}
		if !copied {
			resolved.Roots = slices.Clone(paths.Roots)
			copied = true
		}
		resolved.Roots[i] = name
	}
	return resolved, true
}

func (c *admissionClosure) reverseChanged(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	ref *manifest.Reference,
) bool {
	if ref.NamedResource.KindInfo().SourceArtifact && c.proposal.Request.Changes.Paired() {
		comparison, generated := c.generatedKustomizeRead(id, obj, ref)
		if !generated {
			record := c.contentRecord(id)
			paths := c.referenceReadPaths(ref)
			if len(paths.Files)+len(paths.Roots) == 0 {
				paths.Roots = c.contentRoots(id, obj, &record)[ref.NamedResource]
			}
			comparison, generated = c.generatedRead(ref.NamedResource, &paths)
		}
		if generated {
			// Pending generated reads cannot inherit dirtiness through their producer.
			return comparison.Changed
		}
	}
	comparison := c.outputChanged(id, ref, obj)
	return !comparison.Known || comparison.Changed
}

type kustomizeReadKey struct {
	id, source manifest.NamedResource
	object     manifest.BaseManifest
}

type kustomizeReadPaths struct {
	sides    [2]SourceReadPaths
	complete bool
}

func (c *admissionClosure) generatedKustomizeRead(
	id manifest.NamedResource, obj manifest.BaseManifest, ref *manifest.Reference,
) (change.Comparison, bool) {
	if c.session == nil || c.session.readKustomizeInputs == nil || c.evaluationContext == nil {
		return change.Comparison{}, false
	}
	if !id.KindInfo().Structural || !ref.BindsSource() || !ref.ReadsSourceTree() {
		return change.Comparison{}, false
	}
	source := ref.NamedResource
	return c.compareGeneratedRead(source, func(delta *change.Set) change.Comparison {
		if delta.Len() == 0 {
			return change.Comparison{Known: true}
		}
		if obj == nil {
			record := c.record(id)
			owner := c.declarationProductionOwner(id, record.Parent)
			obj = c.completeHydrate(c.evaluationContext, id, owner, &record)
		}
		key := kustomizeReadKey{id: id, source: source, object: obj}
		reads, ok := c.kustomizeReads[key]
		if !ok {
			reads.sides, reads.complete = c.session.readKustomizeInputs(
				c.evaluationContext, id, obj, c.boundSource(source),
			)
			if c.kustomizeReads == nil {
				c.kustomizeReads = make(map[kustomizeReadKey]kustomizeReadPaths)
			}
			c.kustomizeReads[key] = reads
		}
		comparison := delta.ComparePaths(func(file string) bool {
			return reads.sides[0].matches(file) || reads.sides[1].matches(file)
		})
		comparison.Known = comparison.Changed || reads.complete
		return comparison
	})
}

func (c *admissionClosure) initialSourceChanged(source manifest.NamedResource) bool {
	comparison, generated := c.generatedRead(source, nil)
	if !generated {
		return true
	}
	if !comparison.Known {
		c.requireSourceMaterializers(source)
	}
	return comparison.Changed
}

func (c *admissionClosure) requireSourceMaterializers(source manifest.NamedResource) {
	if !c.proposal.Request.construction {
		return
	}
	demand := func(owner manifest.NamedResource) {
		if owner.Name != "" && owner != source && !owner.KindInfo().Structural {
			c.demandMaterialization(owner)
		}
	}
	peerOwner := c.proposal.Request.Changes.PendingSourceOwner(source, c.context.Side)
	demand(peerOwner)
	productions := c.productions(source)
	for i := range productions {
		if owner := productions[i].Materializer(source); owner != peerOwner {
			demand(owner)
		}
	}
}

func (c *admissionClosure) affectedRoot(source manifest.NamedResource, root string) (string, bool) {
	if strings.Contains(root, "/../") || strings.HasSuffix(root, "/..") {
		resolved, available, known := c.resolveRoot(source, root)
		if !known {
			return "", false
		}
		if available {
			root = resolved
		} else {
			root = "."
		}
	}
	return root, true
}

func (c *admissionClosure) affectedClaim(id manifest.NamedResource, prefix, indexed string) bool {
	claim := prefix
	if claim != "" {
		claim += "/"
	}
	if slices.Contains(c.facts.SelectionIndex().Claims[claim], id) {
		owners := c.facts.SelectionIndex().OwnersOfFile(indexed).Owners
		if !slices.Contains(owners, id) {
			return false
		}
	}
	return true
}

func (c *admissionClosure) contentRoots(
	id manifest.NamedResource,
	obj manifest.BaseManifest,
	record *manifest.DiscoveryRecord,
) map[manifest.NamedResource][]string {
	roots := record.ContentRoots
	if c.incoming(id).Obj != nil && !id.ReportHidden() {
		roots = nil
	}
	if roots == nil && obj != nil {
		roots = map[manifest.NamedResource][]string{}
		c.populateContentRoots(id, obj, roots)
	}

	return roots
}

func (c *admissionClosure) referenceReadPaths(ref *manifest.Reference) SourceReadPaths {
	if c.evaluationContext == nil || !ref.BindsSource() || ref.ChartPath == "" {
		return SourceReadPaths{}
	}
	evidence := c.boundSource(ref.NamedResource)
	if evidence.Delta == nil || evidence.Delta.Len() == 0 {
		return SourceReadPaths{}
	}
	chart := path.Clean(ref.ChartPath)
	paths := c.chartReadPaths(evidence.Input, chart)
	if evidence.Paired && c.context.SideKnown {
		peer := c.chartReadPaths(evidence.Snapshots[1-c.context.Side], chart)
		paths.Files = append(paths.Files, peer.Files...)
		paths.Roots = append(paths.Roots, peer.Roots...)
	}
	for _, file := range ref.Files {
		if name := path.Clean(file); fs.ValidPath(name) && name != "." && strings.IndexByte(name, 0) < 0 {
			paths.Files = append(paths.Files, name)
		}
	}
	return paths
}

type chartReadKey struct {
	input store.ArtifactInput
	chart string
}

type chartReadPaths struct {
	input *store.SourceArtifact
	paths SourceReadPaths
}

func (c *admissionClosure) chartReadPaths(input *store.SourceArtifact, chart string) SourceReadPaths {
	if input == nil || input.FS == nil {
		return SourceReadPaths{}
	}
	key := chartReadKey{input: input.Input(store.ArtifactContent), chart: chart}
	reads := c.chartReads[key]
	for i := range reads {
		if key.input.Content != "" || reads[i].input.SameMaterial(input, store.ArtifactContent) {
			return reads[i].paths
		}
	}
	paths := c.facts.ChartReadPaths(c.evaluationContext, input, chart)
	// Current and peer paths MUST keep their backing arrays when callers append values files.
	paths.Files, paths.Roots = slices.Clip(paths.Files), slices.Clip(paths.Roots)
	if c.evaluationContext.Err() == nil {
		if c.chartReads == nil {
			c.chartReads = make(map[chartReadKey][]chartReadPaths)
		}
		c.chartReads[key] = append(reads, chartReadPaths{input: input, paths: paths})
	}
	return paths
}
