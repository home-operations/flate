package selection

import (
	"cmp"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
	return evidence.Delta.FileChanged(change.FileContext{
		Root: c.context.Root, Origin: evidence.Origin, ToIndex: evidence.ToIndex,
		Input: evidence.Input, Paired: evidence.Paired,
	}, file)
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

func (c *admissionClosure) declarationChanged(
	id manifest.NamedResource,
	object manifest.BaseManifest,
) change.Comparison {
	var document map[string]any
	if c.context.SideKnown && object != nil {
		if c.incoming(id).Obj == object {
			document = c.incoming(id).Doc
		} else if c.objects.GetDeclaration(id) == object {
			_, current, output := c.objects.GetEmission(id)
			document = current
			output.Finish()
		} else if record, _ := c.facts.SelectionIndex().Lookup(id); record.Declaration == object && record.Builder != "" {
			document = c.preparedObject(id).Doc
		}
	}
	return c.proposal.Request.Changes.DeclarationChanged(c.context.Side, c.source(id), id, document)
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
	roots := c.contentRoots(id, obj, &record)
	settled := true
	for source, roots := range roots {
		result := c.affectedSourceRoots(id, source, roots, &record)
		settled = settled && result.settled
		if result.changed {
			return affectedResult{changed: true, settled: settled}
		}
	}
	return affectedResult{settled: settled}
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
			fileOwned = fileOwned || indexed == record.File
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
	if !paired && input != nil {
		relative, err := filepath.Rel(
			c.context.Root,
			input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(root))),
		)
		if err != nil {
			return affectedLocation{}
		}
		base = filepath.ToSlash(relative)
	}
	prefix := base
	if paired && input != nil {
		physical := input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(root)))
		relative, err := filepath.Rel(c.context.Root, physical)
		if err != nil {
			return affectedLocation{}
		}
		prefix = filepath.ToSlash(relative)
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
				if _, input, _ := c.delta(source); input != nil && input.FS != nil {
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

func (c *admissionClosure) incomingContentSource(
	obj manifest.BaseManifest,
	record *manifest.DiscoveryRecord,
) manifest.NamedResource {
	source := manifest.BootstrapSourceID
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.BindsSource() {
			source = ref.NamedResource
			break
		}
	}
	if source == manifest.BootstrapSourceID {
		for _, ref := range record.Refs {
			if ref.KindInfo().SourceArtifact {
				source = ref
				break
			}
		}
	}
	return source
}

func (c *admissionClosure) affectedSourceRoots(
	id, source manifest.NamedResource,
	roots []string,
	record *manifest.DiscoveryRecord,
) affectedResult {
	delta, input, paired := c.delta(source)
	if delta == nil || len(roots) == 0 || record.Pinned && !paired {
		return affectedResult{settled: true}
	}
	files := c.boundSource(source).Files
	settled := true
	for _, root := range roots {
		resolved, known := c.affectedRoot(source, root)
		root = resolved
		if !known {
			settled = false
			continue
		}
		location := c.affectedCoordinates(root, input, paired)
		if !location.known {
			continue
		}
		base := strings.TrimSuffix(location.base, "/")
		for _, file := range files {
			c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{FileEntries: 1})
			suffix, contains := strings.CutPrefix(file.Path, base)
			descendant := contains && strings.HasPrefix(suffix, "/")
			withinRoot := location.base == "." || file.Path == location.base || descendant
			missingCoordinates := paired && input != nil && file.Indexed == ""
			if !withinRoot || missingCoordinates {
				continue
			}
			if c.affectedClaim(id, location.prefix, file.Indexed) {
				return affectedResult{changed: true, settled: settled}
			}
		}
	}
	return affectedResult{settled: settled}
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
	if roots == nil && obj != nil {
		roots = map[manifest.NamedResource][]string{}
		c.populateContentRoots(id, obj, roots)
	}

	if c.incoming(id).Obj != nil && (record.ContentRoots == nil || !id.ReportHidden()) {
		source := c.incomingContentSource(obj, record)
		roots = map[manifest.NamedResource][]string{source: slices.Collect(manifest.RenderRoots(obj))}
	}
	return roots
}
