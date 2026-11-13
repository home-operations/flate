package selection

import (
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (c *admissionClosure) sources(id manifest.NamedResource) iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		var record manifest.DiscoveryRecord
		if c.incoming(id).Obj != nil {
			record = c.record(id)
		} else {
			record = c.contentRecord(id)
		}
		if len(record.ContentRoots) != 0 && c.incoming(id).Obj == nil {
			for source := range record.ContentRoots {
				if !yield(source) {
					return
				}
			}
			return
		}
		c.yieldObjectSources(id, &record, yield)
	}
}

func (c *admissionClosure) sourceFiles(source manifest.NamedResource) iter.Seq[string] {
	return func(yield func(string) bool) {
		evidence := c.boundSource(source)
		if evidence.Delta == nil {
			return
		}
		for _, file := range evidence.Files {
			c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{FileEntries: 1})
			if !yield(file.Indexed) {
				return
			}
		}
	}
}

func (c *admissionClosure) changedClaim(readerValue *manifest.FileReader, file string) bool {
	reader := *readerValue

	evidence := c.boundSource(reader.Source)
	delta, input, paired := evidence.Delta, evidence.Input, evidence.Paired
	if delta == nil {
		return false
	}
	record := c.record(reader.Reader)
	changed := paired && c.fileChanged(reader.Source, file) || !paired && !record.Pinned && delta.Contains(file)
	if !changed {
		return false
	}
	contentChanged := func(reader manifest.FileReader) bool {
		if reader.Declaration == (manifest.NamedResource{}) {
			return true
		}
		if comparison := c.declarationChanged(
			reader.Declaration,
			c.object(reader.Declaration),
		); comparison.Known &&
			!comparison.Changed {
			return false
		}
		object := c.object(reader.Reader)
		matched := false
		for ref := range c.references(reader.Reader, object) {
			if ref.NamedResource != reader.Declaration || !c.reverse(reader.Reader, object, new(ref)) {
				continue
			}
			matched = true
			if comparison := c.outputChanged(reader.Reader, new(ref), object); !comparison.Known || comparison.Changed {
				return true
			}
		}
		return !matched
	}
	return c.completeChangedClaim(
		&completeChangedClaimState{
			reader:         &reader,
			file:           &file,
			evidence:       evidence,
			delta:          &delta,
			input:          &input,
			contentChanged: &contentChanged,
		},
	)
}

func (c *admissionClosure) changedInput(consumer, dependency manifest.NamedResource) bool {
	for source := range c.proposal.Request.Changes.Sources() {
		evidence := c.boundSource(source)
		if c.comparisonInputChanged(consumer, dependency, evidence) {
			return true
		}
	}
	return false
}

type productionCoordinates struct{ relative, indexed string }

func (c *admissionClosure) coordinates(pValue *manifest.Production) productionCoordinates {
	p := *pValue

	if filepath.IsAbs(p.Path) {
		evidence := c.boundSource(p.Source)
		relative, indexed := evidence.Coordinates(c.context.Root, p.Path)
		return productionCoordinates{relative: relative, indexed: indexed}
	}
	return productionCoordinates{relative: p.File, indexed: p.File}
}

//nolint:gocritic // slices.ContainsFunc requires the value-parameter method type.
func (c *admissionClosure) changedProduction(p manifest.Production) bool {
	if p.File == "" {
		return false
	}
	if p.Owner == (manifest.NamedResource{}) && p.Source == manifest.BootstrapSourceID {
		if record, _ := c.facts.SelectionIndex().Lookup(p.Authored); record.Path != "" && record.Path != p.Path {
			return false
		}
	}
	delta, _, paired := c.delta(p.Source)
	if delta == nil {
		return false
	}
	coordinates3 := c.coordinates(new(p))
	if paired {
		return delta.Contains(coordinates3.relative)
	}
	return delta.Contains(coordinates3.indexed)
}

func (c *admissionClosure) reverse(
	id manifest.NamedResource,
	object manifest.BaseManifest,
	refValue *manifest.Reference,
) bool {
	ref := *refValue

	if object != nil {
		refs := manifest.References(object, manifest.ReferenceRelations)
		if occurrence, authored := c.projectedOccurrence(id, object); authored {
			refs = occurrence.references()
		}
		for input := range refs {
			if input.AffectedInput() && input.NamedResource == ref.NamedResource {
				return true
			}
		}
		return false
	}
	occurrence, ok := c.catalogOccurrence(id)
	if !ok {
		return ref.ContentInput()
	}
	for i := range occurrence.Refs {
		original := &occurrence.Refs[i]
		if !slices.Contains(occurrence.Reverse, original.NamedResource) {
			continue
		}
		input := original.NamedResource
		if original.NamespaceDefault {
			input.Namespace = id.Namespace
		}
		if input == ref.NamedResource {
			return true
		}
	}
	return false
}

type completeChangedClaimState struct {
	reader         *manifest.FileReader
	file           *string
	evidence       *SourceFacts
	delta          **change.Set
	input          **store.SourceArtifact
	contentChanged *func(reader manifest.FileReader) bool
}

func (c *admissionClosure) completeChangedClaim(scope *completeChangedClaimState) bool {
	if !(*scope.contentChanged)((*scope.reader)) {
		return false
	}
	readers := c.facts.SelectionIndex().Readers[*scope.file]
	for i := range readers {
		actual := &readers[i]
		c.objects.WorkScope().Admission().Add(&manifest.WorkCounts{ReaderEdges: 1})
		if actual.Reader == scope.reader.Reader {
			if !(*scope.contentChanged)(*actual) {
				continue
			}
			qualified, _, _ := c.delta(actual.Source)
			if qualified == (*scope.delta) {
				return true
			}
		}
	}
	record := c.contentRecord(scope.reader.Reader)
	if roots, known := record.ContentRoots[scope.reader.Source]; known {
		if (*scope.input) != nil {
			relative, err := filepath.Rel(
				scope.evidence.Origin,
				filepath.Join(c.context.Root, filepath.FromSlash((*scope.file))),
			)
			if err != nil {
				return false
			}
			(*scope.file) = filepath.ToSlash(relative)
		}
		return slices.ContainsFunc(roots, func(root string) bool {
			return root == "." || root == "" || manifest.HasEnvsubstReference(root) || (*scope.file) == root ||
				strings.HasPrefix((*scope.file), strings.TrimSuffix(root, "/")+"/")
		})
	}
	return true
}

func (c *admissionClosure) yieldObjectSources(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	yield func(manifest.NamedResource) bool,
) {
	for ref := range manifest.References(c.object(id), manifest.ReferenceRelations) {
		if ref.BindsSource() {
			yield(ref.NamedResource)
			return
		}
	}
	for _, ref := range record.Refs {
		if ref.KindInfo().SourceArtifact {
			yield(ref)
			return
		}
	}
	yield(manifest.BootstrapSourceID)
}

func (c *admissionClosure) comparisonInputChanged(
	consumer, dependency manifest.NamedResource,
	evidence *SourceFacts,
) bool {
	source := evidence.Source
	delta, input, paired := evidence.Delta, evidence.Input, evidence.Paired
	if delta == nil {
		return false
	}
	proofs := c.productions(dependency)
	if len(proofs) != 0 && !slices.ContainsFunc(proofs, func(proof manifest.Production) bool {
		qualified, _, _ := c.delta(proof.Source)
		return qualified == delta
	}) {
		return false
	}
	for id, changed := range delta.Inputs(dependency) {
		if c.comparisonRecordChanged(
			id,
			changed,
			&comparisonInput{
				consumer:   consumer,
				dependency: dependency,
				evidence:   evidence,
				source:     source,
				delta:      delta,
				input:      input,
				paired:     paired,
			},
		) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) fileOwnersUnchanged(indexed string, source manifest.NamedResource) bool {
	owners := c.facts.SelectionIndex().OwnersOfFile(indexed).Owners
	return len(owners) != 0 && !slices.ContainsFunc(owners, func(owner manifest.NamedResource) bool {
		return c.changedClaim(new(manifest.FileReader{Reader: owner, Source: source}), indexed)
	})
}

func (c *admissionClosure) comparisonIndexedFile(
	file string,
	source manifest.NamedResource,
	input *store.SourceArtifact,
	paired bool,
) string {
	indexed := file
	if paired && input != nil {
		indexed = c.coordinates(
			new(manifest.Production{
				Source: source,
				File:   file,
				Path:   filepath.Join(input.LocalPath, filepath.FromSlash(file)),
			}),
		).indexed
	}
	return indexed
}

func (c *admissionClosure) comparisonInputLocal(evidence *SourceFacts, delta *change.Set, file string) bool {
	base := filepath.ToSlash(evidence.ToIndex)
	if base == "" || base != "." && file != base && !strings.HasPrefix(file, base+"/") {
		return false
	}
	_, origins, _ := delta.ForSource(manifest.BootstrapSourceID)
	local := false
	for _, origin := range origins {
		local = local ||
			origin != nil &&
				(origin.LocalPath == c.context.Root || origin.LocalRoot == c.context.Root)
	}
	if (origins[0] != nil || origins[1] != nil) && !local {
		return false
	}
	return true
}

func (c *admissionClosure) inputSourceMatches(id, consumer, source manifest.NamedResource) bool {
	if id.Namespace != "" {
		return true
	}
	readerSource := c.source(consumer)
	if readerSource == source {
		return true
	}
	if source == manifest.BootstrapSourceID {
		_, artifact, paired := c.delta(readerSource)
		if !paired && artifact != nil && artifact.OriginPath(artifact.LocalPath) == c.context.Root {
			return true
		}
	}
	return false
}

type comparisonInput struct {
	consumer, dependency, source manifest.NamedResource
	evidence                     *SourceFacts
	delta                        *change.Set
	input                        *store.SourceArtifact
	paired                       bool
}

func (c *admissionClosure) comparisonRecordChanged(
	id manifest.NamedResource,
	changed change.InputChange,
	scope *comparisonInput,
) bool {
	if scope.consumer == scope.dependency && !scope.delta.Contains(changed.File) {
		return false
	}
	if id.Group != scope.dependency.Group || id.Kind != scope.dependency.Kind || id.Name != scope.dependency.Name ||
		id.Namespace != "" && id.Namespace != scope.dependency.Namespace {
		return false
	}
	file := changed.File
	if occurrence, located := c.catalogOccurrence(
		scope.dependency,
	); changed.Unbound &&
		(located || len(c.facts.SelectionIndex().Claims) != 0 || len(c.facts.SelectionIndex().Readers) != 0) {
		indexed := c.comparisonIndexedFile(file, scope.source, scope.input, scope.paired)
		if located {
			if indexed != c.fileCoordinate(
				filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File)),
			) {
				return false
			}
		} else if c.fileOwnersUnchanged(indexed, scope.source) {
			return false
		}
	}
	if !scope.paired {
		if scope.input == nil {
			return false
		}
		if !c.comparisonInputLocal(scope.evidence, scope.delta, file) {
			return false
		}
	}
	return c.inputSourceMatches(id, scope.consumer, scope.source)
}
