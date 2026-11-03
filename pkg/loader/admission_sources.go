package loader

import (
	"iter"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type admissionSources struct {
	names    []*admissionSource
	bindings map[manifest.NamedResource]*admissionSource
	storage  []admissionSource
	changed  bool
}

type admissionSource struct {
	source                     manifest.NamedResource
	changes, delta, comparison *change.Set
	snapshots                  [2]*store.SourceArtifact
	declarations               [2]manifest.BaseManifest
	declaration                manifest.BaseManifest
	input, initial             *store.SourceArtifact
	origin                     string
	toIndex                    string
	paths                      []string
	bound, paired              bool
}

//nolint:gochecknoglobals // A process-wide pool reuses temporary comparison storage.
var admissionSourcePool = sync.Pool{New: func() any {
	return &admissionSources{bindings: map[manifest.NamedResource]*admissionSource{}}
}}

func (c *admissionClosure) comparisonSources() *admissionSources {
	if c.comparisons == nil {
		c.comparisons = admissionSourcePool.Get().(*admissionSources)
		count := 0
		for range c.loader.Options.Changes.Sources() {
			count++
		}
		c.comparisons.storage = slices.Grow(c.comparisons.storage, count+1)
		for source := range c.loader.Options.Changes.Sources() {
			changes, snapshots, declarations := c.loader.Options.Changes.ForSource(source)
			c.comparisons.storage = append(
				c.comparisons.storage,
				admissionSource{source: source, changes: changes, snapshots: snapshots, declarations: declarations},
			)
		}
		for i := range c.comparisons.storage {
			evidence := &c.comparisons.storage[i]
			c.comparisons.names = append(c.comparisons.names, evidence)
			c.comparisons.bindings[evidence.source] = evidence
		}
		slices.SortFunc(c.comparisons.names, func(a, b *admissionSource) int { return a.source.Compare(b.source) })
	}
	return c.comparisons
}

func (c *admissionClosure) sourceEvidence(source manifest.NamedResource) *admissionSource {
	if source == (manifest.NamedResource{}) {
		source = manifest.BootstrapSourceID
	}
	comparisons := c.comparisonSources()
	if evidence, known := comparisons.bindings[source]; known {
		return evidence
	}
	changes, snapshots, declarations := c.loader.Options.Changes.ForSource(source)
	comparisons.storage = append(
		comparisons.storage,
		admissionSource{source: source, changes: changes, snapshots: snapshots, declarations: declarations},
	)
	evidence := &comparisons.storage[len(comparisons.storage)-1]
	comparisons.bindings[source] = evidence
	return evidence
}

func (c *admissionClosure) sources(id manifest.NamedResource) iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		record := c.record(id)
		if len(record.ContentRoots) != 0 && (c.incoming.Obj == nil || c.incoming.Obj.Named() != id) {
			for source := range record.ContentRoots {
				if !yield(source) {
					return
				}
			}
			return
		}
		for ref := range manifest.References(c.object(id), manifest.ReferenceRelations) {
			if ref.Use == sourceReferenceUse {
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
}

func (c *admissionClosure) sourceFiles(source manifest.NamedResource) iter.Seq[string] {
	return func(yield func(string) bool) {
		evidence := c.boundSource(source)
		if evidence.delta == nil {
			return
		}
		if evidence.paths == nil {
			evidence.paths = evidence.delta.Paths()
		}
		for _, file := range evidence.paths {
			if evidence.paired {
				_, file = c.coordinates(
					manifest.Production{
						Source: source,
						File:   file,
						Path:   filepath.Join(evidence.input.LocalPath, filepath.FromSlash(file)),
					},
				)
			}
			if !yield(file) {
				return
			}
		}
	}
}

func (c *admissionClosure) changedClaim(reader manifest.FileReader, file string) bool {
	evidence := c.boundSource(reader.Source)
	delta, input, paired := evidence.delta, evidence.input, evidence.paired
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
		if changed, known := c.declarationChanged(reader.Declaration, c.object(reader.Declaration)); known && !changed {
			return false
		}
		object := c.object(reader.Reader)
		matched := false
		for ref := range c.references(reader.Reader, object) {
			if ref.NamedResource != reader.Declaration || !c.reverse(reader.Reader, object, ref) {
				continue
			}
			matched = true
			if changed, known := c.outputChanged(reader.Reader, ref, object); !known || changed {
				return true
			}
		}
		return !matched
	}
	return c.completeChangedClaim(&completeChangedClaimState{reader: &reader, file: &file, evidence: &evidence, delta: &delta, input: &input, record: &record, contentChanged: &contentChanged})
}

func (c *admissionClosure) changedInput(consumer, dependency manifest.NamedResource) bool {
	for i := range c.comparisonSources().names {
		evidence := c.bindSource(c.comparisons.names[i])
		source := evidence.source
		delta, input, paired := evidence.delta, evidence.input, evidence.paired
		if delta == nil {
			continue
		}
		proofs := c.productions(dependency)
		if len(proofs) != 0 && !slices.ContainsFunc(proofs, func(proof manifest.Production) bool {
			qualified, _, _ := c.delta(proof.Source)
			return qualified == delta
		}) {
			continue
		}
		for id, changed := range delta.Inputs(dependency) {
			if consumer == dependency && !delta.Contains(changed.File) {
				continue
			}
			if id.Group != dependency.Group || id.Kind != dependency.Kind || id.Name != dependency.Name ||
				id.Namespace != "" && id.Namespace != dependency.Namespace {
				continue
			}
			file := changed.File
			if occurrence, located := c.loader.occurrence(
				dependency,
			); changed.Unbound &&
				(located || len(c.loader.Index.Claims) != 0 || len(c.loader.Index.Readers) != 0) {
				indexed := file
				if paired && input != nil {
					_, indexed = c.coordinates(
						manifest.Production{
							Source: source,
							File:   file,
							Path:   filepath.Join(input.LocalPath, filepath.FromSlash(file)),
						},
					)
				}
				if located {
					if indexed != c.loader.fileCoordinate(
						filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File)),
					) {
						continue
					}
				} else {
					owners, _ := c.loader.Index.OwnersOfFile(indexed)
					if len(owners) != 0 && !slices.ContainsFunc(owners, func(owner manifest.NamedResource) bool {
						return c.changedClaim(manifest.FileReader{Reader: owner, Source: source}, indexed)
					}) {
						continue
					}
				}
			}
			if !paired {
				if input == nil {
					continue
				}
				base := filepath.ToSlash(evidence.toIndex)
				if base == "" || base != "." && file != base && !strings.HasPrefix(file, base+"/") {
					continue
				}
				_, origins, _ := delta.ForSource(manifest.BootstrapSourceID)
				local := false
				for _, origin := range origins {
					local = local ||
						origin != nil &&
							(origin.LocalPath == c.loader.SourceRoot || origin.LocalRoot == c.loader.SourceRoot)
				}
				if (origins[0] != nil || origins[1] != nil) && !local {
					continue
				}
			}
			if id.Namespace != "" {
				return true
			}
			readerSource := c.source(consumer)
			if readerSource == source {
				return true
			}
			if source == manifest.BootstrapSourceID {
				_, artifact, paired := c.delta(readerSource)
				if !paired && artifact != nil && artifact.OriginPath(artifact.LocalPath) == c.loader.SourceRoot {
					return true
				}
			}
		}
	}
	return false
}

func (c *admissionClosure) coordinates(p manifest.Production) (relative, indexed string) {
	relative, indexed = p.File, p.File
	if filepath.IsAbs(p.Path) {
		evidence := c.boundSource(p.Source)
		art := evidence.input
		root := c.loader.SourceRoot
		if art != nil {
			root = art.LocalPath
		}
		rel, err := filepath.Rel(root, p.Path)
		if err != nil {
			return "", ""
		}
		relative = filepath.ToSlash(rel)
		indexed = relative
		if evidence.toIndex != "." {
			physical := p.Path
			if art != nil && evidence.origin != art.LocalPath {
				physical = filepath.Join(evidence.origin, rel)
			}
			rel, err = filepath.Rel(c.loader.SourceRoot, physical)
			if err != nil {
				return "", ""
			}
			indexed = filepath.ToSlash(rel)
		}
	}
	return relative, indexed
}

func (c *admissionClosure) changedProduction(p manifest.Production) bool {
	if p.File == "" {
		return false
	}
	if p.Owner == (manifest.NamedResource{}) && p.Source == manifest.BootstrapSourceID {
		if record, _ := c.loader.Index.Lookup(p.Authored); record.Path != "" && record.Path != p.Path {
			return false
		}
	}
	delta, _, paired := c.delta(p.Source)
	if delta == nil {
		return false
	}
	relative, indexed := c.coordinates(p)
	if paired {
		return delta.Contains(relative)
	}
	return delta.Contains(indexed)
}

func (c *admissionClosure) reverse(
	id manifest.NamedResource,
	object manifest.BaseManifest,
	ref manifest.Reference,
) bool {
	if object != nil {
		for input := range manifest.References(object, manifest.ReferenceRelations) {
			if input.AffectedInput() && input.NamedResource == ref.NamedResource {
				return true
			}
		}
		return false
	}
	if occurrence, ok := c.loader.occurrence(id); ok {
		{
			originalValues := occurrence.facts.refs
			for originalIndex := range originalValues {
				original := originalValues[originalIndex]
				if !slices.Contains(occurrence.facts.reverse, original.NamedResource) {
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
		}
		return false
	}
	return ref.ContentInput()
}

type completeChangedClaimState struct {
	reader         *manifest.FileReader
	file           *string
	evidence       **admissionSource
	delta          **change.Set
	input          **store.SourceArtifact
	record         *manifest.DiscoveryRecord
	contentChanged *func(reader manifest.FileReader) bool
}

func (c *admissionClosure) completeChangedClaim(scope *completeChangedClaimState) bool {
	if !(*scope.contentChanged)((*scope.reader)) {
		return false
	}
	for actual := range c.loader.Index.ReadersOfFile((*scope.file)) {
		if actual.Reader == (*scope.reader).Reader {
			if !(*scope.contentChanged)(actual) {
				continue
			}
			qualified, _, _ := c.delta(actual.Source)
			if qualified == (*scope.delta) {
				return true
			}
		}
	}
	if roots, known := (*scope.record).ContentRoots[(*scope.reader).Source]; known {
		if (*scope.input) != nil {
			relative, err := filepath.Rel((*scope.evidence).origin, filepath.Join(c.loader.SourceRoot, filepath.FromSlash((*scope.file))))
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
