package selection

import (
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Frozen discovery MUST keep the shared catalog witnesses immutable through the session.
type admissionCatalog struct {
	entries       map[manifest.NamedResource]admissionCatalogEntry
	materializers []manifest.NamedResource
}

type admissionCatalogEntry struct {
	occurrence OccurrenceFacts
	object     manifest.BaseManifest
	known      bool
}

func (s *Session) catalogSnapshot() *admissionCatalog {
	if snapshot := s.catalog.Load(); snapshot != nil {
		return snapshot
	}
	snapshot := &admissionCatalog{
		entries: make(map[manifest.NamedResource]admissionCatalogEntry, len(s.facts.SelectionIndex().Records)),
	}
	for id := range s.facts.SelectionIndex().Records {
		occurrence, known := s.facts.Occurrence(id)
		object := occurrence.Object
		if object == nil {
			object = occurrence.Catalog
		}
		if object == nil {
			object = occurrence.Record.Declaration
		}
		if object != nil {
			object = manifest.CatalogObject(object, id)
		}
		snapshot.entries[id] = admissionCatalogEntry{occurrence: occurrence, object: object, known: known}
		if occurrence.CatalogKnown &&
			(occurrence.Policy != nil || len(occurrence.Targets) != 0 || occurrence.NeedsCandidates) ||
			occurrence.Candidate.Obj != nil && occurrence.Record.Builder != "" {
			snapshot.materializers = append(snapshot.materializers, id)
		}
	}
	slices.SortFunc(snapshot.materializers, manifest.NamedResource.Compare)
	if s.catalog.CompareAndSwap(nil, snapshot) {
		return snapshot
	}
	return s.catalog.Load()
}

func (c *admissionClosure) occurrenceFacts(id manifest.NamedResource) (OccurrenceFacts, bool) {
	if c.catalog != nil {
		if entry, known := c.catalog.entries[id]; known {
			return entry.occurrence, entry.known
		}
	}
	return c.facts.Occurrence(id)
}

func (c *admissionClosure) catalogOccurrence(id manifest.NamedResource) (OccurrenceFacts, bool) {
	facts, known := c.occurrenceFacts(id)
	return facts, known && facts.CatalogKnown
}

func (c *admissionClosure) occurrence(id manifest.NamedResource) (OccurrenceFacts, bool) {
	facts, known := c.catalogOccurrence(id)
	return facts, known && !facts.RequiresBuild && facts.Identity.Name == facts.Authored.Name
}

//nolint:ireturn // Immutable catalog witnesses MUST retain their concrete manifest type.
func (c *admissionClosure) catalogWitness(id manifest.NamedResource) manifest.BaseManifest {
	facts, _ := c.occurrenceFacts(id)
	object := facts.Object
	if object == nil {
		object = facts.Catalog
	}
	if object == nil {
		object = facts.Record.Declaration
	}
	if object == nil {
		return nil
	}
	return object
}

//nolint:ireturn // Catalog identity projection MUST retain each concrete manifest type.
func (c *admissionClosure) catalogObject(id manifest.NamedResource) manifest.BaseManifest {
	if c.catalog != nil {
		if entry, known := c.catalog.entries[id]; known {
			return entry.object
		}
	}
	object := c.catalogWitness(id)
	if object == nil {
		return nil
	}
	return manifest.CatalogObject(object, id)
}

func (c *admissionClosure) candidate(id manifest.NamedResource) store.Child {
	facts, _ := c.occurrenceFacts(id)
	return facts.Candidate
}

func (c *admissionClosure) preparedObject(id manifest.NamedResource) store.Child {
	record, _ := c.facts.SelectionIndex().Lookup(id)
	if record.Builder == "" {
		return store.Child{}
	}
	child := c.candidate(id)
	if child.Obj == record.Declaration {
		return child
	}
	facts := c.capturedRoot(id)
	if facts.BuiltChild.Obj == record.Declaration {
		return facts.BuiltChild
	}
	return store.Child{}
}

func (c *admissionClosure) capturedRoot(id manifest.NamedResource) RootEvidence {
	if facts, known := c.capturedRoots[id]; known {
		return facts
	}
	facts := c.facts.RootEvidence(id, c.proposal.reads)
	if c.capturedRoots == nil {
		c.capturedRoots = make(map[manifest.NamedResource]RootEvidence)
	}
	c.capturedRoots[id] = facts
	return facts
}

func (c *admissionClosure) isFileIndexed(id manifest.NamedResource) bool {
	if c.facts.SelectionIndex().IsFileIndexed(id) {
		return true
	}
	record, _ := c.facts.SelectionIndex().Lookup(id)
	if record.Builder != "" && c.candidate(id).Obj != nil {
		return true
	}
	if occurrence, ok := c.occurrence(id); ok && occurrence.Authored.Namespace == "" && id.Namespace != "" {
		id.Namespace = ""
		return c.facts.SelectionIndex().IsFileIndexed(id)
	}
	return false
}

func (c *admissionClosure) fileCoordinate(file string) string {
	if containsPath(c.context.Root, file) {
		relative, _ := filepath.Rel(c.context.Root, file)
		return filepath.ToSlash(relative)
	}
	return filepath.ToSlash(file)
}

func (c *admissionClosure) declarationLocated(id manifest.NamedResource, objects admissionReader) bool {
	record, _ := c.facts.SelectionIndex().Lookup(id)
	return record.Path != "" || record.Builder != "" || objects.GetDeclaration(id) != nil ||
		Root(c.capturedRoot(record.Parent)).InitialIndependent
}

func (c *admissionClosure) occurrenceMount(id manifest.NamedResource, root string) *store.SourceArtifact {
	facts, _ := c.occurrenceFacts(id)
	if facts.Declaration.Source == root {
		return facts.Mounted
	}
	return nil
}

func (c *admissionClosure) sourceView(source manifest.NamedResource, _ string) SourceFacts {
	return c.boundSource(source)
}

//nolint:gocritic // The iterator MUST capture an immutable occurrence snapshot.
func (o OccurrenceFacts) references() iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		for i := range o.Refs {
			ref := o.Refs[i]
			if ref.NamespaceDefault {
				ref.Namespace = o.Identity.Namespace
			}
			if !yield(ref) {
				return
			}
		}
	}
}

func opaqueOccurrence(facts *OccurrenceFacts, targets []manifest.ProducerTarget) bool {
	return facts != nil && facts.Policy != nil && facts.Structural == nil && !facts.HasPath &&
		len(facts.AuthoredTargets) == 0 &&
		len(targets) == 0 &&
		len(facts.TemplateInputs) == 0 &&
		len(facts.TemplateObjects) == 0
}

func containsPath(root, target string) bool {
	relative, err := filepath.Rel(root, target)
	return err == nil && filepath.IsLocal(relative)
}

func escapedPath(relative string) bool { return !filepath.IsLocal(relative) }

const sourceReferenceUse = "source"
