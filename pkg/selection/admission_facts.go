package selection

import (
	"iter"
	"path/filepath"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (c *admissionClosure) catalogOccurrence(id manifest.NamedResource) (OccurrenceFacts, bool) {
	facts, known := c.facts.Occurrence(id)
	return facts, known && facts.CatalogKnown
}

func (c *admissionClosure) occurrence(id manifest.NamedResource) (OccurrenceFacts, bool) {
	facts, known := c.catalogOccurrence(id)
	return facts, known && !facts.RequiresBuild && facts.Identity.Name == facts.Authored.Name
}

//nolint:ireturn // Immutable catalog witnesses MUST retain their concrete manifest type.
func (c *admissionClosure) catalogWitness(id manifest.NamedResource) manifest.BaseManifest {
	facts, _ := c.facts.Occurrence(id)
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
	object := c.catalogWitness(id)
	if object == nil {
		return nil
	}
	return manifest.CatalogObject(object, id)
}

func (c *admissionClosure) candidate(id manifest.NamedResource) store.Child {
	facts, _ := c.facts.Occurrence(id)
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
	facts := c.facts.RootEvidence(id, c.proposal.reads)
	if facts.BuiltChild.Obj == record.Declaration {
		return facts.BuiltChild
	}
	return store.Child{}
}

func (c *admissionClosure) registeredRoot(record *manifest.DiscoveryRecord) string {
	for id := range c.facts.SelectionIndex().Records {
		candidate := c.facts.SelectionIndex().Records[id]
		if candidate.BuilderOwner == record.BuilderOwner && candidate.Builder == record.Builder &&
			candidate.Declaration == record.Declaration {
			return c.facts.RootEvidence(id, c.proposal.reads).PreparedRoot
		}
	}
	return ""
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
		Root(c.facts.RootEvidence(record.Parent, c.proposal.reads)).InitialIndependent
}

func (c *admissionClosure) occurrenceMount(id manifest.NamedResource, root string) *store.SourceArtifact {
	facts, _ := c.facts.Occurrence(id)
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
