package selection

import (
	"cmp"
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Frozen discovery MUST keep the shared catalog witnesses immutable through the session.
type admissionCatalog struct {
	entries       map[manifest.NamedResource]*admissionCatalogEntry
	materializers []manifest.NamedResource
	candidates    []*OccurrenceFacts
	files         []manifest.NamedResource
}

type admissionCatalogEntry struct {
	occurrence     OccurrenceFacts
	contentRoots   map[manifest.NamedResource][]string
	object         manifest.BaseManifest
	known          bool
	nativeExcluded bool
}

func (s *Session) catalogSnapshot(roots []NativeRootFacts) *admissionCatalog {
	if snapshot := s.catalog.Load(); snapshot != nil {
		return snapshot
	}
	nativeDisposition := make(map[manifest.NamedResource]bool, len(roots))
	for i := range roots {
		root := &roots[i]
		id := manifest.NewEntryKustomization(manifest.BootstrapSourceID, root.Path, manifest.NamedResource{}).Named()
		nativeDisposition[id] = !Root(RootEvidence{Native: root}).Construction
	}
	snapshot := &admissionCatalog{
		entries: make(map[manifest.NamedResource]*admissionCatalogEntry),
	}
	for id := range s.facts.SelectionIndex().Records {
		record := s.facts.SelectionIndex().Records[id]
		if record.File != "" && record.Path != "" && record.BuilderOwner == (manifest.NamedResource{}) {
			snapshot.files = append(snapshot.files, id)
		}
		excluded, native := nativeDisposition[id]
		entry, materializer := s.catalogEntry(id, &record, native, excluded)
		if entry == nil {
			continue
		}
		snapshot.entries[id] = entry
		if !materializer {
			continue
		}
		snapshot.materializers = append(snapshot.materializers, id)
		pathSupported := !entry.occurrence.HasPath || entry.occurrence.Policy == nil || len(entry.occurrence.Roots) != 0
		if entry.known && entry.occurrence.CatalogKnown && pathSupported {
			snapshot.candidates = append(snapshot.candidates, &snapshot.entries[id].occurrence)
		}
	}
	slices.SortFunc(snapshot.materializers, manifest.NamedResource.Compare)
	slices.SortFunc(snapshot.candidates, func(a, b *OccurrenceFacts) int { return a.Authored.Compare(b.Authored) })
	snapshot.candidates = slices.Clip(snapshot.candidates)
	slices.SortFunc(snapshot.files, func(a, b manifest.NamedResource) int {
		return cmp.Or(
			strings.Compare(s.facts.SelectionIndex().Records[a].Path, s.facts.SelectionIndex().Records[b].Path),
			a.Compare(b),
		)
	})
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
		record, _ := c.facts.SelectionIndex().Lookup(id)
		object = record.Declaration
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

func (c *admissionClosure) preparedObject(id manifest.NamedResource) store.Child {
	record, _ := c.facts.SelectionIndex().Lookup(id)
	if record.Builder == "" {
		return store.Child{}
	}
	child := c.facts.AdmissionCandidate(id)
	if child.Obj == record.Declaration {
		return child
	}
	child, _ = c.facts.PreparedObject(id, c.proposal.reads)
	if child.Obj == record.Declaration {
		return child
	}
	return store.Child{}
}

func (c *admissionClosure) capturedRoot(id manifest.NamedResource) *RootEvidence {
	if facts, known := c.capturedRoots[id]; known {
		return facts
	}
	facts := c.facts.RootEvidence(id, c.proposal.reads, false)
	if c.capturedRoots == nil {
		c.capturedRoots = make(map[manifest.NamedResource]*RootEvidence)
	}
	c.capturedRoots[id] = &facts
	return &facts
}

func (c *admissionClosure) isFileIndexed(id manifest.NamedResource) bool {
	if c.facts.SelectionIndex().IsFileIndexed(id) {
		return true
	}
	record, _ := c.facts.SelectionIndex().Lookup(id)
	if record.Builder != "" && c.facts.AdmissionCandidate(id).Obj != nil {
		return true
	}
	if occurrence, ok := c.occurrence(id); ok && occurrence.Authored.Namespace == "" && id.Namespace != "" {
		id.Namespace = ""
		return c.facts.SelectionIndex().IsFileIndexed(id)
	}
	return false
}

func (c *admissionClosure) fileCoordinate(file string) string {
	if relative, err := filepath.Rel(c.context.Root, file); err == nil && filepath.IsLocal(relative) {
		return filepath.ToSlash(relative)
	}
	return filepath.ToSlash(file)
}

func (c *admissionClosure) declarationLocated(id manifest.NamedResource, objects admissionReader) bool {
	record, _ := c.facts.SelectionIndex().Lookup(id)
	return record.Path != "" || record.Builder != "" || objects.GetDeclaration(id) != nil ||
		Root(*c.capturedRoot(record.Parent)).InitialIndependent
}

func (c *admissionClosure) occurrenceMount(id manifest.NamedResource, root string) *store.SourceArtifact {
	facts, _ := c.occurrenceFacts(id)
	if facts.Declaration.Source == root {
		return facts.Mounted
	}
	return nil
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

func (s *Session) catalogEntry(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	native, excluded bool,
) (*admissionCatalogEntry, bool) {
	occurrence, known := s.facts.Occurrence(id)
	candidate := s.facts.AdmissionCandidate(id)
	catalogMaterializer := occurrence.CatalogKnown &&
		(occurrence.Policy != nil || len(occurrence.Targets) != 0 || occurrence.NeedsCandidates)
	preparedMaterializer := candidate.Obj != nil && record.Builder != "" && isMaterializerObject(candidate.Obj)
	materializer := catalogMaterializer || preparedMaterializer
	if !materializer && !native && len(occurrence.Roots) == 0 {
		return nil, false
	}
	if record.ContentRoots == nil && len(occurrence.Roots) != 0 {
		source, _, _ := occurrence.SourcePath()
		if source.Name == "" || source.Kind == "" {
			source = manifest.BootstrapSourceID
		}
		record.ContentRoots = map[manifest.NamedResource][]string{source: occurrence.Roots}
	}
	object := occurrence.Object
	if object == nil {
		object = occurrence.Catalog
	}
	if object == nil {
		object = record.Declaration
	}
	if object != nil {
		object = manifest.CatalogObject(object, id)
	}
	return &admissionCatalogEntry{
		occurrence:     occurrence,
		contentRoots:   record.ContentRoots,
		object:         object,
		known:          known,
		nativeExcluded: excluded,
	}, materializer
}
