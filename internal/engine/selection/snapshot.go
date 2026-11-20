package selection

import (
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type projectionFacts interface {
	CatalogObject(manifest.NamedResource) manifest.BaseManifest
	RootEvidence(manifest.NamedResource, *store.ReadSet) RootEvidence
	SourceExternal(manifest.NamedResource, *store.ReadSet) bool
}

// EntryIdentity MUST retain both the physical producer and its comparison key.
type EntryIdentity struct {
	Producer, Key, Source manifest.NamedResource
	Descendant            string
	Documents             map[manifest.NamedResource]struct{}
}

// DocumentFact MUST preserve the accepted child identity for producer conflicts.
type DocumentFact struct {
	Document         map[string]any
	Identity         manifest.NamedResource
	Child            manifest.NamedResource
	GeneratedOverlap bool
}

// EmissionFacts MUST retain actual emitter and reporting parent separately.
type EmissionFacts struct {
	Emitter   manifest.BaseManifest
	Parent    manifest.BaseManifest
	Documents []DocumentFact
}

// Snapshot borrows completed storage. Its owners MUST remain immutable through projection.
type Snapshot struct {
	Store            *store.Store
	Index            *manifest.DiscoveryIndex
	Facts            projectionFacts
	Decision         *Decision
	Entry            EntryIdentity
	Emissions        []EmissionFacts
	Manifests        []manifest.DocumentBatch
	owners           batchOwners
	Failed           map[manifest.NamedResource]store.StatusInfo
	Blocked          map[manifest.NamedResource][]manifest.NamedResource
	Warnings         []manifest.Warning
	ClusterWarnings  []manifest.Warning
	ChangeNamespaces []string
	Err              error
	ErrorIdentity    error
	Complete         bool
	HasResult        bool
	Absent           bool
}

func (d *Decision) restricted() bool { return d != nil && !d.unrestricted }

// CollectEmissions applies committed membership without mutating the captured emission facts.
func (s *Snapshot) CollectEmissions(other *Snapshot) {
	s.collectEmissions(other)
	s.Manifests = collapseOverlaps(s.Manifests, s.owners)
}

func (s *Snapshot) collectEmissions(other *Snapshot) {
	if s.Emissions == nil {
		return
	}
	s.owners = nil
	s.Manifests = make([]manifest.DocumentBatch, 0, len(s.Emissions))
	positions := make(map[manifest.NamedResource]int, len(s.Emissions))
	scope := s.entryScope()
	for _, emission := range s.Emissions {
		id := emission.Emitter.Named()
		var documentScope bool
		if manifest.KindOf(emission.Emitter).Parent {
			documentScope = emission.Emitter.EmissionPolicy().DocumentScope
		}
		output := s.outputsProducer(id, other)
		for i := range emission.Documents {
			document := &emission.Documents[i]
			if scope != nil && !s.entryIncludesDocument(scope, id, document) {
				continue
			}
			if !documentScope && !output && !s.Decision.Reconcile(document.Identity) {
				continue
			}
			parent := emission.Parent.Named()
			position, found := positions[parent]
			if !found {
				position = len(s.Manifests)
				positions[parent] = position
				s.Manifests = append(s.Manifests, manifest.DocumentBatch{Parent: emission.Parent})
			}
			s.owners.record(parent, id, len(s.Manifests[position].Documents), document)
			s.Manifests[position].Documents = append(s.Manifests[position].Documents, document.Document)
			s.Manifests[position].Identities = append(s.Manifests[position].Identities, document.Identity)
		}
	}
}

func (s *Snapshot) entryScope() map[manifest.NamedResource]bool {
	if s.Entry.Descendant == "" {
		return nil
	}
	scope := map[manifest.NamedResource]bool{s.Entry.Producer: true}
	frontier := s.entryDocumentChildren(scope)
	for id, record := range s.Index.Files() { //nolint:gocritic // The index yields immutable record snapshots.
		inEntry := slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
			entrySource := p.Source == s.Entry.Source || p.Source == manifest.BootstrapSourceID
			return entrySource && p.File != "" && containsPath(s.Entry.Descendant, filepath.FromSlash(p.File))
		})
		if inEntry && !scope[id] {
			scope[id] = true
			frontier = append(frontier, id)
		}
	}
	children := s.childrenByParent()
	for i := 0; i < len(frontier); i++ {
		for _, id := range children[frontier[i]] {
			if id != s.Entry.Producer && !scope[id] {
				scope[id] = true
				frontier = append(frontier, id)
			}
		}
	}
	return scope
}

func (s *Snapshot) entryIncludesDocument(
	scope map[manifest.NamedResource]bool,
	emitter manifest.NamedResource,
	doc *DocumentFact,
) bool {
	if emitter == s.Entry.Producer {
		_, inEntry := s.Entry.Documents[doc.Identity]
		return inEntry
	}
	return scope[emitter] || scope[doc.Identity]
}

func (s *Snapshot) entryDocumentChildren(scope map[manifest.NamedResource]bool) []manifest.NamedResource {
	i := slices.IndexFunc(s.Emissions, func(e EmissionFacts) bool { return e.Emitter.Named() == s.Entry.Producer })
	if i < 0 {
		return nil
	}
	var children []manifest.NamedResource
	docs := s.Emissions[i].Documents
	for j := range docs {
		doc := &docs[j]
		child := doc.Child
		_, inEntry := s.Entry.Documents[doc.Identity]
		if inEntry && child != (manifest.NamedResource{}) && !scope[child] &&
			s.Store.EmissionOwner(child) == s.Entry.Producer {
			scope[child] = true
			children = append(children, child)
		}
	}
	return children
}

// Both sides MUST project completed output when either side selects the same producer.
func (s *Snapshot) outputsProducer(id manifest.NamedResource, other *Snapshot) bool {
	if s.Decision.Output(id) {
		return true
	}
	return other != nil && !other.Absent && s.Decision.Reconcile(id) && other.Decision.Output(id)
}

func prepareProjectionSides(runs []Snapshot, request Request) []selectionSide {
	sides := make([]selectionSide, len(runs))
	for i := range runs {
		run := &runs[i]
		sides[i].run, sides[i].request = *run, request
		if run.Facts != nil && run.Store != nil {
			sides[i].reads.Init(run.Store, manifest.BootstrapSourceID, nil)
		}
		sides[i].prepareNamespaces(request.Roots[0].Namespace, run)
	}
	if len(runs) == comparisonSides {
		sides[0].run.collectEmissions(&runs[1])
		sides[1].run.collectEmissions(&runs[0])
	} else {
		sides[0].run.collectEmissions(nil)
	}
	return sides
}

func (s *Snapshot) childrenByParent() map[manifest.NamedResource][]manifest.NamedResource {
	children := map[manifest.NamedResource][]manifest.NamedResource{}
	for _, obj := range s.Store.ListObjects("", "") {
		for parent := range s.Store.RenderedParents(obj.Named(), s.Index) {
			children[parent] = append(children[parent], obj.Named())
		}
	}
	if s.Index != nil {
		for child := range s.Index.Records {
			record := s.Index.Records[child]
			if record.Parent != (manifest.NamedResource{}) {
				children[record.Parent] = append(children[record.Parent], child)
			}
		}
	}
	for parent, ids := range children {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
		children[parent] = slices.Compact(ids)
	}
	return children
}

func (s *selectionSide) catalogObject(id manifest.NamedResource) manifest.BaseManifest {
	var object manifest.BaseManifest
	if s.run.Facts != nil {
		object = s.reads.GetDeclaration(id)
	} else {
		object = s.run.Store.GetDeclaration(id)
	}
	if object != nil {
		return manifest.CatalogObject(object, id)
	}
	if s.run.Facts != nil {
		return s.run.Facts.CatalogObject(id)
	}
	record, _ := s.run.Index.Lookup(id)
	return record.Declaration
}

func (s *selectionSide) rootEvidence(id manifest.NamedResource, _ *manifest.DiscoveryRecord) RootEvidence {
	if s.run.Facts != nil {
		return CaptureRoot(s.run.Facts.RootEvidence(id, &s.reads), &s.reads, s.run.Index)
	}
	return RootEvidence{}
}

func (s *selectionSide) diagnostic(id manifest.NamedResource) manifest.DiagnosticIdentity {
	obj := s.run.Store.GetObject(id)
	if obj == nil {
		record, _ := s.run.Index.Lookup(id)
		obj = s.unchangedDeclaration(id, &record)
	}
	return manifest.DiagnosticFor(id, obj)
}

// ProjectIndex prunes excluded reader edges without mutating the borrowed index.
func ProjectIndex(index *manifest.DiscoveryIndex, decision *Decision) *manifest.DiscoveryIndex {
	if index == nil || decision == nil {
		return index
	}
	var projected *manifest.DiscoveryIndex
	for file, readers := range index.Readers {
		if !slices.ContainsFunc(readers, func(reader manifest.FileReader) bool {
			return !decision.Reconcile(reader.Reader)
		}) {
			continue
		}
		if projected == nil {
			copyIndex := *index
			copyIndex.Readers = maps.Clone(index.Readers)
			projected = &copyIndex
		}
		readers = slices.DeleteFunc(slices.Clone(readers), func(reader manifest.FileReader) bool {
			return !decision.Reconcile(reader.Reader)
		})
		if len(readers) == 0 {
			delete(projected.Readers, file)
		} else {
			projected.Readers[file] = readers
		}
	}
	if projected == nil {
		return index
	}
	return projected
}
