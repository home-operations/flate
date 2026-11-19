package selection

import (
	"maps"
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
	Producer, Key manifest.NamedResource
}

// DocumentFact MUST preserve the emitter's identity before document projection.
type DocumentFact struct {
	Document map[string]any
	Identity manifest.NamedResource
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
func (s *Snapshot) CollectEmissions() {
	if s.Emissions == nil {
		return
	}
	s.Manifests = make([]manifest.DocumentBatch, 0, len(s.Emissions))
	positions := make(map[manifest.NamedResource]int, len(s.Emissions))
	for _, emission := range s.Emissions {
		id := emission.Emitter.Named()
		var documentScope bool
		if manifest.KindOf(emission.Emitter).Parent {
			documentScope = emission.Emitter.EmissionPolicy().DocumentScope
		}
		for _, document := range emission.Documents {
			if !documentScope && !s.Decision.Output(id) && !s.Decision.Reconcile(document.Identity) {
				continue
			}
			parent := emission.Parent.Named()
			position, found := positions[parent]
			if !found {
				position = len(s.Manifests)
				positions[parent] = position
				s.Manifests = append(s.Manifests, manifest.DocumentBatch{Parent: emission.Parent})
			}
			s.Manifests[position].Documents = append(s.Manifests[position].Documents, document.Document)
			s.Manifests[position].Identities = append(s.Manifests[position].Identities, document.Identity)
		}
	}
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
