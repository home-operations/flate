package store

import (
	"encoding/json"
	"iter"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

// Child retains an admitted object and its authoritative sanitized document.
type Child struct {
	Obj         manifest.BaseManifest
	Doc         map[string]any
	Generated   bool
	Productions []manifest.Production
	payload     *SourceArtifact
	output      *ReadSet
}

// Emission is an immutable assessed inventory. Maps and objects are borrowed read-only.
type Emission struct {
	// Reads records the accepted assessment's consumed inputs.
	Reads *ReadSet
	// OutputReads retains provenance for attempted output or the last complete inventory.
	OutputReads  *ReadSet
	payloadReads *ReadSet
	borrowed     map[*ReadSet]struct{}

	family      *manifest.DiscoveryIndex
	Children    []Child
	Manifests   []manifest.DocOccurrence
	Failures    []manifest.DocOccurrence
	Fingerprint string
	Unreadable  []manifest.NamedResource
	Complete    bool
	// Borrowed inventory MUST remain immutable during publication preparation.
	entriesOwned bool
	accepted     bool
	origin       manifest.NamedResource
	// Render retains immutable pre-admission renderer metadata and output.
	Render *manifest.RenderReceipt

	input     manifest.BaseManifest
	inputDoc  map[string]any
	inputHead inputStamp
	revision  uint64
	// Copies share atomic storage; each prepared proposal owns a fresh memo.
	validated      *atomic.Pointer[manifest.BaseManifest]
	documentOwners []manifest.NamedResource
	previous       *Emission
	cached         *RenderedOutput
	entries        []admission
	coverage       []footprint
	binding        sourceBinding
	shadowed       []sourceBinding
}

// RenderedOutput retains keyed render content without assessment registrations.
// Its objects, documents and warning details MUST remain immutable.
type RenderedOutput struct {
	// Render retains immutable pre-admission renderer metadata and output.
	Render      *manifest.RenderReceipt
	Children    []Child
	Manifests   []manifest.DocOccurrence
	Fingerprint string
	Unreadable  []manifest.NamedResource
	warnings    []manifest.Warning
}

func (*Emission) artifact() {}

// Retains reports whether an accepted assessment or borrowed descriptor still owns reads.
func (e *Emission) Retains(reads *ReadSet) bool {
	_, borrowed := e.borrowed[reads]
	return e.Reads == reads || e.OutputReads == reads || e.payloadReads == reads || borrowed
}

// Replay returns completed content matching the caller's canonical input key.
// Stale accepted inventory remains unavailable through GetEmission.
func (e *Emission) Replay(fingerprint string) (RenderedOutput, bool) {
	if e == nil || fingerprint == "" || len(e.Failures) != 0 {
		return RenderedOutput{}, false
	}
	if e.Complete && e.Fingerprint == fingerprint {
		return e.renderedOutput(), true
	}
	if e.cached != nil && e.cached.Fingerprint == fingerprint {
		return *e.cached, true
	}
	return e.previous.Replay(fingerprint)
}

func (e *Emission) renderedOutput() RenderedOutput {
	var warnings []manifest.Warning
	if e.OutputReads != nil {
		warnings = e.OutputReads.warnings
	}
	return RenderedOutput{
		Children: e.Children, Manifests: e.Manifests, Fingerprint: e.Fingerprint,
		Unreadable: e.Unreadable, Render: e.Render, warnings: warnings,
	}
}

// Footprint projects the exact complete inventory accepted by this invocation.
// A nil iterator denotes an unaccepted or incomplete attempt.
func (e *Emission) Footprint() iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] {
	if e == nil || !e.accepted || !e.Complete {
		return nil
	}
	return childFootprint(e.entries, e.origin)
}

func childFootprint(
	entries []admission,
	origin manifest.NamedResource,
) iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] {
	return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		records := make(map[manifest.NamedResource]manifest.DiscoveryRecord, len(entries))
		{
			entryValues := entries
			for entryIndex := range entryValues {
				entry := entryValues[entryIndex]
				id := entry.child.Obj.Named()
				record := records[id]
				if applier, ok := entry.child.Obj.(interface {
					EntryApplication() (manifest.EntryApplication, bool)
				}); ok {
					if _, valid := applier.EntryApplication(); valid && entry.child.Doc != nil {
						if data, err := json.Marshal(entry.child.Doc); err == nil {
							record.Application = string(data)
						}
					}
				}
				if !slices.ContainsFunc(
					entry.child.Productions,
					func(p manifest.Production) bool { return p.Owner == origin },
				) {
					record.Productions = append(
						record.Productions,
						manifest.Production{Owner: origin, Source: entry.binding.source},
					)
				}
				record.Productions = append(record.Productions, entry.child.Productions...)
				source := manifest.BootstrapSourceID
				for ref := range manifest.References(entry.child.Obj, manifest.ReferenceRelations) {
					if ref.Name != "" {
						record.Refs = append(record.Refs, ref.NamedResource)
						if ref.Use == "source" {
							source = ref.NamedResource
						}
					}
				}
				for root := range manifest.RenderRoots(entry.child.Obj) {
					if root == "" {
						root = "."
					}
					if record.ContentRoots == nil {
						record.ContentRoots = map[manifest.NamedResource][]string{}
					}
					record.ContentRoots[source] = append(record.ContentRoots[source], root)
				}
				var authored manifest.Production
				{
					pValues := entry.child.Productions
					for pIndex := range pValues {
						p := pValues[pIndex]
						if p.Path != "" && p.Source == entry.binding.source &&
							manifest.Resolve(p.Authored, id.Namespace, "") == id &&
							(authored.Path == "" || compareProduction(p, authored) < 0) {
							authored = p
						}
					}
				}
				record.Authored, record.Path, record.File = authored.Authored, authored.Path, authored.File
				records[id] = record
				for _, target := range entry.targets {
					record := records[target.NamedResource]
					record.Productions = append(
						record.Productions,
						manifest.Production{Owner: origin, Declaration: id, Source: entry.binding.source},
					)
					records[target.NamedResource] = record
				}
			}
		}
		for _, id := range slices.SortedFunc(maps.Keys(records), manifest.NamedResource.Compare) {
			record := records[id]
			slices.SortFunc(record.Productions, compareProduction)
			record.Productions = slices.Compact(record.Productions)
			slices.SortFunc(record.Refs, manifest.NamedResource.Compare)
			record.Refs = slices.Compact(record.Refs)
			for source, roots := range record.ContentRoots {
				slices.Sort(roots)
				record.ContentRoots[source] = slices.Compact(roots)
			}
			if !yield(id, record) {
				return
			}
		}
	}
}

// ChildFootprint projects a complete parsed batch using the supplied discovery evidence.
// Objects, documents and index records are borrowed and MUST remain immutable.
func (s *Store) ChildFootprint(
	parent manifest.NamedResource,
	children []Child,
	index *manifest.DiscoveryIndex,
) iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] {
	_, _, assessment := s.GetEmission(parent)
	defer assessment.Finish()
	assessment.Children, assessment.family, assessment.origin = children, index, parent
	entries := s.prepareAdmissions(
		parent,
		&assessment,
		assessment.previous,
		make(map[manifest.NamedResource]int, len(children)),
	)
	return childFootprint(entries, parent)
}

// Finish releases predecessor inventory borrowed by an attempt.
func (e *Emission) Finish() { e.previous = nil }

func (e *Emission) shadows(binding sourceBinding) bool {
	return e != nil && slices.ContainsFunc(e.shadowed, func(other sourceBinding) bool { return binding.equal(&other) })
}

type contributor struct {
	slot    *producerSlot
	records []footprint
	binding sourceBinding
}

type footprint struct {
	id          manifest.NamedResource
	record      manifest.DiscoveryRecord
	independent bool
}
type sourceBinding struct {
	source   manifest.NamedResource
	artifact *SourceArtifact
	mutable  *SourceArtifact
}

func (b *sourceBinding) equal(other *sourceBinding) bool {
	if b == other {
		return true
	}
	if b == nil {
		b = &sourceBinding{}
	}
	if other == nil {
		other = &sourceBinding{}
	}
	return b.source == other.source && b.mutable == other.mutable &&
		(b.artifact == other.artifact || b.artifact.Input(ArtifactContent) == other.artifact.Input(ArtifactContent))
}

type admission struct {
	independent      bool
	changed          bool
	objectChanged    bool
	objectHead       *registrationHead
	objectGeneration uint64
	input            *ArtifactInput
	child            Child
	targets          []manifest.ProducerTarget
	binding          sourceBinding
}

type inputStamp struct {
	owner                *producerSlot
	generation, revision uint64
	binding              sourceBinding
}

type producerSlot struct {
	authority

	id      manifest.NamedResource
	current atomic.Pointer[Emission]
	visible atomic.Bool
	hidden  atomic.Bool
}
type membership struct {
	slot        *producerSlot
	position    int
	declaration bool
}
