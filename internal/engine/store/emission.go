package store

import (
	"encoding/json"
	"iter"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

// ChildDocuments retains admission failures beside successfully parsed documents.
type ChildDocuments struct {
	Children []Child
	Visible  []manifest.DocOccurrence
	Failures []manifest.DocOccurrence
}

// Child retains an admitted object and its authoritative sanitized document.
type Child struct {
	Entry             *manifest.EntryApplication
	SourceSupport     manifest.NamedResource
	Obj               manifest.BaseManifest
	Doc               map[string]any
	Generated         bool
	VisibleGeneration bool
	policy            DeclarationPolicy
	Productions       []manifest.Production
	Warnings          []manifest.Warning `json:"-"`
	Ordinal           int                `json:"-"`
	payload           *SourceArtifact
	output            *ReadSet
}

// Emission is an immutable assessed inventory. Maps and objects are borrowed read-only.
type Emission struct {
	// Reads records the accepted assessment's consumed inputs.
	Reads *ReadSet
	// OutputReads retains provenance for attempted output or the last complete inventory.
	OutputReads  *ReadSet
	payloadReads *ReadSet
	borrowed     map[*ReadSet]struct{}

	family       *manifest.DiscoveryIndex
	Children     []Child
	Manifests    []manifest.DocOccurrence
	Failures     []manifest.DocOccurrence
	Fingerprint  string
	Unreadable   []manifest.NamedResource
	Complete     bool
	Construction bool
	// Borrowed inventory MUST remain immutable during publication preparation.
	entriesOwned bool
	accepted     bool
	origin       manifest.NamedResource
	// Render retains immutable pre-admission renderer metadata and output.
	Render *manifest.RenderReceipt

	input         manifest.BaseManifest
	inputDoc      map[string]any
	inputHead     inputStamp
	syncOwnerKeys [2]string
	revision      uint64
	// Copies share atomic storage; each prepared proposal owns a fresh memo.
	validated      *atomic.Pointer[manifest.BaseManifest]
	documentOwners []manifest.NamedResource
	previous       *Emission
	renderCache    *atomic.Pointer[RenderedOutput]
	sources        []manifest.SourceEvidence
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

// DocumentChild reads the accepted binding at the same position as Manifests.
func (e *Emission) DocumentChild(i int) manifest.NamedResource {
	return e.documentOwners[i]
}

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
	if e.renderCache != nil {
		if output := e.renderCache.Load(); output != nil && output.Fingerprint == fingerprint {
			return *output, true
		}
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
				addChildFootprint(records, origin, &entry)
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

// Finish retains completed renderer bytes across rejected admission without retaining its captures.
func (e *Emission) Finish() {
	e.retainRender()
	e.previous = nil
}

func (e *Emission) acceptRender(previous *Emission) {
	if e.renderCache == nil {
		return
	}
	if e.Complete || len(e.Failures) != 0 {
		e.renderCache.Store(nil)
		return
	}
	if previous != nil && previous.Complete && previous.Fingerprint != "" {
		output := previous.renderedOutput()
		e.renderCache.Store(&output)
	}
}

func (e *Emission) retainRender() {
	if e.accepted || e.renderCache == nil || !e.Complete || e.Fingerprint == "" || len(e.Failures) != 0 {
		return
	}
	if current := e.renderCache.Load(); current != nil && current.Fingerprint == e.Fingerprint {
		return
	}
	output := e.renderedOutput()
	if e.Reads != nil {
		output.warnings = slices.Clone(e.Reads.warnings)
	}
	e.renderCache.Store(&output)
}

func (e *Emission) shadows(bindingValue *sourceBinding) bool {
	return e != nil && bindingValue != nil && slices.ContainsFunc(e.shadowed, func(other sourceBinding) bool {
		return bindingValue.equal(&other)
	})
}

type contributor struct {
	slot            *producerSlot
	records         []footprint
	bindings        map[manifest.NamedResource]*sourceBinding
	declaredOutputs bool
}

func (c *contributor) binding(source manifest.NamedResource) *sourceBinding {
	if c == nil {
		return nil
	}
	return c.bindings[source]
}

func (c *contributor) unambiguousSource() manifest.NamedResource {
	if c == nil {
		return manifest.NamedResource{}
	}
	var binding *sourceBinding
	for _, current := range c.bindings {
		if binding != nil && !binding.equal(current) {
			return manifest.NamedResource{}
		}
		binding = current
	}
	return binding.source
}

// An explicit output declaration retains precedence over an independently authored file.
func productionRetiresSource(p *manifest.Production, sourceSupport bool) bool {
	return !sourceSupport || p.Declaration != (manifest.NamedResource{})
}

type indexedSourceSupport struct {
	provisional, retained bool
}

func bareDiscoveryRecord(index *manifest.DiscoveryIndex, id manifest.NamedResource) manifest.DiscoveryRecord {
	if index == nil || id.Namespace == "" {
		return manifest.DiscoveryRecord{}
	}
	id.Namespace = ""
	return index.Records[id]
}

func rawSelf(record *manifest.DiscoveryRecord, id manifest.NamedResource) bool {
	return record.BuilderOwner == (manifest.NamedResource{}) || record.BuilderOwner == id ||
		record.Authored == id && record.File != "" && record.Builder != "" && record.File != record.Builder
}

func rawProductionAuthority(p *manifest.Production, id, source manifest.NamedResource, self bool) bool {
	sourceEligible := source == (manifest.NamedResource{}) || p.Source == source && !provisionalBootstrap(p)
	ownerEligible := p.Owner == (manifest.NamedResource{}) || self && p.Owner == id
	return sourceEligible && ownerEligible
}

func (s *Store) classifySourceSupport(
	id, source manifest.NamedResource,
	record, bare *manifest.DiscoveryRecord,
) indexedSourceSupport {
	provisional := bootstrapSeed(record.Productions)
	retained := source != (manifest.NamedResource{}) && rawDeclarationAuthority(record, id, source, false)
	if len(bare.Productions) != 0 && (provisional || !retained && source != (manifest.NamedResource{})) {
		key := id
		key.Namespace = ""
		self := rawSelf(bare, key)
		s.visitBareProductions(
			id,
			[2][]manifest.Production{record.Productions, bare.Productions},
			false,
			func(p *manifest.Production) bool {
				provisional = provisional && p.Owner != (manifest.NamedResource{})
				retained = retained ||
					source != (manifest.NamedResource{}) && rawProductionAuthority(p, key, source, self)
				return provisional || !retained && source != (manifest.NamedResource{})
			},
		)
	}
	return indexedSourceSupport{provisional: provisional, retained: !provisional && retained}
}

// Bootstrap promotion alone supplies no independent authority against accepted output.
func bootstrapSeed(productions []manifest.Production) bool {
	if slices.ContainsFunc(productions, func(p manifest.Production) bool { return provisionalBootstrap(&p) }) {
		return !slices.ContainsFunc(productions, func(p manifest.Production) bool {
			return p.Owner == (manifest.NamedResource{}) &&
				(p.Source != manifest.BootstrapSourceID || p.Path != "" || p.File != "")
		})
	}
	return len(productions) > 0 && !slices.ContainsFunc(productions, func(p manifest.Production) bool {
		return p.Owner != (manifest.NamedResource{}) || p.Declaration != (manifest.NamedResource{}) ||
			p.Source != manifest.BootstrapSourceID
	})
}

func provisionalBootstrap(p *manifest.Production) bool {
	return p.Owner == (manifest.NamedResource{}) && p.Declaration == (manifest.NamedResource{}) &&
		p.Source == manifest.BootstrapSourceID && p.Path == "" && p.File == ""
}

type coverageState uint8

const (
	coverageNone coverageState = iota
	coverageShadowed
	coverageRetired
)

func (e *Emission) productionCoverage(
	p *manifest.Production,
	binding *sourceBinding,
	source manifest.NamedResource,
	sourceSupport bool,
) coverageState {
	if !e.shadows(binding) {
		return coverageNone
	}
	if source == (manifest.NamedResource{}) || binding.source == source && productionRetiresSource(p, sourceSupport) {
		return coverageRetired
	}
	return coverageShadowed
}

func (s *Store) discoveryBinding(
	index *manifest.DiscoveryIndex,
	owner, source manifest.NamedResource,
) sourceBinding {
	if source != (manifest.NamedResource{}) {
		return bindSourceArtifact(source, s.GetArtifact)
	}
	obj := index.Records[owner].Declaration
	if obj == nil {
		obj = s.GetDeclaration(owner)
	}
	return sourcePathBinding(obj, s.GetArtifact)
}

type footprint struct {
	id                      manifest.NamedResource
	record                  manifest.DiscoveryRecord
	independent, classified bool
}

type sourceBinding struct {
	source   manifest.NamedResource
	artifact *SourceArtifact
	// Padding MUST keep AddObject's warm path within its timing budget.
	_ [8]byte
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
	return b.source == other.source && b.artifact.SameMaterial(other.artifact, ArtifactContent)
}

type admission struct {
	composed         bool
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

func addChildFootprint(
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
	origin manifest.NamedResource,
	entry *admission,
) {
	id := entry.child.Obj.Named()
	record := records[id]
	footprintApplication(&record, entry)
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
	footprintReferenceRoots(&record, entry)
	footprintAuthored(&record, entry, id)
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

func footprintApplication(record *manifest.DiscoveryRecord, entry *admission) {
	if _, valid := manifest.ApplicationFor(entry.child.Obj); valid && entry.child.Doc != nil {
		if data, err := json.Marshal(entry.child.Doc); err == nil {
			record.Application = string(data)
		}
	}
}

func footprintReferenceRoots(record *manifest.DiscoveryRecord, entry *admission) {
	source := manifest.BootstrapSourceID
	for ref := range manifest.References(entry.child.Obj, manifest.ReferenceRelations) {
		if ref.Name != "" {
			record.Refs = append(record.Refs, ref.NamedResource)
			if ref.BindsSource() {
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
}

func footprintAuthored(record *manifest.DiscoveryRecord, entry *admission, id manifest.NamedResource) {
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
}
