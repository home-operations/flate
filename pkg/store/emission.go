package store

import (
	"bytes"
	"cmp"
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

func childFootprint(entries []admission, origin manifest.NamedResource) iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] {
	return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		records := make(map[manifest.NamedResource]manifest.DiscoveryRecord, len(entries))
		for _, entry := range entries {
			id := entry.child.Obj.Named()
			record := records[id]
			if applier, ok := entry.child.Obj.(interface {
				EntryApplication() (manifest.EntryApplication, bool)
			}); ok {
				if _, valid := applier.EntryApplication(); valid && entry.child.Doc != nil {
					if data, err := json.Marshal(entry.child.Doc); err == nil {
						record.Application = string(data)
						if !entry.independent && origin != id {
							record.ApplicationOwner = origin
						}
					}
				}
			}
			if !slices.ContainsFunc(entry.child.Productions, func(p manifest.Production) bool { return p.Owner == origin }) {
				record.Productions = append(record.Productions, manifest.Production{Owner: origin, Source: entry.binding.source})
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
			for _, p := range entry.child.Productions {
				if p.Path != "" && p.Source == entry.binding.source && manifest.Resolve(p.Authored, id.Namespace, "") == id && (authored.Path == "" || compareProduction(p, authored) < 0) {
					authored = p
				}
			}
			record.Authored, record.Path, record.File = authored.Authored, authored.Path, authored.File
			records[id] = record
			for _, target := range entry.targets {
				record := records[target.NamedResource]
				record.Productions = append(record.Productions, manifest.Production{Owner: origin, Declaration: id, Source: entry.binding.source})
				records[target.NamedResource] = record
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
func (s *Store) ChildFootprint(parent manifest.NamedResource, children []Child, index *manifest.DiscoveryIndex) iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord] {
	_, _, assessment := s.GetEmission(parent)
	defer assessment.Finish()
	assessment.Children, assessment.family, assessment.origin = children, index, parent
	entries := s.prepareAdmissions(parent, &assessment, assessment.previous, make(map[manifest.NamedResource]int, len(children)))
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

// ReportHidden classifies the coherent representative's auxiliary provenance.
func (s *Store) ReportHidden(id manifest.NamedResource) bool {
	sh := s.shardFor(id)
	sh.mu.RLock()
	hidden := sh.reportHiddenLocked(id)
	sh.mu.RUnlock()
	return hidden
}

func (sh *shard) reportHiddenLocked(id manifest.NamedResource) bool {
	if id.ReportHidden() {
		return true
	}
	if !sh.cells[id].auxiliary {
		return false
	}
	cell := sh.cells[id]
	return !cell.hidden && (!cell.rooted || !cell.bound())
}

// EmissionOwner returns the generated producer supplying the representative for id.
// Static, hidden or absent representatives have an empty owner. This individual
// read does not form an atomic pair with a separate GetEmission call.
func (s *Store) EmissionOwner(id manifest.NamedResource) manifest.NamedResource {
	sh := s.shardFor(id)
	sh.mu.RLock()
	var view capture
	sh.captureLocked(id, sh.cells[id], &view)
	sh.mu.RUnlock()
	if view.owner != nil {
		return view.owner.id
	}
	return manifest.NamedResource{}
}

// GetDeclaration returns the effective input authorized by static or accepted support.
func (s *Store) GetDeclaration(id manifest.NamedResource) manifest.BaseManifest {
	sh := s.shardFor(id)
	sh.mu.RLock()
	var view capture
	sh.captureLocked(id, sh.cells[id], &view)
	sh.mu.RUnlock()
	return view.declaration()
}

func declarationIndependent(index *manifest.DiscoveryIndex, id manifest.NamedResource) bool {
	record, _ := index.Lookup(id)
	return len(record.Productions) == 0 || slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
		return p.Owner == (manifest.NamedResource{}) || p.Owner == id
	})
}

func (c capture) declaration() manifest.BaseManifest {
	if c.independent || c.accepted || c.owner != nil {
		return c.child.Obj
	}
	return nil
}

// GetEmission borrows valid accepted inventory and binds an attempt to its current input.
// The captured document remains available while declaration authority is parked.
// Render identity is available through Emission.Replay.
// OutputReads preserves validity evidence for withdrawal without exposing invalid inventory.
// Returned slices and their contents MUST be copied before mutation.
func (s *Store) GetEmission(id manifest.NamedResource) (manifest.BaseManifest, map[string]any, Emission) {
	return s.getEmission(nil, id)
}

func (s *Store) getEmission(reads *ReadSet, id manifest.NamedResource) (manifest.BaseManifest, map[string]any, Emission) {
	var obj manifest.BaseManifest
	var view capture
	if reads != nil {
		obj = reads.GetDeclaration(id)
		view = reads.input(id).view
	}
	sh := s.shardFor(id)
	sh.mu.RLock()
	if reads == nil {
		sh.captureLocked(id, sh.cells[id], &view)
	}
	var prev *Emission
	if slot := sh.slots[id]; slot != nil && slot.visible.Load() {
		prev = slot.current.Load()
	}
	sh.mu.RUnlock()
	if reads == nil {
		obj = view.declaration()
	}
	e := Emission{Reads: reads, input: obj, inputDoc: view.child.Doc, previous: prev,
		inputHead: inputStamp{owner: view.owner, generation: view.generation, revision: view.revision, binding: view.sourceBinding()}}
	if prev != nil {
		e.OutputReads = prev.OutputReads
		if prev.OutputReads.Valid() {
			e.Children = prev.Children
			e.Manifests = prev.Manifests
			e.Unreadable = prev.Unreadable
			if reads == nil || prev.Reads == reads {
				e.Failures = prev.Failures
			}
			e.Render = prev.Render
		}
	}
	if obj == nil {
		return nil, view.child.Doc, e
	}
	if c := s.coverage[id]; c != nil {
		e.coverage = c.records
	}
	getArtifact := s.GetArtifact
	if reads != nil {
		getArtifact = reads.GetArtifact
	}
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use == "source" {
			e.binding.source = ref.NamedResource
			if a, ok := getArtifact(ref.NamedResource, ArtifactContent).(*SourceArtifact); ok {
				e.binding.artifact = a
				if a.Content == "" {
					e.binding.mutable = a.mutableMaterial()
				}
			}
			break
		}
	}
	if e.binding.source.Name == "" && s.contributionBinding(id).source == manifest.BootstrapSourceID {
		e.binding.source = manifest.BootstrapSourceID
		if a, ok := getArtifact(manifest.BootstrapSourceID, ArtifactContent).(*SourceArtifact); ok {
			e.binding.artifact = a
			if a.Content == "" {
				e.binding.mutable = a.mutableMaterial()
			}
		}
	}
	return obj, view.child.Doc, e
}

// ReferenceFacts preserves visible data separately from accepted authority and relations.
type ReferenceFacts struct {
	Data          manifest.Data
	Supplied      manifest.Data
	SupplyPath    string
	Accepted      bool
	Productions   []manifest.Production
	ReferenceData []manifest.ReferenceData
}

// Productions merges discovery evidence with accepted reverse memberships into a read-only snapshot.
func (s *Store) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	var result productionResult
	s.visitProductions(id, index, &result)
	return result.out
}

// IncomingProductions projects prospective evidence without the stored target's state.
func (s *Store) IncomingProductions(child Child, index *manifest.DiscoveryIndex) []manifest.Production {
	result := productionResult{incoming: &child}
	s.visitProductions(child.Obj.Named(), index, &result)
	return result.out
}

// ReferenceFacts selects visible data and source-qualified production evidence.
func (s *Store) ReferenceFacts(id manifest.NamedResource, index *manifest.DiscoveryIndex) ReferenceFacts {
	result := productionResult{facts: true}
	s.visitProductions(id, index, &result)
	data, path := s.suppliedObject(id)
	var candidates []manifest.ReferenceData
	if len(result.out) != 0 {
		candidates = s.referenceData(id, index, result.out, nil)
	}
	return ReferenceFacts{Data: result.data, Accepted: result.accepted, Productions: result.out, ReferenceData: candidates, Supplied: data, SupplyPath: path}
}

func (s *Store) referenceData(id manifest.NamedResource, index *manifest.DiscoveryIndex, productions []manifest.Production, reads *ReadSet) []manifest.ReferenceData {
	if index == nil {
		index = s.index.Load()
	}
	var records [2][]manifest.ReferenceData
	if index != nil {
		records[0] = index.Records[id].ReferenceData
		if id.Namespace != "" {
			bare := id
			bare.Namespace = ""
			records[1] = index.Records[bare].ReferenceData
		}
	}
	var out []manifest.ReferenceData
	add := func(candidate manifest.ReferenceData) {
		p := candidate.Production
		if p.Declaration != (manifest.NamedResource{}) {
			var view capture
			if reads != nil {
				reads.GetObject(p.Declaration)
				view = reads.input(p.Declaration).view
			} else {
				sh := s.shardFor(p.Declaration)
				sh.mu.RLock()
				sh.captureLocked(p.Declaration, sh.cells[p.Declaration], &view)
				sh.mu.RUnlock()
			}
			if object := view.child.Obj; object != nil {
				candidate.DeclaredKeys = nil
				binding := view.sourceBinding()
				if (view.owner == nil || view.owner.id == p.Owner) && (p.Source == (manifest.NamedResource{}) || binding.source == (manifest.NamedResource{}) || p.Source == binding.source) {
					for _, target := range manifest.ProducerTargets(object, id) {
						if target.NamedResource == id {
							candidate.DeclaredKeys = target.DeclaredKeys
							break
						}
					}
				}
			}
		}
		if len(candidate.DeclaredKeys) != 0 || candidate.OfflineData != nil {
			if object, ok := candidate.OfflineData.(manifest.BaseManifest); ok && object.Named().Namespace != id.Namespace {
				candidate.OfflineData = object.WithNamespace(id.Namespace).(manifest.Data)
			}
			out = append(out, candidate)
		}
	}
	for _, candidates := range records {
		for _, candidate := range candidates {
			if !slices.Contains(productions, candidate.Production) {
				continue
			}

			getArtifact := s.GetArtifact
			if reads != nil {
				getArtifact = reads.GetArtifact
			}
			if c := s.coverage[candidate.Production.Owner]; c != nil && c.binding.source == candidate.Production.Source && c.binding.artifact != nil {
				artifact, _ := getArtifact(candidate.Production.Source, ArtifactContent).(*SourceArtifact)
				binding := sourceBinding{source: candidate.Production.Source, artifact: artifact}
				if artifact != nil && artifact.Content == "" {
					binding.mutable = artifact.mutableMaterial()
				}
				if !c.binding.equal(&binding) {
					candidate.DeclaredKeys, candidate.OfflineData = nil, nil
				}
			}
			add(candidate)
		}
	}
	for _, p := range productions {
		if p.Declaration == (manifest.NamedResource{}) || slices.ContainsFunc(records[0], func(c manifest.ReferenceData) bool { return c.Production == p }) || slices.ContainsFunc(records[1], func(c manifest.ReferenceData) bool { return c.Production == p }) {
			continue
		}
		add(manifest.ReferenceData{Production: p})
	}
	return out
}

// RenderedParent returns the smallest eligible accepted or discovered rendered owner.
// A nil index uses the installed discovery index.
func (s *Store) RenderedParent(id manifest.NamedResource, index *manifest.DiscoveryIndex) (manifest.NamedResource, bool) {
	result := productionResult{id: id, parentOnly: true}
	s.visitProductions(id, index, &result)
	return result.parent, result.parent != (manifest.NamedResource{})
}

type productionResult struct {
	data          manifest.Data
	out           []manifest.Production
	id            manifest.NamedResource
	parent        manifest.NamedResource
	matched       int
	facts         bool
	accepted      bool
	owned         bool
	parentOnly    bool
	reads         *ReadSet
	captureTarget bool
	incoming      *Child
	watch         bool
}

func (r *productionResult) reset(evidence []manifest.Production) {
	r.out, r.matched, r.owned, r.parent = evidence, 0, false, manifest.NamedResource{}
}

func (r *productionResult) add(p *manifest.Production) {
	if r.parentOnly {
		if p.Owner != (manifest.NamedResource{}) && p.Owner != r.id && p.Authored == (manifest.NamedResource{}) && p.Declaration == (manifest.NamedResource{}) &&
			(r.parent == (manifest.NamedResource{}) || p.Owner.Compare(r.parent) < 0) {
			r.parent = p.Owner
		}
		return
	}
	if !r.owned && r.matched < len(r.out) && (&r.out[r.matched] == p || r.out[r.matched] == *p) {
		r.matched++
		return
	}
	if !r.owned {
		if slices.Contains(r.out[:r.matched], *p) {
			return
		}
		r.out = slices.Clone(r.out[:r.matched])
		r.owned = true
	}
	r.out = append(r.out, *p)
}

// result MUST be fresh for each traversal.
func (s *Store) visitProductions(id manifest.NamedResource, index *manifest.DiscoveryIndex, result *productionResult) {
	reads := result.reads
	if reads != nil && result.captureTarget {
		reads.GetObject(id)
	}
	if index == nil {
		index = s.index.Load()
	}
	var evidence, bare []manifest.Production
	provisional := false
	if index != nil {
		record := index.Records[id]
		evidence = record.Productions
		provisional = bootstrapSeed(record)
		if id.Namespace != "" {
			key := id
			key.Namespace = ""
			bare = index.Records[key].Productions
			if reads != nil && result.captureTarget && len(bare) != 0 {
				reads.GetObject(key)
			}
		}
	}
	if result.incoming != nil {
		evidence = result.incoming.Productions
		if len(evidence) == 0 || slices.ContainsFunc(evidence, func(p manifest.Production) bool {
			return p.Path != "" && manifest.Resolve(p.Authored, id.Namespace, "") == id
		}) {
			bare = nil
		}
		if reads != nil {
			for _, productions := range [2][]manifest.Production{evidence, bare} {
				for _, p := range productions {
					if p.Source != reads.ID() {
						reads.GetArtifact(p.Source, ArtifactContent)
					}
				}
			}
		}
	}
	result.out = evidence
	sh := s.shardFor(id)
	sh.mu.RLock()
	visible := func(p *manifest.Production) {
		if c := s.coverage[p.Owner]; c != nil {
			accepted := c.slot.current.Load()
			if reads != nil {
				reads.sample(id, c.slot, accepted, result.watch)
			}
			if c.slot.hidden.Load() || accepted.shadows(c.binding) {
				return
			}
		}
		result.add(p)
	}
	for i := range evidence {
		if result.incoming == nil {
			visible(&evidence[i])
		} else {
			result.add(&evidence[i])
		}
	}
	sameFamily := func(a, b manifest.Production) bool {
		return a.Source == b.Source && (result.incoming != nil || a.Path == "" || a.File == b.File && a.Authored == b.Authored)
	}
	var first *manifest.Production
	for i, p := range bare {
		if p.Path == "" || p.Authored.Namespace != "" || p.Authored != (manifest.NamedResource{}) && manifest.Resolve(p.Authored, id.Namespace, "") != id {
			continue
		}
		matches := slices.ContainsFunc(evidence, func(e manifest.Production) bool {
			return sameFamily(e, p) && sameProductionContext(s.coverage, e, p)
		})
		if len(evidence) == 0 {
			matches = first == nil || sameFamily(*first, p) && sameProductionContext(s.coverage, *first, p)
		}
		if !matches {
			matches = slices.ContainsFunc(bare[:i], func(e manifest.Production) bool {
				return e.Path != "" && e.Authored.Namespace == "" && sameFamily(e, p) && s.coverage[e.Owner] == nil &&
					(len(evidence) == 0 && sameFamily(*first, e) || slices.ContainsFunc(evidence, func(d manifest.Production) bool {
						return sameFamily(d, e)
					}))
			})
		}
		if matches {
			if first == nil {
				first = &bare[i]
			}
			visible(&bare[i])
		}
	}
	if result.facts {
		var view capture
		sh.captureLocked(id, sh.cells[id], &view)
		result.data, _ = view.child.Obj.(manifest.Data)
		result.accepted = view.accepted || view.owner != nil
	}
	if result.incoming == nil {
		cell := sh.cells[id]
		if cell.hidden {
			result.reset(nil)
		} else {
			if cell.static != nil && cell.retired && provisional {
				result.reset(nil)
			}
			for _, m := range sh.members[id] {
				e := m.slot.current.Load()
				if reads != nil {
					reads.sample(id, m.slot, e, result.watch)
				}
				if !m.slot.visible.Load() {
					continue
				}
				entry := &e.entries[m.position]
				if m.declaration {
					p := manifest.Production{Owner: m.slot.id, Declaration: entry.child.Obj.Named(), Source: entry.binding.source}
					result.add(&p)
					continue
				}
				if !slices.ContainsFunc(entry.child.Productions, func(p manifest.Production) bool { return p.Owner == m.slot.id }) {
					p := manifest.Production{Owner: m.slot.id, Source: entry.binding.source}
					result.add(&p)
				}
				for i := range entry.child.Productions {
					result.add(&entry.child.Productions[i])
				}
			}
		}
	}
	sh.mu.RUnlock()
	if reads != nil {
		reads.activateProducers()
	}
	if !result.owned {
		result.out = result.out[:result.matched]
		for i := 1; i < len(result.out); i++ {
			if compareProduction(result.out[i-1], result.out[i]) >= 0 {
				result.out = slices.Clone(result.out)
				result.owned = true
				break
			}
		}
	}
	if result.owned {
		slices.SortFunc(result.out, compareProduction)
		result.out = slices.Compact(result.out)
	}
}

func sameProductionContext(coverage map[manifest.NamedResource]*contributor, a, b manifest.Production) bool {
	if a.Source != b.Source {
		return false
	}
	ac, bc := coverage[a.Owner], coverage[b.Owner]
	return ac == nil || bc == nil || ac.binding.equal(&bc.binding)
}

func compareProduction(a, b manifest.Production) int {
	return cmp.Or(a.Owner.Compare(b.Owner), a.Declaration.Compare(b.Declaration),
		a.Source.Compare(b.Source), a.Authored.Compare(b.Authored),
		cmp.Compare(a.Path, b.Path), cmp.Compare(a.File, b.File))
}

// SetDiscoveryIndex installs frozen source-qualified evidence before reconciliation.
// Discovery is never rebuilt on the render path.
func (s *Store) SetDiscoveryIndex(index *manifest.DiscoveryIndex) {
	if index == nil || s.index.Load() == index {
		return
	}
	coverage := map[manifest.NamedResource]*contributor{}
	for id, r := range index.Records {
		if len(r.Productions) == 0 {
			continue
		}
		independent := declarationIndependent(index, id)
		for _, p := range r.Productions {
			if p.Owner == (manifest.NamedResource{}) || p.Owner == id {
				continue
			}
			c := coverage[p.Owner]
			if c == nil {
				c = &contributor{binding: sourceBinding{source: p.Source}}
				coverage[p.Owner] = c
			}
			// Each identity is visited once, so repeated owner evidence can only match the tail.
			if len(c.records) == 0 || c.records[len(c.records)-1].id != id {
				c.records = append(c.records, footprint{id: id, record: r, independent: independent})
			}
			if a, ok := s.GetArtifact(p.Source, ArtifactContent).(*SourceArtifact); ok {
				c.binding.artifact = a
				if a.Content == "" {
					c.binding.mutable = a.mutableMaterial()
				}
			}
		}
	}
	for _, contributor := range coverage {
		slices.SortFunc(contributor.records, func(a, b footprint) int { return a.id.Compare(b.id) })
	}
	s.lockAll()
	if s.index.Load() == nil {
		// Every non-independent declaration MUST occur in a contributor footprint.
		for owner, c := range coverage {
			for _, record := range c.records {
				sh := s.shardFor(record.id)
				if cell, exists := sh.cells[record.id]; exists {
					cell.independent = record.independent
					sh.cells[record.id] = cell
				}
			}
			sh := s.shardFor(owner)
			slot := sh.slots[owner]
			if slot == nil {
				slot = &producerSlot{id: owner, links: sh.newHeadLocked(0)}
				slot.visible.Store(sh.objectLocked(owner) != nil)
				slot.hidden.Store(sh.cells[owner].hidden)
				sh.slots[owner] = slot
			}
			c.slot = slot
		}
		s.coverage = coverage
		s.index.Store(index)
	}
	s.unlockAll()
}
func (s *Store) contributionBinding(id manifest.NamedResource) sourceBinding {
	if c := s.coverage[id]; c != nil {
		return c.binding
	}
	return sourceBinding{}
}

// Bootstrap promotion alone supplies no independent authority against accepted output.
func bootstrapSeed(record manifest.DiscoveryRecord) bool {
	seed := false
	for _, p := range record.Productions {
		if p.Owner != (manifest.NamedResource{}) || p.Declaration != (manifest.NamedResource{}) {
			if record.Parent == (manifest.NamedResource{}) {
				return false
			}
			continue
		}
		if p.Source != manifest.BootstrapSourceID {
			return false
		}
		seed = true
	}
	return seed
}

// RecordEmitted atomically accepts completed work, replacing complete inventories and
// merging incomplete progress. The greatest producer identity supplies shared content.
func (s *Store) RecordEmitted(id manifest.NamedResource, next *Emission, payloads map[manifest.NamedResource]*SourceArtifact) {
	next.accepted = false
	if len(payloads) != 0 && next.Reads == nil {
		reads := new(ReadSet)
		reads.Init(s, id, nil)
		reads.Bind(next)
	}
	if !next.Reads.Valid() {
		s.Retire(next.Reads)
		return
	}
	if len(next.Failures) != 0 {
		next.Complete = false
	}
	if !next.Complete {
		next.Fingerprint = ""
		if len(next.Failures) == 0 && next.Reads != nil && slices.ContainsFunc(next.Children, func(child Child) bool { return child.Generated }) {
			for input := next.Reads.inputs; input != nil; input = input.next {
				if details := input.details.Load(); details != nil && input.derived&4 == 0 {
					if artifact, ok := details.artifact.(*SourceArtifact); ok && !artifact.Reads.Valid() {
						return
					}
				}
			}
		}
	}
	ps := s.shardFor(id)
	for {
		ps.mu.RLock()
		slot := ps.slots[id]
		var prev *Emission
		var generation uint64
		if slot != nil {
			prev, generation = slot.current.Load(), slot.generation
		}
		ps.mu.RUnlock()
		candidate := *next
		candidate.origin = id
		candidate.family = s.index.Load()
		for i, child := range candidate.Children {
			if child.Obj == nil {
				continue
			}
			payload, supplied := payloads[child.Obj.Named()]
			output := child.output
			if supplied {
				output = next.Reads
			} else {
				payload = child.payload
			}
			if payload == nil && !supplied && next.Reads != nil && prev != nil {
				for _, old := range prev.Children {
					if old.payload != nil && old.output == next.Reads && old.Obj.Named() == child.Obj.Named() {
						payload, output = old.payload, old.output
						break
					}
				}
			}
			if payload != nil && next.Complete && next.Reads != nil {
				output = next.Reads
			}
			if payload != child.payload || output != child.output {
				if &candidate.Children[0] == &next.Children[0] {
					candidate.Children = slices.Clone(candidate.Children)
				}
				candidate.Children[i].payload, candidate.Children[i].output = payload, output
			}
		}
		candidate.OutputReads = nil
		if next.Complete {
			candidate.OutputReads = next.Reads
		} else if len(next.Failures) != 0 {
			candidate.OutputReads = next.Reads
		} else if prev != nil && prev.OutputReads.Valid() {
			candidate.OutputReads = prev.OutputReads
		} else if len(payloads) != 0 {
			candidate.OutputReads = next.Reads
		}
		unchanged := prev != nil && len(next.Failures) == 0 && len(prev.Failures) == 0 &&
			(next.Complete && len(candidate.Children) == len(prev.entries) && prev.Complete && prev.shadows(next.binding) ||
				!next.Complete && candidate.OutputReads == prev.OutputReads)
		if unchanged {
			for i, child := range candidate.Children {
				incoming := admission{child: child, binding: next.binding}
				equal := i < len(prev.entries) && admissionEqual(incoming, prev.entries[i])
				if !equal && !next.Complete {
					equal = slices.ContainsFunc(prev.entries, func(old admission) bool { return admissionEqual(incoming, old) })
				}
				if !equal {
					unchanged = false
					break
				}
			}
		}
		if unchanged && !next.Complete {
			for _, old := range prev.entries {
				if old.child.Generated && !slices.ContainsFunc(candidate.Children, func(child Child) bool {
					return child.Obj != nil && child.Generated && child.Obj.Named() == old.child.Obj.Named()
				}) {
					unchanged = false
					break
				}
			}
		}
		if prev != nil {
			candidate.shadowed = prev.shadowed
			candidate.documentOwners = prev.documentOwners
		}
		if unchanged && candidate.Fingerprint == "" {
			candidate.Fingerprint = prev.Fingerprint
			if !candidate.sameOutput(prev) {
				candidate.Fingerprint = ""
			}
		}
		proposal := prev
		if !unchanged || !candidate.sameOutput(prev) {
			byID := make(map[manifest.NamedResource]int, len(next.Children))
			prepared := s.prepareAdmissions(id, &candidate, prev, byID)
			proposal = prepareEmission(&candidate, prev, prepared, byID)
		}
		if unchanged && proposal.Fingerprint == "" && prev.Fingerprint != "" {
			proposal.Fingerprint = prev.Fingerprint
			if !proposal.sameOutput(prev) {
				proposal.Fingerprint = ""
			}
		}
		stable := prev != nil && (proposal == prev || sameMembership(prev, proposal) && slices.EqualFunc(prev.Children, proposal.Children, func(a, b Child) bool { return a.payload == b.payload }))
		unchanged = proposal == prev || proposal.sameContent(prev)
		if unchanged && len(proposal.Failures) == 0 && !proposal.Complete && prev.Complete {
			proposal.Complete = true
		}
		unchanged = unchanged && prev.Complete == proposal.Complete
		if unchanged {
			retained := next.Complete && next.Fingerprint != "" && next.Fingerprint == prev.Fingerprint &&
				prev.OutputReads.Valid() && prev.Reads.Valid() && candidate.inputHead == prev.inputHead && prev.Render.Equal(candidate.Render)
			if !retained && (next.Reads != nil && (prev.Reads != next.Reads || prev.OutputReads != candidate.OutputReads) || !prev.Render.Equal(candidate.Render)) {
				rebound := *prev
				rebound.Render = candidate.Render
				rebound.Reads = next.Reads
				rebound.input, rebound.inputDoc, rebound.inputHead = next.input, next.inputDoc, next.inputHead
				rebound.OutputReads = candidate.OutputReads
				proposal = &rebound
			} else {
				proposal = prev
			}
		} else {
			proposal.revision = generation + 1
		}
		if proposal != prev {
			proposal.cached = nil
			if !proposal.Complete && len(proposal.Failures) == 0 && prev != nil {
				proposal.cached = prev.cached
				if prev.Complete && prev.Fingerprint != "" {
					output := prev.renderedOutput()
					proposal.cached = &output
				}
			}
		}
		var result publicationResult
		if stable {
			result = s.commitStable(slot, prev, proposal, generation)
		} else {
			result = s.commitReplacement(id, prev, proposal, generation)
		}
		switch result {
		case publicationRetry:
			continue
		case publicationRejected:
			return
		}
		if next.Complete && proposal.Complete {
			*next = *proposal
			next.Reads = candidate.Reads
			next.input, next.inputDoc, next.inputHead = candidate.input, candidate.inputDoc, candidate.inputHead
			next.binding, next.previous = candidate.binding, candidate.previous
			next.accepted = true
		}
		if prev != nil {
			if !proposal.Retains(prev.OutputReads) {
				s.Retire(prev.OutputReads)
			}
			if !proposal.Retains(prev.payloadReads) {
				s.Retire(prev.payloadReads)
			}
			for reads := range prev.borrowed {
				if !proposal.Retains(reads) {
					s.Retire(reads)
				}
			}
		}
		return
	}
}

func entriesMatch(children []Child, entries []admission) bool {
	if len(children) != len(entries) {
		return false
	}
	for i, child := range children {
		if !childEqual(child, entries[i].child) {
			return false
		}
	}
	return true
}

func (s *Store) prepareChildren(children []Child, index *manifest.DiscoveryIndex) []admission {
	prepared := make([]admission, 0, len(children))
	for _, child := range children {
		if child.Obj != nil {
			prepared = append(prepared, admission{child: child, independent: declarationIndependent(index, child.Obj.Named())})
		}
	}
	return prepared
}

func (s *Store) prepareAdmissions(id manifest.NamedResource, next, prev *Emission, byID map[manifest.NamedResource]int) []admission {
	var projections []ArtifactInput
	prepared := next.entries
	if !next.entriesOwned || !entriesMatch(next.Children, prepared) {
		prepared = s.prepareChildren(next.Children, next.family)
	}
	dynamic := false
	compacted := prepared[:0]
	for i, entry := range prepared {
		child := entry.child
		if child.Obj.Named() == id {
			if len(compacted) == i {
				// ReadSet retains the unfiltered inventory for lazy captures.
				compacted = make([]admission, i, len(prepared)-1)
				copy(compacted, prepared[:i])
			}
			continue
		}
		child.Productions = slices.Clone(child.Productions)
		entry.child, entry.binding = child, next.binding
		if child.payload != nil {
			if projections == nil {
				projections = make([]ArtifactInput, len(prepared))
			}
			projections[len(compacted)] = child.payload.Input(ArtifactContent | ArtifactRevision | ArtifactOrigin)
			entry.input = &projections[len(compacted)]
		}
		if position, exists := byID[child.Obj.Named()]; exists && (child.Generated || compacted[position].child.Generated) {
			previous := compacted[position].child
			if child.Doc != nil && previous.Generated || previous.Doc == nil && !previous.Generated {
				compacted[position] = entry
			}
			continue
		}
		byID[child.Obj.Named()] = len(compacted)
		compacted = append(compacted, entry)
		dynamic = dynamic || manifest.ProducerNeedsCandidates(child.Obj)
	}
	prepared = compacted
	if !dynamic {
		for i := range prepared {
			prepared[i].targets = manifest.ProducerTargets(prepared[i].child.Obj)
		}
		return prepared
	}
	var refs, superseded, retained []manifest.NamedResource
	for _, child := range next.Children {
		if child.Obj != nil {
			for ref := range manifest.References(child.Obj, manifest.ReferenceRelations) {
				refs = append(refs, ref.NamedResource)
			}
		}
	}
	var proven map[manifest.NamedResource][]manifest.NamedResource
	retain := func(declaration, target manifest.NamedResource) {
		if proven == nil {
			proven = map[manifest.NamedResource][]manifest.NamedResource{}
		}
		proven[declaration] = append(proven[declaration], target)
	}
	binding := s.contributionBinding(id)
	coverage := next.coverage
	if next.family != nil && next.family != s.index.Load() {
		binding = next.binding
		coverage = nil
		for target, record := range next.family.Records {
			if slices.ContainsFunc(record.Productions, func(p manifest.Production) bool { return p.Owner == id && p.Source == binding.source }) {
				coverage = append(coverage, footprint{id: target, record: record})
			}
		}
	}
	if next.binding.equal(&binding) {
		shadowed := prev.shadows(next.binding)
		for _, f := range coverage {
			for _, p := range f.record.Productions {
				if p.Owner != id || p.Source != next.binding.source {
					continue
				}
				if p.File == f.record.File && p.Path == f.record.Path {
					if _, replaced := byID[f.id]; replaced {
						superseded = append(superseded, f.record.Refs...)
					} else if !shadowed {
						refs = append(refs, f.record.Refs...)
					}
				}
				if !shadowed && p.Declaration != (manifest.NamedResource{}) {
					retain(p.Declaration, f.id)
				}
			}
		}
	}
	if prev != nil {
		for _, entry := range prev.entries {
			if entry.binding.equal(&next.binding) {
				_, replacement := byID[entry.child.Obj.Named()]
				for ref := range manifest.References(entry.child.Obj, manifest.ReferenceRelations) {
					if replacement {
						superseded = append(superseded, ref.NamedResource)
					} else {
						retained = append(retained, ref.NamedResource)
					}
				}
				for _, target := range entry.targets {
					retain(entry.child.Obj.Named(), target.NamedResource)
				}
			}
		}
	}
	slices.SortFunc(refs, manifest.NamedResource.Compare)
	refs = slices.Compact(refs)
	for i := range prepared {
		child := prepared[i].child
		candidates := slices.Clip(refs)
		for _, target := range proven[child.Obj.Named()] {
			if !slices.Contains(superseded, target) || slices.Contains(retained, target) {
				candidates = append(candidates, target)
			}
		}
		prepared[i].targets = manifest.ProducerTargets(child.Obj, candidates...)
	}
	return prepared
}

func (e *Emission) sameContent(other *Emission) bool {
	return other != nil && len(e.Failures) == 0 && len(other.Failures) == 0 &&
		slices.EqualFunc(other.entries, e.entries, admissionEqual) && e.sameOutput(other)
}

func (e *Emission) sameOutput(other *Emission) bool {
	sameDocuments := len(e.Manifests) == len(other.Manifests) &&
		(len(e.Manifests) == 0 || &e.Manifests[0] == &other.Manifests[0])
	sameInput := other.inputHead.owner == e.inputHead.owner && other.inputHead.generation == e.inputHead.generation &&
		other.inputHead.revision == e.inputHead.revision && other.inputHead.binding.equal(&e.inputHead.binding) && manifest.Equal(other.input, e.input) ||
		e.input != nil && semanticEqual(Child{Obj: other.input, Doc: other.inputDoc}, Child{Obj: e.input, Doc: e.inputDoc})
	return sameInput && other.family == e.family &&
		(sameDocuments || slices.EqualFunc(other.Manifests, e.Manifests, func(a, b manifest.DocOccurrence) bool {
			return a.Ordinal == b.Ordinal && manifest.ValueEqual(a.Document, b.Document)
		})) &&
		other.Fingerprint == e.Fingerprint &&
		slices.Equal(other.documentOwners, e.documentOwners) &&
		other.binding.equal(&e.binding) &&
		slices.Equal(other.Unreadable, e.Unreadable) &&
		slices.EqualFunc(other.shadowed, e.shadowed, func(a, b sourceBinding) bool { return a.equal(&b) }) &&
		slices.EqualFunc(other.coverage, e.coverage, func(a, b footprint) bool {
			return a.id == b.id && slices.Equal(a.record.Productions, b.record.Productions)
		})
}

type publicationResult uint8

const (
	publicationRetry publicationResult = iota
	publicationRejected
	publicationAccepted
)

func (s *Store) commitStable(slot *producerSlot, prev, proposal *Emission, generation uint64) publicationResult {
	if !proposal.Reads.Valid() {
		return publicationRejected
	}
	ps := s.shardFor(slot.id)
	ps.mu.Lock()
	if !proposal.Reads.Valid() {
		ps.mu.Unlock()
		return publicationRejected
	}
	if slot.current.Load() != prev || slot.generation != generation {
		ps.mu.Unlock()
		return publicationRetry
	}
	if proposal.Reads != nil && ps.cells[slot.id].current().generation != proposal.inputHead.generation {
		ps.mu.Unlock()
		return publicationRejected
	}
	if proposal == prev {
		ps.mu.Unlock()
		return publicationAccepted
	}
	if proposal != prev && proposal.revision == prev.revision {
		if ps.artifacts[slot.id].publication == ^uint64(0) {
			ps.mu.Unlock()
			panic("store: publication revision exhausted")
		}
		slot.current.Store(proposal)
		if slot.visible.Load() {
			record := ps.artifacts[slot.id]
			record.inventoryVisible = true
			var view capture
			ps.captureLocked(slot.id, ps.cells[slot.id], &view)
			record.effective = ps.projectArtifactLocked(slot.id, &view, record)
			record.publication++
			ps.artifacts[slot.id] = record
		}
		ps.mu.Unlock()
		return publicationAccepted
	}
	if slot.generation == ^uint64(0) || ps.artifacts[slot.id].publication == ^uint64(0) || ps.artifacts[slot.id].generation == ^uint64(0) || ps.artifacts[slot.id].generation == 0 && !ps.canAdmit(1) {
		ps.mu.Unlock()
		panic("store: authority generation exhausted")
	}
	for _, a := range proposal.entries {
		if a.objectChanged && a.objectHead != nil && a.objectHead.selected.Load() == slot && a.objectHead.stamp() == ^uint64(0) {
			ps.mu.Unlock()
			panic("store: authority generation exhausted")
		}
	}
	for i := range proposal.entries {
		a := &proposal.entries[i]
		if a.objectChanged && a.objectHead != nil && a.objectHead.selected.Load() == slot {
			a.objectGeneration = a.objectHead.stamp() + 1
		}
	}
	slot.authority = ps.advanceLocked(slot.authority)
	slot.current.Store(proposal)
	for _, a := range proposal.entries {
		if a.objectChanged && a.objectHead != nil && a.objectHead.selected.Load() == slot {
			a.objectHead.shard.queue(a.objectHead)
		}
	}
	var artifact eventDispatch
	var events []eventDispatch
	if slot.visible.Load() {
		record := ps.artifacts[slot.id]
		record.inventoryVisible = true
		var view capture
		ps.captureLocked(slot.id, ps.cells[slot.id], &view)
		visible := ps.projectArtifactLocked(slot.id, &view, record)
		ps.publishArtifactLocked(slot.id, &record, visible)
		ps.artifacts[slot.id] = record
		artifact = fireUnderLock(s, EventArtifactUpdated, slot.id)
		artifact.payload = visible
		listeners := s.listeners[EventObjectAdded].snapshot.Load()
		if len(listeners.entries) > 0 {
			for _, a := range proposal.entries {
				if !a.changed {
					continue
				}
				events = append(events, eventDispatch{id: a.child.Obj.Named(), payload: a.child.Obj, listeners: listeners})
				for _, t := range a.targets {
					events = append(events, eventDispatch{id: t.NamedResource, listeners: listeners})
				}
			}
		}
	} else {
		events = nil
	}
	ps.mu.Unlock()
	dispatchEvents(events)
	artifact.run()
	return publicationAccepted
}

func (s *Store) commitReplacement(id manifest.NamedResource, prev, proposal *Emission, generation uint64) publicationResult {
	if !proposal.Reads.Valid() {
		return publicationRejected
	}
	var storage [4]regionState
	changed := s.changedTargets(prev, proposal, storage[:])
	if proposal.shadows(s.contributionBinding(id)) {
		for _, f := range proposal.coverage {
			changed = changed.add(f.id, true, f.independent)
		}
	}
	independent := declarationIndependent(s.index.Load(), id)
	ps := s.shardFor(id)
	s.lockAll()
	slot := ps.slots[id]
	if !proposal.Reads.Valid() {
		s.unlockAll()
		return publicationRejected
	}
	if slot != nil && (slot.current.Load() != prev || slot.generation != generation) || slot == nil && prev != nil {
		s.unlockAll()
		return publicationRetry
	}
	if proposal.Reads != nil && ps.cells[id].current().generation != proposal.inputHead.generation {
		s.unlockAll()
		return publicationRejected
	}
	if slot != nil && slot.generation == ^uint64(0) || ps.artifacts[id].generation == ^uint64(0) || ps.artifacts[id].publication == ^uint64(0) {
		s.unlockAll()
		panic("store: authority generation exhausted")
	}
	region := s.affectedLocked(changed, id)
	if slot == nil {
		slot = &producerSlot{id: id, links: ps.newHeadLocked(0)}
		ps.slots[id] = slot
		slot.visible.Store(ps.objectLocked(id) != nil)
		slot.hidden.Store(ps.cells[id].hidden)
	}
	for _, state := range region.states {
		target := state.id
		sh := s.shardFor(target)
		sh.members[target] = slices.DeleteFunc(sh.members[target], func(m membership) bool { return m.slot == slot })
	}
	// A publication MUST retain the bootstrap authority supporting its own declaration.
	for ancestor, inside := region.position(id); inside; {
		state := &region.states[ancestor]
		if state.before == nil || state.supportsProducer {
			break
		}
		state.supportsProducer = true
		if state.owner == nil {
			break
		}
		ancestor, inside = region.position(state.owner.id)
	}
	for _, entry := range proposal.entries {
		target := entry.child.Obj.Named()
		r, _ := s.index.Load().Lookup(target)
		sh := s.shardFor(target)
		cell := sh.cells[target]
		if bootstrapSeed(r) && cell.static != nil && (!cell.rooted || r.Parent == id) && !cell.retired {
			position, _ := region.position(target)
			if region.states[position].supportsProducer {
				continue
			}
			cell.retired, cell.rooted = true, false
			sh.cells[target] = cell
			region.states[position].seed = true
		}
	}

	for i := range proposal.entries {
		entry := &proposal.entries[i]
		// An admitted object's head MUST remain stable for the store's lifetime.
		if entry.objectHead != nil {
			continue
		}
		target := s.shardFor(entry.child.Obj.Named())
		cell := target.cells[entry.child.Obj.Named()]
		if cell.links == nil {
			cell.links = target.newHeadLocked(cell.generation)
			target.cells[entry.child.Obj.Named()] = cell
		}
		entry.objectHead = cell.links
	}
	slot.authority = ps.advanceLocked(slot.authority)
	slot.current.Store(proposal)
	if !slot.visible.Load() {
		cell := ps.cells[id]
		if !cell.hidden {
			ps.derived = true
			if cell.generation == 0 {
				ps.admitAbsenceLocked(id, objectFacet)
			}
			cell.authority = ps.advanceLocked(cell.current())
			cell.hidden, cell.independent = true, independent
			ps.cells[id] = cell
		}
		slot.hidden.Store(true)
	}
	for position, a := range proposal.entries {
		target := a.child.Obj.Named()
		sh := s.shardFor(target)
		sh.members[target] = append(sh.members[target], membership{slot: slot, position: position})
		for _, t := range a.targets {
			sh := s.shardFor(t.NamedResource)
			sh.members[t.NamedResource] = append(sh.members[t.NamedResource], membership{slot: slot, position: position, declaration: true})
		}
	}
	publications := s.reachabilityLocked(region)
	if slot.visible.Load() {
		record := ps.artifacts[id]
		record.inventoryVisible = true
		var view capture
		ps.captureLocked(id, ps.cells[id], &view)
		visible := ps.projectArtifactLocked(id, &view, record)
		ps.publishArtifactLocked(id, &record, visible)
		ps.artifacts[id] = record
		event := fireUnderLock(s, EventArtifactUpdated, id)
		event.payload = visible
		publications = append(publications, objectPublication{event: event, force: true})
	}
	s.unlockAll()
	dispatchPublications(publications)
	return publicationAccepted
}

func (s *Store) changedTargets(prev, proposal *Emission, storage []regionState) objectRegion {
	changed := newObjectRegion(len(proposal.entries), storage)
	add := func(entry admission, direct bool) {
		changed = changed.add(entry.child.Obj.Named(), direct, entry.independent)
		for _, target := range entry.targets {
			changed = changed.add(target.NamedResource, direct, declarationIndependent(s.index.Load(), target.NamedResource))
		}
	}
	next := 0
	if prev != nil {
		for _, old := range prev.entries {
			if next == len(proposal.entries) || old.child.Obj.Named() != proposal.entries[next].child.Obj.Named() {
				add(old, true)
				continue
			}
			entry := proposal.entries[next]
			direct := entry.objectChanged || entry.changed && !manifest.LabelsEqual(old.child.Obj, entry.child.Obj, [2]map[string]any{old.child.Doc, entry.child.Doc}, ownershipLabel) ||
				!old.binding.equal(&entry.binding) || !slices.Equal(old.child.Productions, entry.child.Productions) ||
				!slices.EqualFunc(old.targets, entry.targets, func(a, b manifest.ProducerTarget) bool {
					return a.NamedResource == b.NamedResource && slices.Equal(a.DeclaredKeys, b.DeclaredKeys)
				})
			add(old, direct)
			add(entry, direct)
			next++
		}
	}
	for _, entry := range proposal.entries[next:] {
		add(entry, true)
	}
	return changed
}

func prepareEmission(next, prev *Emission, prepared []admission, byID map[manifest.NamedResource]int) *Emission {
	if prev != nil && next.Complete && !prev.OutputReads.Valid() {
		for _, old := range prev.entries {
			if i, ok := byID[old.child.Obj.Named()]; ok {
				prepared[i].objectHead, prepared[i].objectGeneration = old.objectHead, old.objectGeneration
				prepared[i].objectChanged = !reconcileEqual(prepared[i].child, old.child)
			}
		}
		prev = nil
	}
	e := *next
	e.entriesOwned = false
	e.validated = new(atomic.Pointer[manifest.BaseManifest])
	e.shadowed = nil
	if prev != nil {
		e.shadowed = slices.Clone(prev.shadowed)
	}
	if (next.Complete || len(next.Failures) != 0) && !e.shadows(next.binding) {
		e.shadowed = append(e.shadowed, next.binding)
	}
	e.previous = nil
	capacity := len(prepared)
	if prev != nil && !next.Complete && len(next.Failures) == 0 {
		capacity += len(prev.entries)
	}
	e.entries = make([]admission, 0, capacity)
	e.Children, e.payloadReads, e.borrowed = nil, nil, nil
	e.Manifests = slices.Clone(next.Manifests)
	e.Failures = slices.Clone(next.Failures)
	e.Unreadable = slices.Clone(next.Unreadable)
	clear(byID)
	for i, a := range prepared {
		byID[a.child.Obj.Named()] = i
	}
	// Stable positions retain reverse membership offsets independently of document order.
	if prev != nil {
		for _, old := range prev.entries {
			id := old.child.Obj.Named()
			if i, ok := byID[id]; ok {
				entry := prepared[i]
				if !next.Complete && entry.child.Generated && old.child.Doc != nil && !old.child.Generated {
					entry = old
				}
				entry.changed = entry.child.Generated != old.child.Generated || !manifest.Equal(entry.child.Obj, old.child.Obj) || !manifest.ValueEqual(entry.child.Doc, old.child.Doc)
				entry.objectChanged = !reconcileEqual(entry.child, old.child)
				entry.objectHead = old.objectHead
				entry.objectGeneration = old.objectGeneration
				e.entries = append(e.entries, entry)
				delete(byID, id)
			} else if !next.Complete && len(next.Failures) == 0 && !old.child.Generated {
				old.changed, old.objectChanged = false, false
				e.entries = append(e.entries, old)
			}
		}
	}
	for _, a := range prepared {
		id := a.child.Obj.Named()
		if _, ok := byID[id]; ok {
			a.changed = true
			e.entries = append(e.entries, a)
			delete(byID, id)
		}
	}
	if len(e.entries) > 0 {
		e.Children = make([]Child, len(e.entries))
		for i, a := range e.entries {
			e.Children[i] = a.child
			if reads := a.child.output; reads != nil {
				if e.payloadReads == nil {
					e.payloadReads = reads
				} else if e.payloadReads != reads {
					if e.borrowed == nil {
						e.borrowed = make(map[*ReadSet]struct{})
					}
					e.borrowed[reads] = struct{}{}
				}
			}
		}
	}
	inherited := prev != nil && slices.EqualFunc(next.Manifests, prev.Manifests, func(a, b manifest.DocOccurrence) bool {
		return a.Ordinal == b.Ordinal && manifest.ValueEqual(a.Document, b.Document)
	})
	if inherited && (!next.Complete || slices.EqualFunc(prev.entries, e.entries, func(a, b admission) bool { return a.child.Obj.Named() == b.child.Obj.Named() })) {
		e.documentOwners = prev.documentOwners
	} else {
		e.documentOwners = bindDocuments(next.Manifests, prepared)
	}
	if !next.Complete && len(next.Failures) == 0 && prev != nil {
		if !inherited {
			visited := make(map[manifest.NamedResource]struct{}, len(prepared))
			for _, entry := range prepared {
				visited[entry.child.Obj.Named()] = struct{}{}
			}
			available := make(map[manifest.NamedResource][]int, len(e.Manifests))
			for i, doc := range e.Manifests {
				key := manifest.DocIdentity(doc.Document)
				available[key] = append(available[key], i)
			}
			used := make([]bool, len(e.Manifests))
			for i, doc := range prev.Manifests {
				owner := prev.documentOwners[i]
				if _, replaced := visited[owner]; owner != (manifest.NamedResource{}) && replaced {
					continue
				}
				present := false
				for _, j := range available[manifest.DocIdentity(doc.Document)] {
					if !used[j] && manifest.ValueEqual(doc.Document, e.Manifests[j].Document) {
						used[j], present = true, true
						break
					}
				}
				if !present {
					e.Manifests = append(e.Manifests, doc)
					e.documentOwners = append(e.documentOwners, owner)
				}
			}
		}
		e.coverage = slices.Clone(e.coverage)
		for _, old := range prev.coverage {
			if !slices.ContainsFunc(e.coverage, func(f footprint) bool {
				return f.id == old.id && slices.Equal(f.record.Productions, old.record.Productions)
			}) {
				e.coverage = append(e.coverage, old)
			}
		}
	}
	return &e
}

func bindDocuments(docs []manifest.DocOccurrence, entries []admission) []manifest.NamedResource {
	byDocument := make(map[manifest.NamedResource][]admission, len(entries))
	for _, entry := range entries {
		if entry.child.Doc != nil {
			key := manifest.DocIdentity(entry.child.Doc)
			byDocument[key] = append(byDocument[key], entry)
		}
	}
	owners := make([]manifest.NamedResource, len(docs))
	for i, doc := range docs {
		for _, entry := range byDocument[manifest.DocIdentity(doc.Document)] {
			if manifest.ValueEqual(doc.Document, entry.child.Doc) {
				owners[i] = entry.child.Obj.Named()
				break
			}
		}
	}
	return owners
}
func sameMembership(a, b *Emission) bool {
	if a.family != b.family || len(a.entries) != len(b.entries) || !slices.EqualFunc(a.shadowed, b.shadowed, func(a, b sourceBinding) bool { return a.equal(&b) }) || !a.binding.equal(&b.binding) ||
		!slices.EqualFunc(a.coverage, b.coverage, func(a, b footprint) bool {
			return a.id == b.id && slices.Equal(a.record.Productions, b.record.Productions)
		}) {
		return false
	}
	for i, x := range a.entries {
		y := b.entries[i]
		if x.child.Obj.Named() != y.child.Obj.Named() || x.child.Generated != y.child.Generated || !x.binding.equal(&y.binding) ||
			!slices.Equal(x.child.Productions, y.child.Productions) || !manifest.LabelsEqual(x.child.Obj, y.child.Obj, [2]map[string]any{x.child.Doc, y.child.Doc}, ownershipLabel) ||
			!slices.EqualFunc(x.targets, y.targets, func(a, b manifest.ProducerTarget) bool { return a.NamedResource == b.NamedResource }) {
			return false
		}
	}
	return true
}

func admissionEqual(a, b admission) bool {
	return a.binding.equal(&b.binding) && childEqual(a.child, b.child)
}

func childEqual(a, b Child) bool {
	return a.Generated == b.Generated && manifest.Equal(a.Obj, b.Obj) && manifest.ValueEqual(a.Doc, b.Doc) && slices.Equal(a.Productions, b.Productions) && a.payload == b.payload && a.output == b.output
}
func semanticEqual(a, b Child) bool {
	if a.Doc == nil || b.Doc == nil {
		if manifest.Equal(a.Obj, b.Obj) {
			return true
		}
		type metadata interface {
			GetLabels() map[string]string
			GetAnnotations() map[string]string
		}
		am, aok := a.Obj.(metadata)
		bm, bok := b.Obj.(metadata)
		if !aok || !bok {
			return false
		}
		if !maps.Equal(am.GetAnnotations(), bm.GetAnnotations()) {
			return false
		}
		if !manifest.MapEqualExcept(am.GetLabels(), bm.GetLabels(), ownershipLabel) {
			return false
		}
		ad, aerr := json.Marshal(a.Obj)
		bd, berr := json.Marshal(b.Obj)
		return aerr == nil && berr == nil && bytes.Equal(ad, bd)
	}
	if len(a.Doc) != len(b.Doc) {
		return false
	}
	for key, value := range a.Doc {
		other, ok := b.Doc[key]
		if !ok {
			return false
		}
		if key != "metadata" {
			if !manifest.ValueEqual(value, other) {
				return false
			}
			continue
		}
		am, aok := value.(map[string]any)
		bm, bok := other.(map[string]any)
		if !aok || !bok {
			return manifest.ValueEqual(value, other)
		}
		if !manifest.MapEqualExcept(am, bm, func(key string) bool { return key == "labels" }) {
			return false
		}
		al, _ := am["labels"].(map[string]any)
		bl, _ := bm["labels"].(map[string]any)
		if !manifest.MapEqualExcept(al, bl, ownershipLabel) {
			return false
		}
	}
	return true
}

func ownershipLabel(key string) bool {
	switch key {
	case "kustomize.toolkit.fluxcd.io/name", "kustomize.toolkit.fluxcd.io/namespace", "helm.toolkit.fluxcd.io/name", "helm.toolkit.fluxcd.io/namespace", "resourceset.fluxcd.controlplane.io/name", "resourceset.fluxcd.controlplane.io/namespace":
		return true
	}
	return false
}

type capture struct {
	excluded     bool
	accepted     bool
	independent  bool
	bound        bool
	auxiliary    bool
	binding      *Input
	support      *Input
	preflight    string
	child        Child
	owner        *producerSlot
	record       *Emission
	revision     uint64
	payload      *SourceArtifact
	payloadInput *ArtifactInput
	payloadOwner *producerSlot
	source       *sourceBinding
	generation   uint64
	position     int
}

func (c *capture) sourceBinding() sourceBinding {
	if c.source != nil {
		return *c.source
	}
	return sourceBinding{}
}

func (sh *shard) objectLocked(id manifest.NamedResource) manifest.BaseManifest {
	if !sh.derived && sh.cells[id].binding == nil {
		return sh.cells[id].static
	}
	return sh.captureLocked(id, sh.cells[id], nil)
}

func (sh *shard) captureLocked(id manifest.NamedResource, cell objectCell, view *capture) manifest.BaseManifest {
	if view != nil {
		*view = capture{independent: cell.independent, accepted: cell.rooted, generation: cell.generation, excluded: cell.excluded, preflight: cell.preflight, binding: cell.binding, support: cell.support}
	}
	if cell.hidden {
		return nil
	}
	obj := cell.static
	bound := cell.bound()
	if view != nil {
		view.bound = bound
	}
	if !bound {
		obj = nil
	}
	var chosen *membership
	for i := range sh.members[id] {
		m := &sh.members[id][i]
		if m.declaration || !m.slot.visible.Load() {
			continue
		}
		if chosen == nil || m.slot.id.Compare(chosen.slot.id) > 0 {
			chosen = m
		}
		if view != nil {
			record := m.slot.current.Load()
			if chosen == m {
				view.auxiliary = record.entries[m.position].child.Generated
			}
			payload := record.entries[m.position].child.payload
			if payload != nil && (view.payloadOwner == nil || m.slot.id.Compare(view.payloadOwner.id) > 0) {
				view.payload, view.payloadOwner = payload, m.slot
				view.payloadInput = record.entries[m.position].input
			}
		}
	}
	if !cell.rooted || !bound {
		if chosen != nil {
			record := chosen.slot.current.Load()
			child := &record.entries[chosen.position].child
			if view != nil {
				view.owner, view.record, view.revision, view.child, view.source = chosen.slot, record, record.publication(), *child, &record.entries[chosen.position].binding
				view.generation = max(cell.generation, record.entries[chosen.position].objectGeneration)
				view.position = chosen.position
			}
			return child.Obj
		}
		if cell.retired {
			obj = nil
		}
	}
	if view != nil {
		view.child.Obj, view.child.Doc = obj, cell.document
	}
	return obj
}

type regionState struct {
	id                                  manifest.NamedResource
	before                              manifest.BaseManifest
	document                            map[string]any
	payload                             *SourceArtifact
	cell                                objectCell
	live, static, direct, visible, seed bool
	independent, supportsProducer       bool
	producerGeneration                  uint64
	owner                               *producerSlot
	source                              *sourceBinding
	bucketGeneration                    uint64
}

type objectRegion struct {
	positions map[manifest.NamedResource]int
	states    []regionState
	reserve   int
}

func newObjectRegion(capacity int, storage []regionState) objectRegion {
	if capacity <= cap(storage) {
		return objectRegion{states: storage[:0], reserve: capacity}
	}
	return objectRegion{
		positions: make(map[manifest.NamedResource]int, capacity),
		states:    make([]regionState, 0, capacity),
	}
}

func (r objectRegion) position(id manifest.NamedResource) (int, bool) {
	if r.positions != nil {
		i, seen := r.positions[id]
		return i, seen
	}
	for i := range r.states {
		if r.states[i].id == id {
			return i, true
		}
	}
	return 0, false
}

func (r objectRegion) add(id manifest.NamedResource, direct, independent bool) objectRegion {
	if i, seen := r.position(id); seen {
		r.states[i].direct = r.states[i].direct || direct
		return r
	}
	if len(r.states) == cap(r.states) && r.positions == nil {
		capacity := max(r.reserve, 2*len(r.states)+1)
		r.positions = make(map[manifest.NamedResource]int, capacity)
		r.states = slices.Grow(r.states, capacity-len(r.states))
		for i := range r.states {
			r.positions[r.states[i].id] = i
		}
	}
	if r.positions != nil {
		r.positions[id] = len(r.states)
	}
	r.states = append(r.states, regionState{id: id, direct: direct, independent: independent})
	return r
}

func (s *Store) affectedLocked(region objectRegion, producer manifest.NamedResource) objectRegion {
	capacity := len(region.states)
	for _, state := range region.states {
		id := state.id
		if slot := s.shardFor(id).slots[id]; slot != nil {
			if e := slot.current.Load(); e != nil {
				capacity += len(e.entries)
				for _, a := range e.entries {
					capacity += len(a.targets)
				}
			}
		}
		if c := s.coverage[id]; c != nil {
			capacity += len(c.records)
		}
	}
	region.reserve = capacity
	if region.positions != nil && capacity > cap(region.states) {
		positions := make(map[manifest.NamedResource]int, capacity)
		for i := range region.states {
			positions[region.states[i].id] = i
		}
		region.positions = positions
		region.states = slices.Grow(region.states, capacity-len(region.states))
	}
	//nolint:intrange // add MUST extend the worklist during traversal.
	for i := 0; i < len(region.states); i++ {
		id := region.states[i].id
		if slot := s.shardFor(id).slots[id]; slot != nil {
			if e := slot.current.Load(); e != nil {
				for _, a := range e.entries {
					region = region.add(a.child.Obj.Named(), false, false)
					for _, t := range a.targets {
						region = region.add(t.NamedResource, false, false)
					}
				}
			}
		}
		if c := s.coverage[id]; c != nil {
			for _, f := range c.records {
				region = region.add(f.id, false, f.independent)
			}
		}
	}
	var admissions [shardCount]uint64
	if producer != (manifest.NamedResource{}) {
		admissions[kindShardIndex(producer.Kind)] = 2
	}
	for i := range region.states {
		state := &region.states[i]
		id := state.id
		sh := s.shardFor(id)
		state.cell = sh.cells[id]
		// Metadata-only cells may predate preparation; frozen facts cannot disagree.
		state.independent = state.independent || state.cell.independent
		state.cell.authority = state.cell.current()
		if state.cell.generation == 0 {
			admissions[kindShardIndex(id.Kind)]++
		}
		artifact := sh.artifacts[id]
		for _, head := range []authority{artifact.authority, artifact.revisionHead, artifact.originHead, artifact.acquisitionHead} {
			if head.generation == 0 {
				admissions[kindShardIndex(id.Kind)]++
			}
		}
		if sh.byName[kindID{id.Group, id.Kind}].links == nil {
			admissions[kindShardIndex(id.Kind)]++
		}
		state.bucketGeneration = sh.byName[kindID{id.Group, id.Kind}].generation
		if slot := sh.slots[id]; slot != nil {
			state.producerGeneration = slot.generation
			if slot.generation == ^uint64(0) {
				s.unlockAll()
				panic("store: authority generation exhausted")
			}
		}
		var view capture
		sh.captureLocked(id, state.cell, &view)
		before := view.child
		state.before, state.document, state.payload = before.Obj, before.Doc, view.payload
		state.owner, state.source = view.owner, view.source
		state.visible = !state.cell.hidden && (state.cell.static != nil || len(sh.members[id]) > 0)
		if state.cell.publication == ^uint64(0) || artifact.publication == ^uint64(0) || state.cell.generation == ^uint64(0) || sh.conditions[id].stamp().generation == ^uint64(0) || artifact.generation == ^uint64(0) || artifact.revisionHead.generation == ^uint64(0) || artifact.originHead.generation == ^uint64(0) || artifact.acquisitionHead.generation == ^uint64(0) || state.bucketGeneration == ^uint64(0) {
			s.unlockAll()
			panic("store: authority generation exhausted")
		}
	}
	for i, count := range admissions {
		if !s.shards[i].canAdmit(count) {
			s.unlockAll()
			panic("store: authority generation exhausted")
		}
	}
	return region
}

func (s *Store) staticAnchorLocked(id manifest.NamedResource, support objectRegion) bool {
	sh := s.shardFor(id)
	cell := sh.cells[id]
	if cell.binding != nil && !cell.binding.current() || cell.support != nil && !cell.support.productionCurrent() {
		return false
	}
	if cell.rooted && cell.static != nil {
		return true
	}
	if cell.static == nil {
		return false
	}
	r, ok := s.index.Load().Lookup(id)
	if !ok {
		return true
	}
	if cell.retired && bootstrapSeed(r) {
		return false
	}
	for _, p := range r.Productions {
		if p.Owner == (manifest.NamedResource{}) || p.Owner == id {
			return true
		}
		slot := s.shardFor(p.Owner).slots[p.Owner]
		if i, inside := support.position(p.Owner); inside {
			if !support.states[i].live {
				continue
			}
		} else if slot != nil && slot.hidden.Load() {
			continue
		}
		if slot == nil || !slot.current.Load().shadows(s.contributionBinding(p.Owner)) {
			return true
		}
	}
	return len(r.Productions) == 0
}

func (s *Store) reachabilityLocked(region objectRegion) []objectPublication {
	queue := make([]int, 0, len(region.states))
	add := func(id manifest.NamedResource) {
		if i, inside := region.position(id); inside && !region.states[i].live {
			region.states[i].live = true
			queue = append(queue, i)
		}
	}
	for _, state := range region.states {
		id := state.id
		live := s.staticAnchorLocked(id, region)
		if !live {
			for _, m := range s.shardFor(id).members[id] {
				if _, inside := region.position(m.slot.id); !inside && m.slot.visible.Load() {
					live = true
					break
				}
			}
		}
		if live {
			add(id)
		}
	}
	//nolint:intrange // add MUST extend the worklist during traversal.
	for i := 0; i < len(queue); i++ {
		id := region.states[queue[i]].id
		var e *Emission
		if slot := s.shardFor(id).slots[id]; slot != nil {
			e = slot.current.Load()
		}
		if e != nil {
			for _, a := range e.entries {
				add(a.child.Obj.Named())
				for _, t := range a.targets {
					add(t.NamedResource)
				}
			}
		}
		if e.shadows(s.contributionBinding(id)) {
			continue
		}
		if c := s.coverage[id]; c != nil {
			for _, f := range c.records {
				if s.shardFor(f.id).cells[f.id].static != nil {
					add(f.id)
				}
			}
		}
	}
	for i := range region.states {
		state := &region.states[i]
		id := state.id
		state.static = s.staticAnchorLocked(id, region)
		sh := s.shardFor(id)
		sh.derived = true
		if slot := sh.slots[id]; slot != nil {
			if (slot.visible.Load() != state.live || slot.hidden.Load() == state.live) && slot.generation == state.producerGeneration {
				slot.authority = sh.advanceLocked(slot.authority)
			}
			slot.visible.Store(state.live)
			slot.hidden.Store(!state.live)
		}
	}
	publications := make([]objectPublication, 0, len(region.states)+1)
	for _, state := range region.states {
		id := state.id
		sh := s.shardFor(id)
		cell := sh.cells[id]
		generation, retired, hidden, auxiliary := cell.generation, cell.retired, cell.hidden, cell.auxiliary
		cell.generation = max(cell.generation, state.cell.generation)
		if cell.generation == 0 && cell.static == nil && len(sh.members[id]) == 0 && sh.slots[id] == nil {
			continue
		}
		if cell.hidden && state.live {
			sh.clearFacetsLocked(id)
		}
		cell.retired, cell.hidden, cell.independent = !state.static, !state.live, state.independent
		var view capture
		sh.captureLocked(id, cell, &view)
		after := view.child
		cell.auxiliary = view.auxiliary
		if state.direct || (state.before == nil) != (after.Obj == nil) || state.owner != view.owner || !state.source.equal(view.source) ||
			cell.rooted != state.cell.rooted && state.cell.static == nil || cell.retired != state.cell.retired || cell.hidden != state.cell.hidden {
			if cell.generation == 0 {
				sh.admitAbsenceLocked(id, objectFacet)
			}
			cell.authority = sh.advanceLocked(cell.current())
		}
		if cell.generation != generation || cell.retired != retired || cell.hidden != hidden || cell.auxiliary != auxiliary {
			sh.cells[id] = cell
		}
		if head := cell.links; head != nil && (head.selected.Load() != view.owner || view.owner != nil && head.position.Load() != int64(view.position)) {
			head.generation.Store(max(cell.generation, head.stamp()))
			if view.owner != nil {
				head.position.Store(int64(view.position))
			}
			head.selected.Store(view.owner)
		}
		key := kindID{id.Group, id.Kind}
		bucket := sh.byName[key]
		if bucket.links == nil && after.Obj != nil {
			sh.admitAbsenceLocked(manifest.NamedResource{Group: id.Group, Kind: id.Kind}, bucketFacet)
			bucket.links = sh.newHeadLocked(bucket.generation)
		}
		if bucket.update(nameID{id.Namespace, id.Name}, after.Obj != nil, state.direct, state.bucketGeneration) {
			bucket.authority = sh.enqueueLocked(bucket.authority)
			sh.byName[key] = bucket
		}
		// Direct removal MUST restore surviving emitted payloads after clearing static facets.
		if state.direct || state.payload != view.payload || state.cell.hidden != cell.hidden {
			record := sh.artifacts[id]
			effective := sh.projectArtifactLocked(id, &view, record)
			if record.effective != effective {
				changed := sh.publishArtifactLocked(id, &record, effective)
				sh.artifacts[id] = record
				if changed && (!cell.hidden || state.payload != nil) {
					event := fireUnderLock(s, EventArtifactUpdated, id)
					event.payload = effective
					if effective == nil {
						event.payload = view.payload
					}
					publications = append(publications, objectPublication{event: event, force: true})
				}
			}
		}
		if (state.direct || state.before != after.Obj) && (after.Obj != nil || state.visible || !cell.hidden) {
			event := fireUnderLock(s, EventObjectAdded, id)
			event.payload = after.Obj
			publications = append(publications, objectPublication{event: event, before: state.before, document: state.document, after: after.Obj, nextDocument: after.Doc, force: state.seed})
		}
	}
	return publications
}

// HasInventory reports whether a producer has published an inventory, including hidden output.
func (s *Store) HasInventory(id manifest.NamedResource) bool {
	sh := s.shardFor(id)
	sh.mu.RLock()
	slot := sh.slots[id]
	present := slot != nil && slot.current.Load() != nil
	sh.mu.RUnlock()
	return present
}

// HasGeneratedObjects reports whether retained inventory contains auxiliary declarations.
func (s *Store) HasGeneratedObjects(id manifest.NamedResource) bool {
	sh := s.shardFor(id)
	sh.mu.RLock()
	var inventory *Emission
	if slot := sh.slots[id]; slot != nil {
		inventory = slot.current.Load()
	}
	sh.mu.RUnlock()
	return inventory != nil && slices.ContainsFunc(inventory.Children, func(child Child) bool { return child.Generated })
}
