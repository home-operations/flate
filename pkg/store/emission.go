package store

import (
	"bytes"
	"cmp"
	"encoding/json"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

// Child retains an admitted object and its authoritative sanitized document.
type Child struct {
	Obj         manifest.BaseManifest
	Doc         map[string]any
	Productions []manifest.Production
}

// Emission is an immutable accepted inventory. Maps and objects are borrowed read-only.
type Emission struct {
	// Reads records the accepted assessment's consumed inputs.
	Reads *ReadSet
	// OutputReads retains the provenance of the last complete inventory.
	OutputReads *ReadSet

	family      *manifest.DiscoveryIndex
	Children    []Child
	Manifests   []map[string]any
	Fingerprint string
	Unreadable  []manifest.NamedResource
	Complete    bool
	Render      []byte

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
	Children    []Child
	Manifests   []map[string]any
	Fingerprint string
	Unreadable  []manifest.NamedResource
	Render      []byte
	warnings    []manifest.Warning
}

func (*Emission) artifact() {}

// Replay returns completed content matching the caller's canonical input key.
// Stale accepted inventory remains unavailable through GetEmission.
func (e *Emission) Replay(fingerprint string) (RenderedOutput, bool) {
	if e == nil || fingerprint == "" {
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

// Finish releases predecessor inventory borrowed by an attempt.
func (e *Emission) Finish() { e.previous = nil }

func (e *Emission) shadows(binding sourceBinding) bool {
	return e != nil && slices.Contains(e.shadowed, binding)
}

type contributor struct {
	slot    *producerSlot
	records []footprint
	binding sourceBinding
}

type footprint struct {
	id     manifest.NamedResource
	record manifest.DiscoveryRecord
}
type sourceBinding struct {
	source    manifest.NamedResource
	key, root string
	mutable   *SourceArtifact
}
type admission struct {
	changed bool
	child   Child
	targets []manifest.ProducerTarget
	binding sourceBinding
}
type inputStamp struct {
	owner                *producerSlot
	generation, revision uint64
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

// GetEmission borrows valid accepted inventory and binds an attempt to its current input.
// Render identity is available through Emission.Replay.
// OutputReads preserves validity evidence for withdrawal without exposing invalid inventory.
func (s *Store) GetEmission(id manifest.NamedResource) (manifest.BaseManifest, Emission) {
	return s.getEmission(nil, id)
}

func (s *Store) getEmission(reads *ReadSet, id manifest.NamedResource) (manifest.BaseManifest, Emission) {
	if reads != nil {
		reads.GetObject(id)
	}
	sh := s.shardFor(id)
	sh.mu.RLock()
	var view capture
	sh.captureLocked(id, sh.cells[id], &view)
	obj := view.child.Obj
	var prev *Emission
	if slot := sh.slots[id]; slot != nil {
		prev = slot.current.Load()
	}
	sh.mu.RUnlock()
	if reads != nil {
		view = reads.input(id).view
		obj = view.child.Obj
	}
	e := Emission{Reads: reads, input: obj, inputDoc: view.child.Doc, previous: prev,
		inputHead: inputStamp{owner: view.owner, generation: view.generation}}
	if view.owner != nil {
		if reads != nil {
			e.inputHead.revision = reads.input(view.owner.id).producer
		} else {
			e.inputHead.revision = view.record.publication()
		}
	}
	if obj == nil {
		return nil, e
	}
	if c := s.coverage[id]; c != nil {
		e.coverage = c.records
	}
	getArtifact := s.GetArtifact
	if reads != nil {
		getArtifact = reads.GetArtifact
	}
	for ref := range manifest.References(obj) {
		if ref.Use == "source" {
			e.binding.source = ref.NamedResource
			if a, ok := getArtifact(ref.NamedResource).(*SourceArtifact); ok {
				e.binding.key, e.binding.root = a.Key, a.LocalPath
				if a.Key == "" {
					e.binding.mutable = a
				}
			}
			break
		}
	}
	if e.binding.source.Name == "" && s.contributionBinding(id).source == manifest.BootstrapSourceID {
		e.binding.source = manifest.BootstrapSourceID
		if a, ok := getArtifact(manifest.BootstrapSourceID).(*SourceArtifact); ok {
			e.binding.key, e.binding.root = a.Key, a.LocalPath
			if a.Key == "" {
				e.binding.mutable = a
			}
		}
	}
	if prev != nil {
		e.OutputReads = prev.OutputReads
		if prev.OutputReads.Valid() {
			e.Children = slices.Clone(prev.Children)
			e.Manifests = slices.Clone(prev.Manifests)
			e.Unreadable = slices.Clone(prev.Unreadable)
			e.Render = prev.Render
		}
	}
	return obj, e
}

// ReferenceFacts preserves visible data separately from accepted authority and relations.
type ReferenceFacts struct {
	Data        manifest.Data
	Accepted    bool
	Productions []manifest.Production
}

// ReferenceFacts selects visible data and source-qualified production evidence.
func (s *Store) ReferenceFacts(id manifest.NamedResource, index *manifest.DiscoveryIndex) ReferenceFacts {
	sh := s.shardFor(id)
	sh.mu.RLock()
	var view capture
	sh.captureLocked(id, sh.cells[id], &view)
	sh.mu.RUnlock()
	data, _ := view.child.Obj.(manifest.Data)
	return ReferenceFacts{Data: data, Accepted: view.accepted || view.owner != nil, Productions: s.Productions(id, index)}
}

// Productions merges discovery evidence with accepted reverse memberships into a read-only snapshot.
func (s *Store) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	return s.productions(nil, id, index, false)
}

func (s *Store) productions(reads *ReadSet, id manifest.NamedResource, index *manifest.DiscoveryIndex, captureTarget bool) []manifest.Production {
	if reads != nil && captureTarget {
		reads.GetObject(id)
	}
	if index == nil {
		index = s.index.Load()
	}
	record, _ := index.Lookup(id)
	provisional := bootstrapSeed(record)
	evidence := record.Productions
	if id.Namespace != "" {
		bare := id
		bare.Namespace = ""
		raw, _ := index.Lookup(bare)
		if reads != nil && captureTarget && len(raw.Productions) != 0 {
			reads.GetObject(bare)
		}
		for _, p := range raw.Productions {
			if p.Path == "" || p.Authored.Namespace != "" {
				continue
			}
			if len(evidence) == 0 || slices.ContainsFunc(evidence, func(e manifest.Production) bool {
				return e.Source == p.Source && (e.Path == "" || e.File == p.File && e.Authored == p.Authored) && sameProductionContext(s.coverage, e, p)
			}) {
				evidence = append(slices.Clone(evidence), p)
			}
		}
	}
	out := evidence
	owned := false
	add := func(p manifest.Production) {
		if !slices.Contains(out, p) {
			if !owned {
				out = slices.Clone(out)
				owned = true
			}
			out = append(out, p)
		}
	}
	sh := s.shardFor(id)
	sh.mu.RLock()
	members := sh.members[id]
	if len(evidence) == 0 && len(members) == 0 {
		sh.mu.RUnlock()
		return nil
	}
	if !sh.cells[id].hidden {
		if sh.cells[id].static != nil && sh.cells[id].retired && provisional {
			evidence, out = nil, nil
		}
		for i, p := range evidence {
			if c := s.coverage[p.Owner]; c != nil {
				accepted := c.slot.current.Load()
				if reads != nil {
					reads.sample(c.slot, accepted)
				}
				if c.slot.hidden.Load() || accepted.shadows(c.binding) {
					if !owned {
						out = slices.Clone(evidence[:i])
						owned = true
					}
					continue
				}
			}
			if owned {
				add(p)
			}
		}
		for _, m := range members {
			if reads != nil {
				reads.sample(m.slot, m.slot.current.Load())
			}
			if !m.slot.visible.Load() {
				continue
			}
			e := m.slot.current.Load()
			if reads != nil {
				reads.sample(m.slot, e)
			}
			entry := e.entries[m.position]
			if m.declaration {
				add(manifest.Production{Owner: m.slot.id, Declaration: entry.child.Obj.Named(), Source: entry.binding.source})
				continue
			}
			if !slices.ContainsFunc(entry.child.Productions, func(p manifest.Production) bool { return p.Owner == m.slot.id }) {
				add(manifest.Production{Owner: m.slot.id, Source: entry.binding.source})
			}
			for _, p := range entry.child.Productions {
				add(p)
			}
		}
	} else {
		out = nil
	}
	sh.mu.RUnlock()
	if reads != nil {
		reads.activateProducers()
	}
	if !owned {
		for i := 1; i < len(out); i++ {
			if compareProduction(out[i-1], out[i]) >= 0 {
				out = slices.Clone(out)
				owned = true
				break
			}
		}
	}
	if owned {
		slices.SortFunc(out, compareProduction)
		out = slices.Compact(out)
	}
	return out
}

func sameProductionContext(coverage map[manifest.NamedResource]*contributor, a, b manifest.Production) bool {
	if a.Source != b.Source {
		return false
	}
	ac, bc := coverage[a.Owner], coverage[b.Owner]
	return ac == nil || bc == nil || ac.binding == bc.binding
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
				c.records = append(c.records, footprint{id, r})
			}
			if a, ok := s.GetArtifact(p.Source).(*SourceArtifact); ok {
				c.binding.key, c.binding.root = a.Key, a.LocalPath
				if a.Key == "" {
					c.binding.mutable = a
				}
			}
		}
	}
	for _, contributor := range coverage {
		slices.SortFunc(contributor.records, func(a, b footprint) int { return a.id.Compare(b.id) })
	}
	s.lockAll()
	if s.index.Load() == nil {
		for owner, c := range coverage {
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
func bootstrapSeed(r manifest.DiscoveryRecord) bool {
	return len(r.Productions) > 0 && !slices.ContainsFunc(r.Productions, func(p manifest.Production) bool {
		return p.Owner != (manifest.NamedResource{}) || p.Declaration != (manifest.NamedResource{}) || p.Source != manifest.BootstrapSourceID
	})
}

// RecordEmitted atomically accepts completed work, replacing complete inventories and
// merging incomplete progress. The greatest producer identity supplies shared content.
func (s *Store) RecordEmitted(id manifest.NamedResource, next *Emission) {
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
		candidate.family = s.index.Load()
		candidate.OutputReads = nil
		if next.Complete {
			candidate.OutputReads = next.Reads
		} else if prev != nil && prev.OutputReads.Valid() {
			candidate.OutputReads = prev.OutputReads
		}
		unchanged := prev != nil &&
			(next.Complete && len(next.Children) == len(prev.entries) && prev.Complete && prev.shadows(next.binding) ||
				!next.Complete && candidate.OutputReads == prev.OutputReads)
		if unchanged {
			for i, child := range next.Children {
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
		stable := prev != nil && (proposal == prev || sameMembership(prev, proposal))
		unchanged = proposal == prev || proposal.sameContent(prev)
		if unchanged && !proposal.Complete && prev.Complete {
			proposal.Complete = true
		}
		unchanged = unchanged && prev.Complete == proposal.Complete
		if unchanged {
			if next.Reads != nil && (prev.Reads != next.Reads || prev.OutputReads != candidate.OutputReads) {
				rebound := *prev
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
			if !proposal.Complete && prev != nil {
				proposal.cached = prev.cached
				if prev.Complete && prev.Fingerprint != "" {
					output := prev.renderedOutput()
					proposal.cached = &output
				}
			}
		}
		if stable {
			if !s.commitStable(slot, prev, proposal, generation) {
				continue
			}
		} else if !s.commitReplacement(id, prev, proposal, generation) {
			continue
		}
		if prev != nil && proposal.OutputReads != prev.OutputReads {
			prev.OutputReads.Retire()
		}
		return
	}
}

func (s *Store) prepareAdmissions(id manifest.NamedResource, next, prev *Emission, byID map[manifest.NamedResource]int) []admission {
	var refs, superseded, retained []manifest.NamedResource
	prepared := make([]admission, 0, len(next.Children))
	dynamic := false
	for _, child := range next.Children {
		if child.Obj == nil {
			continue
		}
		if child.Obj.Named() == id {
			continue
		}
		child.Productions = slices.Clone(child.Productions)
		byID[child.Obj.Named()] = len(prepared)
		prepared = append(prepared, admission{child: child, binding: next.binding})
		dynamic = dynamic || manifest.ProducerNeedsCandidates(child.Obj)
	}
	if !dynamic {
		for i := range prepared {
			prepared[i].targets = manifest.ProducerTargets(prepared[i].child.Obj)
		}
		return prepared
	}
	for _, child := range next.Children {
		if child.Obj != nil {
			for ref := range manifest.References(child.Obj) {
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
	if next.binding == s.contributionBinding(id) {
		shadowed := prev.shadows(next.binding)
		for _, f := range next.coverage {
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
			if entry.binding == next.binding {
				_, replacement := byID[entry.child.Obj.Named()]
				for ref := range manifest.References(entry.child.Obj) {
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
	return other != nil &&
		slices.EqualFunc(other.entries, e.entries, admissionEqual) && e.sameOutput(other)
}

func (e *Emission) sameOutput(other *Emission) bool {
	sameDocuments := len(e.Manifests) == len(other.Manifests) &&
		(len(e.Manifests) == 0 || &e.Manifests[0] == &other.Manifests[0])
	sameInput := other.inputHead == e.inputHead && objectEqual(other.input, e.input) ||
		e.input != nil && semanticEqual(Child{Obj: other.input, Doc: other.inputDoc}, Child{Obj: e.input, Doc: e.inputDoc})
	return sameInput && other.family == e.family &&
		(sameDocuments || slices.EqualFunc(other.Manifests, e.Manifests, func(a, b map[string]any) bool { return specValueEqual(a, b) })) &&
		other.Fingerprint == e.Fingerprint &&
		slices.Equal(other.documentOwners, e.documentOwners) &&
		other.binding == e.binding &&
		slices.Equal(other.Unreadable, e.Unreadable) && slices.Equal(other.Render, e.Render) &&
		slices.Equal(other.shadowed, e.shadowed) &&
		slices.EqualFunc(other.coverage, e.coverage, func(a, b footprint) bool {
			return a.id == b.id && slices.Equal(a.record.Productions, b.record.Productions)
		})
}

func (s *Store) commitStable(slot *producerSlot, prev, proposal *Emission, generation uint64) bool {
	ps := s.shardFor(slot.id)
	ps.mu.Lock()
	if slot.current.Load() != prev || slot.generation != generation {
		ps.mu.Unlock()
		return false
	}
	if proposal == prev && (!slot.visible.Load() || ps.artifacts[slot.id].value == proposal) {
		ps.mu.Unlock()
		return true
	}
	if proposal != prev && proposal.revision == prev.revision {
		slot.current.Store(proposal)
		if slot.visible.Load() {
			record := ps.artifacts[slot.id]
			record.value = proposal
			ps.artifacts[slot.id] = record
		}
		ps.mu.Unlock()
		return true
	}
	if slot.generation == ^uint64(0) || ps.artifacts[slot.id].generation == ^uint64(0) || ps.artifacts[slot.id].generation == 0 && !ps.canAdmit(1) {
		ps.mu.Unlock()
		panic("store: authority generation exhausted")
	}
	slot.authority = ps.advanceLocked(slot.authority)
	slot.current.Store(proposal)
	var artifact eventDispatch
	var events []eventDispatch
	if slot.visible.Load() {
		record := ps.artifacts[slot.id]
		if record.generation == 0 {
			ps.admitAbsenceLocked(slot.id, artifactFacet)
		}
		record.authority, record.value = ps.advanceLocked(record.authority), proposal
		ps.artifacts[slot.id] = record
		artifact = fireUnderLock(s, EventArtifactUpdated, slot.id)
		artifact.payload = proposal
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
	return true
}

func (s *Store) commitReplacement(id manifest.NamedResource, prev, proposal *Emission, generation uint64) bool {
	var storage [4]regionState
	changed := changedTargets(prev, proposal, storage[:])
	if proposal.shadows(s.contributionBinding(id)) {
		for _, f := range proposal.coverage {
			changed = changed.add(f.id, true)
		}
	}
	ps := s.shardFor(id)
	s.lockAll()
	slot := ps.slots[id]
	if slot != nil && (slot.current.Load() != prev || slot.generation != generation) || slot == nil && prev != nil {
		s.unlockAll()
		return false
	}
	if slot != nil && slot.generation == ^uint64(0) || ps.artifacts[id].generation == ^uint64(0) {
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
	for _, entry := range proposal.entries {
		target := entry.child.Obj.Named()
		r, _ := s.index.Load().Lookup(target)
		sh := s.shardFor(target)
		cell := sh.cells[target]
		if bootstrapSeed(r) && cell.static != nil && !cell.rooted && !cell.retired {
			cell.retired = true
			sh.cells[target] = cell
			position, _ := region.position(target)
			region.states[position].seed = true
		}
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
			cell.authority = ps.advanceLocked(cell.authority)
			cell.hidden = true
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
		if record.generation == 0 {
			ps.admitAbsenceLocked(id, artifactFacet)
		}
		record.authority, record.value = ps.advanceLocked(record.authority), proposal
		ps.artifacts[id] = record
		event := fireUnderLock(s, EventArtifactUpdated, id)
		event.payload = proposal
		publications = append(publications, objectPublication{event: event, force: true})
	}
	s.unlockAll()
	dispatchPublications(publications)
	return true
}

func changedTargets(prev, proposal *Emission, storage []regionState) objectRegion {
	changed := newObjectRegion(len(proposal.entries), storage)
	add := func(entry admission, direct bool) {
		changed = changed.add(entry.child.Obj.Named(), direct)
		for _, target := range entry.targets {
			changed = changed.add(target.NamedResource, direct)
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
			direct := entry.changed && !admissionEqual(old, entry) ||
				old.binding != entry.binding || !slices.Equal(old.child.Productions, entry.child.Productions) ||
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
	if prev != nil && !prev.OutputReads.Valid() {
		prev = nil
	}
	e := *next
	e.validated = new(atomic.Pointer[manifest.BaseManifest])
	e.shadowed = nil
	if prev != nil {
		e.shadowed = slices.Clone(prev.shadowed)
	}
	if next.Complete && !e.shadows(next.binding) {
		e.shadowed = append(e.shadowed, next.binding)
	}
	e.previous = nil
	capacity := len(prepared)
	if prev != nil && !next.Complete {
		capacity += len(prev.entries)
	}
	e.entries = make([]admission, 0, capacity)
	e.Children = nil
	e.Manifests = slices.Clone(next.Manifests)
	e.Render = slices.Clone(next.Render)
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
				entry.changed = !objectEqual(entry.child.Obj, old.child.Obj) || !specValueEqual(entry.child.Doc, old.child.Doc)
				e.entries = append(e.entries, entry)
				delete(byID, id)
			} else if !next.Complete {
				old.changed = false
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
		}
	}
	inherited := prev != nil && slices.EqualFunc(next.Manifests, prev.Manifests, func(a, b map[string]any) bool { return specValueEqual(a, b) })
	if inherited && (!next.Complete || slices.EqualFunc(prev.entries, e.entries, func(a, b admission) bool { return a.child.Obj.Named() == b.child.Obj.Named() })) {
		e.documentOwners = prev.documentOwners
	} else {
		e.documentOwners = bindDocuments(next.Manifests, prepared)
	}
	if !next.Complete && prev != nil {
		if !inherited {
			visited := make(map[manifest.NamedResource]struct{}, len(prepared))
			for _, entry := range prepared {
				visited[entry.child.Obj.Named()] = struct{}{}
			}
			available := make(map[manifest.NamedResource][]int, len(e.Manifests))
			for i, doc := range e.Manifests {
				key := manifest.DocIdentity(doc)
				available[key] = append(available[key], i)
			}
			used := make([]bool, len(e.Manifests))
			for i, doc := range prev.Manifests {
				owner := prev.documentOwners[i]
				if _, replaced := visited[owner]; owner != (manifest.NamedResource{}) && replaced {
					continue
				}
				present := false
				for _, j := range available[manifest.DocIdentity(doc)] {
					if !used[j] && specValueEqual(doc, e.Manifests[j]) {
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

func bindDocuments(docs []map[string]any, entries []admission) []manifest.NamedResource {
	byDocument := make(map[manifest.NamedResource][]admission, len(entries))
	for _, entry := range entries {
		if entry.child.Doc != nil {
			key := manifest.DocIdentity(entry.child.Doc)
			byDocument[key] = append(byDocument[key], entry)
		}
	}
	owners := make([]manifest.NamedResource, len(docs))
	for i, doc := range docs {
		for _, entry := range byDocument[manifest.DocIdentity(doc)] {
			if specValueEqual(doc, entry.child.Doc) {
				owners[i] = entry.child.Obj.Named()
				break
			}
		}
	}
	return owners
}
func sameMembership(a, b *Emission) bool {
	if a.family != b.family || len(a.entries) != len(b.entries) || !slices.Equal(a.shadowed, b.shadowed) || a.binding != b.binding ||
		!slices.EqualFunc(a.coverage, b.coverage, func(a, b footprint) bool {
			return a.id == b.id && slices.Equal(a.record.Productions, b.record.Productions)
		}) {
		return false
	}
	for i, x := range a.entries {
		y := b.entries[i]
		if x.child.Obj.Named() != y.child.Obj.Named() || x.binding != y.binding ||
			!slices.Equal(x.child.Productions, y.child.Productions) || !sameLabels(x.child, y.child, ownershipLabel) ||
			!slices.EqualFunc(x.targets, y.targets, func(a, b manifest.ProducerTarget) bool { return a.NamedResource == b.NamedResource }) {
			return false
		}
	}
	return true
}

func sameLabels(a, b Child, excluded func(string) bool) bool {
	type labeled interface{ GetLabels() map[string]string }
	if x, ok := a.Obj.(labeled); ok {
		y, ok := b.Obj.(labeled)
		return ok && maps.Equal(x.GetLabels(), y.GetLabels())
	}
	adoc, bdoc := a.Doc, b.Doc
	if raw, ok := a.Obj.(*manifest.RawObject); ok && adoc == nil {
		adoc = raw.Spec
	}
	if raw, ok := b.Obj.(*manifest.RawObject); ok && bdoc == nil {
		bdoc = raw.Spec
	}
	am, _ := adoc["metadata"].(map[string]any)
	bm, _ := bdoc["metadata"].(map[string]any)
	al, _ := am["labels"].(map[string]any)
	bl, _ := bm["labels"].(map[string]any)
	if excluded == nil {
		return maps.EqualFunc(al, bl, specValueEqual)
	}
	return mapEqualExcept(al, bl, excluded)
}

func admissionEqual(a, b admission) bool {
	return a.binding == b.binding && childEqual(a.child, b.child)
}

func childEqual(a, b Child) bool {
	return objectEqual(a.Obj, b.Obj) && specValueEqual(a.Doc, b.Doc) && slices.Equal(a.Productions, b.Productions)
}
func semanticEqual(a, b Child) bool {
	if a.Doc == nil || b.Doc == nil {
		if objectEqual(a.Obj, b.Obj) {
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
		if !mapEqualExcept(am.GetLabels(), bm.GetLabels(), ownershipLabel) {
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
			if !specValueEqual(value, other) {
				return false
			}
			continue
		}
		am, aok := value.(map[string]any)
		bm, bok := other.(map[string]any)
		if !aok || !bok {
			return specValueEqual(value, other)
		}
		if !mapEqualExcept(am, bm, func(key string) bool { return key == "labels" }) {
			return false
		}
		al, _ := am["labels"].(map[string]any)
		bl, _ := bm["labels"].(map[string]any)
		if !mapEqualExcept(al, bl, ownershipLabel) {
			return false
		}
	}
	return true
}
func mapEqualExcept[V any](a, b map[string]V, excluded func(string) bool) bool {
	for key, value := range a {
		if excluded(key) {
			continue
		}
		other, ok := b[key]
		if !ok || !specValueEqual(value, other) {
			return false
		}
	}
	for key := range b {
		if !excluded(key) {
			if _, ok := a[key]; !ok {
				return false
			}
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
	excluded   bool
	accepted   bool
	binding    *Input
	support    *Input
	bound      bool
	preflight  string
	child      Child
	owner      *producerSlot
	record     *Emission
	generation uint64
}

func (sh *shard) objectLocked(id manifest.NamedResource) manifest.BaseManifest {
	if !sh.derived && sh.cells[id].binding == nil {
		return sh.cells[id].static
	}
	return sh.captureLocked(id, sh.cells[id], nil)
}

func (sh *shard) captureLocked(id manifest.NamedResource, cell objectCell, view *capture) manifest.BaseManifest {
	if view != nil {
		*view = capture{accepted: cell.rooted, generation: cell.generation, excluded: cell.excluded, preflight: cell.preflight, binding: cell.binding, support: cell.support}
	}
	if cell.hidden {
		return nil
	}
	obj := cell.static
	bound := (cell.binding == nil || cell.binding.current()) && (cell.support == nil || cell.support.productionCurrent())
	if view != nil {
		view.bound = bound
	}
	if !bound {
		obj = nil
	}
	if (!cell.rooted || !bound) && len(sh.members) > 0 {
		var chosen *membership
		for i := range sh.members[id] {
			m := &sh.members[id][i]
			if !m.declaration && m.slot.visible.Load() && (chosen == nil || m.slot.id.Compare(chosen.slot.id) > 0) {
				chosen = m
			}
		}
		if chosen != nil {
			record := chosen.slot.current.Load()
			child := record.entries[chosen.position].child
			if view != nil {
				view.owner, view.record, view.child = chosen.slot, record, child
			}
			return child.Obj
		}
		if cell.retired {
			obj = nil
		}
	}
	if view != nil {
		view.child.Obj = obj
	}
	return obj
}

type regionState struct {
	id                                  manifest.NamedResource
	before                              manifest.BaseManifest
	document                            map[string]any
	cell                                objectCell
	live, static, direct, visible, seed bool
	producerGeneration                  uint64
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

func (r objectRegion) add(id manifest.NamedResource, direct bool) objectRegion {
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
	r.states = append(r.states, regionState{id: id, direct: direct})
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
					region = region.add(a.child.Obj.Named(), false)
					for _, t := range a.targets {
						region = region.add(t.NamedResource, false)
					}
				}
			}
		}
		if c := s.coverage[id]; c != nil {
			for _, f := range c.records {
				region = region.add(f.id, false)
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
		if state.cell.generation == 0 {
			admissions[kindShardIndex(id.Kind)]++
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
		state.before, state.document = before.Obj, before.Doc
		state.visible = !state.cell.hidden && (state.cell.static != nil || len(sh.members[id]) > 0)
		if state.cell.generation == ^uint64(0) || sh.conditions[id].stamp().generation == ^uint64(0) || sh.artifacts[id].generation == ^uint64(0) || state.bucketGeneration == ^uint64(0) {
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
		if cell.generation == 0 && cell.static == nil && len(sh.members[id]) == 0 && sh.slots[id] == nil {
			continue
		}
		if cell.hidden && state.live {
			sh.clearFacetsLocked(id)
		}
		cell.retired, cell.hidden = !state.static, !state.live
		var view capture
		sh.captureLocked(id, cell, &view)
		after := view.child
		if state.direct || after.Obj != state.before || cell.static != state.cell.static ||
			cell.rooted != state.cell.rooted || cell.retired != state.cell.retired || cell.hidden != state.cell.hidden {
			if cell.generation == 0 {
				sh.admitAbsenceLocked(id, objectFacet)
			}
			cell.authority = sh.advanceLocked(cell.authority)
			sh.cells[id] = cell
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
