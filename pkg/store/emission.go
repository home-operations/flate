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
	Children    []Child
	Manifests   []map[string]any
	Fingerprint string
	Unreadable  []manifest.NamedResource
	Complete    bool
	Render      []byte

	input    manifest.BaseManifest
	inputDoc map[string]any
	// Copies share atomic storage; each prepared proposal owns a fresh memo.
	validated      *atomic.Pointer[manifest.BaseManifest]
	documentOwners []manifest.NamedResource
	previous       *Emission
	entries        []admission
	coverage       []footprint
	binding        sourceBinding
	shadowed       []sourceBinding
}

func (*Emission) artifact() {}

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
	child   Child
	targets []manifest.ProducerTarget
	binding sourceBinding
}
type producerSlot struct {
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
	_, owner := sh.representativeLocked(id)
	sh.mu.RUnlock()
	return owner
}

// GetEmission borrows the accepted inventory and binds an attempt to its current input.
func (s *Store) GetEmission(id manifest.NamedResource) (manifest.BaseManifest, Emission) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	view, _ := sh.representativeLocked(id)
	obj := view.Obj
	var prev *Emission
	if slot := sh.slots[id]; slot != nil {
		prev = slot.current.Load()
	}
	sh.mu.RUnlock()
	e := Emission{input: obj, inputDoc: view.Doc, previous: prev}
	if obj == nil {
		return nil, e
	}
	if c := s.coverage[id]; c != nil {
		e.coverage = c.records
	}
	for ref := range manifest.References(obj) {
		if ref.Use == "source" {
			e.binding.source = ref.NamedResource
			if a, ok := s.GetArtifact(ref.NamedResource).(*SourceArtifact); ok {
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
		if a, ok := s.GetArtifact(manifest.BootstrapSourceID).(*SourceArtifact); ok {
			e.binding.key, e.binding.root = a.Key, a.LocalPath
			if a.Key == "" {
				e.binding.mutable = a
			}
		}
	}
	if prev != nil {
		e.Fingerprint = prev.Fingerprint
		e.Children = slices.Clone(prev.Children)
		e.Manifests = slices.Clone(prev.Manifests)
		e.Unreadable = slices.Clone(prev.Unreadable)
		e.Render = prev.Render
	}
	return obj, e
}

// Productions merges discovery evidence with accepted reverse memberships into a read-only snapshot.
func (s *Store) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	if index == nil {
		index = s.index.Load()
	}
	record, _ := index.Lookup(id)
	provisional := bootstrapSeed(record)
	evidence := record.Productions
	promotable := slices.ContainsFunc(evidence, func(p manifest.Production) bool { return p.Path != "" })
	if !promotable && id.Namespace != "" {
		bare := id
		bare.Namespace = ""
		raw, _ := index.Lookup(bare)
		for _, p := range raw.Productions {
			if p.Path == "" || p.Authored.Namespace != "" {
				continue
			}
			if len(evidence) == 0 || slices.ContainsFunc(evidence, func(e manifest.Production) bool {
				return e.Source == p.Source && sameProductionContext(s.coverage, e, p)
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
	if !sh.hidden[id] {
		if sh.static[id] != nil && sh.retired[id] && provisional {
			evidence, out = nil, nil
		}
		for i, p := range evidence {
			if c := s.coverage[p.Owner]; c != nil {
				accepted := c.slot.current.Load()
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
			if !m.slot.visible.Load() {
				continue
			}
			e := m.slot.current.Load()
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
				slot = &producerSlot{id: owner}
				slot.visible.Store(sh.objectLocked(owner) != nil)
				slot.hidden.Store(sh.hidden[owner])
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
		if slot != nil {
			prev = slot.current.Load()
		}
		ps.mu.RUnlock()
		candidate := *next
		unchanged := prev != nil && len(next.Children) == len(prev.entries) &&
			(!next.Complete || prev.Complete && prev.shadows(next.binding))
		if unchanged {
			for i, child := range next.Children {
				if !admissionEqual(admission{child: child, binding: next.binding}, prev.entries[i]) {
					unchanged = false
					break
				}
			}
		}
		if prev != nil {
			candidate.shadowed = prev.shadowed
			candidate.documentOwners = prev.documentOwners
		}
		proposal := prev
		if !unchanged || !candidate.sameOutput(prev) {
			byID := make(map[manifest.NamedResource]int, len(next.Children))
			prepared := s.prepareAdmissions(id, next, prev, byID)
			proposal = prepareEmission(next, prev, prepared, byID)
		}
		stable := prev != nil && (proposal == prev || sameMembership(prev, proposal))
		var seeds []manifest.NamedResource
		for _, entry := range proposal.entries {
			target := entry.child.Obj.Named()
			record, _ := s.index.Load().Lookup(target)
			if !bootstrapSeed(record) {
				continue
			}
			sh := s.shardFor(target)
			sh.mu.RLock()
			if sh.static[target] != nil && !sh.rooted[target] && !sh.retired[target] {
				seeds = append(seeds, target)
			}
			sh.mu.RUnlock()
		}
		unchanged = proposal == prev || proposal.sameContent(prev)
		if unchanged && !proposal.Complete && prev.Complete {
			proposal.Complete = true
		}
		unchanged = unchanged && prev.Complete == proposal.Complete
		if stable && len(seeds) == 0 {
			if s.commitStable(slot, prev, proposal, unchanged) {
				return
			}
		} else if s.commitReplacement(id, prev, proposal, seeds) {
			return
		}
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
	return slices.EqualFunc(other.Manifests, e.Manifests, func(a, b map[string]any) bool { return specValueEqual(a, b) }) &&
		other.Fingerprint == e.Fingerprint &&
		slices.Equal(other.documentOwners, e.documentOwners) &&
		other.binding == e.binding && objectEqual(other.input, e.input) &&
		slices.Equal(other.Unreadable, e.Unreadable) && slices.Equal(other.Render, e.Render) &&
		slices.Equal(other.shadowed, e.shadowed) &&
		slices.EqualFunc(other.coverage, e.coverage, func(a, b footprint) bool {
			return a.id == b.id && slices.Equal(a.record.Productions, b.record.Productions)
		})
}

func (s *Store) commitStable(slot *producerSlot, prev, proposal *Emission, unchanged bool) bool {
	id := slot.id
	ps := s.shardFor(id)
	ps.mu.Lock()
	if slot.current.Load() != prev {
		ps.mu.Unlock()
		return false
	}
	if unchanged {
		ps.mu.Unlock()
		return true
	}
	slot.current.Store(proposal)
	var dispatch func()
	if slot.visible.Load() {
		ps.artifacts[id] = proposal
		dispatch = fireUnderLock(s, EventArtifactUpdated, id, Artifact(proposal))
	}
	visible := slot.visible.Load()
	ps.mu.Unlock()
	if !visible {
		return true
	}
	for i, a := range proposal.entries {
		if objectEqual(a.child.Obj, prev.entries[i].child.Obj) && specValueEqual(a.child.Doc, prev.entries[i].child.Doc) {
			continue
		}
		s.fireObject(a.child.Obj.Named())
		for _, t := range a.targets {
			s.fireObject(t.NamedResource)
		}
	}
	if dispatch != nil {
		dispatch()
	}
	return true
}

func (s *Store) commitReplacement(id manifest.NamedResource, prev, proposal *Emission, seeds []manifest.NamedResource) bool {
	ps := s.shardFor(id)
	s.lockAll()
	slot := ps.slots[id]
	var current *Emission
	if slot != nil {
		current = slot.current.Load()
	}
	if current != prev {
		s.unlockAll()
		return false
	}

	if slot == nil {
		slot = &producerSlot{id: id}
		ps.slots[id] = slot
		slot.visible.Store(ps.objectLocked(id) != nil)
		slot.hidden.Store(ps.hidden[id])
	}
	changed := changedTargets(prev, proposal)
	if proposal.shadows(s.contributionBinding(id)) {
		for _, f := range proposal.coverage {
			changed[f.id] = struct{}{}
		}
	}
	direct := maps.Clone(changed)
	region := s.affectedLocked(changed)
	type publication struct {
		before, after Child
		visible       bool
		dispatch      func()
	}
	publications := make(map[manifest.NamedResource]publication, len(region))
	for target := range region {
		sh := s.shardFor(target)
		before, _ := sh.representativeLocked(target)
		publications[target] = publication{
			before:  before,
			visible: !sh.hidden[target] && (sh.static[target] != nil || len(sh.members[target]) > 0),
		}
	}
	for target := range changed {
		sh := s.shardFor(target)
		sh.members[target] = slices.DeleteFunc(sh.members[target], func(m membership) bool { return m.slot == slot })
	}
	for _, target := range seeds {
		sh := s.shardFor(target)
		if !sh.rooted[target] {
			sh.retired[target] = true
		}
	}
	slot.current.Store(proposal)
	if !slot.visible.Load() {
		ps.hidden[id] = true
		slot.hidden.Store(true)
	}
	for position, a := range proposal.entries {
		target := a.child.Obj.Named()
		sh := s.shardFor(target)
		sh.members[target] = append(sh.members[target], membership{slot: slot, position: position})
		sh.objects[target] = a.child.Obj
		for _, t := range a.targets {
			sh := s.shardFor(t.NamedResource)
			sh.members[t.NamedResource] = append(sh.members[t.NamedResource], membership{slot: slot, position: position, declaration: true})
		}
	}
	s.reachabilityLocked(region)
	for target := range region {
		sh := s.shardFor(target)
		change := publications[target]
		change.after, _ = sh.representativeLocked(target)
		obj := change.after.Obj
		if obj != nil {
			sh.setLocked(target, obj)
		} else {
			delete(sh.byName[kindID{target.Group, target.Kind}], nameID{target.Namespace, target.Name})
		}
		if _, touched := direct[target]; (touched || change.before.Obj != obj) && (obj != nil || change.visible || !sh.hidden[target]) {
			change.dispatch = fireUnderLock(s, EventObjectAdded, target, obj)
		}
		publications[target] = change
	}
	var artifactDispatch func()
	if slot.visible.Load() {
		ps.artifacts[id] = proposal
		artifactDispatch = fireUnderLock(s, EventArtifactUpdated, id, Artifact(proposal))
	}
	s.unlockAll()
	dispatches := make([]eventDispatch, 0, len(region)+1)
	for target, change := range publications {
		// Equal supported seeds only gain a document; retiring a provisional root still needs an arrival.
		if change.dispatch != nil && (slices.Contains(seeds, target) || change.before.Obj == nil || change.after.Obj == nil || !objectEqual(change.before.Obj, change.after.Obj) || change.before.Doc != nil && !specValueEqual(change.before.Doc, change.after.Doc)) {
			dispatches = append(dispatches, eventDispatch{target, change.dispatch})
		}
	}
	if artifactDispatch != nil {
		dispatches = append(dispatches, eventDispatch{id, artifactDispatch})
	}
	dispatchEvents(dispatches)
	return true
}

func changedTargets(prev, proposal *Emission) map[manifest.NamedResource]struct{} {
	changed := map[manifest.NamedResource]struct{}{}
	for _, inventory := range []*Emission{prev, proposal} {
		if inventory == nil {
			continue
		}
		for _, entry := range inventory.entries {
			changed[entry.child.Obj.Named()] = struct{}{}
			for _, target := range entry.targets {
				changed[target.NamedResource] = struct{}{}
			}
		}
	}
	return changed
}

func prepareEmission(next, prev *Emission, prepared []admission, byID map[manifest.NamedResource]int) *Emission {
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
				e.entries = append(e.entries, prepared[i])
				delete(byID, id)
			} else if !next.Complete {
				e.entries = append(e.entries, old)
			}
		}
	}
	for _, a := range prepared {
		id := a.child.Obj.Named()
		if _, ok := byID[id]; ok {
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
				key := documentIdentity(doc)
				available[key] = append(available[key], i)
			}
			used := make([]bool, len(e.Manifests))
			for i, doc := range prev.Manifests {
				owner := prev.documentOwners[i]
				if _, replaced := visited[owner]; owner != (manifest.NamedResource{}) && replaced {
					continue
				}
				present := false
				for _, j := range available[documentIdentity(doc)] {
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
func documentIdentity(doc map[string]any) manifest.NamedResource {
	name, namespace := manifest.DocMetadata(doc)
	raw := manifest.RawObject{APIVersion: manifest.DocAPIVersion(doc), Kind: manifest.DocKind(doc), Name: name, Namespace: namespace}
	return raw.Named()
}
func bindDocuments(docs []map[string]any, entries []admission) []manifest.NamedResource {
	byDocument := make(map[manifest.NamedResource][]admission, len(entries))
	for _, entry := range entries {
		if entry.child.Doc != nil {
			key := documentIdentity(entry.child.Doc)
			byDocument[key] = append(byDocument[key], entry)
		}
	}
	owners := make([]manifest.NamedResource, len(docs))
	for i, doc := range docs {
		for _, entry := range byDocument[documentIdentity(doc)] {
			if specValueEqual(doc, entry.child.Doc) {
				owners[i] = entry.child.Obj.Named()
				break
			}
		}
	}
	return owners
}
func sameMembership(a, b *Emission) bool {
	if len(a.entries) != len(b.entries) || !slices.Equal(a.shadowed, b.shadowed) || a.binding != b.binding {
		return false
	}
	for i, x := range a.entries {
		y := b.entries[i]
		if x.child.Obj.Named() != y.child.Obj.Named() || !slices.EqualFunc(x.targets, y.targets, func(a, b manifest.ProducerTarget) bool {
			return a.NamedResource == b.NamedResource
		}) {
			return false
		}
	}
	return true
}
func admissionEqual(a, b admission) bool {
	return a.binding == b.binding && objectEqual(a.child.Obj, b.child.Obj) && specValueEqual(a.child.Doc, b.child.Doc) && slices.Equal(a.child.Productions, b.child.Productions)
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
func (s *Store) fireObject(id manifest.NamedResource) {
	sh := s.shardFor(id)
	sh.mu.RLock()
	obj := sh.objectLocked(id)
	dispatch := fireUnderLock(s, EventObjectAdded, id, obj)
	sh.mu.RUnlock()
	dispatch()
}

func (sh *shard) objectLocked(id manifest.NamedResource) manifest.BaseManifest {
	if len(sh.members) == 0 && len(sh.hidden) == 0 && len(sh.retired) == 0 {
		return sh.objects[id]
	}
	if sh.rooted[id] {
		return sh.static[id]
	}
	child, _ := sh.representativeLocked(id)
	return child.Obj
}
func (sh *shard) representativeLocked(id manifest.NamedResource) (Child, manifest.NamedResource) {
	if len(sh.members) == 0 && len(sh.hidden) == 0 && len(sh.retired) == 0 {
		return Child{Obj: sh.objects[id]}, manifest.NamedResource{}
	}
	if sh.hidden[id] {
		return Child{}, manifest.NamedResource{}
	}
	if sh.rooted[id] && sh.static[id] != nil {
		return Child{Obj: sh.static[id]}, manifest.NamedResource{}
	}
	var chosen *membership
	for i := range sh.members[id] {
		m := &sh.members[id][i]
		if !m.declaration && m.slot.visible.Load() && (chosen == nil || m.slot.id.Compare(chosen.slot.id) > 0) {
			chosen = m
		}
	}
	if chosen != nil {
		return chosen.slot.current.Load().entries[chosen.position].child, chosen.slot.id
	}
	if sh.rooted[id] || !sh.retired[id] {
		return Child{Obj: sh.static[id]}, manifest.NamedResource{}
	}
	return Child{}, manifest.NamedResource{}
}
func (s *Store) affectedLocked(changed map[manifest.NamedResource]struct{}) map[manifest.NamedResource]struct{} {
	queue := slices.Collect(maps.Keys(changed))
	add := func(target manifest.NamedResource) {
		if _, seen := changed[target]; !seen {
			changed[target] = struct{}{}
			queue = append(queue, target)
		}
	}
	//nolint:intrange // add MUST extend the worklist during traversal.
	for i := 0; i < len(queue); i++ {
		id := queue[i]
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
		// Frozen support changes with its owner even when another source emitted.
		if c := s.coverage[id]; c != nil {
			for _, f := range c.records {
				add(f.id)
			}
		}
	}
	return changed
}

type supportState struct {
	live, static bool
}

func (s *Store) staticAnchorLocked(id manifest.NamedResource, support map[manifest.NamedResource]supportState) bool {
	sh := s.shardFor(id)
	if sh.rooted[id] && sh.static[id] != nil {
		return true
	}
	if sh.static[id] == nil {
		return false
	}
	r, ok := s.index.Load().Lookup(id)
	if !ok {
		return true
	}
	if sh.retired[id] && bootstrapSeed(r) {
		return false
	}
	for _, p := range r.Productions {
		if p.Owner == (manifest.NamedResource{}) || p.Owner == id {
			return true
		}
		slot := s.shardFor(p.Owner).slots[p.Owner]
		if state, inside := support[p.Owner]; inside {
			if !state.live {
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

func (s *Store) reachabilityLocked(region map[manifest.NamedResource]struct{}) {
	support := make(map[manifest.NamedResource]supportState, len(region))
	// In-region owners must be proven locally, never by pre-replacement visibility.
	for id := range region {
		support[id] = supportState{}
	}
	queue := []manifest.NamedResource{}
	add := func(id manifest.NamedResource) {
		if state, inside := support[id]; inside && !state.live {
			state.live = true
			support[id] = state
			queue = append(queue, id)
		}
	}
	for id := range region {
		live := s.staticAnchorLocked(id, support)
		if !live {
			for _, m := range s.shardFor(id).members[id] {
				if _, inside := region[m.slot.id]; !inside && m.slot.visible.Load() {
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
		id := queue[i]
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
				if s.shardFor(f.id).static[f.id] != nil {
					add(f.id)
				}
			}
		}
	}
	for id, state := range support {
		state.static = s.staticAnchorLocked(id, support)
		support[id] = state
	}
	for id, state := range support {
		sh := s.shardFor(id)
		sh.retired[id] = !state.static
		was := sh.hidden[id]
		sh.hidden[id] = !state.live
		if slot := sh.slots[id]; slot != nil {
			slot.visible.Store(state.live)
			slot.hidden.Store(!state.live)
		}
		if was && state.live {
			delete(sh.conditions, id)
			delete(sh.artifacts, id)
		}
	}
}
