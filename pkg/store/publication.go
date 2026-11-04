package store

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// RecordEmitted atomically accepts completed work, replacing complete inventories and
// merging incomplete progress. The greatest producer identity supplies shared content.
func (s *Store) RecordEmitted(
	id manifest.NamedResource,
	next *Emission,
	payloads map[manifest.NamedResource]*SourceArtifact,
) {
	next.accepted = false
	if len(payloads) != 0 && next.Reads == nil {
		reads := new(ReadSet)
		reads.Init(s, id, nil)
		reads.Bind(next)
	}
	if !next.Reads.Valid() {
		return
	}
	if len(next.Failures) != 0 {
		next.Complete = false
	}
	if !next.Complete {
		next.Fingerprint = ""
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
					if old.Obj.Named() == child.Obj.Named() && old.payload != nil && old.output == next.Reads {
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
					equal = slices.ContainsFunc(
						prev.entries,
						func(old admission) bool { return admissionEqual(incoming, old) },
					)
				}
				if !equal {
					unchanged = false
					break
				}
			}
		}
		if unchanged && !next.Complete {
			{
				oldValues := prev.entries
				for oldIndex := range oldValues {
					old := oldValues[oldIndex]
					if old.child.Generated && !slices.ContainsFunc(candidate.Children, func(child Child) bool {
						return child.Obj != nil && child.Generated && child.Obj.Named() == old.child.Obj.Named()
					}) {
						unchanged = false
						break
					}
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
		stable := prev != nil &&
			(proposal == prev || sameMembership(prev, proposal) && slices.EqualFunc(prev.Children, proposal.Children, func(a, b Child) bool { return a.payload == b.payload }))
		unchanged = proposal == prev || proposal.sameContent(prev)
		if unchanged && len(proposal.Failures) == 0 && !proposal.Complete && prev.Complete {
			proposal.Complete = true
		}
		unchanged = unchanged && prev.Complete == proposal.Complete
		if unchanged {
			retained := next.Complete && next.Fingerprint != "" && next.Fingerprint == prev.Fingerprint &&
				prev.OutputReads.Valid() && prev.Reads.Valid() && candidate.inputHead == prev.inputHead && prev.Render.Equal(candidate.Render)
			if !retained &&
				(next.Reads != nil && (prev.Reads != next.Reads || prev.OutputReads != candidate.OutputReads) || !prev.Render.Equal(candidate.Render)) {
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
		case publicationAccepted:
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
			prepared = append(
				prepared,
				admission{child: child, independent: declarationIndependent(index, child.Obj.Named())},
			)
		}
	}
	return prepared
}

func (s *Store) prepareAdmissions(
	id manifest.NamedResource,
	next, prev *Emission,
	byID map[manifest.NamedResource]int,
) []admission {
	var projections []ArtifactInput
	prepared := next.entries
	if !next.entriesOwned || !entriesMatch(next.Children, prepared) {
		prepared = s.prepareChildren(next.Children, next.family)
	}
	dynamic := false
	compacted := prepared[:0]
	{
		entryValues2 := prepared
		for i := range entryValues2 {
			entry := entryValues2[i]
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
			if position, exists := byID[child.Obj.Named()]; exists &&
				(child.Generated || compacted[position].child.Generated) {
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
		{
			//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
			for target, record := range next.family.Records {
				if slices.ContainsFunc(
					record.Productions,
					func(p manifest.Production) bool { return p.Owner == id && p.Source == binding.source },
				) {
					coverage = append(coverage, footprint{id: target, record: record})
				}
			}
		}
	}
	if next.binding.equal(&binding) {
		shadowed := prev.shadows(next.binding)
		{
			fValues := coverage
			for fIndex := range fValues {
				f := fValues[fIndex]
				{
					pValues6 := f.record.Productions
					for pIndex5 := range pValues6 {
						p := pValues6[pIndex5]
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
		}
	}
	if prev != nil {
		{
			entryValues3 := prev.entries
			for entryIndex2 := range entryValues3 {
				entry := entryValues3[entryIndex2]
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
		e.input != nil &&
			semanticEqual(Child{Obj: other.input, Doc: other.inputDoc}, Child{Obj: e.input, Doc: e.inputDoc})
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
