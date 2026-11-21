package store

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// AddWarning retains each complete direct warning payload once, ignoring incoming Count.
//
//nolint:gocritic // The SDK preserves this value input and its snapshot boundary.
func (s *Store) AddWarning(w manifest.Warning) {
	detail := ""
	if len(w.Detail) > 0 {
		detail = fmt.Sprintf("%q", w.Detail)
	} else {
		w.Detail = nil
	}
	related := ""
	if len(w.Related) > 0 {
		//nolint:errchkjson // Related contains only string resource identities with no custom JSON methods.
		encoded, _ := json.Marshal(w.Related)
		related = string(encoded)
	} else {
		w.Related = nil
	}
	key := warningKey{w.Resource, w.Category, w.Message, detail, related}

	s.warnMu.Lock()
	defer s.warnMu.Unlock()
	if _, ok := s.warnIndex[key]; ok {
		return
	}
	if s.warnIndex == nil {
		s.warnIndex = make(map[warningKey]struct{})
	}
	w.Detail = slices.Clone(w.Detail)
	w.Related = slices.Clone(w.Related)
	w.Count = 1
	s.warnIndex[key] = struct{}{}
	s.warnOrder = append(s.warnOrder, w)
}

// Warnings returns a copy of the collected advisories ordered by
// manifest.CompareWarning, or nil when none were recorded.
func (s *Store) Warnings() []manifest.Warning {
	return s.AssessProducers().Warnings
}

// ProducerAssessment captures completed diagnostics and shared generated sync identities.
type ProducerAssessment struct {
	Warnings []manifest.Warning
	// GeneratedOverlaps indexes direct document contributions by emitter and child.
	GeneratedOverlaps map[manifest.NamedResource]map[manifest.NamedResource]struct{}
}

// AssessProducers collects warnings and overlap from visible accepted inventories.
func (s *Store) AssessProducers() ProducerAssessment {
	var assessment ProducerAssessment
	s.warnMu.Lock()
	out := slices.Clone(s.warnOrder)
	s.warnMu.Unlock()
	out = s.appendAssessmentWarnings(out)
	start := len(out)
	conflicts := make(map[manifest.NamedResource]int)
	derived := make(map[manifest.NamedResource]manifest.NamedResource)
	for _, sh := range s.shards {
		byID := sh.warningContributions()
		for id, contributions := range byID {
			warning, declaration := s.assessContributions(id, contributions, &assessment)
			if warning.Count == 0 {
				continue
			}
			if declaration != (manifest.NamedResource{}) {
				derived[id] = declaration
			}
			conflicts[id] = len(out)
			out = append(out, warning)
		}
	}
	// Identical ownership conflicts on generated children belong to their authored declaration.
	for id, declaration := range derived {
		authored, ok := conflicts[declaration]
		generated := conflicts[id]
		if ok && declaration != id && slices.Equal(out[generated].Related, out[authored].Related) {
			out[generated].Count = 0
		}
	}
	if len(derived) != 0 {
		producers := slices.DeleteFunc(out[start:], func(w manifest.Warning) bool { return w.Count == 0 })
		out = out[:start+len(producers)]
	}
	slices.SortFunc(out, manifest.CompareWarning)
	assessment.Warnings = s.compactWarnings(out)
	return assessment
}

const minCompetingProducers = 2

func (s *Store) assessContributions(
	id manifest.NamedResource, contributions []warningContribution, assessment *ProducerAssessment,
) (manifest.Warning, manifest.NamedResource) {
	if len(contributions) < minCompetingProducers {
		return manifest.Warning{}, manifest.NamedResource{}
	}
	sync := generatedSyncContribution(contributions)
	conflict := conflictingContributions(contributions, sync)
	if !conflict && sync == nil {
		return manifest.Warning{}, manifest.NamedResource{}
	}
	owner := s.EmissionOwner(id)
	if sync != nil && owner != (manifest.NamedResource{}) {
		assessment.recordGeneratedOverlap(id, contributions)
	}
	if !conflict {
		return manifest.Warning{}, manifest.NamedResource{}
	}
	return s.producerConflict(id, owner, contributions)
}

func generatedSyncContribution(contributions []warningContribution) *warningContribution {
	index := slices.IndexFunc(contributions, func(c warningContribution) bool {
		return *c.syncOwnerKeys != ([2]string{}) && c.directDocument()
	})
	if index < 0 {
		return nil
	}
	sync := &contributions[index]
	for i := range contributions {
		c := &contributions[i]
		if c.owner != sync.owner && c.directDocument() {
			return sync
		}
	}
	return nil
}

func (a *ProducerAssessment) recordGeneratedOverlap(id manifest.NamedResource, contributions []warningContribution) {
	if a.GeneratedOverlaps == nil {
		a.GeneratedOverlaps = make(map[manifest.NamedResource]map[manifest.NamedResource]struct{})
	}
	for i := range contributions {
		c := &contributions[i]
		if !c.directDocument() {
			continue
		}
		children := a.GeneratedOverlaps[c.owner]
		if children == nil {
			children = make(map[manifest.NamedResource]struct{})
			a.GeneratedOverlaps[c.owner] = children
		}
		children[id] = struct{}{}
	}
}

func conflictingContributions(contributions []warningContribution, sync *warningContribution) bool {
	first := contributions[0].child
	payload := first.payload
	var keys [2]string
	if sync != nil {
		keys = *sync.syncOwnerKeys
	}
	for i := range contributions {
		child := contributions[i].child
		if payload == nil {
			payload = child.payload
		}
		if sync != nil && contributions[i].directDocument() &&
			!manifest.LabelsEqual(nil, nil, [2]map[string]any{sync.child.Doc, child.Doc},
				func(key string) bool { return key != keys[0] && key != keys[1] }) {
			return true
		}
		if i != 0 && !semanticEqual(*first, *child) {
			return true
		}
		if i != 0 && payload != nil && child.payload != nil && !artifactValueEqual(payload, child.payload) {
			return true
		}
	}
	return false
}

func (s *Store) producerConflict(
	id, owner manifest.NamedResource, contributions []warningContribution,
) (manifest.Warning, manifest.NamedResource) {
	slices.SortFunc(contributions, func(a, b warningContribution) int { return a.owner.Compare(b.owner) })
	owners := make([]string, len(contributions))
	related := make([]manifest.NamedResource, 0, len(contributions)+1)
	for i := range contributions {
		producer := contributions[i].owner
		owners[i] = manifest.DiagnosticFor(producer, s.GetObject(producer)).String()
		related = append(related, producer)
	}
	winner := "static object"
	var declaration manifest.NamedResource
	if owner != (manifest.NamedResource{}) {
		winner = manifest.DiagnosticFor(owner, s.GetObject(owner)).String()
		related = append(related, owner)
		declaration = commonGenerationDeclaration(contributions)
	}
	return manifest.Warning{
		Resource: id, Category: manifest.WarnConflictingProducers, Count: 1,
		Message: "conflicting producers; using " + winner, Detail: owners, Related: related,
	}, declaration
}

func commonGenerationDeclaration(contributions []warningContribution) manifest.NamedResource {
	var declaration manifest.NamedResource
	for i := range contributions {
		c := &contributions[i]
		if !c.child.Generated || len(c.child.Productions) != 1 {
			return manifest.NamedResource{}
		}
		production := &c.child.Productions[0]
		if i == 0 {
			declaration = production.Declaration
		}
		if production.Owner != c.owner || production.Declaration != declaration {
			return manifest.NamedResource{}
		}
	}
	return declaration
}

func (s *Store) assessments(match func(*acceptedAssessment) bool) []*ReadSet {
	var bundles []*ReadSet
	for _, sh := range s.shards {
		sh.mu.RLock()
		for id := range sh.cells {
			cell := sh.cells[id]
			if cell.links != nil {
				for i := range cell.links.assessments {
					accepted := cell.links.assessments[i].Load()
					if accepted != nil && accepted.reads != nil && (match == nil || match(accepted)) {
						bundles = append(bundles, accepted.reads)
					}
				}
			}
		}
		sh.mu.RUnlock()
	}
	return slices.DeleteFunc(bundles, s.obsoleteAssessment)
}

func (s *Store) obsoleteAssessment(reads *ReadSet) bool {
	if !reads.Valid() {
		return true
	}
	if !reads.retired.Load() {
		return false
	}
	for input := reads.inputs; input != nil; input = input.next {
		sh := s.shardFor(input.Ref.NamedResource)
		sh.mu.RLock()
		for kind := objectFacet; kind <= bucketFacet; kind++ {
			cell := input.cell(kind)
			if cell == nil || cell.head == nil {
				continue
			}
			if sh.headLocked(headKey{input.Ref.NamedResource, kind}).generation != cell.generation.Load() {
				sh.mu.RUnlock()
				return true
			}
		}
		sh.mu.RUnlock()
	}
	return false
}

type warningKey struct {
	resource                           manifest.NamedResource
	category, message, detail, related string
}

type warningContribution struct {
	owner         manifest.NamedResource
	child         *Child
	syncOwnerKeys *[2]string
}

func (c *warningContribution) directDocument() bool {
	return !c.child.Generated && c.child.Doc != nil
}

func (s *Store) compactWarnings(out []manifest.Warning) []manifest.Warning {
	n := 0
	for i := 0; i < len(out); {
		warning := out[i]
		warning.Count = 1
		warning.Detail = slices.Clone(warning.Detail)
		warning.Related = slices.Clone(warning.Related)
		if len(warning.Related) == 0 {
			warning.Related = nil
		}
		if warning.Subject.ID == (manifest.NamedResource{}) && warning.Resource != (manifest.NamedResource{}) {
			warning.Subject = manifest.DiagnosticFor(warning.Resource, s.GetObject(warning.Resource))
		}
		if len(warning.Detail) == 0 {
			warning.Detail = nil
		}
		j := i + 1
		for j < len(out) && manifest.CompareWarning(out[i], out[j]) == 0 {
			warning.Count++
			j++
		}
		out[n] = warning
		n++
		i = j
	}
	return out[:n]
}

func (s *Store) appendAssessmentWarnings(out []manifest.Warning) []manifest.Warning {
	for _, reads := range s.assessments(nil) {
		start := len(out)
		{
			warningValues := reads.warnings
			for warningIndex := range warningValues {
				warning := warningValues[warningIndex]
				if !slices.ContainsFunc(
					out[start:],
					func(w manifest.Warning) bool { return manifest.CompareWarning(w, warning) == 0 },
				) {
					out = append(out, warning)
				}
			}
		}
	}
	return out
}

func (sh *shard) warningContributions() map[manifest.NamedResource][]warningContribution {
	byID := map[manifest.NamedResource][]warningContribution{}
	sh.mu.RLock()
	for id, members := range sh.members {
		if len(members) < minCompetingProducers || sh.derived && sh.cells[id].hidden {
			continue
		}
		for _, m := range members {
			if !m.declaration && m.slot.visible.Load() {
				record := m.slot.current.Load()
				byID[id] = append(byID[id], warningContribution{
					owner:         m.slot.id,
					child:         &record.entries[m.position].child,
					syncOwnerKeys: &record.syncOwnerKeys,
				})
			}
		}
	}
	sh.mu.RUnlock()
	return byID
}
